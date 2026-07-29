"""
JSON serialization utilities for eventsource types.

This module provides utilities for JSON serialization of common types
that are not natively JSON-serializable, such as UUIDs and datetimes.

`json_dumps`/`json_loads` are backed by `orjson`, a core dependency (not
optional -- there is no stdlib fallback). orjson serializes UUID and
datetime natively, is a compiled extension, and is meaningfully faster
than stdlib `json` for the payload shapes this library persists in
`event_outbox` and `dead_letter_queue`.

The encoder's contract, beyond "valid JSON":

- A UUID dict key serializes (via `orjson.OPT_NON_STR_KEYS`). Historically
  this was called out as a divergence from a stdlib fallback; there is no
  fallback anymore, so it is simply the encoder's behavior.
- A non-finite float (`inf`/`-inf`/`nan`) raises `ValueError`. orjson would
  otherwise silently substitute JSON `null`, which is data corruption for
  a persisted value -- rejecting at the point of serialization is safer
  than a lossy round-trip.
- An integer outside `[-2**63, 2**64-1]` raises `ValueError`. orjson only
  supports 64-bit integers and otherwise raises a bare, unhelpful
  `TypeError: Integer exceeds 64-bit range` -- rejecting explicitly with a
  message naming the value and the supported range is strictly more
  useful than that.

Both rejections are enforced by `_validate_json_safe_values`, a single
pre-serialization scan over the object graph (dicts, lists, tuples --
including subclasses of each -- and their contents).

See `docs/reference/serialization-limits.md` for the full, user-facing
list of this encoder's constraints (integer range, non-finite floats,
subclass handling, output format, unsupported types).

Example:
    >>> from eventsource.serialization import json_dumps, json_loads
    >>> from uuid import uuid4
    >>>
    >>> data = {"id": uuid4()}
    >>> json_str = json_dumps(data)
    >>> parsed = json_loads(json_str)
"""

import json
import math
from datetime import datetime
from typing import Any
from uuid import UUID

import orjson

_isfinite = math.isfinite
_INT_MIN = -(2**63)
_INT_MAX = 2**64 - 1


class EventSourceJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles UUID and datetime objects.

    `json_dumps`/`json_loads` no longer use this class -- orjson (a core
    dependency) serializes UUID and datetime natively, so there is no
    stdlib-backed path left that needs it. It is kept because
    `eventsource.repositories.outbox` still calls stdlib
    `json.dumps(event_data, cls=EventSourceJSONEncoder)` directly at two
    call sites (bypassing `json_dumps` entirely) -- migrating those call
    sites is out of scope for this change. It is also public API,
    re-exported from `eventsource.serialization`, `eventsource.repositories`,
    and the top-level `eventsource` package.

    Example:
        >>> import json
        >>> from uuid import uuid4
        >>> from datetime import datetime, UTC
        >>>
        >>> data = {"id": uuid4(), "timestamp": datetime.now(UTC)}
        >>> json_str = json.dumps(data, cls=EventSourceJSONEncoder)
    """

    def default(self, obj: Any) -> Any:
        """
        Convert non-serializable objects to JSON-serializable formats.

        Args:
            obj: Object to serialize

        Returns:
            JSON-serializable representation

        Raises:
            TypeError: If object type is not supported
        """
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


def _orjson_default(obj: Any) -> Any:
    """
    Fallback handler for `orjson.dumps` for types it does not handle natively.

    orjson natively serializes UUID and datetime, so this mainly needs to
    cover one thing orjson does not handle natively: `float` subclasses
    (e.g. `numpy.float64`). orjson serializes `str`/`int`/`dict`/`list`
    subclasses natively via their base-type behavior, but NOT `float`
    subclasses -- those fall through to here. Non-finite raises the same
    `ValueError` used by `_validate_json_safe_values`; finite converts to a
    plain `float` so it serializes like an ordinary float.

    Everything else genuinely unsupported raises `TypeError`.
    """
    if isinstance(obj, float):
        if not _isfinite(obj):
            raise ValueError(f"Out of range float values are not JSON compliant: {obj}")
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# Leaf types that can never contain a float or an out-of-range int, checked
# by exact type() first (fast path) before falling through to the
# isinstance() slow path below.
_LEAF_TYPES = (str, type(None))


def _validate_json_safe_values(obj: Any) -> None:
    """
    Raise `ValueError` if `obj` contains a non-finite float (inf/-inf/nan)
    or an out-of-range integer, anywhere in a nested dict/list/tuple
    structure -- including inside subclasses of dict/list/tuple, and
    including float/int subclasses themselves (e.g. `numpy.float64`, an
    `IntEnum`).

    Both checks exist because orjson would otherwise either silently
    corrupt the value (non-finite float -> JSON `null`) or raise an
    unhelpful bare `TypeError: Integer exceeds 64-bit range` with no
    indication of which field or value caused it. Raising here, at the
    point the bad value enters serialization, is strictly better than
    either.

    `bool` is deliberately never range-checked: it's an `int` subclass, but
    orjson (and stdlib) serialize it as `true`/`false`, not as a number, so
    it can never be "out of range." Checked before the general `int`
    handling in both the fast (`type() is`) and slow (`isinstance()`)
    paths -- `isinstance(True, int)` is `True`, so `bool` must be excluded
    explicitly, not implied by "not an int."

    Falls through to `isinstance()` whenever the exact-type fast path
    doesn't match, so a `dict`/`list`/`tuple` *subclass* is still walked
    into (orjson traverses such a subclass natively, so skipping it would
    silently let a bad value through), and a `float`/`int` *subclass* value
    is still checked (orjson serializes `int` subclasses -- e.g. `IntEnum`
    -- and, via `_orjson_default`, `float` subclasses too).
    """
    stack = [obj]
    push = stack.append
    pop = stack.pop
    while stack:
        current = pop()
        t = type(current)
        if t is float:
            if not _isfinite(current):
                raise ValueError(f"Out of range float values are not JSON compliant: {current}")
        elif t is bool:
            continue
        elif t is int:
            if not (_INT_MIN <= current <= _INT_MAX):
                raise ValueError(
                    "Integer out of range for JSON serialization "
                    f"(must be within [-2**63, 2**64-1]): {current}"
                )
        elif t is dict:
            for k, v in current.items():
                push(k)
                push(v)
        elif t is list or t is tuple:
            for item in current:
                push(item)
        elif t in _LEAF_TYPES:
            continue
        elif isinstance(current, bool):
            # Must precede the isinstance(current, int) check below --
            # isinstance(True, int) is True, and bool is never range-checked.
            continue
        elif isinstance(current, float):
            # float subclass (e.g. numpy.float64), not caught by `t is float`.
            if not _isfinite(current):
                raise ValueError(f"Out of range float values are not JSON compliant: {current}")
        elif isinstance(current, int):
            # int subclass (e.g. an IntEnum), not caught by `t is int`.
            if not (_INT_MIN <= current <= _INT_MAX):
                raise ValueError(
                    "Integer out of range for JSON serialization "
                    f"(must be within [-2**63, 2**64-1]): {current}"
                )
        elif isinstance(current, dict):
            # dict subclass, not caught by `t is dict`.
            for k, v in current.items():
                push(k)
                push(v)
        elif isinstance(current, (list, tuple)):
            # list/tuple subclass, not caught by `t is list or t is tuple`.
            for item in current:
                push(item)
        # Anything else (UUID, datetime, str subclasses, arbitrary objects)
        # cannot contain a float or int and is left alone.


def json_dumps(obj: Any) -> str:
    """
    Serialize object to JSON string with UUID and datetime support.

    Backed by `orjson` (a core dependency): fast, and serializes UUID and
    datetime natively.

    Rejects non-finite floats (`inf`, `-inf`, `nan`) and integers outside
    `[-2**63, 2**64-1]` by raising `ValueError` before handing off to
    orjson -- see `_validate_json_safe_values` for why.

    Args:
        obj: Object to serialize

    Returns:
        JSON string representation

    Raises:
        ValueError: If `obj` contains a non-finite float or an
            out-of-range integer anywhere in its structure.

    Example:
        >>> from uuid import uuid4
        >>> data = {"event_id": uuid4()}
        >>> json_str = json_dumps(data)
    """
    _validate_json_safe_values(obj)
    return orjson.dumps(obj, default=_orjson_default, option=orjson.OPT_NON_STR_KEYS).decode()


def json_loads(s: str | bytes) -> Any:
    """
    Deserialize JSON string (or bytes) to Python object.

    Backed by `orjson.loads`, which accepts both `str` and `bytes`.

    Note: UUID and datetime strings are NOT automatically converted
    back to their original types - that's the application's responsibility.

    Args:
        s: JSON string or bytes to deserialize

    Returns:
        Python object representation
    """
    return orjson.loads(s)


__all__ = [
    "EventSourceJSONEncoder",
    "json_dumps",
    "json_loads",
]
