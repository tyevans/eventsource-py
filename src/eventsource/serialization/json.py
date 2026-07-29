"""
JSON serialization utilities for eventsource types.

This module provides utilities for JSON serialization of common types
that are not natively JSON-serializable, such as UUIDs and datetimes.

`json_dumps` uses `orjson` when the optional `orjson` extra is installed,
falling back to stdlib `json` otherwise. The two paths are kept
byte-identical for every shape this library needs (see
`tests/unit/serialization/test_json.py::TestOrjsonParity`), with two
documented exceptions:

- A UUID dict key: stdlib cannot encode one at all (raises `TypeError`),
  while orjson can (with `OPT_NON_STR_KEYS`). Left unmatched because no
  prior build could ever have persisted one, so there's nothing to drift
  from.
- A non-finite float (`inf`/`-inf`/`nan`): both paths *raise* `ValueError`
  rather than diverging. stdlib's default would emit non-standard
  `Infinity`/`NaN` tokens and orjson would silently substitute `null` --
  neither is acceptable for a persisted value, so both are rejected at the
  point of serialization instead.

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

try:
    import orjson

    ORJSON_AVAILABLE = True
except ImportError:  # pragma: no cover - exercised in the no-orjson environment
    ORJSON_AVAILABLE = False


class EventSourceJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles UUID and datetime objects.

    This encoder extends the standard JSONEncoder to support serialization of:
    - UUID objects: Converted to string representation
    - datetime objects: Converted to ISO 8601 format string

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


_isfinite = math.isfinite


def _orjson_default(obj: Any) -> Any:
    """
    Fallback handler for `orjson.dumps` for types it does not handle natively.

    orjson natively serializes UUID and datetime, so this mainly needs to
    cover whatever `EventSourceJSONEncoder.default` covers beyond that --
    plus one thing stdlib's `default()` never has to handle: `float`
    subclasses (e.g. `numpy.float64`). orjson serializes `str`/`int`/`dict`/
    `list` subclasses natively via their base-type behavior, but NOT `float`
    subclasses -- those fall through to here. stdlib's `json.dumps` treats a
    float subclass as a plain float without ever calling `default()` for it,
    so to match: reject a non-finite float subclass with the identical
    `ValueError` used everywhere else, and convert a finite one to a plain
    `float` so it serializes the same way stdlib would.

    Everything else genuinely unsupported still raises `TypeError`, so
    `orjson.dumps` fails the same way (same exception type, same code path
    shape) as stdlib for types neither encoder handles.
    """
    if isinstance(obj, float):
        if not _isfinite(obj):
            raise ValueError(f"Out of range float values are not JSON compliant: {obj}")
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


# Leaf types that can never contain a float, checked by exact type() first
# (fast path) before falling through to the isinstance() slow path below.
_LEAF_TYPES = (str, int, bool, type(None))


def _reject_non_finite_floats(obj: Any) -> None:
    """
    Raise `ValueError` if `obj` contains a non-finite float (inf/-inf/nan),
    anywhere in a nested dict/list/tuple structure -- including inside
    subclasses of dict/list/tuple, and including `float` subclasses
    themselves (e.g. `numpy.float64`).

    Needed only on the orjson path: `orjson.dumps` has no `allow_nan`
    equivalent and silently converts non-finite floats to JSON `null`
    (confirmed -- no `OPT_*` flag covers this, and floats are handled
    natively by orjson so `default=` never sees them either). The stdlib
    fallback gets the same behavior for free via `allow_nan=False`.

    Both branches must reject rather than diverge: the stdlib default
    (`allow_nan=True`) emits the non-standard bare tokens `Infinity`/`NaN`,
    which most JSON parsers -- and PostgreSQL `jsonb` -- reject outright,
    while orjson would silently substitute `null`. Depending on which
    branch a given deployment took, the same input either fails at the DB
    boundary (informative but too late to identify the field) or silently
    loses the value (uninformative and never surfaces at all). Raising here,
    at the point the bad value enters serialization, beats both.

    Correctness over the naive `type(x) is dict` version: a `dict`/`list`/
    `tuple` *subclass* is False under `type(x) is dict`, so an earlier
    version of this scan walked past it without examining its contents --
    while orjson traverses that same subclass natively and serializes its
    contents (including any non-finite float inside), so the scan skipping
    it silently reintroduced exactly the corruption it exists to prevent.
    Fixed by falling through to `isinstance()` whenever the exact-type fast
    path doesn't match, rather than giving up. Leaf types that can never
    contain a float (str, int, bool, None) are excluded up front by exact
    type so ordinary payloads mostly stay on the cheap path; anything else
    -- including a container subclass, a float subclass, or a genuinely
    unrelated type like UUID/datetime -- pays for an `isinstance()` check,
    which is what correctness requires here.
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
        elif t is dict:
            for k, v in current.items():
                push(k)
                push(v)
        elif t is list or t is tuple:
            for item in current:
                push(item)
        elif t in _LEAF_TYPES:
            continue
        elif isinstance(current, float):
            # float subclass (e.g. numpy.float64), not caught by `t is float`.
            if not _isfinite(current):
                raise ValueError(f"Out of range float values are not JSON compliant: {current}")
        elif isinstance(current, dict):
            # dict subclass, not caught by `t is dict`.
            for k, v in current.items():
                push(k)
                push(v)
        elif isinstance(current, (list, tuple)):
            # list/tuple subclass, not caught by `t is list or t is tuple`.
            for item in current:
                push(item)
        # Anything else (UUID, datetime, str/int/bool subclasses, arbitrary
        # objects) cannot contain a float and is left alone.


def json_dumps(obj: Any) -> str:
    """
    Serialize object to JSON string with UUID and datetime support.

    Uses `orjson` when the optional `orjson` extra is installed (faster, and
    natively supports UUID/datetime), falling back to
    `json.dumps(..., cls=EventSourceJSONEncoder)` otherwise. Both paths
    produce byte-identical output for the shapes exercised by this library
    (compact separators, no `\\uXXXX`-escaping, no non-str dict keys other
    than UUID) -- see
    `tests/unit/serialization/test_json.py::TestOrjsonParity`. This matters
    because encoded payloads persist in `event_outbox` and
    `dead_letter_queue`; two deployments of the same library version must
    encode identically regardless of whether the extra is installed.

    Non-finite floats (`inf`, `-inf`, `nan`) raise `ValueError` from both
    paths rather than silently diverging -- see `_reject_non_finite_floats`.

    Args:
        obj: Object to serialize

    Returns:
        JSON string representation

    Raises:
        ValueError: If `obj` contains a non-finite float anywhere in its
            structure.

    Example:
        >>> from uuid import uuid4
        >>> data = {"event_id": uuid4()}
        >>> json_str = json_dumps(data)
    """
    if ORJSON_AVAILABLE:
        # orjson has no allow_nan=False equivalent, so non-finite floats
        # must be rejected explicitly before handing off to the C encoder.
        _reject_non_finite_floats(obj)
        return orjson.dumps(obj, default=_orjson_default, option=orjson.OPT_NON_STR_KEYS).decode()
    # separators=(",", ":") matches orjson's compact (no-whitespace) output.
    # ensure_ascii=False matches orjson, which always emits raw UTF-8 and has
    # no option to \uXXXX-escape non-ASCII -- without this, stdlib and orjson
    # would encode identical non-ASCII payloads (names, addresses, emoji) to
    # different bytes depending only on whether the orjson extra is
    # installed, in the same event_outbox / dead_letter_queue table.
    # allow_nan=False matches the orjson path's explicit rejection above --
    # without it, stdlib would silently emit the non-standard Infinity/NaN
    # tokens that most JSON parsers (and PostgreSQL jsonb) reject anyway.
    return json.dumps(
        obj,
        cls=EventSourceJSONEncoder,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def json_loads(s: str | bytes) -> Any:
    """
    Deserialize JSON string (or bytes) to Python object.

    Uses `orjson.loads` when the optional `orjson` extra is installed,
    falling back to `json.loads` otherwise. Both are standard JSON decoders
    and agree on output for valid JSON input.

    Accepts `bytes` (widened from a `str`-only signature) because
    `_dialect.json_result` passes through whatever the DB driver returns --
    sometimes `bytes` -- and both `json.loads` and `orjson.loads` decode
    `bytes` natively, so narrowing to `str` at this call site would just
    force a redundant decode.

    Note: UUID and datetime strings are NOT automatically converted
    back to their original types - that's the application's responsibility.

    Args:
        s: JSON string or bytes to deserialize

    Returns:
        Python object representation
    """
    if ORJSON_AVAILABLE:
        return orjson.loads(s)
    return json.loads(s)


__all__ = [
    "EventSourceJSONEncoder",
    "ORJSON_AVAILABLE",
    "json_dumps",
    "json_loads",
]
