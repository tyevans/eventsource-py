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

- A UUID dict key serializes (via `orjson.OPT_NON_STR_KEYS`).
- An integer outside `[-2**63, 2**64-1]` raises `ValueError`. orjson only
  supports 64-bit integers and otherwise raises a bare, unhelpful
  `TypeError: Integer exceeds 64-bit range`; `json_dumps` catches that and
  re-raises `ValueError` so the failure mode is documented rather than an
  opaque orjson internal. This is a translation of orjson's own error, not
  a pre-emptive scan -- see `json_dumps` for why that distinction matters.
- A non-finite `float` **subclass** value (e.g. `numpy.float64(inf)`) is
  NOT rejected, and neither is a plain non-finite `float` -- both silently
  become JSON `null`. (An earlier design tried to reject the subclass case
  specifically via `_orjson_default`; verified by direct execution that
  orjson swallows any exception raised inside a `default=` callback and
  replaces it with a generic message, so that design could not work --
  see `_orjson_default`'s docstring for the full account.)

**There is deliberately no pre-serialization scan for non-finite floats or
out-of-range integers anymore** (there was, in earlier revisions of this
module -- see git history / `task-2b-report.md` rounds 2-4). That scan
made this wrapper 14.5x slower than raw `orjson.dumps` and, measured
against a realistic payload, slower than the stdlib encoder it replaced --
which defeated the entire point of taking on `orjson` as a core
dependency. Two things changed instead:

1. **Non-finite float rejection moved upstream**, to `DomainEvent.model_config`
   (`allow_inf_nan=False` in `src/eventsource/events/base.py`) -- pydantic
   rejects `inf`/`-inf`/`nan` with `ValidationError` at event construction,
   before serialization is ever reached, for every event built through
   `DomainEvent`. This is strictly better coverage than the old scan: it
   fires at the earliest possible point, not the latest.
2. **Integer range rejection stays here**, but as error translation
   instead of pre-emptive scanning -- `json_dumps` catches orjson's own
   `TypeError` and converts it to `ValueError`, costing nothing on the
   success path.

**Residual risk, accepted deliberately:** a non-finite float inside a
payload that does NOT go through `DomainEvent` validation -- a hand-built
`dict`, DLQ error metadata, anything constructed and passed to
`json_dumps` directly -- is no longer rejected. orjson will silently
convert it to JSON `null`. See `docs/reference/serialization-limits.md`
for this stated as an explicit limitation, not a hidden one.

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
from datetime import datetime
from typing import Any
from uuid import UUID

import orjson


class EventSourceJSONEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that handles UUID and datetime objects.

    `json_dumps`/`json_loads` no longer use this class -- orjson (a core
    dependency) serializes UUID and datetime natively, so there is no
    stdlib-backed path left that needs it. It is kept because
    `eventsource.adapters.postgresql.outbox` and
    `eventsource.adapters.sqlite.outbox` still call stdlib
    `json.dumps(event_data, cls=EventSourceJSONEncoder)` directly at their
    `add_event` call sites (bypassing `json_dumps` entirely) -- migrating
    those call sites is out of scope for this change. It is also public
    API, re-exported from `eventsource.serialization` and the top-level
    `eventsource` package.

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
    subclasses -- those fall through to here. Converts to a plain `float`
    so it serializes exactly like an ordinary `float` -- including the
    residual-risk case: a non-finite `float` subclass value (finiteness
    intentionally NOT checked here) becomes JSON `null`, the same as a
    non-finite plain `float`. See "Deviation from the original design"
    below for why this doesn't raise for that case.

    Everything else genuinely unsupported raises `TypeError`.

    Deviation from the original design, found by direct execution: an
    earlier version of this function raised `ValueError` for a non-finite
    `float` subclass. That never actually worked, and the bug was
    invisible while a pre-serialization scan (since removed) intercepted
    non-finite floats -- including subclass ones -- before `orjson.dumps`
    was ever called, so this branch was dead code in practice. Verified
    directly: `orjson.dumps` swallows *any* exception raised inside a
    `default=` callback -- including `ValueError`, and even a `TypeError`
    with a custom message -- and replaces it with its own generic
    `TypeError: Type is not JSON serializable: <ClassName>`, discarding
    whatever the callback actually raised. There is no way to signal
    "this specific value is out of range" back through that hook; the
    callback can only choose "serializable" (return a value) or "not
    serializable at all" (any exception, indistinguishable to the
    caller). Given that constraint, converting unconditionally (matching
    plain-`float` behavior) is more honest than raising while claiming
    the caller gets a `ValueError` naming the problem -- they would not;
    they would get orjson's generic message instead.
    """
    if isinstance(obj, float):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


_ORJSON_INT_RANGE_MESSAGE = "Integer exceeds 64-bit range"


def json_dumps(obj: Any) -> str:
    """
    Serialize object to JSON string with UUID and datetime support.

    Backed by `orjson` (a core dependency): fast, and serializes UUID and
    datetime natively. Deliberately does NOT pre-scan `obj` for non-finite
    floats or out-of-range integers before calling `orjson.dumps` -- see
    the module docstring for why (a prior scan-based version made this
    wrapper multiples slower than raw `orjson.dumps`, and slower than the
    stdlib encoder it replaced).

    Integers outside `[-2**63, 2**64-1]` still raise `ValueError`, but via
    translation: `orjson.dumps` raises a bare `TypeError: Integer exceeds
    64-bit range` for these, with no indication of which value or field
    caused it; this catches that specific `TypeError` and re-raises
    `ValueError` with the supported range stated. This costs nothing on
    the success path -- it's a `try/except`, not a scan.

    Non-finite floats (`inf`, `-inf`, `nan`) are NOT rejected here. See the
    module docstring's "Residual risk" note: rejection for these moved to
    `DomainEvent.model_config` (`allow_inf_nan=False`), which covers every
    event but not hand-built dicts passed to `json_dumps` directly -- those
    will silently serialize as JSON `null`, unchanged from raw orjson
    behavior.

    Args:
        obj: Object to serialize

    Returns:
        JSON string representation

    Raises:
        ValueError: If `obj` contains an integer outside
            `[-2**63, 2**64-1]` anywhere in its structure.

    Example:
        >>> from uuid import uuid4
        >>> data = {"event_id": uuid4()}
        >>> json_str = json_dumps(data)
    """
    try:
        return orjson.dumps(obj, default=_orjson_default, option=orjson.OPT_NON_STR_KEYS).decode()
    except TypeError as exc:
        if _ORJSON_INT_RANGE_MESSAGE in str(exc):
            raise ValueError(
                "Integer out of range for JSON serialization (must be within [-2**63, 2**64-1])"
            ) from exc
        raise


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
