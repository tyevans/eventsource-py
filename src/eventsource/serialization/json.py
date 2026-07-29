"""
JSON serialization utilities for eventsource types.

This module provides utilities for JSON serialization of common types
that are not natively JSON-serializable, such as UUIDs and datetimes.

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


def _orjson_default(obj: Any) -> Any:
    """
    Fallback handler for `orjson.dumps` for types it does not handle natively.

    orjson natively serializes UUID and datetime, so this only needs to cover
    whatever `EventSourceJSONEncoder.default` covers beyond that -- currently
    nothing, since UUID/datetime are the only types it special-cases. This
    exists so `orjson.dumps` raises the same `TypeError` (via the same code
    path shape) as stdlib for genuinely unsupported types, rather than a
    differently-worded orjson error.
    """
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def json_dumps(obj: Any) -> str:
    """
    Serialize object to JSON string with UUID and datetime support.

    Uses `orjson` when the optional `orjson` extra is installed (faster, and
    natively supports UUID/datetime), falling back to
    `json.dumps(..., cls=EventSourceJSONEncoder)` otherwise. Both paths
    produce byte-identical output for the shapes exercised by this library
    (compact separators, no non-str dict keys other than UUID) -- see
    `tests/unit/serialization/test_json.py::TestOrjsonParity`. This matters
    because encoded payloads persist in `event_outbox` and
    `dead_letter_queue`; two deployments of the same library version must
    encode identically regardless of whether the extra is installed.

    Args:
        obj: Object to serialize

    Returns:
        JSON string representation

    Example:
        >>> from uuid import uuid4
        >>> data = {"event_id": uuid4()}
        >>> json_str = json_dumps(data)
    """
    if ORJSON_AVAILABLE:
        return orjson.dumps(obj, default=_orjson_default, option=orjson.OPT_NON_STR_KEYS).decode()
    # separators=(",", ":") matches orjson's compact (no-whitespace) output.
    return json.dumps(obj, cls=EventSourceJSONEncoder, separators=(",", ":"))


def json_loads(s: str | bytes) -> Any:
    """
    Deserialize JSON string (or bytes) to Python object.

    Uses `orjson.loads` when the optional `orjson` extra is installed,
    falling back to `json.loads` otherwise. Both are standard JSON decoders
    and agree on output for valid JSON input.

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
