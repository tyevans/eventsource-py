"""JSON serialization utilities for eventsource types backed by `orjson`."""

from typing import Any

import orjson


def _orjson_default(obj: Any) -> Any:
    """Fallback handler for `orjson.dumps` converting `float` subclasses to `float`."""
    if isinstance(obj, float):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


_ORJSON_INT_RANGE_MESSAGE = "Integer exceeds 64-bit range"


def json_dumps(obj: Any) -> str:
    """Serialize object to JSON string with UUID/datetime support; translates 64-bit int overflow to ValueError."""
    try:
        return orjson.dumps(obj, default=_orjson_default, option=orjson.OPT_NON_STR_KEYS).decode()
    except TypeError as exc:
        if _ORJSON_INT_RANGE_MESSAGE in str(exc):
            raise ValueError(
                "Integer out of range for JSON serialization (must be within [-2**63, 2**64-1])"
            ) from exc
        raise


def json_loads(s: str | bytes) -> Any:
    """Deserialize JSON string or bytes to a Python object via `orjson.loads`."""
    return orjson.loads(s)


__all__ = ["json_dumps", "json_loads"]
