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


def json_dumps(obj: Any) -> str:
    """
    Serialize object to JSON string with UUID and datetime support.

    Convenience function that uses EventSourceJSONEncoder.

    Args:
        obj: Object to serialize

    Returns:
        JSON string representation

    Example:
        >>> from uuid import uuid4
        >>> data = {"event_id": uuid4()}
        >>> json_str = json_dumps(data)
    """
    return json.dumps(obj, cls=EventSourceJSONEncoder)


def json_loads(s: str) -> Any:
    """
    Deserialize JSON string to Python object.

    Note: This is a simple wrapper around json.loads.
    UUID and datetime strings are NOT automatically converted
    back to their original types - that's the application's responsibility.

    Args:
        s: JSON string to deserialize

    Returns:
        Python object representation
    """
    return json.loads(s)


__all__ = [
    "EventSourceJSONEncoder",
    "json_dumps",
    "json_loads",
]
