"""
Serialization utilities for eventsource.

This module provides serialization utilities for common types used
throughout the eventsource library, particularly JSON serialization
with support for UUIDs and datetimes.

Example:
    >>> from eventsource.serialization import json_dumps, EventSourceJSONEncoder
    >>> from uuid import uuid4
    >>>
    >>> data = {"id": uuid4()}
    >>> json_str = json_dumps(data)
"""

from eventsource.serialization.json import (
    EventSourceJSONEncoder,
    json_dumps,
    json_loads,
)

__all__ = [
    "EventSourceJSONEncoder",
    "json_dumps",
    "json_loads",
]
