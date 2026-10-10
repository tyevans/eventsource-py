"""
Serialization utilities for eventsource.

This module provides serialization utilities for common types used
throughout the eventsource library, particularly JSON serialization
with support for UUIDs and datetimes.

Example:
    >>> from eventsource.adapters.serialization import json_dumps, EventSourceJSONEncoder
    >>> from uuid import uuid4
    >>>
    >>> data = {"id": uuid4()}
    >>> json_str = json_dumps(data)
"""

from eventsource.adapters.serialization.delta import (
    DeltaChainError,
    DeltaCodec,
    DeltaIntegrityError,
    DeltaPayload,
    is_delta_dict,
)
from eventsource.adapters.serialization.json import (
    EventSourceJSONEncoder,
    json_dumps,
    json_loads,
)

__all__ = [
    "DeltaChainError",
    "DeltaCodec",
    "DeltaIntegrityError",
    "DeltaPayload",
    "EventSourceJSONEncoder",
    "is_delta_dict",
    "json_dumps",
    "json_loads",
]
