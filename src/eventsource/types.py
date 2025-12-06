"""Common type definitions for the eventsource library."""

from typing import TypeVar
from uuid import UUID

from pydantic import BaseModel

# Type variable for aggregate state
TState = TypeVar("TState", bound=BaseModel)

# Type aliases for clarity and documentation
AggregateId = UUID
EventId = UUID
TenantId = UUID | None
CorrelationId = UUID
CausationId = UUID | None

# Version type for optimistic locking
Version = int

# Stream position types
StreamPosition = int
GlobalPosition = int
