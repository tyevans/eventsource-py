"""
Repository implementations for the eventsource library.

This module provides infrastructure repositories for:

- **Checkpoint tracking**: Track projection positions for resumption
- **Dead Letter Queue (DLQ)**: Store failed events for retry/investigation
- **Outbox**: Transactional outbox pattern for reliable event publishing

Each repository type provides:
- A Protocol (interface) defining the contract
- PostgreSQL implementation for production use
- In-memory implementation for testing
"""

# Checkpoint repository
# JSON utilities
from eventsource.repositories._json import (
    EventSourceJSONEncoder,
    json_dumps,
    json_loads,
)
from eventsource.repositories.checkpoint import (
    CheckpointData,
    CheckpointRepository,
    CheckpointRepositoryProtocol,
    InMemoryCheckpointRepository,
    LagMetrics,
    PostgreSQLCheckpointRepository,
)

# DLQ repository
from eventsource.repositories.dlq import (
    DLQEntry,
    DLQRepository,
    DLQRepositoryProtocol,
    DLQStats,
    InMemoryDLQRepository,
    PostgreSQLDLQRepository,
    ProjectionFailureCount,
)

# Outbox repository
from eventsource.repositories.outbox import (
    InMemoryOutboxRepository,
    OutboxEntry,
    OutboxRepository,
    OutboxRepositoryProtocol,
    OutboxStats,
    PostgreSQLOutboxRepository,
)

__all__ = [
    # Checkpoint
    "CheckpointRepository",
    "CheckpointRepositoryProtocol",
    "PostgreSQLCheckpointRepository",
    "InMemoryCheckpointRepository",
    "CheckpointData",
    "LagMetrics",
    # DLQ
    "DLQRepository",
    "DLQRepositoryProtocol",
    "PostgreSQLDLQRepository",
    "InMemoryDLQRepository",
    "DLQEntry",
    "DLQStats",
    "ProjectionFailureCount",
    # Outbox
    "OutboxRepository",
    "OutboxRepositoryProtocol",
    "PostgreSQLOutboxRepository",
    "InMemoryOutboxRepository",
    "OutboxEntry",
    "OutboxStats",
    # JSON utilities
    "EventSourceJSONEncoder",
    "json_dumps",
    "json_loads",
]
