"""
Repository implementations for the eventsource library.

This module provides infrastructure repositories for:

- **Checkpoint tracking**: Track projection positions for resumption
- **Dead Letter Queue (DLQ)**: Store failed events for retry/investigation
- **Outbox**: Transactional outbox pattern for reliable event publishing

Each repository type provides:
- A Protocol (interface) defining the contract
- PostgreSQL implementation for production use
- SQLite implementation for lightweight deployments
- In-memory implementation for testing

Naming Convention:
    Repository methods follow these naming patterns:

    - get_{entity}()      - Fetch a single entity by ID
    - list_{entities}()   - Fetch multiple entities with filtering
    - add_{entity}()      - Create a new entity
    - update_{entity}()   - Update an existing entity
    - delete_{entity}()   - Delete an entity
    - save_{entity}()     - Upsert (create or update)

    Some methods have both styles available for backward compatibility.
    For example, both get_failed_events() and list_failed_events() work.
    Prefer the list_* form when fetching multiple items.
"""

# Checkpoint repository
# JSON utilities (re-exported from serialization module)
from eventsource.repositories.checkpoint import (
    CheckpointData,
    CheckpointRepository,
    CheckpointRepositoryProtocol,
    InMemoryCheckpointRepository,
    LagMetrics,
    PostgreSQLCheckpointRepository,
    SQLiteCheckpointRepository,
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
    SQLiteDLQRepository,
)

# Outbox repository
from eventsource.repositories.outbox import (
    InMemoryOutboxRepository,
    OutboxEntry,
    OutboxRepository,
    OutboxRepositoryProtocol,
    OutboxStats,
    PostgreSQLOutboxRepository,
    SQLiteOutboxRepository,
)
from eventsource.serialization import (
    EventSourceJSONEncoder,
    json_dumps,
    json_loads,
)

__all__ = [
    # Checkpoint
    "CheckpointRepository",
    "CheckpointRepositoryProtocol",
    "PostgreSQLCheckpointRepository",
    "SQLiteCheckpointRepository",
    "InMemoryCheckpointRepository",
    "CheckpointData",
    "LagMetrics",
    # DLQ
    "DLQRepository",
    "DLQRepositoryProtocol",
    "PostgreSQLDLQRepository",
    "SQLiteDLQRepository",
    "InMemoryDLQRepository",
    "DLQEntry",
    "DLQStats",
    "ProjectionFailureCount",
    # Outbox
    "OutboxRepository",
    "OutboxRepositoryProtocol",
    "PostgreSQLOutboxRepository",
    "SQLiteOutboxRepository",
    "InMemoryOutboxRepository",
    "OutboxEntry",
    "OutboxStats",
    # JSON utilities
    "EventSourceJSONEncoder",
    "json_dumps",
    "json_loads",
]
