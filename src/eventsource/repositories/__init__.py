"""
Repository implementations for the eventsource library.

This module provides the transactional outbox repository: reliable event
publishing via the transactional outbox pattern. Checkpoint and DLQ
repositories moved to `eventsource.ports` (interfaces) and
`eventsource.adapters` (implementations).

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

    Prefer the list_* form when fetching multiple items.
"""

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
