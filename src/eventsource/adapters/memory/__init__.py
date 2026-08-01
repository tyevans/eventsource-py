"""In-process memory adapters implementing the store, snapshot, checkpoint, DLQ, outbox, read model, lock, and leader-election ports."""

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.coordination import (
    InMemoryLeaderElector,
    SharedLeaderState,
)
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.locks import InMemoryLockManager
from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
from eventsource.adapters.memory.readmodels import InMemoryReadModelRepository
from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore

__all__ = [
    "InMemoryCheckpointRepository",
    "InMemoryDLQRepository",
    "InMemoryEventStore",
    "InMemoryLeaderElector",
    "InMemoryLockManager",
    "InMemoryOutboxRepository",
    "InMemoryReadModelRepository",
    "InMemorySnapshotStore",
    "SharedLeaderState",
]
