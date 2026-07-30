"""In-process memory adapter implementing the store ports."""

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import MemoryEventStore

__all__ = ["InMemorySnapshotStore", "MemoryEventStore"]
