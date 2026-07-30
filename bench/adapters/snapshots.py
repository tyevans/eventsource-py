"""SnapshotStore adapters. SQL backends are added by later tasks."""

from bench.adapters.base import BenchAdapter
from eventsource.snapshots.in_memory import InMemorySnapshotStore
from eventsource.snapshots.interface import SnapshotStore


class MemorySnapshotAdapter(BenchAdapter[SnapshotStore]):
    name = "memory"

    async def create(self) -> SnapshotStore:
        return InMemorySnapshotStore(enable_tracing=False)


SNAPSHOT_ADAPTERS: dict[str, type[BenchAdapter[SnapshotStore]]] = {
    MemorySnapshotAdapter.name: MemorySnapshotAdapter,
}
