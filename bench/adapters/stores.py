"""EventStore adapters. SQL backends are added by later tasks."""

from bench.adapters.base import BenchAdapter
from eventsource import InMemoryEventStore
from eventsource.stores.interface import EventStore


class MemoryStoreAdapter(BenchAdapter[EventStore]):
    name = "memory"

    async def create(self) -> EventStore:
        return InMemoryEventStore(enable_tracing=False)


STORE_ADAPTERS: dict[str, type[BenchAdapter[EventStore]]] = {
    MemoryStoreAdapter.name: MemoryStoreAdapter,
}
