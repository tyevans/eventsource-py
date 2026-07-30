"""EventBus adapters. Broker backends are added by later tasks."""

from bench.adapters.base import BusAdapter
from eventsource import InMemoryEventBus
from eventsource.bus.interface import EventBus


class MemoryBusAdapter(BusAdapter):
    name = "memory"

    async def create(self) -> EventBus:
        return InMemoryEventBus(enable_tracing=False)

    async def destroy(self, resource: EventBus) -> None:
        await resource.shutdown()


BUS_ADAPTERS: dict[str, type[BusAdapter]] = {
    MemoryBusAdapter.name: MemoryBusAdapter,
}
