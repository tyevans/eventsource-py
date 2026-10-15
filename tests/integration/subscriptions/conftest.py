"""Integration test fixtures for subscription management."""

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import pytest_asyncio

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import SubscriptionManager
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import register_event
from eventsource.ports.positions import ExpectedVersion


@register_event
class SubTestOrderCreated(DomainEvent):
    aggregate_type: str = "SubTestOrder"
    order_number: str = ""
    amount: float = 0.0


@register_event
class SubTestOrderShipped(DomainEvent):
    aggregate_type: str = "SubTestOrder"
    tracking_number: str = ""


@register_event
class SubTestOrderCancelled(DomainEvent):
    aggregate_type: str = "SubTestOrder"
    reason: str = ""


TestOrderCreated = SubTestOrderCreated
TestOrderShipped = SubTestOrderShipped
TestOrderCancelled = SubTestOrderCancelled


class _BaseTestProjection:
    def __init__(self) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def _wait(self, count: int, timeout: float, interval: float) -> bool:
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(interval)
        return True


class CollectingProjection(_BaseTestProjection):
    def __init__(self, name: str | None = None) -> None:
        super().__init__()
        self.event_ids: set[str] = set()
        self._name = name

    async def handle(self, event: DomainEvent) -> None:
        async with self._lock:
            event_id_str = str(event.event_id)
            if event_id_str not in self.event_ids:
                self.events.append(event)
                self.event_ids.add(event_id_str)
                self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 5.0) -> bool:
        return await self._wait(count, timeout, 0.05)

    def get_event_at_position(self, position: int) -> DomainEvent | None:
        return self.events[position] if 0 <= position < len(self.events) else None

    def has_duplicates(self) -> bool:
        return len(self.events) != len(self.event_ids)


class FailingProjection(_BaseTestProjection):
    def __init__(self, fail_on_event_types: set[str] | None = None) -> None:
        super().__init__()
        self.failures: list[tuple[DomainEvent, Exception]] = []
        self.fail_on_event_types = fail_on_event_types or set()

    async def handle(self, event: DomainEvent) -> None:
        async with self._lock:
            if event.event_type in self.fail_on_event_types:
                error = ValueError(f"Intentional failure for {event.event_type}")
                self.failures.append((event, error))
                raise error
            self.events.append(event)
            self.event_count += 1


class SlowProjection(_BaseTestProjection):
    def __init__(self, delay: float = 0.1) -> None:
        super().__init__()
        self.delay = delay

    async def handle(self, event: DomainEvent) -> None:
        await asyncio.sleep(self.delay)
        async with self._lock:
            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
        return await self._wait(count, timeout, 0.1)


@pytest_asyncio.fixture
async def in_memory_event_store() -> AsyncGenerator[InMemoryEventStore]:
    yield InMemoryEventStore()


@pytest_asyncio.fixture
async def in_memory_event_bus() -> AsyncGenerator[InMemoryEventBus]:
    bus = InMemoryEventBus(enable_tracing=False)
    yield bus
    await bus.shutdown()


@pytest_asyncio.fixture
async def in_memory_checkpoint_repo() -> AsyncGenerator[InMemoryCheckpointRepository]:
    yield InMemoryCheckpointRepository(enable_tracing=False)


@pytest_asyncio.fixture
async def subscription_manager(
    in_memory_event_store: InMemoryEventStore,
    in_memory_event_bus: InMemoryEventBus,
    in_memory_checkpoint_repo: InMemoryCheckpointRepository,
) -> AsyncGenerator[SubscriptionManager]:
    mgr = SubscriptionManager(in_memory_event_store, in_memory_event_bus, in_memory_checkpoint_repo)
    yield mgr
    if mgr.is_running:
        await mgr.stop()


@pytest.fixture
def collecting_projection() -> CollectingProjection:
    return CollectingProjection()


async def _append_event(store: InMemoryEventStore, event: DomainEvent) -> DomainEvent:
    await store.append(
        StreamId(aggregate_id=event.aggregate_id, category="SubTestOrder"),
        [event],
        ExpectedVersion.no_stream(),
    )
    return event


async def populate_event_store(
    store: InMemoryEventStore, count: int, start_index: int = 0
) -> list[DomainEvent]:
    events: list[DomainEvent] = []
    for i in range(count):
        ev = SubTestOrderCreated(
            aggregate_id=uuid4(), order_number=f"ORD-{start_index + i:05d}", amount=100.0 + i
        )
        events.append(await _append_event(store, ev))
    return events


async def populate_event_store_with_types(
    store: InMemoryEventStore, created_count: int, shipped_count: int, cancelled_count: int = 0
) -> list[DomainEvent]:
    events = await populate_event_store(store, created_count)
    for _ in range(shipped_count):
        ev = SubTestOrderShipped(aggregate_id=uuid4(), tracking_number=f"TRK-{len(events):05d}")
        events.append(await _append_event(store, ev))
    for _ in range(cancelled_count):
        ev = SubTestOrderCancelled(aggregate_id=uuid4(), reason=f"Reason {len(events)}")
        events.append(await _append_event(store, ev))
    return events


async def publish_live_event(
    store: InMemoryEventStore, bus: InMemoryEventBus, order_number: str, amount: float = 100.0
) -> SubTestOrderCreated:
    event = SubTestOrderCreated(aggregate_id=uuid4(), order_number=order_number, amount=amount)
    await _append_event(store, event)
    await bus.publish([event])
    return event
