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


class CollectingProjection:
    """Projection that collects events for verification."""

    def __init__(self, name: str | None = None) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.event_ids: set[str] = set()
        self._lock = asyncio.Lock()
        self._name = name

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        async with self._lock:
            event_id_str = str(event.event_id)
            if event_id_str not in self.event_ids:
                self.events.append(event)
                self.event_ids.add(event_id_str)
                self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 5.0) -> bool:
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True

    def get_event_at_position(self, position: int) -> DomainEvent | None:
        return self.events[position] if 0 <= position < len(self.events) else None

    def has_duplicates(self) -> bool:
        return len(self.events) != len(self.event_ids)


class FailingProjection:
    """Projection that fails on specified event types."""

    def __init__(self, fail_on_event_types: set[str] | None = None) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.failures: list[tuple[DomainEvent, Exception]] = []
        self.fail_on_event_types = fail_on_event_types or set()
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        async with self._lock:
            if event.event_type in self.fail_on_event_types:
                error = ValueError(f"Intentional failure for {event.event_type}")
                self.failures.append((event, error))
                raise error
            self.events.append(event)
            self.event_count += 1


class SlowProjection:
    """Projection that processes events slowly."""

    def __init__(self, delay: float = 0.1) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.delay = delay
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        await asyncio.sleep(self.delay)
        async with self._lock:
            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 10.0) -> bool:
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.1)
        return True


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
    manager = SubscriptionManager(
        event_store=in_memory_event_store,
        event_bus=in_memory_event_bus,
        checkpoint_repo=in_memory_checkpoint_repo,
    )
    yield manager
    if manager.is_running:
        await manager.stop()


@pytest.fixture
def collecting_projection() -> CollectingProjection:
    return CollectingProjection()


async def _append_event(store: InMemoryEventStore, event: DomainEvent) -> DomainEvent:
    stream_id = StreamId(aggregate_id=event.aggregate_id, category="SubTestOrder")
    await store.append(stream_id, [event], ExpectedVersion.no_stream())
    return event


async def populate_event_store(
    store: InMemoryEventStore, count: int, start_index: int = 0
) -> list[DomainEvent]:
    events = []
    for i in range(count):
        event = SubTestOrderCreated(
            aggregate_id=uuid4(), order_number=f"ORD-{start_index + i:05d}", amount=100.0 + i
        )
        events.append(await _append_event(store, event))
    return events


async def populate_event_store_with_types(
    store: InMemoryEventStore, created_count: int, shipped_count: int, cancelled_count: int = 0
) -> list[DomainEvent]:
    events: list[DomainEvent] = []
    for i in range(created_count):
        event = SubTestOrderCreated(
            aggregate_id=uuid4(), order_number=f"ORD-{len(events):05d}", amount=100.0 + i
        )
        events.append(await _append_event(store, event))
    for _i in range(shipped_count):
        event = SubTestOrderShipped(aggregate_id=uuid4(), tracking_number=f"TRK-{len(events):05d}")
        events.append(await _append_event(store, event))
    for _i in range(cancelled_count):
        event = SubTestOrderCancelled(aggregate_id=uuid4(), reason=f"Reason {len(events)}")
        events.append(await _append_event(store, event))
    return events


async def publish_live_event(
    store: InMemoryEventStore, bus: InMemoryEventBus, order_number: str, amount: float = 100.0
) -> SubTestOrderCreated:
    event = SubTestOrderCreated(aggregate_id=uuid4(), order_number=order_number, amount=amount)
    await _append_event(store, event)
    await bus.publish([event])
    return event
