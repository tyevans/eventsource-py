"""Unit tests for batch dispatch in CatchUpRunner (task #7)."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import CatchUpRunner
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion, Position

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class BatchTestEvent(DomainEvent):
    aggregate_type: str = "BatchAggregate"
    data: str = "test"


class RecordingBatchSubscriber:
    """A subscriber with handle_batch(); handle() runs only on fallback."""

    def __init__(self, fail_batch: bool = False) -> None:
        self.batch_calls: list[list[DomainEvent]] = []
        self.single_calls: list[DomainEvent] = []
        self.fail_batch = fail_batch

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [BatchTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.single_calls.append(event)

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        if self.fail_batch:
            raise RuntimeError("batch processing exploded")
        self.batch_calls.append(list(events))


class NonBatchSubscriber:
    """A plain subscriber -- no handle_batch -- for the control case."""

    def __init__(self) -> None:
        self.single_calls: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [BatchTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.single_calls.append(event)


@pytest.fixture
def event_store() -> InMemoryEventStore:
    return InMemoryEventStore(event_registry=_REGISTRY)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    return InMemoryCheckpointRepository(enable_tracing=False)


async def add_events(store: InMemoryEventStore, count: int) -> list[DomainEvent]:
    events = []
    for i in range(count):
        aggregate_id = uuid4()
        event = BatchTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="BatchAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )
        events.append(event)
    return events


async def current(store: InMemoryEventStore) -> Position:
    position = await store.current_position()
    assert position is not None
    return position


def _make_sub(
    subscriber: RecordingBatchSubscriber | NonBatchSubscriber,
    name: str = "batch-sub",
    batch_size: int = 10,
    continue_on_error: bool = False,
    aggregate_types: tuple[str, ...] | None = None,
) -> Subscription:
    config = SubscriptionConfig(
        batch_size=batch_size,
        checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
        continue_on_error=continue_on_error,
        aggregate_types=aggregate_types,
    )
    return Subscription(name=name, config=config, subscriber=subscriber)


class TestBatchDispatch:
    async def test_handle_batch_called_with_multiple_events(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        subscriber = RecordingBatchSubscriber()
        subscription = _make_sub(subscriber)
        await add_events(event_store, 5)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed and result.events_processed == 5
        assert len(subscriber.batch_calls) == 1 and len(subscriber.batch_calls[0]) == 5
        assert subscriber.single_calls == []

    async def test_position_advances_only_after_batch_settles(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        subscriber = RecordingBatchSubscriber()
        subscription = _make_sub(subscriber)
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        target = await current(event_store)
        await runner.run_until_position(target)

        assert subscription.last_processed_position == target
        assert await checkpoint_repo.get_position(subscription_id="batch-sub") == target

    async def test_multiple_batches_each_dispatched_separately(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        subscriber = RecordingBatchSubscriber()
        subscription = _make_sub(subscriber, batch_size=3)
        await add_events(event_store, 7)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed and result.events_processed == 7
        assert [len(c) for c in subscriber.batch_calls] == [3, 3, 1]


class TestBatchFallback:
    async def test_batch_failure_falls_back_to_single_event_delivery(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        subscriber = RecordingBatchSubscriber(fail_batch=True)
        subscription = _make_sub(subscriber, continue_on_error=True)
        events = await add_events(event_store, 4)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed and result.events_processed == 4
        assert subscriber.single_calls == events and len(subscriber.batch_calls) == 0


class TestBatchFilterInteraction:
    async def test_filtered_events_excluded_from_batch(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        subscriber = RecordingBatchSubscriber()
        subscription = _make_sub(subscriber, aggregate_types=("OtherAggregate",))
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed and result.events_processed == 0
        assert subscriber.batch_calls == [] and subscriber.single_calls == []


class TestNonBatchSubscriberUnaffected:
    async def test_non_batch_subscriber_uses_single_event_path(
        self, event_store: InMemoryEventStore, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        subscriber = NonBatchSubscriber()
        subscription = _make_sub(subscriber, name="single-sub")
        events = await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed and result.events_processed == 3
        assert subscriber.single_calls == events
