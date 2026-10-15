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
    """Simple test event for batch-dispatch tests."""

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


class TestBatchDispatch:
    """A batch-capable subscriber's handle_batch() is actually called."""

    async def test_handle_batch_called_with_multiple_events(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        subscriber = RecordingBatchSubscriber()
        config = SubscriptionConfig(
            batch_size=10, checkpoint_strategy=CheckpointStrategy.EVERY_BATCH
        )
        subscription = Subscription(name="batch-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 5)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert result.events_processed == 5
        assert len(subscriber.batch_calls) == 1
        assert len(subscriber.batch_calls[0]) == 5
        assert subscriber.single_calls == []

    async def test_position_advances_only_after_batch_settles(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        subscriber = RecordingBatchSubscriber()
        config = SubscriptionConfig(
            batch_size=10, checkpoint_strategy=CheckpointStrategy.EVERY_BATCH
        )
        subscription = Subscription(name="batch-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        target = await current(event_store)
        await runner.run_until_position(target)

        assert subscription.last_processed_position == target
        checkpoint = await checkpoint_repo.get_position(subscription_id="batch-sub")
        assert checkpoint == target

    async def test_multiple_batches_each_dispatched_separately(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        subscriber = RecordingBatchSubscriber()
        config = SubscriptionConfig(
            batch_size=3, checkpoint_strategy=CheckpointStrategy.EVERY_BATCH
        )
        subscription = Subscription(name="batch-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 7)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert result.events_processed == 7
        assert [len(c) for c in subscriber.batch_calls] == [3, 3, 1]


class TestBatchFallback:
    """A handle_batch() that raises falls back to per-event delivery."""

    async def test_batch_failure_falls_back_to_single_event_delivery(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        subscriber = RecordingBatchSubscriber(fail_batch=True)
        config = SubscriptionConfig(
            batch_size=10,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
            continue_on_error=True,
        )
        subscription = Subscription(name="batch-sub", config=config, subscriber=subscriber)
        events = await add_events(event_store, 4)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert result.events_processed == 4
        assert subscriber.single_calls == events
        assert len(subscriber.batch_calls) == 0


class TestBatchFilterInteraction:
    """Filtering happens before dispatch; filtered-out events never reach handle_batch()."""

    async def test_filtered_events_excluded_from_batch(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        subscriber = RecordingBatchSubscriber()
        config = SubscriptionConfig(
            batch_size=10,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
            aggregate_types=("OtherAggregate",),
        )
        subscription = Subscription(name="batch-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert result.events_processed == 0
        assert subscriber.batch_calls == []
        assert subscriber.single_calls == []


class TestNonBatchSubscriberUnaffected:
    """A subscriber without handle_batch still goes through handle()."""

    async def test_non_batch_subscriber_uses_single_event_path(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        subscriber = NonBatchSubscriber()
        config = SubscriptionConfig(
            batch_size=10, checkpoint_strategy=CheckpointStrategy.EVERY_BATCH
        )
        subscription = Subscription(name="single-sub", config=config, subscriber=subscriber)
        events = await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert result.events_processed == 3
        assert subscriber.single_calls == events
