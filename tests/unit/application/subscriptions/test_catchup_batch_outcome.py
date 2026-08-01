"""Catch-up terminates on reaching the target, not on an empty delivery.

An all-filtered batch delivers nothing but still advances position, so
breaking on a zero delivered-count conflated "genuinely caught up" with
"nothing matched this batch" -- and reported `completed=False` for a
heavily-filtered subscription that had in fact made progress.
"""

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
from eventsource.ports.positions import ExpectedVersion

pytestmark = pytest.mark.asyncio


class FilteredOutEvent(DomainEvent):
    """Type A -- never matches the subscription's filter."""

    aggregate_type: str = "BatchOutcomeAggregate"


class DeliveredEvent(DomainEvent):
    """Type B -- the only type the subscription subscribes to."""

    aggregate_type: str = "BatchOutcomeAggregate"


class RecordingSubscriber:
    def __init__(self) -> None:
        self.delivered: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [DeliveredEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.delivered.append(event)


async def _append(store: InMemoryEventStore, types: list[type[DomainEvent]]) -> None:
    """Append one event of each given type, each on its own stream."""
    for event_type in types:
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="BatchOutcomeAggregate")
        await store.append(
            stream, [event_type(aggregate_id=aggregate_id)], ExpectedVersion.no_stream()
        )


def _make_runner(store: InMemoryEventStore, subscriber: RecordingSubscriber) -> CatchUpRunner:
    config = SubscriptionConfig(
        batch_size=2,
        event_types=(DeliveredEvent,),
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    )
    subscription = Subscription(
        name="BatchOutcomeSubscription",
        config=config,
        subscriber=subscriber,
    )
    return CatchUpRunner(
        store,
        InMemoryCheckpointRepository(),
        subscription,
        enable_metrics=False,
        enable_tracing=False,
    )


class TestAllFilteredBatches:
    async def test_a_wholly_filtered_feed_still_reaches_the_target(self) -> None:
        store = InMemoryEventStore()
        # Five type-A events at batch_size=2 -- three batches, every one
        # of them delivering nothing.
        await _append(store, [FilteredOutEvent] * 5)
        target = await store.current_position()
        assert target is not None

        subscriber = RecordingSubscriber()
        runner = _make_runner(store, subscriber)

        result = await runner.run_until_position(target)

        assert result.completed is True
        assert result.events_processed == 0
        assert result.final_position == target
        assert subscriber.delivered == []

    async def test_an_interior_all_filtered_batch_does_not_end_catch_up(self) -> None:
        store = InMemoryEventStore()
        # batch_size=2: [B, A], [A, A], [A, B] -- the middle batch
        # delivers nothing, and the final B is only reached if the loop
        # keeps going.
        await _append(
            store,
            [
                DeliveredEvent,
                FilteredOutEvent,
                FilteredOutEvent,
                FilteredOutEvent,
                FilteredOutEvent,
                DeliveredEvent,
            ],
        )
        target = await store.current_position()
        assert target is not None

        subscriber = RecordingSubscriber()
        runner = _make_runner(store, subscriber)

        result = await runner.run_until_position(target)

        assert result.completed is True
        assert result.events_processed == 2
        assert len(subscriber.delivered) == 2
