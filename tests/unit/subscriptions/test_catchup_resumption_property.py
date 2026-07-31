"""
Property test for ADR 0019 decision 5: no-skip catch-up resumption.

Across arbitrary stop/resume points, the CatchUpRunner must deliver every
event exactly once, in position order. A resumption that reads
`from_position=checkpoint` inclusively would redeliver an event; one that
advances past an undelivered event would skip it. Both are single-comparison
errors in the batch loop, and both fail this property.
"""

from uuid import uuid4

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import MemoryEventStore
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.ports.positions import ExpectedVersion
from eventsource.subscriptions import CheckpointStrategy, Subscription, SubscriptionConfig
from eventsource.subscriptions.runners import CatchUpRunner

pytestmark = pytest.mark.asyncio


class ResumptionPropertyEvent(DomainEvent):
    """Minimal event for the resumption property."""

    event_type: str = "ResumptionPropertyEvent"
    aggregate_type: str = "ResumptionPropertyAggregate"


class RecordingSubscriber:
    """Subscriber that records the order of delivered events."""

    def __init__(self) -> None:
        self.delivered: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [ResumptionPropertyEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.delivered.append(event)


async def _populate(store: MemoryEventStore, batches: list[int]) -> None:
    """Append `sum(batches)` events across distinct streams, one stream per batch."""
    for batch_len in batches:
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="ResumptionPropertyAggregate")
        events = [ResumptionPropertyEvent(aggregate_id=aggregate_id) for _ in range(batch_len)]
        await store.append(stream, events, ExpectedVersion.no_stream())


def _make_runner(
    event_store: MemoryEventStore,
    checkpoint_repo: InMemoryCheckpointRepository,
    subscriber: RecordingSubscriber,
    batch_size: int,
    start_position,
) -> CatchUpRunner:
    config = SubscriptionConfig(
        batch_size=batch_size,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    )
    subscription = Subscription(
        name="ResumptionPropertySubscription",
        config=config,
        subscriber=subscriber,
        last_processed_position=start_position,
    )
    return CatchUpRunner(
        event_store,
        checkpoint_repo,
        subscription,
        enable_metrics=False,
        enable_tracing=False,
    )


@settings(max_examples=50, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(
    batches=st.lists(st.integers(min_value=1, max_value=6), min_size=1, max_size=8),
    batch_size=st.integers(min_value=1, max_value=5),
    restart_after=st.integers(min_value=0, max_value=20),
)
async def test_catchup_delivers_every_event_exactly_once_across_a_restart(
    batches: list[int], batch_size: int, restart_after: int
) -> None:
    """Random stream shapes, batch sizes and restart points: every event is
    delivered exactly once, in position order.

    A resumption that reads `from_position=checkpoint` inclusively would
    redeliver; one that advances past an undelivered event would skip. Both
    are single-comparison errors in the batch loop, and both fail here.
    """
    event_store = MemoryEventStore()
    checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)

    await _populate(event_store, batches)

    total_events = sum(batches)
    # Clamp rather than assume-discard: any restart point beyond the total
    # is equivalent to "let the first runner finish".
    clamped_restart_after = min(restart_after, total_events)

    target_position = await event_store.current_position()
    assert target_position is not None

    first_subscriber = RecordingSubscriber()
    first_runner = _make_runner(event_store, checkpoint_repo, first_subscriber, batch_size, None)

    # Stop the first runner once it has delivered at least
    # `clamped_restart_after` events (it can only stop at batch boundaries).
    if clamped_restart_after == 0:
        pass
    else:
        while len(first_subscriber.delivered) < clamped_restart_after:
            batch_result = await first_runner._process_batch(target_position)
            if batch_result == 0:
                break

    checkpoint_position = await checkpoint_repo.get_position("ResumptionPropertySubscription")

    second_subscriber = RecordingSubscriber()
    second_runner = _make_runner(
        event_store, checkpoint_repo, second_subscriber, batch_size, checkpoint_position
    )
    await second_runner.run_until_position(target_position)

    delivered_ids = [e.event_id for e in first_subscriber.delivered] + [
        e.event_id for e in second_subscriber.delivered
    ]
    expected_ids = [envelope.event.event_id async for envelope in event_store.read_all()]

    assert delivered_ids == expected_ids
    assert len(set(delivered_ids)) == len(delivered_ids)
