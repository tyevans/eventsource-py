"""
Unit tests for `BatchSubscriber`-only subscribers on the live path.

Covers task #38. `BatchSubscriber` requires only `subscribed_to()` and
`handle_batch()` -- not `handle()`. Catch-up honors that, but the live runner
called `subscriber.handle(event)` unconditionally, so a subscriber that
implements exactly the published Protocol raised `AttributeError` **per event**
the moment its subscription went live. Each one was recorded as a handler
failure, counted toward the circuit breaker, and (with `continue_on_error`)
could fill the DLQ with events whose handler was never actually reached. The
docs said such a subscriber "will not receive live events", which reads like a
silent no-op rather than a per-event exception.

The fix delivers a one-event batch rather than batching the live path (task
#34, still open): `handle_batch([event])` is the subscriber's only handler, so
it is the honest call, and per-envelope stop/pause granularity is untouched.

`handle()` is still preferred whenever it exists, so nothing changes for a
subscriber that implements both.
"""

import asyncio
from uuid import uuid4

import pytest

from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.subscriptions import (
    CheckpointStrategy,
    Subscription,
    SubscriptionConfig,
)
from eventsource.application.subscriptions.runners import LiveRunner
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion

_REGISTRY = EventRegistry()

BOUND = 5.0


@register_event(registry=_REGISTRY)
class LiveBatchEvent(DomainEvent):
    """Simple test event for live batch-only dispatch tests."""

    aggregate_type: str = "LiveBatchAggregate"
    data: str = "test"


class BatchOnlySubscriber:
    """Implements exactly `BatchSubscriber` -- no `handle()` at all.

    Deliberately does not define `handle`, because defining it (even to
    raise) would not reproduce the defect: the bug was an `AttributeError`
    from the attribute being absent.
    """

    def __init__(self) -> None:
        self.batch_calls: list[list[DomainEvent]] = []
        self.saw_event = asyncio.Event()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveBatchEvent]

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.batch_calls.append(list(events))
        self.saw_event.set()


class BothSubscriber:
    """Implements both handlers -- the control. `handle()` must still win on
    the live path, so this fix changes nothing for existing subscribers."""

    def __init__(self) -> None:
        self.single_calls: list[DomainEvent] = []
        self.batch_calls: list[list[DomainEvent]] = []
        self.saw_event = asyncio.Event()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [LiveBatchEvent]

    async def handle(self, event: DomainEvent) -> None:
        self.single_calls.append(event)
        self.saw_event.set()

    async def handle_batch(self, events: list[DomainEvent]) -> None:
        self.batch_calls.append(list(events))
        self.saw_event.set()


@pytest.fixture
def event_store() -> InMemoryEventStore:
    return InMemoryEventStore(event_registry=_REGISTRY)


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    return InMemoryCheckpointRepository(enable_tracing=False)


@pytest.fixture
def event_bus() -> InMemoryEventBus:
    return InMemoryEventBus()


async def add_event(store: InMemoryEventStore, data: str) -> None:
    aggregate_id = uuid4()
    await store.append(
        StreamId(aggregate_id=aggregate_id, category="LiveBatchAggregate"),
        [LiveBatchEvent(aggregate_id=aggregate_id, data=data)],
        ExpectedVersion.no_stream(),
    )


def make_config() -> SubscriptionConfig:
    return SubscriptionConfig(
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        continue_on_error=True,
    )


class TestBatchOnlySubscriberOnLivePath:
    async def test_batch_only_subscriber_receives_live_events(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """A subscriber implementing exactly `BatchSubscriber` is delivered to
        on the live path, rather than raising AttributeError per event."""
        subscriber = BatchOnlySubscriber()
        subscription = Subscription(
            name="batch-only-live", config=make_config(), subscriber=subscriber
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()
        try:
            await add_event(event_store, "e0")
            await event_bus.publish([LiveBatchEvent(aggregate_id=uuid4(), data="__wake__")])
            await asyncio.wait_for(subscriber.saw_event.wait(), timeout=BOUND)
        finally:
            await runner.stop()

        assert len(subscriber.batch_calls) == 1
        assert len(subscriber.batch_calls[0]) == 1
        assert subscriber.batch_calls[0][0].data == "e0"

    async def test_batch_only_subscriber_records_no_failures(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """The delivery is a success, not a swallowed handler failure. This is
        the assertion that separates a real fix from one that merely stops the
        exception escaping: `events_failed` stayed at zero only if the handler
        was actually reached."""
        subscriber = BatchOnlySubscriber()
        subscription = Subscription(
            name="batch-only-stats", config=make_config(), subscriber=subscriber
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()
        try:
            await add_event(event_store, "e0")
            await event_bus.publish([LiveBatchEvent(aggregate_id=uuid4(), data="__wake__")])
            await asyncio.wait_for(subscriber.saw_event.wait(), timeout=BOUND)
        finally:
            await runner.stop()

        assert runner.stats.events_failed == 0
        assert runner.stats.events_processed == 1

    async def test_subscriber_with_both_handlers_still_uses_handle(
        self,
        event_bus: InMemoryEventBus,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """`handle()` still wins on the live path when present, so this fix is
        invisible to every subscriber that already worked. Live batching is a
        separate, still-open decision (#34)."""
        subscriber = BothSubscriber()
        subscription = Subscription(name="both-live", config=make_config(), subscriber=subscriber)
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )
        await runner.start()
        try:
            await add_event(event_store, "e0")
            await event_bus.publish([LiveBatchEvent(aggregate_id=uuid4(), data="__wake__")])
            await asyncio.wait_for(subscriber.saw_event.wait(), timeout=BOUND)
        finally:
            await runner.stop()

        assert len(subscriber.single_calls) == 1
        assert subscriber.batch_calls == []
