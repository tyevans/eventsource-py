"""Unit tests for buffer draining and lag reconciliation on subscription transition failure."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventBus, InMemoryEventStore
from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.application.subscriptions.config import create_catch_up_config
from eventsource.application.subscriptions.runners.live import LiveRunner
from eventsource.application.subscriptions.subscription import Subscription
from eventsource.application.subscriptions.transition import (
    TransitionCoordinator,
    TransitionPhase,
)
from eventsource.domain import StreamId, register_event
from eventsource.domain.event import DomainEvent
from eventsource.ports.positions import ExpectedVersion, Position


@register_event
class TransitionRecoveryEvent(DomainEvent):
    aggregate_id: Any = uuid4()
    aggregate_type: str = "TestAggregate"
    event_type: str = "TransitionRecoveryEvent"
    sequence: int = 0


class MockSubscriber:
    """Subscriber that implements the Subscriber protocol."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.received: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [TransitionRecoveryEvent]

    async def handle(self, event: DomainEvent) -> None:
        if self.fail:
            raise RuntimeError("Simulated subscriber failure")
        self.received.append(event)


class TestTransitionRecovery:
    @pytest.mark.asyncio
    async def test_transition_failure_clears_buffered_events_and_reconciles_lag(self) -> None:
        """Transition failure during catchup clears buffered live events and reconciles lag."""
        event_store, event_bus = InMemoryEventStore(), InMemoryEventBus()
        checkpoint_repo, subscriber = InMemoryCheckpointRepository(), MockSubscriber()

        evt1 = TransitionRecoveryEvent(sequence=1)
        await event_store.append(
            StreamId(aggregate_id=evt1.aggregate_id, category=evt1.aggregate_type),
            [evt1],
            ExpectedVersion.no_stream(),
        )

        subscription = Subscription(
            name="test-sub-recovery", subscriber=subscriber, config=create_catch_up_config()
        )
        coordinator = TransitionCoordinator(
            event_store=event_store,
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            subscription=subscription,
        )

        async def failing_read_all(*args: Any, **kwargs: Any) -> Any:
            if False:
                yield
            raise RuntimeError("Simulated catchup storage failure")

        event_store.read_all = failing_read_all  # type: ignore[assignment]
        result = await coordinator.execute()

        assert result.success is False
        assert result.phase_reached == TransitionPhase.FAILED
        assert result.error is not None

        live_runner = coordinator.live_runner
        assert live_runner is not None
        assert live_runner.buffer_size == 0
        assert live_runner.is_running is False
        assert subscription.lag == 0

    @pytest.mark.asyncio
    async def test_pause_failure_recovery_clears_pause_buffer_and_reconciles_lag(self) -> None:
        """Pause-resume failure clears pause buffer and reconciles lag."""
        event_store, event_bus = InMemoryEventStore(), InMemoryEventBus()
        checkpoint_repo, subscriber = InMemoryCheckpointRepository(), MockSubscriber()
        subscription = Subscription(
            name="test-pause-recovery", subscriber=subscriber, config=create_catch_up_config()
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )

        await runner.start(buffer_events=False)
        await subscription.pause()

        await event_bus.publish([TransitionRecoveryEvent(sequence=1)])
        await event_bus.publish([TransitionRecoveryEvent(sequence=2)])
        assert runner.pause_buffer_size == 2

        await subscription.record_events_seen(2)
        assert subscription.lag == 2

        dropped = await runner.clear_buffer()
        assert dropped == 2
        assert runner.pause_buffer_size == 0
        assert runner.events_buffered_during_pause == 0
        assert subscription.lag == 0
        await runner.stop()

    @pytest.mark.asyncio
    async def test_live_runner_clear_buffer_on_stop(self) -> None:
        """LiveRunner.stop() clears buffered wakes and reconciles lag."""
        event_store, event_bus = InMemoryEventStore(), InMemoryEventBus()
        checkpoint_repo, subscriber = InMemoryCheckpointRepository(), MockSubscriber()
        subscription = Subscription(
            name="test-live-clear-stop", subscriber=subscriber, config=create_catch_up_config()
        )
        runner = LiveRunner(
            event_bus=event_bus,
            checkpoint_repo=checkpoint_repo,
            event_feed=event_store,
            subscription=subscription,
        )

        await runner.start(buffer_events=True)
        assert runner.buffer_size == 0

        await event_bus.publish([TransitionRecoveryEvent(sequence=10)])
        await event_bus.publish([TransitionRecoveryEvent(sequence=11)])
        assert runner.buffer_size == 2

        await subscription.record_events_seen(2)
        assert subscription.lag == 2

        await runner.stop()
        assert runner.buffer_size == 0
        assert runner.is_running is False
        assert subscription.lag == 0

    @pytest.mark.asyncio
    async def test_reconcile_lag_with_target_lag(self) -> None:
        """Subscription.reconcile_lag resets seen counter accurately."""
        subscription = Subscription(
            name="test-reconcile-counter",
            subscriber=MockSubscriber(),
            config=create_catch_up_config(),
        )

        await subscription.record_events_seen(10)
        for i in range(5):
            await subscription.record_event_processed(
                position=Position(store_id="memory", key=(i + 1,)),
                event_id=uuid4(),
                event_type="TransitionRecoveryEvent",
            )
        assert subscription.lag == 5

        await subscription.reconcile_lag(0)
        assert subscription.lag == 0

        await subscription.reconcile_lag(2)
        assert subscription.lag == 2
