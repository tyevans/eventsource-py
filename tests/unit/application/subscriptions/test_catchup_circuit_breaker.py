"""
Unit tests for the two-breaker circuit-breaker wiring in CatchUpRunner.

Covers task #18: handler failures now feed a `handler_circuit_breaker`
(guarding `handle()`/`handle_batch()`), and checkpoint-save/read-batch
retries feed a separate `infra_circuit_breaker`. The regression these
tests exist to pin: an earlier implementation shared one CircuitBreaker
between both, so a run of handler failures opened the circuit and then
immediately blocked the following checkpoint-save too, crashing the
subscription (reproduced via
tests/unit/application/subscriptions/test_subscription_manager.py::
TestErrorHandling::test_continue_on_error_mode, flaky at ~15%). Each
breaker must open under its own failure domain only.
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
from eventsource.application.subscriptions.retry import CircuitState
from eventsource.application.subscriptions.runners import CatchUpRunner
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.positions import ExpectedVersion, Position

_REGISTRY = EventRegistry()


@register_event(registry=_REGISTRY)
class CircuitBreakerTestEvent(DomainEvent):
    """Simple test event for circuit-breaker tests."""

    aggregate_type: str = "CircuitBreakerAggregate"
    data: str = "test"


class AlwaysFailingSubscriber:
    """A subscriber whose handle() always raises."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [CircuitBreakerTestEvent]

    async def handle(self, event: DomainEvent) -> None:
        raise ValueError("handler is broken")


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
        event = CircuitBreakerTestEvent(aggregate_id=aggregate_id, data=f"event_{i}")
        await store.append(
            StreamId(aggregate_id=aggregate_id, category="CircuitBreakerAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )
        events.append(event)
    return events


async def current(store: InMemoryEventStore) -> Position:
    position = await store.current_position()
    assert position is not None
    return position


class TestHandlerFailuresDoNotBlockCheckpointing:
    """The regression this wiring exists to fix."""

    async def test_handler_breaker_opens_but_checkpoint_still_succeeds(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """5 consecutive handler failures (default threshold) open the
        handler breaker. The EVERY_BATCH checkpoint-save that follows must
        still succeed -- it is guarded by a separate breaker that never saw
        a failure."""
        subscriber = AlwaysFailingSubscriber()
        config = SubscriptionConfig(
            batch_size=10,
            checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
            continue_on_error=True,
        )
        subscription = Subscription(name="cb-sub", config=config, subscriber=subscriber)
        await add_events(event_store, 5)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        target = await current(event_store)
        result = await runner.run_until_position(target)

        # Catch-up completed despite every event failing -- the checkpoint
        # save that used to crash the whole run because it shared a breaker
        # with the failing handler now succeeds.
        assert result.completed
        assert result.error is None
        assert subscription.events_failed == 5

        assert runner.handler_circuit_breaker is not None
        assert runner.handler_circuit_breaker.state == CircuitState.OPEN

        # The infra breaker never saw a failure: read-batch and
        # checkpoint-save both succeeded.
        assert runner.infra_circuit_breaker is not None
        assert runner.infra_circuit_breaker.state == CircuitState.CLOSED
        assert runner.infra_circuit_breaker.failure_count == 0

        # And the checkpoint was actually persisted, not silently skipped.
        checkpoint = await checkpoint_repo.get_position(subscription_id="cb-sub")
        assert checkpoint == target

    async def test_continue_on_error_mode_regression(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        """Deterministic replay of the failure found in
        test_subscription_manager.py::TestErrorHandling::test_continue_on_error_mode
        (there exercised through SubscriptionManager end to end; here
        directly through the runner). Run in a loop by the test suite's own
        retries/CI reruns is not needed -- this must pass every time, not
        ~85% of the time."""
        subscriber = AlwaysFailingSubscriber()
        config = SubscriptionConfig(continue_on_error=True)
        subscription = Subscription(name="cb-sub-2", config=config, subscriber=subscriber)
        await add_events(event_store, 5)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert result.error is None
        assert subscription.events_failed == 5


class TestBreakersAreIndependent:
    """Each breaker opens under its own failure domain only."""

    async def test_handler_failures_do_not_open_infra_breaker(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        subscriber = AlwaysFailingSubscriber()
        config = SubscriptionConfig(
            batch_size=10,
            circuit_breaker_failure_threshold=2,
            continue_on_error=True,
        )
        subscription = Subscription(name="cb-sub-3", config=config, subscriber=subscriber)
        await add_events(event_store, 5)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        await runner.run_until_position(await current(event_store))

        assert runner.handler_circuit_breaker is not None
        assert runner.handler_circuit_breaker.state == CircuitState.OPEN
        assert runner.infra_circuit_breaker is not None
        assert runner.infra_circuit_breaker.state == CircuitState.CLOSED

    async def test_circuit_breaker_disabled_leaves_both_breakers_none(
        self,
        event_store: InMemoryEventStore,
        checkpoint_repo: InMemoryCheckpointRepository,
    ) -> None:
        subscriber = AlwaysFailingSubscriber()
        config = SubscriptionConfig(circuit_breaker_enabled=False, continue_on_error=True)
        subscription = Subscription(name="cb-sub-4", config=config, subscriber=subscriber)
        await add_events(event_store, 3)
        runner = CatchUpRunner(event_store, checkpoint_repo, subscription)

        result = await runner.run_until_position(await current(event_store))

        assert result.completed
        assert runner.handler_circuit_breaker is None
        assert runner.infra_circuit_breaker is None
