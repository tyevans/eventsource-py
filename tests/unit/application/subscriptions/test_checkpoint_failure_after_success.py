"""A checkpoint failure after a successful handler call is a checkpoint error.

Contract (LiveRunner, matching CatchUpRunner): a handler success is a success.
When the checkpoint save that follows raises, the event is not counted as
failed, the error handler (and so the DLQ) is not invoked for it, and the
checkpoint failure is logged with error_summary before it propagates.
"""

import asyncio
import logging
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
from eventsource.application.subscriptions.error_handling import SubscriptionErrorHandler
from eventsource.application.subscriptions.runners import CatchUpRunner, LiveRunner
from eventsource.observability.error_summary import error_summary
from eventsource.ports.positions import Position
from tests.unit.application.subscriptions.test_live_runner import _fixtures as _live
from tests.unit.application.subscriptions.test_live_runner._fixtures import (
    LiveTestEvent,
    MockLiveSubscriber,
    append_event,
    wake,
)

# Re-export the live-runner fixtures under their own names for this module.
event_bus = _live.event_bus
event_store = _live.event_store


class _FailingCheckpointRepo(InMemoryCheckpointRepository):
    """Checkpoint store whose every save raises a non-transient error."""

    def __init__(self) -> None:
        super().__init__(enable_tracing=False)
        self.save_error = RuntimeError("checkpoint store unavailable")
        self.save_attempts = 0

    async def save_position(
        self,
        subscription_id: str,
        position: Position,
        event_id: object,
        event_type: str,
    ) -> None:
        self.save_attempts += 1
        raise self.save_error


class _SpyErrorHandler(SubscriptionErrorHandler):
    """Error handler that records every error dispatched to it (the DLQ path)."""

    def __init__(self, subscription_name: str) -> None:
        super().__init__(subscription_name)
        self.dispatched: list[Exception] = []

    async def handle_error(self, error, envelope=None, event=None, retry_count=0):  # type: ignore[override]
        self.dispatched.append(error)
        return await super().handle_error(
            error, envelope=envelope, event=event, retry_count=retry_count
        )


def _checkpoint_error_records(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [r for r in caplog.records if r.levelno >= logging.ERROR and "checkpoint save failed" in r.getMessage().lower()]  # fmt: skip


@pytest.mark.asyncio
@pytest.mark.parametrize("continue_on_error", [True, False])
async def test_live_checkpoint_failure_after_success_is_not_an_event_failure(
    event_bus: InMemoryEventBus,
    event_store: InMemoryEventStore,
    caplog: pytest.LogCaptureFixture,
    continue_on_error: bool,
) -> None:
    subscriber = MockLiveSubscriber()
    repo = _FailingCheckpointRepo()
    errors = _SpyErrorHandler("CheckpointFailLive")
    config = SubscriptionConfig(
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
        continue_on_error=continue_on_error,
    )
    subscription = Subscription(name="CheckpointFailLive", config=config, subscriber=subscriber)
    runner = LiveRunner(
        event_bus=event_bus,
        checkpoint_repo=repo,
        event_feed=event_store,
        subscription=subscription,
        error_handler=errors,
    )
    await runner.start()

    await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data="handled"))
    with caplog.at_level(logging.ERROR):
        await wake(event_bus)
        await asyncio.sleep(0.01)

    assert len(subscriber.handled_events) == 1
    assert repo.save_attempts >= 1
    assert runner.stats.events_processed == 1
    assert runner.stats.events_failed == 0
    assert subscription.events_failed == 0
    assert errors.dispatched == []
    records = _checkpoint_error_records(caplog)
    assert records, "checkpoint failure must be logged as a checkpoint error"
    assert records[0].__dict__["error"] == error_summary(repo.save_error)
    # The failure surfaces to the bus as a handler error instead of vanishing.
    assert event_bus.get_stats()["handler_errors"] == 1


@pytest.mark.asyncio
async def test_catchup_checkpoint_failure_after_success_is_not_an_event_failure(
    event_store: InMemoryEventStore,
) -> None:
    subscriber = MockLiveSubscriber()
    repo = _FailingCheckpointRepo()
    errors = _SpyErrorHandler("CheckpointFailCatchUp")
    config = SubscriptionConfig(checkpoint_strategy=CheckpointStrategy.EVERY_EVENT)
    subscription = Subscription(name="CheckpointFailCatchUp", config=config, subscriber=subscriber)
    target = await append_event(event_store, LiveTestEvent(aggregate_id=uuid4(), data="handled"))
    runner = CatchUpRunner(
        event_store, repo, subscription, error_handler=errors, enable_tracing=False
    )

    result = await runner.run_until_position(target)

    assert len(subscriber.handled_events) == 1
    assert result.error is repo.save_error
    assert subscription.events_failed == 0
    assert errors.dispatched == []
