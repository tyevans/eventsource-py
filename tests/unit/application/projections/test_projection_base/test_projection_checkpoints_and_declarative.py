"""Unit tests for CheckpointTrackingProjection, DeclarativeProjection, and LagMetrics."""

import logging
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
)
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerSignatureError
from tests.unit.application.projections.test_projection_base._fixtures import (
    BrokenCheckpoints,
    CountingProjection,
    FailingDeclarativeProjection,
    FailingProjection,
    MultiHandlerDeclarativeProjection,
    MyCustomProjection,
    OrderCreated,
    OrderShipped,
    PublicHandlerDeclarativeProjection,
    SimpleDeclarativeProjection,
    TestProjection,
    TwoParamDeclarativeProjection,
)


class TestCheckpointTrackingProjection:
    """Tests for CheckpointTrackingProjection."""

    def test_cannot_instantiate_without_subscribed_to(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """CheckpointTrackingProjection requires subscribed_to implementation."""

        class IncompleteProjection(CheckpointTrackingProjection):
            async def _process_event(self, event: DomainEvent) -> None:
                pass

        with pytest.raises(TypeError, match="abstract"):
            IncompleteProjection(checkpoint_repo=checkpoint_repo)  # type: ignore[abstract]

    @pytest.mark.asyncio
    async def test_successful_event_processing_updates_checkpoint(
        self, checkpoint_repo: InMemoryCheckpointRepository, dlq_repo: InMemoryDLQRepository
    ) -> None:
        """Successful event processing updates checkpoint."""
        projection = TestProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)
        assert len(projection.events_processed) == 1
        assert await checkpoint_repo.get_checkpoint("TestProjection") == event.event_id

    @pytest.mark.asyncio
    async def test_failed_event_retries_with_backoff(
        self, checkpoint_repo: InMemoryCheckpointRepository, dlq_repo: InMemoryDLQRepository
    ) -> None:
        """Failed event processing retries with exponential backoff."""
        projection = FailingProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        with pytest.raises(ValueError, match="Processing failed"):
            await projection.handle(event)
        assert projection.attempt_count == 3

    @pytest.mark.asyncio
    async def test_failed_event_goes_to_dlq(
        self, checkpoint_repo: InMemoryCheckpointRepository, dlq_repo: InMemoryDLQRepository
    ) -> None:
        """Event goes to DLQ after all retries exhausted."""
        projection = FailingProjection(
            checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo, max_retries=2
        )
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        with pytest.raises(ValueError):
            await projection.handle(event)
        failed_events = await dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event.event_id
        assert failed_events[0].projection_name == "FailingProjection"

    @pytest.mark.asyncio
    async def test_checkpoint_failure_does_not_rerun_the_handler_or_dlq_the_event(
        self, dlq_repo: InMemoryDLQRepository
    ) -> None:
        """Checkpoint store outage is a liveness problem, not a poison event."""
        projection = CountingProjection(checkpoint_repo=BrokenCheckpoints(), dlq_repo=dlq_repo)
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        with pytest.raises(ConnectionError):
            await projection.handle(event)
        assert projection.applied == [event.event_id], "the handler must run exactly once"
        assert await dlq_repo.get_failed_events() == []

    @pytest.mark.asyncio
    async def test_failed_event_reraises_without_dlq_repo(
        self, checkpoint_repo: InMemoryCheckpointRepository, caplog: pytest.LogCaptureFixture
    ) -> None:
        """With no dlq_repo configured, the original exception still re-raises."""
        projection = FailingProjection(
            checkpoint_repo=checkpoint_repo, dlq_repo=None, max_retries=2
        )
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        with (
            caplog.at_level(logging.CRITICAL),
            pytest.raises(ValueError, match="Processing failed"),
        ):
            await projection.handle(event)
        critical_records = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(critical_records) == 1
        assert "NO DLQ entry was recorded" in critical_records[0].message
        assert "sent to DLQ" not in critical_records[0].message

    @pytest.mark.asyncio
    async def test_failed_event_logs_no_entry_when_dlq_write_fails(
        self, checkpoint_repo: InMemoryCheckpointRepository, caplog: pytest.LogCaptureFixture
    ) -> None:
        """When DLQ write fails, critical log states no entry was recorded."""
        failing_dlq_repo = AsyncMock()
        failing_dlq_repo.add_failed_event.side_effect = RuntimeError("DLQ unavailable")
        projection = FailingProjection(
            checkpoint_repo=checkpoint_repo, dlq_repo=failing_dlq_repo, max_retries=2
        )
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        with (
            caplog.at_level(logging.CRITICAL),
            pytest.raises(ValueError, match="Processing failed"),
        ):
            await projection.handle(event)
        critical_records = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        no_entry_records = [r for r in critical_records if "NO DLQ entry was recorded" in r.message]
        assert len(no_entry_records) == 1
        assert "sent to DLQ" not in no_entry_records[0].message

    @pytest.mark.asyncio
    async def test_get_checkpoint_returns_last_processed(
        self, checkpoint_repo: InMemoryCheckpointRepository, dlq_repo: InMemoryDLQRepository
    ) -> None:
        """get_checkpoint returns last processed event ID."""
        projection = TestProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        assert await projection.get_checkpoint() is None
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)
        assert await projection.get_checkpoint() == str(event.event_id)

    @pytest.mark.asyncio
    async def test_reset_clears_checkpoint(
        self, checkpoint_repo: InMemoryCheckpointRepository, dlq_repo: InMemoryDLQRepository
    ) -> None:
        """reset clears checkpoint."""
        projection = TestProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)
        assert await projection.get_checkpoint() is not None
        await projection.reset()
        assert await projection.get_checkpoint() is None

    @pytest.mark.asyncio
    async def test_projection_name_defaults_to_class_name(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """projection_name defaults to class name."""
        projection = MyCustomProjection(checkpoint_repo=checkpoint_repo)
        assert projection.projection_name == "MyCustomProjection"

    @pytest.mark.asyncio
    async def test_bare_projection_disables_checkpoint_tracking(self) -> None:
        """No checkpoint_repo means checkpoint tracking is disabled entirely."""
        projection = TestProjection()
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert await projection.get_checkpoint() is None


class TestDeclarativeProjection:
    """Tests for DeclarativeProjection with @handles decorator."""

    @pytest.mark.asyncio
    async def test_handles_decorator_registers_handler(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """@handles decorator registers handler method."""
        projection = SimpleDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        assert OrderCreated in projection.subscribed_to()
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(projection.events_handled) == 1

    @pytest.mark.asyncio
    async def test_multiple_handlers(self, checkpoint_repo: InMemoryCheckpointRepository) -> None:
        """Multiple @handles decorators work correctly."""
        projection = MultiHandlerDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        assert len(projection.subscribed_to()) == 2
        assert OrderCreated in projection.subscribed_to()
        assert OrderShipped in projection.subscribed_to()
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        await projection.handle(OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001"))
        assert len(projection.created_events) == 1
        assert len(projection.shipped_events) == 1

    @pytest.mark.asyncio
    async def test_handler_with_two_params(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """Handler with conn and event parameters works."""
        projection = TwoParamDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(projection.events_handled) == 1
        assert projection.conn_values[0] is None

    def test_non_async_handler_raises_error(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """Non-async handler raises ValueError."""
        with pytest.raises(ValueError, match="must be async"):

            class BadProjection(DeclarativeProjection):
                @handles(OrderCreated)
                def _handle_order_created(self, event: OrderCreated) -> None:
                    pass

            BadProjection(checkpoint_repo=checkpoint_repo)

    def test_handler_with_wrong_param_count_raises_error(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """Handler with wrong parameter count raises HandlerSignatureError."""
        with pytest.raises(HandlerSignatureError, match="1 or 2 parameters"):

            class BadProjection(DeclarativeProjection):
                @handles(OrderCreated)
                async def _handle_order_created(self, a, b, c: OrderCreated) -> None:
                    pass

            BadProjection(checkpoint_repo=checkpoint_repo)

    @pytest.mark.asyncio
    async def test_unhandled_event_ignored_by_default(
        self, checkpoint_repo: InMemoryCheckpointRepository, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Unhandled event type is silently ignored by default."""
        projection = SimpleDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        with caplog.at_level(logging.WARNING):
            await projection.handle(OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001"))
        assert not any("No handler registered" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_public_handler_method_discovered(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """Public handler methods (without underscore) are discovered."""
        projection = PublicHandlerDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        assert OrderCreated in projection.subscribed_to()
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(projection.events_handled) == 1


class TestDeclarativeProjectionWithDLQ:
    """Tests for DeclarativeProjection error handling and DLQ."""

    @pytest.mark.asyncio
    async def test_failing_handler_goes_to_dlq(
        self, checkpoint_repo: InMemoryCheckpointRepository, dlq_repo: InMemoryDLQRepository
    ) -> None:
        """Failing handler sends event to DLQ."""

        class FailingProjection(FailingDeclarativeProjection):
            pass

        projection = FailingProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        with pytest.raises(ValueError):
            await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        failed_events = await dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert "Handler error" in failed_events[0].error_message


class TestLagMetrics:
    """Tests for projection lag metrics."""

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_none_initially(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """get_lag_metrics returns None when no checkpoint exists."""
        projection = TestProjection(checkpoint_repo=checkpoint_repo)
        assert await projection.get_lag_metrics() is None

    @pytest.mark.asyncio
    async def test_get_lag_metrics_returns_data_after_processing(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        """get_lag_metrics returns data after processing events."""
        projection = TestProjection(checkpoint_repo=checkpoint_repo)
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        metrics = await projection.get_lag_metrics()
        assert metrics is not None
        assert metrics["projection_name"] == "TestProjection"
        assert metrics["events_processed"] == 1
