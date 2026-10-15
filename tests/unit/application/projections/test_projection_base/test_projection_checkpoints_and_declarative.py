"""Unit tests for CheckpointTrackingProjection, DeclarativeProjection, and LagMetrics."""

import logging
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.application.projections.base import (
    CheckpointTrackingProjection,
    DeclarativeProjection,
)
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerSignatureError
from tests.unit.application.projections.test_projection_base import _fixtures as fx


def _order(num: str = "ORD-001") -> fx.OrderCreated:
    return fx.OrderCreated(aggregate_id=uuid4(), order_number=num)


def _shipped(trk: str = "TRK-001") -> fx.OrderShipped:
    return fx.OrderShipped(aggregate_id=uuid4(), tracking_number=trk)


class TestCheckpointTrackingProjection:
    def test_cannot_instantiate_without_subscribed_to(self, checkpoint_repo) -> None:
        class IncompleteProjection(CheckpointTrackingProjection):
            async def _process_event(self, event: DomainEvent) -> None:
                pass

        with pytest.raises(TypeError, match="abstract"):
            IncompleteProjection(checkpoint_repo=checkpoint_repo)  # type: ignore[abstract]

    async def test_successful_event_processing_updates_checkpoint(
        self, checkpoint_repo, dlq_repo
    ) -> None:
        projection = fx.TestProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        event = _order()
        await projection.handle(event)
        assert len(projection.events_processed) == 1
        assert await checkpoint_repo.get_checkpoint("TestProjection") == event.event_id

    async def test_failed_event_retries_with_backoff(self, checkpoint_repo, dlq_repo) -> None:
        projection = fx.FailingProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        with pytest.raises(ValueError, match="Processing failed"):
            await projection.handle(_order())
        assert projection.attempt_count == 3

    async def test_failed_event_goes_to_dlq(self, checkpoint_repo, dlq_repo) -> None:
        projection = fx.FailingProjection(checkpoint_repo, dlq_repo, max_retries=2)
        event = _order()
        with pytest.raises(ValueError):
            await projection.handle(event)
        failed = await dlq_repo.get_failed_events()
        assert len(failed) == 1 and failed[0].event_id == event.event_id
        assert failed[0].projection_name == "FailingProjection"

    async def test_checkpoint_failure_does_not_rerun_the_handler_or_dlq_the_event(
        self, dlq_repo
    ) -> None:
        projection = fx.CountingProjection(
            checkpoint_repo=fx.BrokenCheckpoints(), dlq_repo=dlq_repo
        )
        event = _order()
        with pytest.raises(ConnectionError):
            await projection.handle(event)
        assert projection.applied == [event.event_id], "the handler must run exactly once"
        assert await dlq_repo.get_failed_events() == []

    async def test_failed_event_reraises_without_dlq_repo(self, checkpoint_repo, caplog) -> None:
        projection = fx.FailingProjection(checkpoint_repo, None, max_retries=2)
        with caplog.at_level(logging.CRITICAL), pytest.raises(ValueError):
            await projection.handle(_order())
        crit = [r for r in caplog.records if r.levelno == logging.CRITICAL]
        assert len(crit) == 1 and "NO DLQ entry was recorded" in crit[0].message
        assert "sent to DLQ" not in crit[0].message

    async def test_failed_event_logs_no_entry_when_dlq_write_fails(
        self, checkpoint_repo, caplog
    ) -> None:
        failing_dlq_repo = AsyncMock(add_failed_event=AsyncMock(side_effect=RuntimeError("DLQ")))
        projection = fx.FailingProjection(checkpoint_repo, failing_dlq_repo, max_retries=2)
        with caplog.at_level(logging.CRITICAL), pytest.raises(ValueError):
            await projection.handle(_order())
        no_entry = [r for r in caplog.records if "NO DLQ entry was recorded" in r.message]
        assert len(no_entry) == 1 and "sent to DLQ" not in no_entry[0].message

    async def test_get_checkpoint_returns_last_processed(self, checkpoint_repo, dlq_repo) -> None:
        projection = fx.TestProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        assert await projection.get_checkpoint() is None
        event = _order()
        await projection.handle(event)
        assert await projection.get_checkpoint() == str(event.event_id)

    async def test_reset_clears_checkpoint(self, checkpoint_repo, dlq_repo) -> None:
        projection = fx.TestProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        await projection.handle(_order())
        assert await projection.get_checkpoint() is not None
        await projection.reset()
        assert await projection.get_checkpoint() is None

    async def test_projection_name_defaults_to_class_name(self, checkpoint_repo) -> None:
        proj = fx.MyCustomProjection(checkpoint_repo=checkpoint_repo)
        assert proj.projection_name == "MyCustomProjection"

    async def test_bare_projection_disables_checkpoint_tracking(self) -> None:
        projection = fx.TestProjection()
        await projection.handle(_order())
        assert await projection.get_checkpoint() is None


class TestDeclarativeProjection:
    async def test_handles_decorator_registers_handler(self, checkpoint_repo) -> None:
        projection = fx.SimpleDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        assert fx.OrderCreated in projection.subscribed_to()
        await projection.handle(_order())
        assert len(projection.events_handled) == 1

    async def test_multiple_handlers(self, checkpoint_repo) -> None:
        projection = fx.MultiHandlerDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        assert set(projection.subscribed_to()) == {fx.OrderCreated, fx.OrderShipped}
        await projection.handle(_order())
        await projection.handle(_shipped())
        assert len(projection.created_events) == 1 and len(projection.shipped_events) == 1

    async def test_handler_with_two_params(self, checkpoint_repo) -> None:
        projection = fx.TwoParamDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        await projection.handle(_order())
        assert len(projection.events_handled) == 1 and projection.conn_values[0] is None

    def test_non_async_handler_raises_error(self, checkpoint_repo) -> None:
        with pytest.raises(ValueError, match="must be async"):

            class BadProjection(DeclarativeProjection):
                @handles(fx.OrderCreated)
                def _handle_order_created(self, event: fx.OrderCreated) -> None:
                    pass

            BadProjection(checkpoint_repo=checkpoint_repo)

    def test_handler_with_wrong_param_count_raises_error(self, checkpoint_repo) -> None:
        with pytest.raises(HandlerSignatureError, match="1 or 2 parameters"):

            class BadProjection(DeclarativeProjection):
                @handles(fx.OrderCreated)
                async def _handle_order_created(self, a, b, c: fx.OrderCreated) -> None:
                    pass

            BadProjection(checkpoint_repo=checkpoint_repo)

    async def test_unhandled_event_ignored_by_default(self, checkpoint_repo, caplog) -> None:
        projection = fx.SimpleDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        with caplog.at_level(logging.WARNING):
            await projection.handle(_shipped())
        assert not any("No handler registered" in rec.message for rec in caplog.records)

    async def test_public_handler_method_discovered(self, checkpoint_repo) -> None:
        projection = fx.PublicHandlerDeclarativeProjection(checkpoint_repo=checkpoint_repo)
        assert fx.OrderCreated in projection.subscribed_to()
        await projection.handle(_order())
        assert len(projection.events_handled) == 1


class TestDeclarativeProjectionWithDLQ:
    async def test_failing_handler_goes_to_dlq(self, checkpoint_repo, dlq_repo) -> None:
        class FailingDecl(fx.FailingDeclarativeProjection):
            pass

        projection = FailingDecl(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        with pytest.raises(ValueError):
            await projection.handle(_order())
        failed_events = await dlq_repo.get_failed_events()
        assert len(failed_events) == 1 and "Handler error" in failed_events[0].error_message


class TestLagMetrics:
    async def test_get_lag_metrics_returns_none_initially(self, checkpoint_repo) -> None:
        assert await fx.TestProjection(checkpoint_repo=checkpoint_repo).get_lag_metrics() is None

    async def test_get_lag_metrics_returns_data_after_processing(self, checkpoint_repo) -> None:
        projection = fx.TestProjection(checkpoint_repo=checkpoint_repo)
        await projection.handle(_order())
        metrics = await projection.get_lag_metrics()
        assert metrics is not None and metrics["projection_name"] == "TestProjection"
        assert metrics["events_processed"] == 1
