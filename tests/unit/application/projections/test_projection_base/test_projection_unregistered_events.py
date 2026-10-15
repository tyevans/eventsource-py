"""Unit tests for unregistered event handling in DeclarativeProjection and DatabaseProjection."""

import logging
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.application.projections.base import DeclarativeProjection
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import UnhandledEventError
from tests.unit.application.projections.test_projection_base._fixtures import (
    OrderCancelled,
    OrderCreated,
    OrderShipped,
)


class TestProjectionUnregisteredEventHandling:
    """Tests for configurable unregistered event handling in projections (TD-012)."""

    def test_default_mode_is_ignore(self, checkpoint_repo: InMemoryCheckpointRepository) -> None:
        class TestProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = TestProjection(checkpoint_repo=checkpoint_repo)
        assert projection.unregistered_event_handling == "ignore"

    @pytest.mark.asyncio
    async def test_ignore_mode_silently_accepts_unknown_events(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        class IgnoreProjection(DeclarativeProjection):
            unregistered_event_handling = "ignore"

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = IgnoreProjection(checkpoint_repo=checkpoint_repo)
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
        await projection.handle(event)
        assert await projection.get_checkpoint() == str(event.event_id)

    @pytest.mark.asyncio
    async def test_error_mode_raises_unhandled_event_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        class StrictProjection(DeclarativeProjection):
            unregistered_event_handling = "error"
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = StrictProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with pytest.raises(UnhandledEventError) as exc_info:
            await projection.handle(event)

        assert "OrderShipped" in str(exc_info.value)
        assert "StrictProjection" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_warn_mode_logs_warning(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        class WarnProjection(DeclarativeProjection):
            unregistered_event_handling = "warn"

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = WarnProjection(checkpoint_repo=checkpoint_repo)
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with caplog.at_level(logging.WARNING):
            await projection.handle(event)

        assert "No handler registered" in caplog.text
        assert "OrderShipped" in caplog.text
        assert await projection.get_checkpoint() == str(event.event_id)

    @pytest.mark.asyncio
    async def test_error_message_includes_available_handlers(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
    ) -> None:
        class MultiHandlerProjection(DeclarativeProjection):
            unregistered_event_handling = "error"
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

            @handles(OrderCancelled)
            async def _handle_order_cancelled(self, event: OrderCancelled) -> None:
                pass

        projection = MultiHandlerProjection(checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo)
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with pytest.raises(UnhandledEventError) as exc_info:
            await projection.handle(event)

        error = exc_info.value
        assert "OrderCreated" in error.available_handlers
        assert "OrderCancelled" in error.available_handlers
        assert error.event_type == "OrderShipped"
        assert error.handler_class == "MultiHandlerProjection"

    @pytest.mark.asyncio
    async def test_handled_event_is_not_affected_by_mode(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        events_handled: list[DomainEvent] = []

        class StrictProjection(DeclarativeProjection):
            unregistered_event_handling = "error"

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                events_handled.append(event)

        projection = StrictProjection(checkpoint_repo=checkpoint_repo)
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(events_handled) == 1

    def test_subclass_can_override_handling_mode(
        self, checkpoint_repo: InMemoryCheckpointRepository
    ) -> None:
        class BaseProjection(DeclarativeProjection):
            unregistered_event_handling = "ignore"

            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        class StrictSubclass(BaseProjection):
            unregistered_event_handling = "error"

        assert (
            BaseProjection(checkpoint_repo=checkpoint_repo).unregistered_event_handling == "ignore"
        )
        assert (
            StrictSubclass(checkpoint_repo=checkpoint_repo).unregistered_event_handling == "error"
        )

    @pytest.mark.asyncio
    async def test_backwards_compatibility_existing_warning_behavior(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        class LegacyProjection(DeclarativeProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                pass

        projection = LegacyProjection(checkpoint_repo=checkpoint_repo)
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with caplog.at_level(logging.WARNING):
            await projection.handle(event)

        assert "No handler registered" not in caplog.text


class TestDatabaseProjectionUnregisteredEventHandling:
    """Tests for unregistered event handling in DatabaseProjection."""

    @pytest.mark.asyncio
    async def test_database_projection_error_mode(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
        mock_session_factory,
    ) -> None:
        factory, _, _ = mock_session_factory

        class StrictDBProjection(DatabaseProjection):
            unregistered_event_handling = "error"
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = StrictDBProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo
        )
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

        with pytest.raises(UnhandledEventError) as exc_info:
            await projection.handle(event)

        assert "OrderShipped" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_database_projection_ignore_mode(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
    ) -> None:
        factory, _, _ = mock_session_factory

        class IgnoreDBProjection(DatabaseProjection):
            unregistered_event_handling = "ignore"

            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = IgnoreDBProjection(session_factory=factory, checkpoint_repo=checkpoint_repo)
        event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
        await projection.handle(event)
        assert await projection.get_checkpoint() == str(event.event_id)
