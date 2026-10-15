"""Unit tests for ReadModelProjection runtime errors, checkpoints, cleanup, events, tracing, and imports."""

import logging
from unittest.mock import patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.domain.decorators import handles
from eventsource.domain.exceptions import UnhandledEventError
from tests.unit.readmodels.test_projection._fixtures import (
    OrderCreated,
    OrderShipped,
    OrderSummary,
)

_PATCH_TARGET = "eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"


class _NoopProjection(ReadModelProjection[OrderSummary]):
    @handles(OrderCreated)
    async def _handle_order_created(self, repo, event: OrderCreated) -> None:
        pass


class _FailingProjection(ReadModelProjection[OrderSummary]):
    MAX_RETRIES = 1
    RETRY_BACKOFF_BASE = 0

    @handles(OrderCreated)
    async def _handle_order_created(self, repo, event: OrderCreated) -> None:
        raise ValueError("Handler error")


class TestRuntimeErrors:
    @pytest.mark.asyncio
    async def test_runtime_error_if_process_event_called_directly(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        factory, _, _ = mock_postgresql_session_factory
        projection = _NoopProjection(
            session_factory=factory, model_class=OrderSummary, checkpoint_repo=checkpoint_repo
        )
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        with pytest.raises(RuntimeError, match="requires repository"):
            await projection._process_event(event)


class TestCheckpointBehavior:
    @pytest.mark.asyncio
    async def test_checkpoint_updated_on_success(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(_NoopProjection):
            pass

        projection = TestProjection(
            session_factory=factory, model_class=OrderSummary, checkpoint_repo=checkpoint_repo
        )
        with patch(_PATCH_TARGET):
            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            await projection.handle(event)
            checkpoint = await checkpoint_repo.get_checkpoint("TestProjection")
            assert checkpoint == event.event_id

    @pytest.mark.asyncio
    async def test_checkpoint_not_updated_on_error(
        self, mock_postgresql_session_factory, checkpoint_repo, dlq_repo
    ) -> None:
        factory, _, _ = mock_postgresql_session_factory

        class FailingProjection(_FailingProjection):
            pass

        projection = FailingProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )
        with patch(_PATCH_TARGET):
            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            with pytest.raises(ValueError, match="Handler error"):
                await projection.handle(event)
            checkpoint = await checkpoint_repo.get_checkpoint("FailingProjection")
            assert checkpoint is None


class TestRepositoryCleanup:
    @pytest.mark.asyncio
    async def test_repository_is_none_after_handle_completes(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        factory, _, _ = mock_postgresql_session_factory
        projection = _NoopProjection(
            session_factory=factory, model_class=OrderSummary, checkpoint_repo=checkpoint_repo
        )
        assert projection._current_repository is None
        with patch(_PATCH_TARGET):
            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            await projection.handle(event)
        assert projection._current_repository is None

    @pytest.mark.asyncio
    async def test_repository_cleared_even_on_error(
        self, mock_postgresql_session_factory, checkpoint_repo, dlq_repo
    ) -> None:
        factory, _, _ = mock_postgresql_session_factory
        projection = _FailingProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )
        with patch(_PATCH_TARGET):
            event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            with pytest.raises(ValueError):
                await projection.handle(event)
        assert projection._current_repository is None


class TestUnregisteredEventHandling:
    @pytest.mark.asyncio
    async def test_unhandled_event_ignored_by_default(
        self, mock_postgresql_session_factory, checkpoint_repo, caplog
    ) -> None:
        factory, _, _ = mock_postgresql_session_factory
        projection = _NoopProjection(
            session_factory=factory, model_class=OrderSummary, checkpoint_repo=checkpoint_repo
        )
        with patch(_PATCH_TARGET):
            event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
            with caplog.at_level(logging.WARNING):
                await projection.handle(event)
            assert not any("No handler registered" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_error_mode_raises_for_unhandled_event(
        self, mock_postgresql_session_factory, checkpoint_repo, dlq_repo
    ) -> None:
        factory, _, _ = mock_postgresql_session_factory

        class StrictProjection(_NoopProjection):
            unregistered_event_handling = "error"
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

        projection = StrictProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )
        with patch(_PATCH_TARGET):
            event = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")
            with pytest.raises(UnhandledEventError) as exc_info:
                await projection.handle(event)
            assert "OrderShipped" in str(exc_info.value)


class TestTracingConfiguration:
    def test_tracing_disabled_by_default(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        factory, _, _ = mock_postgresql_session_factory
        projection = _NoopProjection(
            session_factory=factory, model_class=OrderSummary, checkpoint_repo=checkpoint_repo
        )
        assert projection._enable_tracing is False

    def test_tracing_can_be_enabled(self, mock_postgresql_session_factory, checkpoint_repo) -> None:
        factory, _, _ = mock_postgresql_session_factory
        projection = _NoopProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=True,
        )
        assert projection._enable_tracing is True


class TestImports:
    def test_can_import_from_readmodels_module(self) -> None:
        from eventsource.adapters.sql.readmodel_projection import (
            ReadModelProjection as RMProjection,
        )

        assert RMProjection is not None

    def test_is_in_readmodels_all(self) -> None:
        import eventsource

        assert "ReadModelProjection" in eventsource.__all__
