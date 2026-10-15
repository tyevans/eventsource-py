"""Unit tests for DatabaseProjection with database connection and transaction support."""

import logging
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.domain.decorators import handles
from tests.unit.application.projections.test_projection_base._fixtures import (
    FailingDatabaseProjection,
    MixedDatabaseProjection,
    MultiHandlerDatabaseProjection,
    OrderCreated,
    OrderShipped,
    SimpleDatabaseProjection,
    SingleParamDatabaseProjection,
)


class TestDatabaseProjection:
    """Tests for DatabaseProjection with database connection support."""

    @pytest.mark.asyncio
    async def test_handler_receives_valid_connection(
        self, checkpoint_repo: InMemoryCheckpointRepository, mock_session_factory
    ) -> None:
        factory, _, mock_conn = mock_session_factory
        projection = SimpleDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo
        )
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(projection.events_handled) == 1
        assert projection.received_conn == [mock_conn]

    @pytest.mark.asyncio
    async def test_single_param_handler_still_works(
        self, checkpoint_repo: InMemoryCheckpointRepository, mock_session_factory
    ) -> None:
        factory, _, _ = mock_session_factory
        projection = SingleParamDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo
        )
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)
        assert projection.events_handled == [event]

    @pytest.mark.asyncio
    async def test_checkpoint_updated_on_success(
        self, checkpoint_repo: InMemoryCheckpointRepository, mock_session_factory
    ) -> None:
        factory, _, _ = mock_session_factory

        class TestProjection(DatabaseProjection):
            @handles(OrderCreated)
            async def _handle_order_created(self, conn, event: OrderCreated) -> None:
                pass

        projection = TestProjection(session_factory=factory, checkpoint_repo=checkpoint_repo)
        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
        await projection.handle(event)
        assert await checkpoint_repo.get_checkpoint("TestProjection") == event.event_id

    @pytest.mark.asyncio
    async def test_checkpoint_not_updated_on_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
        mock_session_factory,
    ) -> None:
        factory, _, _ = mock_session_factory

        class FailingProjection(FailingDatabaseProjection):
            pass

        projection = FailingProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo
        )
        with pytest.raises(ValueError, match="Handler error"):
            await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert await checkpoint_repo.get_checkpoint("FailingProjection") is None

    @pytest.mark.asyncio
    async def test_transaction_rollback_on_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
        mock_session_factory,
    ) -> None:
        factory, session, _ = mock_session_factory
        exit_called: list[bool] = []
        original_aexit = session.begin.return_value.__aexit__

        async def capture_aexit(*args):
            exit_called.append(True)
            return False if args[0] is not None else await original_aexit(*args)

        session.begin.return_value.__aexit__ = capture_aexit
        projection = FailingDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo
        )
        with pytest.raises(ValueError):
            await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert len(exit_called) >= 1

    @pytest.mark.asyncio
    async def test_multiple_handlers_share_same_connection(
        self, checkpoint_repo: InMemoryCheckpointRepository, mock_session_factory
    ) -> None:
        factory, _, mock_conn = mock_session_factory
        projection = MultiHandlerDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo
        )
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        await projection.handle(OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001"))
        assert len(projection.received_conns) == 2
        assert projection.received_conns[0] is mock_conn
        assert projection.received_conns[1] is mock_conn

    @pytest.mark.asyncio
    async def test_runtime_error_if_process_event_called_directly(
        self, checkpoint_repo: InMemoryCheckpointRepository, mock_session_factory
    ) -> None:
        factory, _, _ = mock_session_factory
        projection = SimpleDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo
        )
        with pytest.raises(RuntimeError, match="requires database connection"):
            await projection._process_event(
                OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            )

    @pytest.mark.asyncio
    async def test_unhandled_event_ignored_by_default(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        mock_session_factory,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        factory, _, _ = mock_session_factory
        projection = SimpleDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo
        )
        with caplog.at_level(logging.WARNING):
            await projection.handle(OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001"))
        assert not any("No handler registered" in rec.message for rec in caplog.records)

    @pytest.mark.asyncio
    async def test_connection_is_none_after_handle_completes(
        self, checkpoint_repo: InMemoryCheckpointRepository, mock_session_factory
    ) -> None:
        factory, _, _ = mock_session_factory
        projection = SimpleDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo
        )
        assert projection._current_connection is None
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert projection._current_connection is None

    @pytest.mark.asyncio
    async def test_connection_cleared_even_on_error(
        self,
        checkpoint_repo: InMemoryCheckpointRepository,
        dlq_repo: InMemoryDLQRepository,
        mock_session_factory,
    ) -> None:
        factory, _, _ = mock_session_factory
        projection = FailingDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo, dlq_repo=dlq_repo
        )
        with pytest.raises(ValueError):
            await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        assert projection._current_connection is None

    @pytest.mark.asyncio
    async def test_mixed_handler_signatures(
        self, checkpoint_repo: InMemoryCheckpointRepository, mock_session_factory
    ) -> None:
        factory, _, mock_conn = mock_session_factory
        projection = MixedDatabaseProjection(
            session_factory=factory, checkpoint_repo=checkpoint_repo
        )
        await projection.handle(OrderCreated(aggregate_id=uuid4(), order_number="ORD-001"))
        await projection.handle(OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001"))
        assert len(projection.two_param_events) == 1 and len(projection.one_param_events) == 1
        assert projection.received_conns == [mock_conn]

    def test_can_import_from_projections_module(self) -> None:
        from eventsource.adapters.sql.projection import DatabaseProjection as DBProjection

        assert DBProjection is not None

    def test_can_import_from_main_package(self) -> None:
        from eventsource import DatabaseProjection as DBProjection

        assert DBProjection is not None
