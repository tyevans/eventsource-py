"""Unit tests for ReadModelProjection construction, initialization, and handler routing."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.domain.decorators import handles
from tests.unit.readmodels.test_projection._fixtures import (
    OrderCreated,
    OrderShipped,
    OrderSummary,
)


class TestReadModelProjectionConstruction:
    """Tests for ReadModelProjection construction and initialization."""

    def test_constructor_sets_model_class(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test that constructor stores model class."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection._model_class == OrderSummary

    def test_model_class_property(self, mock_postgresql_session_factory, checkpoint_repo) -> None:
        """Test model_class property returns correct class."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection.model_class == OrderSummary

    def test_inherits_from_database_projection(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test that ReadModelProjection inherits from DatabaseProjection."""
        from eventsource.adapters.sql.projection import DatabaseProjection

        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert isinstance(projection, DatabaseProjection)

    def test_projection_name_defaults_to_class_name(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test projection_name defaults to class name."""
        factory, _, _ = mock_postgresql_session_factory

        class MyOrderProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = MyOrderProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection.projection_name == "MyOrderProjection"

    def test_repository_is_none_outside_handle_context(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test repository property is None outside of handle() context."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert projection.repository is None

    def test_repr_includes_model_info(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test __repr__ includes model class and table name."""
        factory, _, _ = mock_postgresql_session_factory

        class TestProjection(ReadModelProjection[OrderSummary]):
            pass

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        repr_str = repr(projection)
        assert "TestProjection" in repr_str
        assert "OrderSummary" in repr_str
        assert "order_summaries" in repr_str


class TestReadModelProjectionHandlerRouting:
    """Tests for handler routing and repository injection."""

    @pytest.mark.asyncio
    async def test_handler_receives_repository(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test that handlers receive repository as context."""
        factory, _, _ = mock_postgresql_session_factory
        received_repos = []
        events_handled = []

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                received_repos.append(repo)
                events_handled.append(event)

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with patch(
            "eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"
        ) as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo.return_value = mock_repo_instance

            await projection.handle(event)

            assert len(events_handled) == 1
            assert len(received_repos) == 1
            assert received_repos[0] is mock_repo_instance

    @pytest.mark.asyncio
    async def test_single_param_handler_still_works(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test that single-parameter handlers (event only) still work."""
        factory, _, _ = mock_postgresql_session_factory
        events_handled = []

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, event: OrderCreated) -> None:
                events_handled.append(event)

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        event = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")

        with patch("eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"):
            await projection.handle(event)

            assert len(events_handled) == 1
            assert events_handled[0] == event

    @pytest.mark.asyncio
    async def test_multiple_handlers(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test multiple handlers work correctly."""
        factory, _, _ = mock_postgresql_session_factory
        created_events = []
        shipped_events = []

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                created_events.append(event)

            @handles(OrderShipped)
            async def _handle_order_shipped(self, repo, event: OrderShipped) -> None:
                shipped_events.append(event)

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        assert len(projection.subscribed_to()) == 2
        assert OrderCreated in projection.subscribed_to()
        assert OrderShipped in projection.subscribed_to()

        with patch("eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"):
            created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

            await projection.handle(created)
            await projection.handle(shipped)

            assert len(created_events) == 1
            assert len(shipped_events) == 1

    @pytest.mark.asyncio
    async def test_mixed_handler_signatures(
        self, mock_postgresql_session_factory, checkpoint_repo
    ) -> None:
        """Test projection can have both 1-param and 2-param handlers."""
        factory, _, _ = mock_postgresql_session_factory
        one_param_events = []
        two_param_events = []
        received_repos = []

        class TestProjection(ReadModelProjection[OrderSummary]):
            @handles(OrderCreated)
            async def _handle_order_created(self, repo, event: OrderCreated) -> None:
                received_repos.append(repo)
                two_param_events.append(event)

            @handles(OrderShipped)
            async def _handle_order_shipped(self, event: OrderShipped) -> None:
                one_param_events.append(event)

        projection = TestProjection(
            session_factory=factory,
            model_class=OrderSummary,
            checkpoint_repo=checkpoint_repo,
        )

        with patch(
            "eventsource.adapters.postgresql.readmodels.PostgreSQLReadModelRepository"
        ) as mock_repo:
            mock_repo_instance = MagicMock()
            mock_repo.return_value = mock_repo_instance

            created = OrderCreated(aggregate_id=uuid4(), order_number="ORD-001")
            shipped = OrderShipped(aggregate_id=uuid4(), tracking_number="TRK-001")

            await projection.handle(created)
            await projection.handle(shipped)

            assert len(two_param_events) == 1
            assert len(one_param_events) == 1
            assert len(received_repos) == 1
            assert received_repos[0] is mock_repo_instance
