"""
Unit tests for ReadModelProjection handler registry integration.

These tests verify that ReadModelProjection integrates properly with:
- The @handles decorator for declarative event handling
- The HandlerRegistry for handler discovery and validation
- Proper event routing to handler methods
- Both single-param and two-param handler signatures
"""

from unittest.mock import MagicMock

import pytest
from pydantic import Field

from eventsource.events import DomainEvent
from eventsource.projections import handles
from eventsource.readmodels import ReadModel, ReadModelProjection

# =============================================================================
# Test Events
# =============================================================================


class TestEvent(DomainEvent):
    """Test event for handler integration tests."""

    event_type: str = "TestEvent"
    aggregate_type: str = "Test"
    value: str = Field(..., description="Test value")


class SecondTestEvent(DomainEvent):
    """Second test event for multiple handler tests."""

    event_type: str = "SecondTestEvent"
    aggregate_type: str = "Test"
    data: str = Field(default="default", description="Test data")


class UnhandledEvent(DomainEvent):
    """Event type with no handler for testing unregistered events."""

    event_type: str = "UnhandledEvent"
    aggregate_type: str = "Test"


# =============================================================================
# Test Models
# =============================================================================


class TestModel(ReadModel):
    """Test read model for handler integration tests."""

    name: str


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_session_factory():
    """Create a mock async session factory."""
    # Create mock connection with PostgreSQL dialect
    mock_conn = MagicMock()
    mock_conn.dialect = MagicMock()
    mock_conn.dialect.name = "postgresql"

    # Create mock session
    mock_session = MagicMock()
    mock_session.connection = MagicMock(return_value=mock_conn)

    # Mock the context managers
    mock_session.__aenter__ = MagicMock(return_value=mock_session)
    mock_session.__aexit__ = MagicMock(return_value=None)

    # Create mock transaction context manager
    mock_begin = MagicMock()
    mock_begin.__aenter__ = MagicMock(return_value=None)
    mock_begin.__aexit__ = MagicMock(return_value=None)
    mock_session.begin = MagicMock(return_value=mock_begin)

    # Create factory that returns the mock session
    mock_factory = MagicMock()
    mock_factory.return_value = mock_session
    mock_factory.__call__ = MagicMock(return_value=mock_session)

    return mock_factory


# =============================================================================
# Test Classes
# =============================================================================


class TestHandlerRegistryIntegration:
    """Tests for @handles decorator integration with ReadModelProjection."""

    def test_handles_decorator_discovered(self, mock_session_factory) -> None:
        """Test that @handles decorated methods are discovered."""

        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        subscribed = projection.subscribed_to()
        assert TestEvent in subscribed

    def test_handler_signature_validation(self, mock_session_factory) -> None:
        """Test that handler signature validation accepts repo+event."""

        # This should not raise ValueError
        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        # If we get here without exception, validation passed
        assert projection.subscribed_to() == [TestEvent]

    def test_single_param_handler_works(self, mock_session_factory) -> None:
        """Test that single parameter handlers still work."""

        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test(self, event: TestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        assert projection.subscribed_to() == [TestEvent]

    def test_multiple_handlers_discovered(self, mock_session_factory) -> None:
        """Test that multiple @handles decorated methods are discovered."""

        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

            @handles(SecondTestEvent)
            async def _on_second(self, repo, event: SecondTestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        subscribed = projection.subscribed_to()
        assert len(subscribed) == 2
        assert TestEvent in subscribed
        assert SecondTestEvent in subscribed

    def test_handler_registry_accessible(self, mock_session_factory) -> None:
        """Test that the internal handler registry is properly initialized."""

        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        # Verify handler registry is accessible and has correct handler count
        assert hasattr(projection, "_handler_registry")
        assert projection._handler_registry.handler_count == 1

    def test_handler_info_has_correct_param_count(self, mock_session_factory) -> None:
        """Test that HandlerInfo correctly reports parameter count."""

        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test_two_params(self, repo, event: TestEvent) -> None:
                pass

            @handles(SecondTestEvent)
            async def _on_test_one_param(self, event: SecondTestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        # Get handler info for each event type
        two_param_info = projection._handler_registry.get_handler(TestEvent)
        one_param_info = projection._handler_registry.get_handler(SecondTestEvent)

        assert two_param_info is not None
        assert two_param_info.param_count == 2

        assert one_param_info is not None
        assert one_param_info.param_count == 1

    def test_handler_info_is_async(self, mock_session_factory) -> None:
        """Test that handler is recognized as async."""

        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        handler_info = projection._handler_registry.get_handler(TestEvent)
        assert handler_info is not None
        assert handler_info.is_async is True


class TestExports:
    """Tests for module exports."""

    def test_readmodels_package_exports_projection(self) -> None:
        """Test that ReadModelProjection is exported from readmodels."""
        from eventsource.readmodels import ReadModelProjection

        assert ReadModelProjection is not None

    def test_top_level_exports_projection(self) -> None:
        """Test that ReadModelProjection is exported from eventsource."""
        from eventsource import ReadModelProjection

        assert ReadModelProjection is not None

    def test_handles_decorator_importable_from_eventsource(self) -> None:
        """Test @handles decorator can be imported from top-level eventsource."""
        from eventsource import handles

        assert handles is not None
        assert callable(handles)

    def test_handles_decorator_importable_from_projections(self) -> None:
        """Test @handles decorator can be imported from projections module."""
        from eventsource.projections import handles

        assert handles is not None
        assert callable(handles)


class TestHandlerValidation:
    """Tests for handler validation during projection construction."""

    def test_non_async_handler_raises_value_error(self, mock_session_factory) -> None:
        """Test that non-async handlers raise ValueError."""

        with pytest.raises(ValueError, match="must be async"):

            class BadProjection(ReadModelProjection[TestModel]):
                @handles(TestEvent)
                def _on_test(self, repo, event: TestEvent) -> None:  # Missing async
                    pass

            BadProjection(
                session_factory=mock_session_factory,
                model_class=TestModel,
            )

    def test_zero_param_handler_raises_value_error(self, mock_session_factory) -> None:
        """Test that handlers with no parameters raise ValueError."""

        with pytest.raises(ValueError, match="invalid signature"):

            class BadProjection(ReadModelProjection[TestModel]):
                @handles(TestEvent)
                async def _on_test(self) -> None:  # No event parameter
                    pass

            BadProjection(
                session_factory=mock_session_factory,
                model_class=TestModel,
            )

    def test_three_param_handler_raises_value_error(self, mock_session_factory) -> None:
        """Test that handlers with 3+ parameters raise ValueError.

        Note: The error message uses 'invalid signature' rather than 'must accept'."""

        with pytest.raises(ValueError, match="invalid signature"):

            class BadProjection(ReadModelProjection[TestModel]):
                @handles(TestEvent)
                async def _on_test(self, repo, extra, event: TestEvent) -> None:  # Too many params
                    pass

            BadProjection(
                session_factory=mock_session_factory,
                model_class=TestModel,
            )


class TestSubscribedTo:
    """Tests for subscribed_to() method behavior."""

    def test_subscribed_to_returns_event_types(self, mock_session_factory) -> None:
        """Test subscribed_to() returns all handled event types."""

        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

            @handles(SecondTestEvent)
            async def _on_second(self, event: SecondTestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        subscribed = projection.subscribed_to()

        assert isinstance(subscribed, list)
        assert len(subscribed) == 2
        assert TestEvent in subscribed
        assert SecondTestEvent in subscribed

    def test_subscribed_to_returns_empty_for_no_handlers(self, mock_session_factory) -> None:
        """Test subscribed_to() returns empty list if no handlers defined."""

        class EmptyProjection(ReadModelProjection[TestModel]):
            pass

        projection = EmptyProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        assert projection.subscribed_to() == []


class TestUnregisteredEventHandling:
    """Tests for unregistered event handling configuration."""

    def test_default_unregistered_event_handling_is_ignore(self, mock_session_factory) -> None:
        """Test that default unregistered_event_handling is 'ignore'."""

        class TestProjection(ReadModelProjection[TestModel]):
            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

        projection = TestProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        # Access via handler registry
        assert projection._handler_registry._unregistered_event_handling == "ignore"

    def test_can_set_unregistered_event_handling_to_warn(self, mock_session_factory) -> None:
        """Test that unregistered_event_handling can be set to 'warn'."""

        class WarnProjection(ReadModelProjection[TestModel]):
            unregistered_event_handling = "warn"

            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

        projection = WarnProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        assert projection._handler_registry._unregistered_event_handling == "warn"

    def test_can_set_unregistered_event_handling_to_error(self, mock_session_factory) -> None:
        """Test that unregistered_event_handling can be set to 'error'."""

        class StrictProjection(ReadModelProjection[TestModel]):
            unregistered_event_handling = "error"

            @handles(TestEvent)
            async def _on_test(self, repo, event: TestEvent) -> None:
                pass

        projection = StrictProjection(
            session_factory=mock_session_factory,
            model_class=TestModel,
        )

        assert projection._handler_registry._unregistered_event_handling == "error"


class TestNoCircularImports:
    """Tests to ensure no circular import issues."""

    def test_import_from_eventsource(self) -> None:
        """Test imports from eventsource without circular import errors."""
        from eventsource import (
            DomainEvent,
            ReadModelProjection,
            handles,
        )

        assert DomainEvent is not None
        assert ReadModelProjection is not None
        assert handles is not None

    def test_import_from_readmodels(self) -> None:
        """Test imports from eventsource.readmodels without circular import errors."""
        from eventsource.readmodels import (
            ReadModel,
            ReadModelProjection,
            ReadModelRepository,
        )

        assert ReadModel is not None
        assert ReadModelProjection is not None
        assert ReadModelRepository is not None

    def test_import_handles_from_projections(self) -> None:
        """Test importing handles from projections module."""
        from eventsource.handlers import (
            get_handled_event_type,
            is_event_handler,
        )
        from eventsource.projections import handles

        assert handles is not None
        assert get_handled_event_type is not None
        assert is_event_handler is not None
