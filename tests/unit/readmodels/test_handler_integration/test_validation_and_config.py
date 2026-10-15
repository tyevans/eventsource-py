"""Unit tests for ReadModelProjection handler validation, subscriptions, and configuration."""

import pytest

from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
from eventsource.application.projections import handles

from .conftest import (
    HandlerIntegrationEvent as TestEvent,
)
from .conftest import (
    HandlerIntegrationModel as TestModel,
)
from .conftest import (
    SecondHandlerIntegrationEvent as SecondTestEvent,
)


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
        """Test that handlers with no parameters raise HandlerSignatureError."""
        from eventsource.domain.exceptions import HandlerSignatureError

        with pytest.raises(HandlerSignatureError, match="invalid signature"):

            class BadProjection(ReadModelProjection[TestModel]):
                @handles(TestEvent)
                async def _on_test(self) -> None:  # No event parameter
                    pass

            BadProjection(
                session_factory=mock_session_factory,
                model_class=TestModel,
            )

    def test_three_param_handler_raises_value_error(self, mock_session_factory) -> None:
        """Test that handlers with 3+ parameters raise HandlerSignatureError.

        Note: The error message uses 'invalid signature' rather than 'must accept'."""
        from eventsource.domain.exceptions import HandlerSignatureError

        with pytest.raises(HandlerSignatureError, match="invalid signature"):

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
        """Test imports from the read-model ports/adapters without circular import errors."""
        from eventsource.adapters.sql.readmodel_projection import ReadModelProjection
        from eventsource.ports.readmodels import ReadModel, ReadModelRepository

        assert ReadModel is not None
        assert ReadModelProjection is not None
        assert ReadModelRepository is not None

    def test_import_handles_from_projections(self) -> None:
        """Test importing handles from projections module."""
        from eventsource.application.projections import handles
        from eventsource.domain.decorators import (
            get_handled_event_type,
            is_event_handler,
        )

        assert handles is not None
        assert get_handled_event_type is not None
        assert is_event_handler is not None
