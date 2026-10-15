"""Unit tests for ReadModelProjection handler registry discovery and module exports."""

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
        """Test that ReadModelProjection is exported from its adapter module."""
        from eventsource.adapters.sql.readmodel_projection import ReadModelProjection

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
        from eventsource.application.projections import handles

        assert handles is not None
        assert callable(handles)
