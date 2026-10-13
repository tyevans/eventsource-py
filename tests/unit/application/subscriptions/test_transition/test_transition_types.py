"""Tests for TransitionResult, TransitionPhase, and transition imports."""

from eventsource.application.subscriptions.transition import (
    TransitionPhase,
    TransitionResult,
)
from eventsource.ports.positions import Position


class TestTransitionResult:
    """Tests for TransitionResult dataclass."""

    def test_successful_result(self):
        """Test successful result has expected values."""
        result = TransitionResult(
            success=True,
            catchup_events_processed=100,
            buffer_events_processed=10,
            final_position=Position(store_id="test", key=(110,)),
            phase_reached=TransitionPhase.LIVE,
        )
        assert result.success is True
        assert result.catchup_events_processed == 100
        assert result.buffer_events_processed == 10
        assert result.final_position == Position(store_id="test", key=(110,))
        assert result.phase_reached == TransitionPhase.LIVE
        assert result.error is None

    def test_failed_result(self):
        """Test failed result has error."""
        error = ValueError("Test error")
        result = TransitionResult(
            success=False,
            catchup_events_processed=50,
            buffer_events_processed=0,
            final_position=Position(store_id="test", key=(50,)),
            phase_reached=TransitionPhase.FAILED,
            error=error,
        )
        assert result.success is False
        assert result.phase_reached == TransitionPhase.FAILED
        assert result.error is error


class TestTransitionPhase:
    """Tests for TransitionPhase enum."""

    def test_all_phases_exist(self):
        """Test all expected phases exist."""
        assert TransitionPhase.NOT_STARTED.value == "not_started"
        assert TransitionPhase.INITIAL_CATCHUP.value == "initial_catchup"
        assert TransitionPhase.LIVE_SUBSCRIBED.value == "live_subscribed"
        assert TransitionPhase.FINAL_CATCHUP.value == "final_catchup"
        assert TransitionPhase.PROCESSING_BUFFER.value == "processing_buffer"
        assert TransitionPhase.LIVE.value == "live"
        assert TransitionPhase.FAILED.value == "failed"


class TestTransitionImports:
    """Tests for module imports and exports."""

    def test_import_from_subscriptions_module(self):
        """Test transition classes can be imported from subscriptions module."""
        from eventsource.application.subscriptions import (
            StartFromResolver,
            TransitionCoordinator,
            TransitionPhase,
            TransitionResult,
        )

        assert TransitionCoordinator is not None
        assert TransitionPhase is not None
        assert TransitionResult is not None
        assert StartFromResolver is not None

    def test_import_from_transition_module(self):
        """Test transition classes can be imported from transition module."""
        from eventsource.application.subscriptions.transition import (
            StartFromResolver,
            TransitionCoordinator,
            TransitionPhase,
            TransitionResult,
        )

        assert TransitionCoordinator is not None
        assert TransitionPhase is not None
        assert TransitionResult is not None
        assert StartFromResolver is not None

    def test_all_exports(self):
        """Test __all__ contains expected exports."""
        from eventsource.application.subscriptions import transition

        assert "TransitionCoordinator" in transition.__all__
        assert "TransitionPhase" in transition.__all__
        assert "TransitionResult" in transition.__all__
        assert "StartFromResolver" in transition.__all__
