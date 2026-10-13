"""Tests for CutoverResult dataclass."""

from eventsource.ports.migration.models import CutoverResult


class TestCutoverResult:
    """Tests for CutoverResult dataclass."""

    def test_successful_cutover(self) -> None:
        """Test creating a successful cutover result."""
        result = CutoverResult(
            success=True,
            duration_ms=50.5,
            events_synced=10,
        )
        assert result.success is True
        assert result.duration_ms == 50.5
        assert result.events_synced == 10
        assert result.error_message is None
        assert result.rolled_back is False

    def test_failed_cutover(self) -> None:
        """Test creating a failed cutover result."""
        result = CutoverResult(
            success=False,
            duration_ms=150.0,
            error_message="Timeout exceeded",
            rolled_back=True,
        )
        assert result.success is False
        assert result.error_message == "Timeout exceeded"
        assert result.rolled_back is True

    def test_within_timeout_true(self) -> None:
        """Test within_timeout returns True for fast cutover."""
        result = CutoverResult(
            success=True,
            duration_ms=50.0,
        )
        assert result.within_timeout is True

    def test_within_timeout_false(self) -> None:
        """Test within_timeout returns False for slow cutover."""
        result = CutoverResult(
            success=False,
            duration_ms=150.0,
        )
        assert result.within_timeout is False

    def test_within_timeout_boundary(self) -> None:
        """Test within_timeout at exactly 100ms boundary."""
        # Exactly 100ms should be False (< 100, not <=)
        result = CutoverResult(success=True, duration_ms=100.0)
        assert result.within_timeout is False

        result = CutoverResult(success=True, duration_ms=99.9)
        assert result.within_timeout is True
