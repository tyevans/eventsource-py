"""Tests for SyncLag dataclass."""

from datetime import datetime

import pytest

from eventsource.ports.migration.models import SyncLag


class TestSyncLag:
    """Tests for SyncLag dataclass."""

    def test_creation(self) -> None:
        """Test creating a SyncLag."""
        timestamp = datetime.now()
        lag = SyncLag(
            events=50,
            source_position=1000,
            target_position=950,
            timestamp=timestamp,
        )
        assert lag.events == 50
        assert lag.source_position == 1000
        assert lag.target_position == 950
        assert lag.timestamp == timestamp

    def test_is_converged_true(self) -> None:
        """Test is_converged returns True when lag is zero."""
        lag = SyncLag(
            events=0,
            source_position=1000,
            target_position=1000,
            timestamp=datetime.now(),
        )
        assert lag.is_converged is True

    def test_is_converged_false(self) -> None:
        """Test is_converged returns False when lag is non-zero."""
        lag = SyncLag(
            events=10,
            source_position=1000,
            target_position=990,
            timestamp=datetime.now(),
        )
        assert lag.is_converged is False

    def test_lag_ms(self) -> None:
        """Test lag_ms property."""
        lag = SyncLag(
            events=100,
            source_position=1000,
            target_position=900,
            timestamp=datetime.now(),
        )
        assert lag.lag_ms == 100.0

    def test_is_within_threshold_true(self) -> None:
        """Test is_within_threshold returns True when lag is below threshold."""
        lag = SyncLag(
            events=50,
            source_position=1000,
            target_position=950,
            timestamp=datetime.now(),
        )
        assert lag.is_within_threshold(100) is True
        assert lag.is_within_threshold(50) is True

    def test_is_within_threshold_false(self) -> None:
        """Test is_within_threshold returns False when lag exceeds threshold."""
        lag = SyncLag(
            events=100,
            source_position=1000,
            target_position=900,
            timestamp=datetime.now(),
        )
        assert lag.is_within_threshold(50) is False
        assert lag.is_within_threshold(99) is False

    def test_bounded_count_never_within_threshold(self) -> None:
        """A bounded count is a lower bound, so it satisfies no threshold.

        `events` here would pass on its face; the flag says the real
        backlog is unknown and larger, so answering True would be
        answering on no evidence.
        """
        lag = SyncLag(
            events=5,
            source_position=None,
            target_position=None,
            timestamp=datetime.now(),
            count_is_bounded=True,
        )
        assert lag.is_within_threshold(100) is False
        assert lag.is_within_threshold(5) is False

    def test_is_frozen(self) -> None:
        """Test that SyncLag is frozen (immutable)."""
        lag = SyncLag(
            events=50,
            source_position=1000,
            target_position=950,
            timestamp=datetime.now(),
        )
        with pytest.raises(AttributeError):
            lag.events = 100  # type: ignore[misc]
