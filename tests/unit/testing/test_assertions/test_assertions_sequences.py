"""Tests for event count and sequence assertions."""

from __future__ import annotations

import pytest

from eventsource.testing.assertions import EventAssertions
from tests.unit.testing.test_assertions._fixtures import (
    SampleCreated,
    SampleDeleted,
    SampleUpdated,
)


class TestAssertEventCount:
    """Tests for assert_event_count method."""

    def test_passes_for_correct_count(self, multi_event_assertions: EventAssertions) -> None:
        """Test passes when count matches."""
        multi_event_assertions.assert_event_count(3)
        # Should not raise

    def test_raises_for_wrong_count(self, multi_event_assertions: EventAssertions) -> None:
        """Test raises when count doesn't match."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_count(5)

        assert "expected 5 events" in str(exc_info.value)
        assert "got 3" in str(exc_info.value)

    def test_zero_count_passes_when_empty(self, empty_assertions: EventAssertions) -> None:
        """Test count 0 passes when no events."""
        empty_assertions.assert_event_count(0)
        # Should not raise

    def test_error_includes_event_types(self, multi_event_assertions: EventAssertions) -> None:
        """Test error message includes event types."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_count(10)

        error_msg = str(exc_info.value)
        assert "SampleCreated" in error_msg
        assert "SampleUpdated" in error_msg
        assert "SampleDeleted" in error_msg


class TestAssertEventSequence:
    """Tests for assert_event_sequence method."""

    def test_passes_for_correct_sequence(self, multi_event_assertions: EventAssertions) -> None:
        """Test passes when sequence matches."""
        multi_event_assertions.assert_event_sequence(
            [
                SampleCreated,
                SampleUpdated,
                SampleDeleted,
            ]
        )
        # Should not raise

    def test_raises_for_wrong_count(self, multi_event_assertions: EventAssertions) -> None:
        """Test raises when count differs."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_sequence(
                [
                    SampleCreated,
                    SampleUpdated,
                ]
            )

        assert "expected 2 events" in str(exc_info.value)
        assert "got 3" in str(exc_info.value)

    def test_raises_for_wrong_order(self, multi_event_assertions: EventAssertions) -> None:
        """Test raises when order differs."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_sequence(
                [
                    SampleUpdated,  # Wrong - should be SampleCreated
                    SampleCreated,
                    SampleDeleted,
                ]
            )

        assert "position 0" in str(exc_info.value)
        assert "expected SampleUpdated" in str(exc_info.value)
        assert "got SampleCreated" in str(exc_info.value)

    def test_empty_sequence_passes_when_empty(self, empty_assertions: EventAssertions) -> None:
        """Test empty sequence passes when no events."""
        empty_assertions.assert_event_sequence([])
        # Should not raise

    def test_includes_full_sequence_in_error(self, multi_event_assertions: EventAssertions) -> None:
        """Test error includes full expected and actual sequences."""
        with pytest.raises(AssertionError) as exc_info:
            multi_event_assertions.assert_event_sequence(
                [
                    SampleCreated,
                    SampleDeleted,  # Wrong order
                    SampleUpdated,
                ]
            )

        error_msg = str(exc_info.value)
        assert "Full sequence:" in error_msg
        assert "Expected:" in error_msg
        assert "Actual:" in error_msg
