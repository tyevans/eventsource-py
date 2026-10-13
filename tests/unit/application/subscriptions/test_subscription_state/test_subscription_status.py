"""Unit tests for SubscriptionStatus dataclass, get_status method, and string representation."""

from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionState,
    SubscriptionStatus,
)
from tests.unit.application.subscriptions.test_subscription_state._fixtures import pos


class TestSubscriptionStatus:
    """Tests for SubscriptionStatus dataclass."""

    def test_status_creation(self):
        """Test creating a status snapshot."""
        status = SubscriptionStatus(
            name="TestSubscription",
            state="live",
            position=pos(42),
            lag_events=10,
            events_processed=100,
            events_failed=2,
            last_processed_at="2024-01-01T12:00:00",
            started_at="2024-01-01T11:00:00",
            uptime_seconds=3600.0,
        )
        assert status.name == "TestSubscription"
        assert status.state == "live"
        assert status.position == pos(42)
        assert status.lag_events == 10
        assert status.events_processed == 100
        assert status.events_failed == 2
        assert status.last_processed_at == "2024-01-01T12:00:00"
        assert status.started_at == "2024-01-01T11:00:00"
        assert status.uptime_seconds == 3600.0
        assert status.error is None

    def test_status_with_error(self):
        """Test status with error."""
        status = SubscriptionStatus(
            name="TestSubscription",
            state="error",
            position=pos(42),
            lag_events=0,
            events_processed=100,
            events_failed=1,
            last_processed_at="2024-01-01T12:00:00",
            started_at="2024-01-01T11:00:00",
            uptime_seconds=3600.0,
            error="Connection timeout",
        )
        assert status.error == "Connection timeout"

    def test_status_to_dict(self):
        """Test status to_dict conversion."""
        status = SubscriptionStatus(
            name="TestSubscription",
            state="live",
            position=pos(42),
            lag_events=10,
            events_processed=100,
            events_failed=2,
            last_processed_at="2024-01-01T12:00:00",
            started_at="2024-01-01T11:00:00",
            uptime_seconds=3600.0,
        )
        result = status.to_dict()

        assert result["name"] == "TestSubscription"
        assert result["state"] == "live"
        assert result["position"] == pos(42).to_str()
        assert result["lag_events"] == 10
        assert result["events_processed"] == 100
        assert result["events_failed"] == 2
        assert result["last_processed_at"] == "2024-01-01T12:00:00"
        assert result["started_at"] == "2024-01-01T11:00:00"
        assert result["uptime_seconds"] == 3600.0
        assert result["error"] is None

    def test_status_is_frozen(self):
        """Test status is immutable."""
        status = SubscriptionStatus(
            name="TestSubscription",
            state="live",
            position=pos(42),
            lag_events=10,
            events_processed=100,
            events_failed=2,
            last_processed_at=None,
            started_at=None,
            uptime_seconds=0.0,
        )
        with pytest.raises(AttributeError):
            status.position = pos(100)  # type: ignore[misc]


class TestSubscriptionGetStatus:
    """Tests for Subscription.get_status() method."""

    @pytest.mark.asyncio
    async def test_get_status_returns_snapshot(self, subscription: Subscription):
        """Test get_status returns a status snapshot."""
        status = subscription.get_status()
        assert isinstance(status, SubscriptionStatus)

    @pytest.mark.asyncio
    async def test_get_status_includes_name(self, subscription: Subscription):
        """Test status includes subscription name."""
        status = subscription.get_status()
        assert status.name == "TestSubscription"

    @pytest.mark.asyncio
    async def test_get_status_includes_state(self, subscription: Subscription):
        """Test status includes current state."""
        await subscription.transition_to(SubscriptionState.LIVE)
        status = subscription.get_status()
        assert status.state == "live"

    @pytest.mark.asyncio
    async def test_get_status_includes_position(self, subscription: Subscription):
        """Test status includes position."""
        await subscription.record_event_processed(
            position=pos(42),
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        status = subscription.get_status()
        assert status.position == pos(42)

    @pytest.mark.asyncio
    async def test_get_status_includes_lag(self, subscription: Subscription):
        """Test status includes lag."""
        await subscription.record_events_seen(100)
        await subscription.record_event_processed(
            position=pos(50),
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        status = subscription.get_status()
        assert status.lag_events == 99

    @pytest.mark.asyncio
    async def test_get_status_includes_statistics(self, subscription: Subscription):
        """Test status includes statistics."""
        await subscription.record_event_processed(
            position=pos(1),
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        await subscription.record_event_failed(Exception("Test error"))

        status = subscription.get_status()
        assert status.events_processed == 1
        assert status.events_failed == 1

    @pytest.mark.asyncio
    async def test_get_status_includes_timestamps(self, subscription: Subscription):
        """Test status includes timestamps."""
        await subscription.record_event_processed(
            position=pos(1),
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        status = subscription.get_status()
        assert status.started_at is not None
        assert status.last_processed_at is not None

    @pytest.mark.asyncio
    async def test_get_status_includes_error(self, subscription: Subscription):
        """Test status includes error when present."""
        await subscription.record_event_failed(ValueError("Test error"))
        status = subscription.get_status()
        assert "Test error" in status.error


class TestStringRepresentation:
    """Tests for string representation."""

    def test_str_representation(self, subscription: Subscription):
        """Test __str__ method."""
        result = str(subscription)
        assert "TestSubscription" in result
        assert "starting" in result
        # Nothing processed yet: the token placeholder, not a position.
        assert "pos=-" in result

    @pytest.mark.asyncio
    async def test_str_representation_after_processing(self, subscription: Subscription):
        """Test __str__ after processing events."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.record_event_processed(
            position=pos(42),
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        result = str(subscription)
        assert "live" in result
        assert f"pos={pos(42).to_str()}" in result
