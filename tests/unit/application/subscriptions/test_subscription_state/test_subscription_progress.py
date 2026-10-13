"""Unit tests for Subscription position tracking, statistics, and lag calculation."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import Subscription
from tests.unit.application.subscriptions.test_subscription_state._fixtures import pos


class TestPositionTracking:
    """Tests for position tracking."""

    @pytest.mark.asyncio
    async def test_record_event_processed_updates_position(self, subscription: Subscription):
        """Test recording event processed updates position."""
        event_id = uuid4()
        await subscription.record_event_processed(
            position=pos(42),
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert subscription.last_processed_position == pos(42)

    @pytest.mark.asyncio
    async def test_record_event_processed_updates_event_id(self, subscription: Subscription):
        """Test recording event processed updates event_id."""
        event_id = uuid4()
        await subscription.record_event_processed(
            position=pos(42),
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert subscription.last_event_id == event_id

    @pytest.mark.asyncio
    async def test_record_event_processed_updates_event_type(self, subscription: Subscription):
        """Test recording event processed updates event_type."""
        event_id = uuid4()
        await subscription.record_event_processed(
            position=pos(42),
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert subscription.last_event_type == "OrderCreated"

    @pytest.mark.asyncio
    async def test_record_event_processed_increments_counter(self, subscription: Subscription):
        """Test recording event processed increments counter."""
        event_id = uuid4()
        await subscription.record_event_processed(
            position=pos(1),
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert subscription.events_processed == 1

        await subscription.record_event_processed(
            position=pos(2),
            event_id=uuid4(),
            event_type="OrderShipped",
        )
        assert subscription.events_processed == 2

    @pytest.mark.asyncio
    async def test_record_event_processed_updates_last_processed_at(
        self, subscription: Subscription
    ):
        """Test recording event processed updates timestamp."""
        before = datetime.now(UTC)
        event_id = uuid4()
        await subscription.record_event_processed(
            position=pos(42),
            event_id=event_id,
            event_type="OrderCreated",
        )
        after = datetime.now(UTC)

        assert subscription.last_processed_at is not None
        assert before <= subscription.last_processed_at <= after


class TestStatisticsTracking:
    """Tests for statistics tracking."""

    @pytest.mark.asyncio
    async def test_record_event_failed_increments_counter(self, subscription: Subscription):
        """Test recording event failed increments counter."""
        await subscription.record_event_failed(Exception("Test error"))
        assert subscription.events_failed == 1

        await subscription.record_event_failed(Exception("Another error"))
        assert subscription.events_failed == 2

    @pytest.mark.asyncio
    async def test_record_event_failed_stores_error(self, subscription: Subscription):
        """Test recording event failed stores the error."""
        error = ValueError("Test error")
        await subscription.record_event_failed(error)
        assert subscription.last_error is error

    @pytest.mark.asyncio
    async def test_record_event_failed_updates_error_timestamp(self, subscription: Subscription):
        """Test recording event failed updates error timestamp."""
        before = datetime.now(UTC)
        await subscription.record_event_failed(Exception("Test error"))
        after = datetime.now(UTC)

        assert subscription.last_error_at is not None
        assert before <= subscription.last_error_at <= after


class TestLagCalculation:
    """Tests for lag calculation."""

    @pytest.mark.asyncio
    async def test_lag_with_fresh_subscription(self, subscription: Subscription):
        """Test lag is 0 for a fresh subscription."""
        assert subscription.lag == 0

    @pytest.mark.asyncio
    async def test_lag_after_events_seen_none_processed(self, subscription: Subscription):
        """Test lag equals events seen when nothing has been processed."""
        await subscription.record_events_seen(10)
        assert subscription.lag == 10

    @pytest.mark.asyncio
    async def test_lag_decreases_as_events_are_processed(self, subscription: Subscription):
        """Test lag falls as events are processed against events seen."""
        await subscription.record_events_seen(10)
        for _ in range(4):
            await subscription.record_event_processed(
                position=pos(0),
                event_id=uuid4(),
                event_type="OrderCreated",
            )
        assert subscription.lag == 6

    @pytest.mark.asyncio
    async def test_lag_is_never_negative(self, subscription: Subscription):
        """Test processing more events than were seen clamps lag at 0."""
        await subscription.record_events_seen(2)
        for _ in range(5):
            await subscription.record_event_processed(
                position=pos(0),
                event_id=uuid4(),
                event_type="OrderCreated",
            )
        assert subscription.lag == 0

    @pytest.mark.asyncio
    async def test_lag_unaffected_by_position_without_processing(self, subscription: Subscription):
        """Test lag does not change when last_processed_position changes
        without a record_event_processed call -- lag is no longer derived
        from position at all."""
        await subscription.record_events_seen(10)
        assert subscription.lag == 10
        subscription.last_processed_position = pos(999)
        assert subscription.lag == 10
