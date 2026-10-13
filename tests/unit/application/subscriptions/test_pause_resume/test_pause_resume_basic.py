"""Unit tests for subscription pause/resume basic functionality and properties."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    PauseReason,
    Subscription,
    SubscriptionState,
)
from tests.unit.application.subscriptions.test_pause_resume._fixtures import pos


class TestPauseReasonEnum:
    """Tests for PauseReason enum."""

    def test_manual_value(self):
        """Test MANUAL has correct value."""
        assert PauseReason.MANUAL.value == "manual"

    def test_backpressure_value(self):
        """Test BACKPRESSURE has correct value."""
        assert PauseReason.BACKPRESSURE.value == "backpressure"

    def test_maintenance_value(self):
        """Test MAINTENANCE has correct value."""
        assert PauseReason.MAINTENANCE.value == "maintenance"

    def test_all_reasons_exist(self):
        """Test all expected reasons exist."""
        reasons = list(PauseReason)
        assert len(reasons) == 3
        assert PauseReason.MANUAL in reasons
        assert PauseReason.BACKPRESSURE in reasons
        assert PauseReason.MAINTENANCE in reasons


class TestSubscriptionPause:
    """Tests for Subscription.pause() method."""

    @pytest.mark.asyncio
    async def test_pause_from_catching_up(self, subscription: Subscription):
        """Test pause from CATCHING_UP state."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.pause()
        assert subscription.state == SubscriptionState.PAUSED

    @pytest.mark.asyncio
    async def test_pause_from_live(self, subscription: Subscription):
        """Test pause from LIVE state."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.state == SubscriptionState.PAUSED

    @pytest.mark.asyncio
    async def test_pause_stores_previous_state_catching_up(self, subscription: Subscription):
        """Test pause stores CATCHING_UP as previous state."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.pause()
        assert subscription.state_before_pause == SubscriptionState.CATCHING_UP

    @pytest.mark.asyncio
    async def test_pause_stores_previous_state_live(self, subscription: Subscription):
        """Test pause stores LIVE as previous state."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.state_before_pause == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_pause_with_manual_reason(self, subscription: Subscription):
        """Test pause with MANUAL reason."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause(reason=PauseReason.MANUAL)
        assert subscription.pause_reason == PauseReason.MANUAL

    @pytest.mark.asyncio
    async def test_pause_with_backpressure_reason(self, subscription: Subscription):
        """Test pause with BACKPRESSURE reason."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause(reason=PauseReason.BACKPRESSURE)
        assert subscription.pause_reason == PauseReason.BACKPRESSURE

    @pytest.mark.asyncio
    async def test_pause_with_maintenance_reason(self, subscription: Subscription):
        """Test pause with MAINTENANCE reason."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause(reason=PauseReason.MAINTENANCE)
        assert subscription.pause_reason == PauseReason.MAINTENANCE

    @pytest.mark.asyncio
    async def test_pause_default_reason_is_manual(self, subscription: Subscription):
        """Test pause defaults to MANUAL reason."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.pause_reason == PauseReason.MANUAL

    @pytest.mark.asyncio
    async def test_pause_sets_paused_at_timestamp(self, subscription: Subscription):
        """Test pause sets paused_at timestamp."""
        await subscription.transition_to(SubscriptionState.LIVE)
        before = datetime.now(UTC)
        await subscription.pause()
        after = datetime.now(UTC)

        assert subscription.paused_at is not None
        assert before <= subscription.paused_at <= after

    @pytest.mark.asyncio
    async def test_pause_clears_pause_event(self, subscription: Subscription):
        """Test pause clears the internal pause event."""
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription._pause_event.is_set()  # Initially set
        await subscription.pause()
        assert not subscription._pause_event.is_set()  # Cleared after pause

    @pytest.mark.asyncio
    async def test_pause_maintains_position(self, subscription: Subscription):
        """Test pause does not change last_processed_position."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.record_event_processed(
            position=pos(42),
            event_id=uuid4(),
            event_type="TestEvent",
        )
        await subscription.pause()
        assert subscription.last_processed_position == pos(42)


class TestSubscriptionResume:
    """Tests for Subscription.resume() method."""

    @pytest.mark.asyncio
    async def test_resume_from_paused_to_catching_up(self, subscription: Subscription):
        """Test resume returns to CATCHING_UP if paused during catch-up."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.pause()
        await subscription.resume()
        assert subscription.state == SubscriptionState.CATCHING_UP

    @pytest.mark.asyncio
    async def test_resume_from_paused_to_live(self, subscription: Subscription):
        """Test resume returns to LIVE if paused during live."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        await subscription.resume()
        assert subscription.state == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_resume_clears_pause_reason(self, subscription: Subscription):
        """Test resume clears the pause reason."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause(reason=PauseReason.MAINTENANCE)
        assert subscription.pause_reason == PauseReason.MAINTENANCE
        await subscription.resume()
        assert subscription.pause_reason is None

    @pytest.mark.asyncio
    async def test_resume_clears_paused_at(self, subscription: Subscription):
        """Test resume clears the paused_at timestamp."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.paused_at is not None
        await subscription.resume()
        assert subscription.paused_at is None

    @pytest.mark.asyncio
    async def test_resume_sets_pause_event(self, subscription: Subscription):
        """Test resume sets the internal pause event."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert not subscription._pause_event.is_set()  # Cleared after pause
        await subscription.resume()
        assert subscription._pause_event.is_set()  # Set after resume

    @pytest.mark.asyncio
    async def test_resume_maintains_position(self, subscription: Subscription):
        """Test resume does not change last_processed_position."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.record_event_processed(
            position=pos(42),
            event_id=uuid4(),
            event_type="TestEvent",
        )
        await subscription.pause()
        await subscription.resume()
        assert subscription.last_processed_position == pos(42)


class TestPauseProperties:
    """Tests for pause-related properties."""

    @pytest.mark.asyncio
    async def test_is_paused_true_when_paused(self, subscription: Subscription):
        """Test is_paused is True when in PAUSED state."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.is_paused is True

    @pytest.mark.asyncio
    async def test_is_paused_false_when_not_paused(self, subscription: Subscription):
        """Test is_paused is False when not in PAUSED state."""
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.is_paused is False

    @pytest.mark.asyncio
    async def test_pause_duration_seconds_during_pause(self, subscription: Subscription):
        """Test pause_duration_seconds returns duration while paused."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()

        await asyncio.sleep(0.1)  # Wait a bit

        duration = subscription.pause_duration_seconds
        assert duration is not None
        assert duration >= 0.1

    @pytest.mark.asyncio
    async def test_pause_duration_seconds_none_when_not_paused(self, subscription: Subscription):
        """Test pause_duration_seconds is None when not paused."""
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.pause_duration_seconds is None

    @pytest.mark.asyncio
    async def test_state_before_pause_none_when_not_paused(self, subscription: Subscription):
        """Test state_before_pause is None initially."""
        assert subscription.state_before_pause is None

    @pytest.mark.asyncio
    async def test_pause_reason_none_when_not_paused(self, subscription: Subscription):
        """Test pause_reason is None when not paused."""
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.pause_reason is None


class TestPauseResumeModuleImports:
    """Tests for module imports."""

    def test_import_pause_reason_from_subscriptions(self):
        """Test PauseReason can be imported from subscriptions module."""
        from eventsource.application.subscriptions import PauseReason

        assert PauseReason is not None
        assert hasattr(PauseReason, "MANUAL")
        assert hasattr(PauseReason, "BACKPRESSURE")
        assert hasattr(PauseReason, "MAINTENANCE")

    def test_import_pause_reason_from_subscription_module(self):
        """Test PauseReason can be imported from subscription module."""
        from eventsource.application.subscriptions.subscription import PauseReason

        assert PauseReason is not None
