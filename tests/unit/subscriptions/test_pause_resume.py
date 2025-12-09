"""
Unit tests for subscription pause/resume functionality (PHASE3-005).

Tests cover:
- PauseReason enum values
- Subscription.pause() method
- Subscription.resume() method
- State transitions during pause/resume
- Pause tracking fields (reason, timestamp, duration)
- wait_if_paused() behavior
- Invalid state transitions
- Concurrent pause/resume operations
"""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.protocols import EventSubscriber
from eventsource.subscriptions import (
    PauseReason,
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
    SubscriptionStateError,
)

# Test fixtures


class MockSubscriber(EventSubscriber):
    """Mock subscriber for testing."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return []

    async def handle(self, event: DomainEvent) -> None:
        pass


@pytest.fixture
def mock_subscriber() -> MockSubscriber:
    """Create a mock subscriber."""
    return MockSubscriber()


@pytest.fixture
def config() -> SubscriptionConfig:
    """Create a default configuration."""
    return SubscriptionConfig()


@pytest.fixture
def subscription(mock_subscriber: MockSubscriber, config: SubscriptionConfig) -> Subscription:
    """Create a subscription for testing."""
    return Subscription(
        name="TestSubscription",
        config=config,
        subscriber=mock_subscriber,
    )


# PauseReason Enum Tests


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


# Subscription Pause Tests


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
            position=42,
            event_id=uuid4(),
            event_type="TestEvent",
        )
        await subscription.pause()
        assert subscription.last_processed_position == 42


# Subscription Pause Invalid State Tests


class TestSubscriptionPauseInvalidStates:
    """Tests for invalid pause operations."""

    @pytest.mark.asyncio
    async def test_pause_from_starting_raises_error(self, subscription: Subscription):
        """Test pause from STARTING raises error."""
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.pause()
        assert "Cannot pause from state starting" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_pause_from_paused_raises_error(self, subscription: Subscription):
        """Test pause from PAUSED raises error."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()

        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.pause()
        assert "Cannot pause from state paused" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_pause_from_stopped_raises_error(self, subscription: Subscription):
        """Test pause from STOPPED raises error."""
        await subscription.transition_to(SubscriptionState.STOPPED)

        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.pause()
        assert "Cannot pause from state stopped" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_pause_from_error_raises_error(self, subscription: Subscription):
        """Test pause from ERROR raises error."""
        await subscription.transition_to(SubscriptionState.ERROR)

        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.pause()
        assert "Cannot pause from state error" in str(exc_info.value)


# Subscription Resume Tests


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
            position=42,
            event_id=uuid4(),
            event_type="TestEvent",
        )
        await subscription.pause()
        await subscription.resume()
        assert subscription.last_processed_position == 42


# Subscription Resume Invalid State Tests


class TestSubscriptionResumeInvalidStates:
    """Tests for invalid resume operations."""

    @pytest.mark.asyncio
    async def test_resume_from_starting_raises_error(self, subscription: Subscription):
        """Test resume from STARTING raises error."""
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state starting" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resume_from_catching_up_raises_error(self, subscription: Subscription):
        """Test resume from CATCHING_UP raises error."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)

        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state catching_up" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resume_from_live_raises_error(self, subscription: Subscription):
        """Test resume from LIVE raises error."""
        await subscription.transition_to(SubscriptionState.LIVE)

        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state live" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resume_from_stopped_raises_error(self, subscription: Subscription):
        """Test resume from STOPPED raises error."""
        await subscription.transition_to(SubscriptionState.STOPPED)

        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state stopped" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resume_from_error_raises_error(self, subscription: Subscription):
        """Test resume from ERROR raises error."""
        await subscription.transition_to(SubscriptionState.ERROR)

        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state error" in str(exc_info.value)


# Pause Properties Tests


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


# Wait If Paused Tests


class TestWaitIfPaused:
    """Tests for wait_if_paused() method."""

    @pytest.mark.asyncio
    async def test_wait_if_paused_returns_false_when_not_paused(self, subscription: Subscription):
        """Test wait_if_paused returns False immediately when not paused."""
        await subscription.transition_to(SubscriptionState.LIVE)
        was_paused = await subscription.wait_if_paused()
        assert was_paused is False

    @pytest.mark.asyncio
    async def test_wait_if_paused_blocks_until_resume(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        """Test wait_if_paused blocks until resume is called."""
        subscription = Subscription(
            name="TestSubscription",
            config=config,
            subscriber=mock_subscriber,
        )
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()

        # Track when wait completes
        wait_completed = asyncio.Event()
        was_paused_result = None

        async def wait_task():
            nonlocal was_paused_result
            was_paused_result = await subscription.wait_if_paused()
            wait_completed.set()

        # Start waiting
        task = asyncio.create_task(wait_task())

        # Wait is blocked
        await asyncio.sleep(0.05)
        assert not wait_completed.is_set()

        # Resume to unblock
        await subscription.resume()

        # Wait for completion
        await asyncio.wait_for(wait_completed.wait(), timeout=1.0)
        assert was_paused_result is True

        await task


# Concurrent Operations Tests


class TestConcurrentPauseResume:
    """Tests for concurrent pause/resume operations."""

    @pytest.mark.asyncio
    async def test_concurrent_pause_calls(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        """Test concurrent pause calls are handled safely."""
        subscription = Subscription(
            name="TestSubscription",
            config=config,
            subscriber=mock_subscriber,
        )
        await subscription.transition_to(SubscriptionState.LIVE)

        # Multiple concurrent pause attempts
        results = await asyncio.gather(
            subscription.pause(),
            subscription.pause(),
            return_exceptions=True,
        )

        # One should succeed, others should fail
        errors = [r for r in results if isinstance(r, Exception)]
        assert len(errors) >= 1  # At least one should fail

        # Subscription should be paused
        assert subscription.state == SubscriptionState.PAUSED

    @pytest.mark.asyncio
    async def test_rapid_pause_resume_cycles(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        """Test rapid pause/resume cycles are handled correctly."""
        subscription = Subscription(
            name="TestSubscription",
            config=config,
            subscriber=mock_subscriber,
        )
        await subscription.transition_to(SubscriptionState.LIVE)

        # Rapid pause/resume cycles
        for _ in range(10):
            await subscription.pause()
            assert subscription.state == SubscriptionState.PAUSED
            await subscription.resume()
            assert subscription.state == SubscriptionState.LIVE


# Multiple Pause/Resume Tests


class TestMultiplePauseResumeCycles:
    """Tests for multiple pause/resume cycles."""

    @pytest.mark.asyncio
    async def test_multiple_pause_resume_preserves_position(self, subscription: Subscription):
        """Test multiple pause/resume cycles preserve position."""
        await subscription.transition_to(SubscriptionState.LIVE)

        for i in range(5):
            await subscription.record_event_processed(
                position=i,
                event_id=uuid4(),
                event_type="TestEvent",
            )
            await subscription.pause()
            await subscription.resume()

        # Position should be last recorded position
        assert subscription.last_processed_position == 4

    @pytest.mark.asyncio
    async def test_pause_resume_from_different_states(self, subscription: Subscription):
        """Test pause/resume from both CATCHING_UP and LIVE states."""
        # From CATCHING_UP
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.pause()
        assert subscription.state_before_pause == SubscriptionState.CATCHING_UP
        await subscription.resume()
        assert subscription.state == SubscriptionState.CATCHING_UP

        # Transition to LIVE
        await subscription.transition_to(SubscriptionState.LIVE)

        # From LIVE
        await subscription.pause()
        assert subscription.state_before_pause == SubscriptionState.LIVE
        await subscription.resume()
        assert subscription.state == SubscriptionState.LIVE


# Module Import Tests


class TestPauseResumeModuleImports:
    """Tests for module imports."""

    def test_import_pause_reason_from_subscriptions(self):
        """Test PauseReason can be imported from subscriptions module."""
        from eventsource.subscriptions import PauseReason

        assert PauseReason is not None
        assert hasattr(PauseReason, "MANUAL")
        assert hasattr(PauseReason, "BACKPRESSURE")
        assert hasattr(PauseReason, "MAINTENANCE")

    def test_import_pause_reason_from_subscription_module(self):
        """Test PauseReason can be imported from subscription module."""
        from eventsource.subscriptions.subscription import PauseReason

        assert PauseReason is not None
