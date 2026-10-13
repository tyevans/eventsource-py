"""Unit tests for subscription pause/resume invalid states and concurrency scenarios."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
    SubscriptionStateError,
)
from tests.unit.application.subscriptions.test_pause_resume._fixtures import (
    MockSubscriber,
    pos,
)


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


class TestMultiplePauseResumeCycles:
    """Tests for multiple pause/resume cycles."""

    @pytest.mark.asyncio
    async def test_multiple_pause_resume_preserves_position(self, subscription: Subscription):
        """Test multiple pause/resume cycles preserve position."""
        await subscription.transition_to(SubscriptionState.LIVE)

        for i in range(5):
            await subscription.record_event_processed(
                position=pos(i),
                event_id=uuid4(),
                event_type="TestEvent",
            )
            await subscription.pause()
            await subscription.resume()

        # Position should be last recorded position
        assert subscription.last_processed_position == pos(4)

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
