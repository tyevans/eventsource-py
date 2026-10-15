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
from tests.unit.application.subscriptions.test_pause_resume._fixtures import MockSubscriber, pos


class TestSubscriptionPauseInvalidStates:
    @pytest.mark.asyncio
    async def test_pause_from_starting_raises_error(self, subscription: Subscription):
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.pause()
        assert "Cannot pause from state starting" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_pause_from_paused_raises_error(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.pause()
        assert "Cannot pause from state paused" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_pause_from_stopped_raises_error(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.STOPPED)
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.pause()
        assert "Cannot pause from state stopped" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_pause_from_error_raises_error(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.ERROR)
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.pause()
        assert "Cannot pause from state error" in str(exc_info.value)


class TestSubscriptionResumeInvalidStates:
    @pytest.mark.asyncio
    async def test_resume_from_starting_raises_error(self, subscription: Subscription):
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state starting" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resume_from_catching_up_raises_error(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state catching_up" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resume_from_live_raises_error(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state live" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resume_from_stopped_raises_error(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.STOPPED)
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state stopped" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_resume_from_error_raises_error(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.ERROR)
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.resume()
        assert "Cannot resume from state error" in str(exc_info.value)


class TestWaitIfPaused:
    @pytest.mark.asyncio
    async def test_wait_if_paused_returns_false_when_not_paused(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        assert await subscription.wait_if_paused() is False

    @pytest.mark.asyncio
    async def test_wait_if_paused_blocks_until_resume(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        subscription = Subscription(
            name="TestSubscription", config=config, subscriber=mock_subscriber
        )
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        wait_completed = asyncio.Event()
        was_paused_result = None

        async def wait_task():
            nonlocal was_paused_result
            was_paused_result = await subscription.wait_if_paused()
            wait_completed.set()

        task = asyncio.create_task(wait_task())
        await asyncio.sleep(0.05)
        assert not wait_completed.is_set()
        await subscription.resume()
        await asyncio.wait_for(wait_completed.wait(), timeout=1.0)
        assert was_paused_result is True
        await task


class TestConcurrentPauseResume:
    @pytest.mark.asyncio
    async def test_concurrent_pause_calls(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        subscription = Subscription(
            name="TestSubscription", config=config, subscriber=mock_subscriber
        )
        await subscription.transition_to(SubscriptionState.LIVE)
        results = await asyncio.gather(
            subscription.pause(), subscription.pause(), return_exceptions=True
        )
        errors = [r for r in results if isinstance(r, Exception)]
        assert len(errors) >= 1
        assert subscription.state == SubscriptionState.PAUSED

    @pytest.mark.asyncio
    async def test_rapid_pause_resume_cycles(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        subscription = Subscription(
            name="TestSubscription", config=config, subscriber=mock_subscriber
        )
        await subscription.transition_to(SubscriptionState.LIVE)
        for _ in range(10):
            await subscription.pause()
            assert subscription.state == SubscriptionState.PAUSED
            await subscription.resume()
            assert subscription.state == SubscriptionState.LIVE


class TestMultiplePauseResumeCycles:
    @pytest.mark.asyncio
    async def test_multiple_pause_resume_preserves_position(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.LIVE)
        for i in range(5):
            await subscription.record_event_processed(
                position=pos(i), event_id=uuid4(), event_type="TestEvent"
            )
            await subscription.pause()
            await subscription.resume()
        assert subscription.last_processed_position == pos(4)

    @pytest.mark.asyncio
    async def test_pause_resume_from_different_states(self, subscription: Subscription):
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.pause()
        assert subscription.state_before_pause == SubscriptionState.CATCHING_UP
        await subscription.resume()
        assert subscription.state == SubscriptionState.CATCHING_UP
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.pause()
        assert subscription.state_before_pause == SubscriptionState.LIVE
        await subscription.resume()
        assert subscription.state == SubscriptionState.LIVE
