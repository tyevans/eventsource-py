"""Unit tests for Subscription creation, state transitions, lifecycle properties, and errors."""

from datetime import datetime

import pytest

from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
    SubscriptionStateError,
)
from tests.unit.application.subscriptions.test_subscription_state._fixtures import MockSubscriber

S, Err = SubscriptionState, SubscriptionStateError


class TestSubscriptionCreation:
    def test_create_subscription(self, mock_subscriber: MockSubscriber, config: SubscriptionConfig):
        sub = Subscription(name="TestSubscription", config=config, subscriber=mock_subscriber)
        assert (sub.name, sub.config, sub.subscriber) == ("TestSubscription", config, mock_subscriber)  # fmt: skip

    def test_initial_state_is_starting(self, subscription: Subscription):
        assert subscription.state == S.STARTING

    def test_initial_position_is_none(self, subscription: Subscription):
        assert subscription.last_processed_position is None

    def test_initial_event_id_is_none(self, subscription: Subscription):
        assert subscription.last_event_id is None

    def test_initial_event_type_is_none(self, subscription: Subscription):
        assert subscription.last_event_type is None

    def test_initial_events_processed_is_zero(self, subscription: Subscription):
        assert subscription.events_processed == 0

    def test_initial_events_failed_is_zero(self, subscription: Subscription):
        assert subscription.events_failed == 0

    def test_initial_last_processed_at_is_none(self, subscription: Subscription):
        assert subscription.last_processed_at is None

    def test_started_at_is_set(self, subscription: Subscription):
        assert isinstance(subscription.started_at, datetime)

    def test_initial_last_error_is_none(self, subscription: Subscription):
        assert subscription.last_error is None

    def test_initial_last_error_at_is_none(self, subscription: Subscription):
        assert subscription.last_error_at is None


async def _walk(sub: Subscription, *states: SubscriptionState) -> Subscription:
    for s in states:
        await sub.transition_to(s)
    return sub


class TestSubscriptionStateTransitions:
    @pytest.mark.asyncio
    async def test_transition_starting_to_catching_up(self, subscription: Subscription):
        assert (await _walk(subscription, S.CATCHING_UP)).state == S.CATCHING_UP

    @pytest.mark.asyncio
    async def test_transition_starting_to_live(self, subscription: Subscription):
        assert (await _walk(subscription, S.LIVE)).state == S.LIVE

    @pytest.mark.asyncio
    async def test_transition_starting_to_stopped(self, subscription: Subscription):
        assert (await _walk(subscription, S.STOPPED)).state == S.STOPPED

    @pytest.mark.asyncio
    async def test_transition_starting_to_error(self, subscription: Subscription):
        assert (await _walk(subscription, S.ERROR)).state == S.ERROR

    @pytest.mark.asyncio
    async def test_transition_catching_up_to_live(self, subscription: Subscription):
        assert (await _walk(subscription, S.CATCHING_UP, S.LIVE)).state == S.LIVE

    @pytest.mark.asyncio
    async def test_transition_catching_up_to_paused(self, subscription: Subscription):
        assert (await _walk(subscription, S.CATCHING_UP, S.PAUSED)).state == S.PAUSED

    @pytest.mark.asyncio
    async def test_transition_live_to_catching_up(self, subscription: Subscription):
        assert (await _walk(subscription, S.LIVE, S.CATCHING_UP)).state == S.CATCHING_UP

    @pytest.mark.asyncio
    async def test_transition_live_to_paused(self, subscription: Subscription):
        assert (await _walk(subscription, S.LIVE, S.PAUSED)).state == S.PAUSED

    @pytest.mark.asyncio
    async def test_transition_paused_to_live(self, subscription: Subscription):
        assert (await _walk(subscription, S.LIVE, S.PAUSED, S.LIVE)).state == S.LIVE

    @pytest.mark.asyncio
    async def test_transition_paused_to_catching_up(self, subscription: Subscription):
        assert (await _walk(subscription, S.CATCHING_UP, S.PAUSED, S.CATCHING_UP)).state == S.CATCHING_UP  # fmt: skip

    @pytest.mark.asyncio
    async def test_transition_error_to_starting(self, subscription: Subscription):
        assert (await _walk(subscription, S.ERROR, S.STARTING)).state == S.STARTING

    @pytest.mark.asyncio
    async def test_previous_state_tracked(self, subscription: Subscription):
        assert subscription.previous_state is None
        assert (await _walk(subscription, S.CATCHING_UP)).previous_state == S.STARTING

    @pytest.mark.asyncio
    async def test_previous_state_updated_on_each_transition(self, subscription: Subscription):
        assert (await _walk(subscription, S.CATCHING_UP)).previous_state == S.STARTING
        assert (await _walk(subscription, S.LIVE)).previous_state == S.CATCHING_UP


class TestInvalidStateTransitions:
    @pytest.mark.asyncio
    async def test_starting_to_paused_raises_error(self, subscription: Subscription):
        with pytest.raises(Err, match="Cannot transition from starting to paused"):
            await subscription.transition_to(S.PAUSED)

    @pytest.mark.asyncio
    async def test_catching_up_to_starting_raises_error(self, subscription: Subscription):
        await subscription.transition_to(S.CATCHING_UP)
        with pytest.raises(Err, match="Cannot transition from catching_up to starting"):
            await subscription.transition_to(S.STARTING)

    @pytest.mark.asyncio
    async def test_stopped_to_any_raises_error(self, subscription: Subscription):
        await subscription.transition_to(S.STOPPED)
        for state in S:
            with pytest.raises(Err, match="Cannot transition from stopped"):
                await subscription.transition_to(state)

    @pytest.mark.asyncio
    async def test_error_to_non_starting_raises_error(self, subscription: Subscription):
        await subscription.transition_to(S.ERROR)
        for state in [s for s in S if s != S.STARTING]:
            with pytest.raises(Err, match="Cannot transition from error"):
                await subscription.transition_to(state)

    @pytest.mark.asyncio
    async def test_error_message_includes_valid_transitions(self, subscription: Subscription):
        with pytest.raises(Err, match="Valid transitions:"):
            await subscription.transition_to(S.PAUSED)


class TestSubscriptionProperties:
    @pytest.mark.asyncio
    async def test_is_running_when_catching_up(self, subscription: Subscription):
        assert (await _walk(subscription, S.CATCHING_UP)).is_running is True

    @pytest.mark.asyncio
    async def test_is_running_when_live(self, subscription: Subscription):
        assert (await _walk(subscription, S.LIVE)).is_running is True

    @pytest.mark.asyncio
    async def test_is_running_when_paused(self, subscription: Subscription):
        assert (await _walk(subscription, S.CATCHING_UP, S.PAUSED)).is_running is False

    @pytest.mark.asyncio
    async def test_is_running_when_stopped(self, subscription: Subscription):
        assert (await _walk(subscription, S.STOPPED)).is_running is False

    @pytest.mark.asyncio
    async def test_is_running_when_error(self, subscription: Subscription):
        assert (await _walk(subscription, S.ERROR)).is_running is False

    @pytest.mark.asyncio
    async def test_is_terminal_when_stopped(self, subscription: Subscription):
        assert (await _walk(subscription, S.STOPPED)).is_terminal is True

    @pytest.mark.asyncio
    async def test_is_terminal_when_error(self, subscription: Subscription):
        assert (await _walk(subscription, S.ERROR)).is_terminal is True

    @pytest.mark.asyncio
    async def test_is_terminal_when_running(self, subscription: Subscription):
        assert (await _walk(subscription, S.LIVE)).is_terminal is False

    def test_uptime_seconds(self, subscription: Subscription):
        assert subscription.uptime_seconds >= 0 and isinstance(subscription.uptime_seconds, float)


class TestSetError:
    @pytest.mark.asyncio
    async def test_set_error_records_error(self, subscription: Subscription):
        error = ValueError("Test error")
        await subscription.set_error(error)
        assert subscription.last_error is error

    @pytest.mark.asyncio
    async def test_set_error_transitions_to_error_state(self, subscription: Subscription):
        await subscription.set_error(ValueError("Test error"))
        assert subscription.state == S.ERROR

    @pytest.mark.asyncio
    async def test_set_error_increments_failed_counter(self, subscription: Subscription):
        await subscription.set_error(ValueError("Test error"))
        assert subscription.events_failed == 1
