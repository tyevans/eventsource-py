"""Unit tests for Subscription creation, state transitions, lifecycle properties, and errors."""

from datetime import datetime

import pytest

from eventsource.application.subscriptions import (
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
    SubscriptionStateError,
)
from tests.unit.application.subscriptions.test_subscription_state._fixtures import (
    MockSubscriber,
)


class TestSubscriptionCreation:
    """Tests for Subscription creation and initialization."""

    def test_create_subscription(self, mock_subscriber: MockSubscriber, config: SubscriptionConfig):
        """Test creating a subscription."""
        subscription = Subscription(
            name="TestSubscription",
            config=config,
            subscriber=mock_subscriber,
        )
        assert subscription.name == "TestSubscription"
        assert subscription.config == config
        assert subscription.subscriber == mock_subscriber

    def test_initial_state_is_starting(self, subscription: Subscription):
        """Test initial state is STARTING."""
        assert subscription.state == SubscriptionState.STARTING

    def test_initial_position_is_none(self, subscription: Subscription):
        """Test initial position is None -- nothing processed yet."""
        assert subscription.last_processed_position is None

    def test_initial_event_id_is_none(self, subscription: Subscription):
        """Test initial event_id is None."""
        assert subscription.last_event_id is None

    def test_initial_event_type_is_none(self, subscription: Subscription):
        """Test initial event_type is None."""
        assert subscription.last_event_type is None

    def test_initial_events_processed_is_zero(self, subscription: Subscription):
        """Test initial events_processed is 0."""
        assert subscription.events_processed == 0

    def test_initial_events_failed_is_zero(self, subscription: Subscription):
        """Test initial events_failed is 0."""
        assert subscription.events_failed == 0

    def test_initial_last_processed_at_is_none(self, subscription: Subscription):
        """Test initial last_processed_at is None."""
        assert subscription.last_processed_at is None

    def test_started_at_is_set(self, subscription: Subscription):
        """Test started_at is set on creation."""
        assert subscription.started_at is not None
        assert isinstance(subscription.started_at, datetime)

    def test_initial_last_error_is_none(self, subscription: Subscription):
        """Test initial last_error is None."""
        assert subscription.last_error is None

    def test_initial_last_error_at_is_none(self, subscription: Subscription):
        """Test initial last_error_at is None."""
        assert subscription.last_error_at is None


class TestSubscriptionStateTransitions:
    """Tests for state transitions."""

    @pytest.mark.asyncio
    async def test_transition_starting_to_catching_up(self, subscription: Subscription):
        """Test transition from STARTING to CATCHING_UP."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        assert subscription.state == SubscriptionState.CATCHING_UP

    @pytest.mark.asyncio
    async def test_transition_starting_to_live(self, subscription: Subscription):
        """Test transition from STARTING to LIVE."""
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.state == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_transition_starting_to_stopped(self, subscription: Subscription):
        """Test transition from STARTING to STOPPED."""
        await subscription.transition_to(SubscriptionState.STOPPED)
        assert subscription.state == SubscriptionState.STOPPED

    @pytest.mark.asyncio
    async def test_transition_starting_to_error(self, subscription: Subscription):
        """Test transition from STARTING to ERROR."""
        await subscription.transition_to(SubscriptionState.ERROR)
        assert subscription.state == SubscriptionState.ERROR

    @pytest.mark.asyncio
    async def test_transition_catching_up_to_live(self, subscription: Subscription):
        """Test transition from CATCHING_UP to LIVE."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.state == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_transition_catching_up_to_paused(self, subscription: Subscription):
        """Test transition from CATCHING_UP to PAUSED."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.PAUSED)
        assert subscription.state == SubscriptionState.PAUSED

    @pytest.mark.asyncio
    async def test_transition_live_to_catching_up(self, subscription: Subscription):
        """Test transition from LIVE to CATCHING_UP (falls behind)."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        assert subscription.state == SubscriptionState.CATCHING_UP

    @pytest.mark.asyncio
    async def test_transition_live_to_paused(self, subscription: Subscription):
        """Test transition from LIVE to PAUSED."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.transition_to(SubscriptionState.PAUSED)
        assert subscription.state == SubscriptionState.PAUSED

    @pytest.mark.asyncio
    async def test_transition_paused_to_live(self, subscription: Subscription):
        """Test transition from PAUSED to LIVE."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.transition_to(SubscriptionState.PAUSED)
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.state == SubscriptionState.LIVE

    @pytest.mark.asyncio
    async def test_transition_paused_to_catching_up(self, subscription: Subscription):
        """Test transition from PAUSED to CATCHING_UP."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.PAUSED)
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        assert subscription.state == SubscriptionState.CATCHING_UP

    @pytest.mark.asyncio
    async def test_transition_error_to_starting(self, subscription: Subscription):
        """Test transition from ERROR to STARTING (restart)."""
        await subscription.transition_to(SubscriptionState.ERROR)
        await subscription.transition_to(SubscriptionState.STARTING)
        assert subscription.state == SubscriptionState.STARTING

    @pytest.mark.asyncio
    async def test_previous_state_tracked(self, subscription: Subscription):
        """Test previous state is tracked after transition."""
        assert subscription.previous_state is None
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        assert subscription.previous_state == SubscriptionState.STARTING

    @pytest.mark.asyncio
    async def test_previous_state_updated_on_each_transition(self, subscription: Subscription):
        """Test previous state is updated on each transition."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        assert subscription.previous_state == SubscriptionState.STARTING
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.previous_state == SubscriptionState.CATCHING_UP


class TestInvalidStateTransitions:
    """Tests for invalid state transitions."""

    @pytest.mark.asyncio
    async def test_starting_to_paused_raises_error(self, subscription: Subscription):
        """Test STARTING -> PAUSED raises SubscriptionStateError."""
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.transition_to(SubscriptionState.PAUSED)
        assert "Cannot transition from starting to paused" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_catching_up_to_starting_raises_error(self, subscription: Subscription):
        """Test CATCHING_UP -> STARTING raises SubscriptionStateError."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.transition_to(SubscriptionState.STARTING)
        assert "Cannot transition from catching_up to starting" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_stopped_to_any_raises_error(self, subscription: Subscription):
        """Test STOPPED -> any raises SubscriptionStateError."""
        await subscription.transition_to(SubscriptionState.STOPPED)
        for state in SubscriptionState:
            with pytest.raises(SubscriptionStateError) as exc_info:
                await subscription.transition_to(state)
            assert "Cannot transition from stopped" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_error_to_non_starting_raises_error(self, subscription: Subscription):
        """Test ERROR -> non-STARTING raises SubscriptionStateError."""
        await subscription.transition_to(SubscriptionState.ERROR)
        for state in [
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
            SubscriptionState.PAUSED,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        ]:
            with pytest.raises(SubscriptionStateError) as exc_info:
                await subscription.transition_to(state)
            assert "Cannot transition from error" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_error_message_includes_valid_transitions(self, subscription: Subscription):
        """Test error message includes valid transitions."""
        with pytest.raises(SubscriptionStateError) as exc_info:
            await subscription.transition_to(SubscriptionState.PAUSED)
        error_msg = str(exc_info.value)
        assert "Valid transitions:" in error_msg


class TestSubscriptionProperties:
    """Tests for subscription properties."""

    @pytest.mark.asyncio
    async def test_is_running_when_catching_up(self, subscription: Subscription):
        """Test is_running is True when catching up."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        assert subscription.is_running is True

    @pytest.mark.asyncio
    async def test_is_running_when_live(self, subscription: Subscription):
        """Test is_running is True when live."""
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.is_running is True

    @pytest.mark.asyncio
    async def test_is_running_when_paused(self, subscription: Subscription):
        """Test is_running is False when paused."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.PAUSED)
        assert subscription.is_running is False

    @pytest.mark.asyncio
    async def test_is_running_when_stopped(self, subscription: Subscription):
        """Test is_running is False when stopped."""
        await subscription.transition_to(SubscriptionState.STOPPED)
        assert subscription.is_running is False

    @pytest.mark.asyncio
    async def test_is_running_when_error(self, subscription: Subscription):
        """Test is_running is False when error."""
        await subscription.transition_to(SubscriptionState.ERROR)
        assert subscription.is_running is False

    @pytest.mark.asyncio
    async def test_is_terminal_when_stopped(self, subscription: Subscription):
        """Test is_terminal is True when stopped."""
        await subscription.transition_to(SubscriptionState.STOPPED)
        assert subscription.is_terminal is True

    @pytest.mark.asyncio
    async def test_is_terminal_when_error(self, subscription: Subscription):
        """Test is_terminal is True when error."""
        await subscription.transition_to(SubscriptionState.ERROR)
        assert subscription.is_terminal is True

    @pytest.mark.asyncio
    async def test_is_terminal_when_running(self, subscription: Subscription):
        """Test is_terminal is False when running."""
        await subscription.transition_to(SubscriptionState.LIVE)
        assert subscription.is_terminal is False

    def test_uptime_seconds(self, subscription: Subscription):
        """Test uptime_seconds calculation."""
        uptime = subscription.uptime_seconds
        assert uptime >= 0
        assert isinstance(uptime, float)


class TestSetError:
    """Tests for set_error method."""

    @pytest.mark.asyncio
    async def test_set_error_records_error(self, subscription: Subscription):
        """Test set_error records the error."""
        error = ValueError("Test error")
        await subscription.set_error(error)
        assert subscription.last_error is error

    @pytest.mark.asyncio
    async def test_set_error_transitions_to_error_state(self, subscription: Subscription):
        """Test set_error transitions to ERROR state."""
        await subscription.set_error(ValueError("Test error"))
        assert subscription.state == SubscriptionState.ERROR

    @pytest.mark.asyncio
    async def test_set_error_increments_failed_counter(self, subscription: Subscription):
        """Test set_error increments failed counter."""
        await subscription.set_error(ValueError("Test error"))
        assert subscription.events_failed == 1
