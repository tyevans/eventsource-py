"""
Unit tests for subscription state machine.

Tests all state transitions, subscription class behavior, position tracking,
statistics, and status snapshots.
"""

import asyncio
import contextlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.protocols import EventSubscriber
from eventsource.subscriptions import (
    VALID_TRANSITIONS,
    BatchHandler,
    EventHandler,
    Subscription,
    SubscriptionConfig,
    SubscriptionState,
    SubscriptionStateError,
    SubscriptionStatus,
    is_valid_transition,
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


# SubscriptionState Enum Tests


class TestSubscriptionStateEnum:
    """Tests for SubscriptionState enum."""

    def test_starting_value(self):
        """Test STARTING has correct value."""
        assert SubscriptionState.STARTING.value == "starting"

    def test_catching_up_value(self):
        """Test CATCHING_UP has correct value."""
        assert SubscriptionState.CATCHING_UP.value == "catching_up"

    def test_live_value(self):
        """Test LIVE has correct value."""
        assert SubscriptionState.LIVE.value == "live"

    def test_paused_value(self):
        """Test PAUSED has correct value."""
        assert SubscriptionState.PAUSED.value == "paused"

    def test_stopped_value(self):
        """Test STOPPED has correct value."""
        assert SubscriptionState.STOPPED.value == "stopped"

    def test_error_value(self):
        """Test ERROR has correct value."""
        assert SubscriptionState.ERROR.value == "error"

    def test_all_states_exist(self):
        """Test all expected states exist."""
        states = list(SubscriptionState)
        assert len(states) == 6
        assert SubscriptionState.STARTING in states
        assert SubscriptionState.CATCHING_UP in states
        assert SubscriptionState.LIVE in states
        assert SubscriptionState.PAUSED in states
        assert SubscriptionState.STOPPED in states
        assert SubscriptionState.ERROR in states


# State Transition Validation Tests


class TestValidTransitions:
    """Tests for VALID_TRANSITIONS dict."""

    def test_starting_transitions(self):
        """Test valid transitions from STARTING."""
        expected = {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }
        assert VALID_TRANSITIONS[SubscriptionState.STARTING] == expected

    def test_catching_up_transitions(self):
        """Test valid transitions from CATCHING_UP."""
        expected = {
            SubscriptionState.LIVE,
            SubscriptionState.PAUSED,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }
        assert VALID_TRANSITIONS[SubscriptionState.CATCHING_UP] == expected

    def test_live_transitions(self):
        """Test valid transitions from LIVE."""
        expected = {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.PAUSED,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }
        assert VALID_TRANSITIONS[SubscriptionState.LIVE] == expected

    def test_paused_transitions(self):
        """Test valid transitions from PAUSED."""
        expected = {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }
        assert VALID_TRANSITIONS[SubscriptionState.PAUSED] == expected

    def test_stopped_is_terminal(self):
        """Test STOPPED has no valid transitions (terminal)."""
        assert VALID_TRANSITIONS[SubscriptionState.STOPPED] == set()

    def test_error_can_restart(self):
        """Test ERROR can transition to STARTING (restart)."""
        expected = {SubscriptionState.STARTING}
        assert VALID_TRANSITIONS[SubscriptionState.ERROR] == expected


class TestIsValidTransition:
    """Tests for is_valid_transition function."""

    def test_starting_to_catching_up_valid(self):
        """Test STARTING -> CATCHING_UP is valid."""
        assert is_valid_transition(
            SubscriptionState.STARTING,
            SubscriptionState.CATCHING_UP,
        )

    def test_starting_to_live_valid(self):
        """Test STARTING -> LIVE is valid."""
        assert is_valid_transition(
            SubscriptionState.STARTING,
            SubscriptionState.LIVE,
        )

    def test_starting_to_stopped_valid(self):
        """Test STARTING -> STOPPED is valid."""
        assert is_valid_transition(
            SubscriptionState.STARTING,
            SubscriptionState.STOPPED,
        )

    def test_starting_to_error_valid(self):
        """Test STARTING -> ERROR is valid."""
        assert is_valid_transition(
            SubscriptionState.STARTING,
            SubscriptionState.ERROR,
        )

    def test_starting_to_paused_invalid(self):
        """Test STARTING -> PAUSED is invalid."""
        assert not is_valid_transition(
            SubscriptionState.STARTING,
            SubscriptionState.PAUSED,
        )

    def test_catching_up_to_live_valid(self):
        """Test CATCHING_UP -> LIVE is valid."""
        assert is_valid_transition(
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
        )

    def test_catching_up_to_paused_valid(self):
        """Test CATCHING_UP -> PAUSED is valid."""
        assert is_valid_transition(
            SubscriptionState.CATCHING_UP,
            SubscriptionState.PAUSED,
        )

    def test_catching_up_to_stopped_valid(self):
        """Test CATCHING_UP -> STOPPED is valid."""
        assert is_valid_transition(
            SubscriptionState.CATCHING_UP,
            SubscriptionState.STOPPED,
        )

    def test_catching_up_to_error_valid(self):
        """Test CATCHING_UP -> ERROR is valid."""
        assert is_valid_transition(
            SubscriptionState.CATCHING_UP,
            SubscriptionState.ERROR,
        )

    def test_catching_up_to_starting_invalid(self):
        """Test CATCHING_UP -> STARTING is invalid."""
        assert not is_valid_transition(
            SubscriptionState.CATCHING_UP,
            SubscriptionState.STARTING,
        )

    def test_live_to_catching_up_valid(self):
        """Test LIVE -> CATCHING_UP is valid (falls behind)."""
        assert is_valid_transition(
            SubscriptionState.LIVE,
            SubscriptionState.CATCHING_UP,
        )

    def test_live_to_paused_valid(self):
        """Test LIVE -> PAUSED is valid."""
        assert is_valid_transition(
            SubscriptionState.LIVE,
            SubscriptionState.PAUSED,
        )

    def test_live_to_stopped_valid(self):
        """Test LIVE -> STOPPED is valid."""
        assert is_valid_transition(
            SubscriptionState.LIVE,
            SubscriptionState.STOPPED,
        )

    def test_live_to_error_valid(self):
        """Test LIVE -> ERROR is valid."""
        assert is_valid_transition(
            SubscriptionState.LIVE,
            SubscriptionState.ERROR,
        )

    def test_live_to_starting_invalid(self):
        """Test LIVE -> STARTING is invalid."""
        assert not is_valid_transition(
            SubscriptionState.LIVE,
            SubscriptionState.STARTING,
        )

    def test_paused_to_catching_up_valid(self):
        """Test PAUSED -> CATCHING_UP is valid."""
        assert is_valid_transition(
            SubscriptionState.PAUSED,
            SubscriptionState.CATCHING_UP,
        )

    def test_paused_to_live_valid(self):
        """Test PAUSED -> LIVE is valid."""
        assert is_valid_transition(
            SubscriptionState.PAUSED,
            SubscriptionState.LIVE,
        )

    def test_paused_to_stopped_valid(self):
        """Test PAUSED -> STOPPED is valid."""
        assert is_valid_transition(
            SubscriptionState.PAUSED,
            SubscriptionState.STOPPED,
        )

    def test_paused_to_error_valid(self):
        """Test PAUSED -> ERROR is valid."""
        assert is_valid_transition(
            SubscriptionState.PAUSED,
            SubscriptionState.ERROR,
        )

    def test_paused_to_starting_invalid(self):
        """Test PAUSED -> STARTING is invalid."""
        assert not is_valid_transition(
            SubscriptionState.PAUSED,
            SubscriptionState.STARTING,
        )

    def test_stopped_to_any_invalid(self):
        """Test STOPPED -> any state is invalid (terminal)."""
        for state in SubscriptionState:
            assert not is_valid_transition(
                SubscriptionState.STOPPED,
                state,
            )

    def test_error_to_starting_valid(self):
        """Test ERROR -> STARTING is valid (restart)."""
        assert is_valid_transition(
            SubscriptionState.ERROR,
            SubscriptionState.STARTING,
        )

    def test_error_to_other_invalid(self):
        """Test ERROR -> other states is invalid."""
        for state in SubscriptionState:
            if state != SubscriptionState.STARTING:
                assert not is_valid_transition(
                    SubscriptionState.ERROR,
                    state,
                )


# Subscription Class Tests


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

    def test_initial_position_is_zero(self, subscription: Subscription):
        """Test initial position is 0."""
        assert subscription.last_processed_position == 0

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


# Position Tracking Tests


class TestPositionTracking:
    """Tests for position tracking."""

    @pytest.mark.asyncio
    async def test_record_event_processed_updates_position(self, subscription: Subscription):
        """Test recording event processed updates position."""
        event_id = uuid4()
        await subscription.record_event_processed(
            position=42,
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert subscription.last_processed_position == 42

    @pytest.mark.asyncio
    async def test_record_event_processed_updates_event_id(self, subscription: Subscription):
        """Test recording event processed updates event_id."""
        event_id = uuid4()
        await subscription.record_event_processed(
            position=42,
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert subscription.last_event_id == event_id

    @pytest.mark.asyncio
    async def test_record_event_processed_updates_event_type(self, subscription: Subscription):
        """Test recording event processed updates event_type."""
        event_id = uuid4()
        await subscription.record_event_processed(
            position=42,
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert subscription.last_event_type == "OrderCreated"

    @pytest.mark.asyncio
    async def test_record_event_processed_increments_counter(self, subscription: Subscription):
        """Test recording event processed increments counter."""
        event_id = uuid4()
        await subscription.record_event_processed(
            position=1,
            event_id=event_id,
            event_type="OrderCreated",
        )
        assert subscription.events_processed == 1

        await subscription.record_event_processed(
            position=2,
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
            position=42,
            event_id=event_id,
            event_type="OrderCreated",
        )
        after = datetime.now(UTC)

        assert subscription.last_processed_at is not None
        assert before <= subscription.last_processed_at <= after


# Statistics Tracking Tests


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


# Lag Calculation Tests


class TestLagCalculation:
    """Tests for lag calculation."""

    @pytest.mark.asyncio
    async def test_lag_with_no_max_position(self, subscription: Subscription):
        """Test lag is 0 when max position not set."""
        assert subscription.lag == 0

    @pytest.mark.asyncio
    async def test_lag_calculation(self, subscription: Subscription):
        """Test lag calculation."""
        await subscription.update_max_position(100)
        await subscription.record_event_processed(
            position=50,
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        assert subscription.lag == 50

    @pytest.mark.asyncio
    async def test_lag_is_never_negative(self, subscription: Subscription):
        """Test lag is never negative."""
        await subscription.update_max_position(50)
        await subscription.record_event_processed(
            position=100,
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        assert subscription.lag == 0

    @pytest.mark.asyncio
    async def test_lag_is_zero_when_caught_up(self, subscription: Subscription):
        """Test lag is 0 when caught up."""
        await subscription.update_max_position(100)
        await subscription.record_event_processed(
            position=100,
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        assert subscription.lag == 0


# Property Tests


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


# SubscriptionStatus Tests


class TestSubscriptionStatus:
    """Tests for SubscriptionStatus dataclass."""

    def test_status_creation(self):
        """Test creating a status snapshot."""
        status = SubscriptionStatus(
            name="TestSubscription",
            state="live",
            position=42,
            lag_events=10,
            events_processed=100,
            events_failed=2,
            last_processed_at="2024-01-01T12:00:00",
            started_at="2024-01-01T11:00:00",
            uptime_seconds=3600.0,
        )
        assert status.name == "TestSubscription"
        assert status.state == "live"
        assert status.position == 42
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
            position=42,
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
            position=42,
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
        assert result["position"] == 42
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
            position=42,
            lag_events=10,
            events_processed=100,
            events_failed=2,
            last_processed_at=None,
            started_at=None,
            uptime_seconds=0.0,
        )
        with pytest.raises(AttributeError):
            status.position = 100  # type: ignore[misc]


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
            position=42,
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        status = subscription.get_status()
        assert status.position == 42

    @pytest.mark.asyncio
    async def test_get_status_includes_lag(self, subscription: Subscription):
        """Test status includes lag."""
        await subscription.update_max_position(100)
        await subscription.record_event_processed(
            position=50,
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        status = subscription.get_status()
        assert status.lag_events == 50

    @pytest.mark.asyncio
    async def test_get_status_includes_statistics(self, subscription: Subscription):
        """Test status includes statistics."""
        await subscription.record_event_processed(
            position=1,
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
            position=1,
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


# Set Error Tests


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


# String Representation Tests


class TestStringRepresentation:
    """Tests for string representation."""

    def test_str_representation(self, subscription: Subscription):
        """Test __str__ method."""
        result = str(subscription)
        assert "TestSubscription" in result
        assert "starting" in result
        assert "pos=0" in result

    @pytest.mark.asyncio
    async def test_str_representation_after_processing(self, subscription: Subscription):
        """Test __str__ after processing events."""
        await subscription.transition_to(SubscriptionState.LIVE)
        await subscription.record_event_processed(
            position=42,
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        result = str(subscription)
        assert "live" in result
        assert "pos=42" in result


# Concurrency Tests


class TestConcurrency:
    """Tests for concurrent access safety."""

    @pytest.mark.asyncio
    async def test_concurrent_state_transitions_are_serialized(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        """Test concurrent state transitions are serialized."""
        subscription = Subscription(
            name="TestSubscription",
            config=config,
            subscriber=mock_subscriber,
        )

        # Attempt concurrent transitions
        async def transition_to_catching_up():
            await subscription.transition_to(SubscriptionState.CATCHING_UP)

        async def transition_to_live():
            # Wait a bit to ensure first transition starts
            await asyncio.sleep(0.001)
            # This should succeed since we'll be in CATCHING_UP
            with contextlib.suppress(SubscriptionStateError):
                await subscription.transition_to(SubscriptionState.LIVE)

        # Run concurrently
        await asyncio.gather(
            transition_to_catching_up(),
            transition_to_live(),
        )

        # State should be either CATCHING_UP or LIVE (both valid end states)
        assert subscription.state in {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
        }

    @pytest.mark.asyncio
    async def test_concurrent_event_recording(
        self, mock_subscriber: MockSubscriber, config: SubscriptionConfig
    ):
        """Test concurrent event recording is safe."""
        subscription = Subscription(
            name="TestSubscription",
            config=config,
            subscriber=mock_subscriber,
        )

        async def record_event(position: int):
            await subscription.record_event_processed(
                position=position,
                event_id=uuid4(),
                event_type="OrderCreated",
            )

        # Record 100 events concurrently
        await asyncio.gather(*[record_event(i) for i in range(100)])

        # All events should be counted
        assert subscription.events_processed == 100


# Type Alias Tests


class TestTypeAliases:
    """Tests for EventHandler and BatchHandler type aliases."""

    def test_event_handler_type_alias(self):
        """Test EventHandler type alias can be used."""

        async def my_handler(event: DomainEvent) -> None:
            pass

        handler: EventHandler = my_handler
        assert callable(handler)

    def test_batch_handler_type_alias(self):
        """Test BatchHandler type alias can be used."""
        from collections.abc import Sequence

        async def my_batch_handler(events: Sequence[DomainEvent]) -> None:
            pass

        handler: BatchHandler = my_batch_handler
        assert callable(handler)


# Module Import Tests


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_subscription_module(self):
        """Test imports from subscription module."""
        from eventsource.subscriptions.subscription import (
            VALID_TRANSITIONS,
            BatchHandler,
            EventHandler,
            Subscription,
            SubscriptionState,
            SubscriptionStatus,
            is_valid_transition,
        )

        assert SubscriptionState is not None
        assert SubscriptionStatus is not None
        assert Subscription is not None
        assert is_valid_transition is not None
        assert VALID_TRANSITIONS is not None
        assert EventHandler is not None
        assert BatchHandler is not None

    def test_import_from_subscriptions_package(self):
        """Test imports from subscriptions package."""
        from eventsource.subscriptions import (
            VALID_TRANSITIONS,
            BatchHandler,
            EventHandler,
            Subscription,
            SubscriptionState,
            SubscriptionStatus,
            is_valid_transition,
        )

        assert SubscriptionState is not None
        assert SubscriptionStatus is not None
        assert Subscription is not None
        assert is_valid_transition is not None
        assert VALID_TRANSITIONS is not None
        assert EventHandler is not None
        assert BatchHandler is not None
