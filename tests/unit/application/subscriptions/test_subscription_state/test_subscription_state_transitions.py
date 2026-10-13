"""Unit tests for SubscriptionState enum and state transition mapping."""

from eventsource.application.subscriptions import (
    VALID_TRANSITIONS,
    SubscriptionState,
    is_valid_transition,
)


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
        """Test all expected states are defined."""
        expected_states = {
            "starting",
            "catching_up",
            "live",
            "paused",
            "stopped",
            "error",
        }
        actual_states = {state.value for state in SubscriptionState}
        assert actual_states == expected_states


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
