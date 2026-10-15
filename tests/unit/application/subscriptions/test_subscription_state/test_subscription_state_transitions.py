"""Unit tests for SubscriptionState enum and state transition mapping."""

from eventsource.application.subscriptions import (
    VALID_TRANSITIONS,
    SubscriptionState,
    is_valid_transition,
)


class TestSubscriptionStateEnum:
    """Tests for SubscriptionState enum."""

    def test_starting_value(self):
        assert SubscriptionState.STARTING.value == "starting"

    def test_catching_up_value(self):
        assert SubscriptionState.CATCHING_UP.value == "catching_up"

    def test_live_value(self):
        assert SubscriptionState.LIVE.value == "live"

    def test_paused_value(self):
        assert SubscriptionState.PAUSED.value == "paused"

    def test_stopped_value(self):
        assert SubscriptionState.STOPPED.value == "stopped"

    def test_error_value(self):
        assert SubscriptionState.ERROR.value == "error"

    def test_all_states_exist(self):
        expected = {"starting", "catching_up", "live", "paused", "stopped", "error"}
        assert {state.value for state in SubscriptionState} == expected


class TestValidTransitions:
    """Tests for VALID_TRANSITIONS dict."""

    def test_starting_transitions(self):
        expected = {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }
        assert VALID_TRANSITIONS[SubscriptionState.STARTING] == expected

    def test_catching_up_transitions(self):
        expected = {
            SubscriptionState.LIVE,
            SubscriptionState.PAUSED,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }
        assert VALID_TRANSITIONS[SubscriptionState.CATCHING_UP] == expected

    def test_live_transitions(self):
        expected = {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.PAUSED,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }
        assert VALID_TRANSITIONS[SubscriptionState.LIVE] == expected

    def test_paused_transitions(self):
        expected = {
            SubscriptionState.CATCHING_UP,
            SubscriptionState.LIVE,
            SubscriptionState.STOPPED,
            SubscriptionState.ERROR,
        }
        assert VALID_TRANSITIONS[SubscriptionState.PAUSED] == expected

    def test_stopped_is_terminal(self):
        assert VALID_TRANSITIONS[SubscriptionState.STOPPED] == set()

    def test_error_can_restart(self):
        assert VALID_TRANSITIONS[SubscriptionState.ERROR] == {SubscriptionState.STARTING}


class TestIsValidTransition:
    """Tests for is_valid_transition function."""

    def test_starting_to_catching_up_valid(self):
        assert is_valid_transition(SubscriptionState.STARTING, SubscriptionState.CATCHING_UP)

    def test_starting_to_live_valid(self):
        assert is_valid_transition(SubscriptionState.STARTING, SubscriptionState.LIVE)

    def test_starting_to_stopped_valid(self):
        assert is_valid_transition(SubscriptionState.STARTING, SubscriptionState.STOPPED)

    def test_starting_to_error_valid(self):
        assert is_valid_transition(SubscriptionState.STARTING, SubscriptionState.ERROR)

    def test_starting_to_paused_invalid(self):
        assert not is_valid_transition(SubscriptionState.STARTING, SubscriptionState.PAUSED)

    def test_catching_up_to_live_valid(self):
        assert is_valid_transition(SubscriptionState.CATCHING_UP, SubscriptionState.LIVE)

    def test_catching_up_to_paused_valid(self):
        assert is_valid_transition(SubscriptionState.CATCHING_UP, SubscriptionState.PAUSED)

    def test_catching_up_to_stopped_valid(self):
        assert is_valid_transition(SubscriptionState.CATCHING_UP, SubscriptionState.STOPPED)

    def test_catching_up_to_error_valid(self):
        assert is_valid_transition(SubscriptionState.CATCHING_UP, SubscriptionState.ERROR)

    def test_catching_up_to_starting_invalid(self):
        assert not is_valid_transition(SubscriptionState.CATCHING_UP, SubscriptionState.STARTING)

    def test_live_to_catching_up_valid(self):
        assert is_valid_transition(SubscriptionState.LIVE, SubscriptionState.CATCHING_UP)

    def test_live_to_paused_valid(self):
        assert is_valid_transition(SubscriptionState.LIVE, SubscriptionState.PAUSED)

    def test_live_to_stopped_valid(self):
        assert is_valid_transition(SubscriptionState.LIVE, SubscriptionState.STOPPED)

    def test_live_to_error_valid(self):
        assert is_valid_transition(SubscriptionState.LIVE, SubscriptionState.ERROR)

    def test_live_to_starting_invalid(self):
        assert not is_valid_transition(SubscriptionState.LIVE, SubscriptionState.STARTING)

    def test_paused_to_catching_up_valid(self):
        assert is_valid_transition(SubscriptionState.PAUSED, SubscriptionState.CATCHING_UP)

    def test_paused_to_live_valid(self):
        assert is_valid_transition(SubscriptionState.PAUSED, SubscriptionState.LIVE)

    def test_paused_to_stopped_valid(self):
        assert is_valid_transition(SubscriptionState.PAUSED, SubscriptionState.STOPPED)

    def test_paused_to_error_valid(self):
        assert is_valid_transition(SubscriptionState.PAUSED, SubscriptionState.ERROR)

    def test_paused_to_starting_invalid(self):
        assert not is_valid_transition(SubscriptionState.PAUSED, SubscriptionState.STARTING)

    def test_stopped_to_any_invalid(self):
        for state in SubscriptionState:
            assert not is_valid_transition(SubscriptionState.STOPPED, state)

    def test_error_to_starting_valid(self):
        assert is_valid_transition(SubscriptionState.ERROR, SubscriptionState.STARTING)

    def test_error_to_other_invalid(self):
        for state in SubscriptionState:
            if state != SubscriptionState.STARTING:
                assert not is_valid_transition(SubscriptionState.ERROR, state)
