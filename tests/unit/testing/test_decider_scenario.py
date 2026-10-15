"""Tests for DeciderScenario."""

from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.domain.exceptions import CommandRejectedError
from eventsource.testing import DeciderScenario
from tests.unit.domain.test_decider_aggregate import (
    Account,
    AccountOpened,
    AccountState,
    DepositMoney,
    MoneyDeposited,
    OpenAccount,
)


class TestGivenWhenThen:
    def test_then_events_asserts_types_in_order(self) -> None:
        agg_id = uuid4()
        (
            DeciderScenario(Account)
            .given(AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner="alice"))
            .when(DepositMoney(account_id=agg_id, amount=5.0))
            .then_events(MoneyDeposited)
        )

    def test_then_events_fails_on_wrong_type(self) -> None:
        agg_id = uuid4()
        scenario = (
            DeciderScenario(Account)
            .given(AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner="alice"))
            .when(DepositMoney(account_id=agg_id, amount=5.0))
        )
        with pytest.raises(AssertionError):
            scenario.then_events(AccountOpened)

    def test_then_rejected_default_type_and_match(self) -> None:
        (
            DeciderScenario(Account)
            .when(DepositMoney(account_id=uuid4(), amount=5.0))
            .then_rejected(match="not open")
        )

    def test_then_rejected_accepts_custom_exception(self) -> None:
        (
            DeciderScenario(Account)
            .when(DepositMoney(account_id=uuid4(), amount=5.0))
            .then_rejected(CommandRejectedError)
        )

    def test_then_events_reports_unexpected_rejection(self) -> None:
        scenario = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            scenario.then_events(MoneyDeposited)

    def test_then_rejected_fails_when_events_produced(self) -> None:
        scenario = DeciderScenario(Account).when(OpenAccount(account_id=uuid4(), owner="alice"))
        with pytest.raises(AssertionError):
            scenario.then_rejected()

    def test_events_property_exposes_produced_events(self) -> None:
        scenario = DeciderScenario(Account).when(OpenAccount(account_id=uuid4(), owner="alice"))
        assert len(scenario.events) == 1
        assert isinstance(scenario.events[0], AccountOpened)

    def test_three_function_form(self) -> None:
        (
            DeciderScenario(
                decide=Account.decide,
                evolve=Account.evolve,
                initial_state=Account.initial_state,
            )
            .when(OpenAccount(account_id=uuid4(), owner="alice"))
            .then_events(AccountOpened)
        )

    def test_when_before_then_required(self) -> None:
        with pytest.raises(AssertionError, match="when"):
            DeciderScenario(Account).then_events(AccountOpened)

    def test_when_clears_stale_error_from_prior_rejection(self) -> None:
        scenario = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        scenario.then_rejected()  # sanity: first when() did reject
        scenario.when(OpenAccount(account_id=uuid4(), owner="alice"))
        scenario.then_events(AccountOpened)


class TestMultiAggregateDeciderScenario:
    def test_multi_aggregate_state_isolation(self) -> None:
        id_1 = uuid4()
        id_2 = uuid4()

        scenario = DeciderScenario(Account).given(
            AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"),
            AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"),
            MoneyDeposited(aggregate_id=id_1, aggregate_version=2, amount=100.0),
            MoneyDeposited(aggregate_id=id_2, aggregate_version=2, amount=50.0),
        )

        # States are isolated per aggregate ID
        assert scenario.get_state(id_1).owner == "alice"
        assert scenario.get_state(id_1).balance == 100.0
        assert scenario.get_state(id_2).owner == "bob"
        assert scenario.get_state(id_2).balance == 50.0

        # Command for aggregate 1 evaluates against aggregate 1's state
        scenario.when(DepositMoney(account_id=id_1, amount=25.0))
        scenario.then_events(MoneyDeposited)
        assert len(scenario.events) == 1
        assert scenario.events[0].aggregate_id == id_1
        assert scenario.events[0].amount == 25.0

        # Command for aggregate 2 evaluates against aggregate 2's state
        scenario.when(DepositMoney(account_id=id_2, amount=10.0))
        scenario.then_events(MoneyDeposited)
        assert len(scenario.events) == 1
        assert scenario.events[0].aggregate_id == id_2
        assert scenario.events[0].amount == 10.0

    def test_multi_aggregate_explicit_aggregate_id(self) -> None:
        id_1 = uuid4()
        id_2 = uuid4()

        scenario = DeciderScenario(Account).given(
            AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"),
            AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"),
        )

        # Deposit using explicit aggregate_id kwarg
        scenario.when(DepositMoney(account_id=id_1, amount=15.0), aggregate_id=id_1)
        scenario.then_events(MoneyDeposited)
        assert scenario.events[0].aggregate_id == id_1

    def test_command_targeting_fresh_aggregate_in_multi_aggregate_scenario(self) -> None:
        id_1 = uuid4()
        id_2 = uuid4()

        scenario = DeciderScenario(Account).given(
            AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice")
        )

        # Command targets new aggregate id_2 (not in history)
        scenario.when(OpenAccount(account_id=id_2, owner="bob"))
        scenario.then_events(AccountOpened)
        assert scenario.events[0].aggregate_id == id_2

    def test_states_property_and_state_property_in_multi_aggregate(self) -> None:
        id_1 = uuid4()
        id_2 = uuid4()

        scenario = DeciderScenario(Account)
        # Empty scenario returns initial state
        assert scenario.state.is_open is False

        scenario.given(AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"))
        # Single aggregate returns that state
        assert scenario.state.owner == "alice"

        scenario.given(AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"))
        # Multi-aggregate .states dict returns all states
        assert len(scenario.states) == 2
        assert scenario.states[id_1].owner == "alice"
        assert scenario.states[id_2].owner == "bob"

        # .state raises ValueError when ambiguous (multiple aggregates and no last target)
        with pytest.raises(ValueError, match="Multiple aggregate states exist"):
            _ = scenario.state

    def test_unresolvable_aggregate_id_raises_value_error(self) -> None:
        id_1 = uuid4()
        id_2 = uuid4()

        scenario = DeciderScenario(Account).given(
            AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"),
            AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"),
        )

        class UnknownCommand:
            pass

        with pytest.raises(ValueError, match="Multiple aggregates exist"):
            scenario.when(UnknownCommand())

    def test_given_accepts_nested_sequences(self) -> None:
        id_1 = uuid4()
        id_2 = uuid4()

        events_batch = [
            AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"),
            AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"),
        ]
        scenario = DeciderScenario(Account).given(events_batch)
        assert len(scenario.states) == 2
        assert scenario.get_state(id_1).owner == "alice"
        assert scenario.get_state(id_2).owner == "bob"


class TestWhenAllAndThenState:
    def test_when_all_folds_commands_sequentially_and_collects_events(self) -> None:
        agg_id = uuid4()
        scenario = (
            DeciderScenario(Account)
            .when_all(
                OpenAccount(account_id=agg_id, owner="alice"),
                DepositMoney(account_id=agg_id, amount=40.0),
                DepositMoney(account_id=agg_id, amount=15.0),
            )
            .then_events(AccountOpened, MoneyDeposited, MoneyDeposited)
            .then_state(lambda s: s.is_open and s.owner == "alice" and s.balance == 55.0)
        )
        assert len(scenario.events) == 3

    def test_when_all_stops_on_first_rejection(self) -> None:
        agg_id = uuid4()
        (
            DeciderScenario(Account)
            .when_all(
                DepositMoney(account_id=agg_id, amount=10.0),
                OpenAccount(account_id=agg_id, owner="alice"),
            )
            .then_rejected(match="not open")
        )

    def test_then_state_accepts_exact_state_value(self) -> None:
        agg_id = uuid4()
        (
            DeciderScenario(Account)
            .when_all(
                OpenAccount(account_id=agg_id, owner="alice"),
                DepositMoney(account_id=agg_id, amount=25.0),
            )
            .then_state(AccountState(owner="alice", balance=25.0, is_open=True))
        )

    def test_then_state_fails_on_mismatch_or_false_predicate(self) -> None:
        agg_id = uuid4()
        scenario = DeciderScenario(Account).when(OpenAccount(account_id=agg_id, owner="alice"))

        with pytest.raises(AssertionError, match="state predicate"):
            scenario.then_state(lambda s: s.balance > 0)

        with pytest.raises(AssertionError, match="expected state"):
            scenario.then_state(AccountState(owner="bob", balance=0.0, is_open=True))

    def test_then_state_fails_when_command_was_rejected(self) -> None:
        scenario = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            scenario.then_state(lambda s: True)

    def test_private_state_shim_is_removed(self) -> None:
        assert not hasattr(DeciderScenario, "_state")


class TestGivenCommands:
    def test_given_commands_alone_with_when_and_then_state(self) -> None:
        agg_id = uuid4()
        scenario = (
            DeciderScenario(Account)
            .given_commands(
                OpenAccount(account_id=agg_id, owner="alice"),
                DepositMoney(account_id=agg_id, amount=20.0),
            )
            .when(DepositMoney(account_id=agg_id, amount=15.0))
            .then_events(MoneyDeposited)
            .then_state(AccountState(owner="alice", balance=35.0, is_open=True))
        )
        assert len(scenario.events) == 1
        assert isinstance(scenario.events[0], MoneyDeposited)
        assert scenario.events[0].amount == 15.0

    def test_given_commands_chained_with_given_and_when_all_in_any_order(self) -> None:
        agg_1 = uuid4()
        (
            DeciderScenario(Account)
            .given(AccountOpened(aggregate_id=agg_1, aggregate_version=1, owner="alice"))
            .given_commands(DepositMoney(account_id=agg_1, amount=10.0))
            .when_all(
                DepositMoney(account_id=agg_1, amount=5.0),
                DepositMoney(account_id=agg_1, amount=25.0),
            )
            .then_events(MoneyDeposited, MoneyDeposited)
            .then_state(AccountState(owner="alice", balance=40.0, is_open=True))
        )

        agg_2 = uuid4()
        (
            DeciderScenario(Account)
            .given_commands(OpenAccount(account_id=agg_2, owner="bob"))
            .given(MoneyDeposited(aggregate_id=agg_2, aggregate_version=2, amount=30.0))
            .when(DepositMoney(account_id=agg_2, amount=12.0))
            .then_events(MoneyDeposited)
            .then_state(AccountState(owner="bob", balance=42.0, is_open=True))
        )

    @given(
        owner=st.text(min_size=1, max_size=20),
        deposits=st.lists(
            st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False),
            max_size=10,
        ),
        final_deposit=st.floats(
            min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False
        ),
    )
    def test_given_commands_equivalent_to_given_emitted_events(
        self, owner: str, deposits: list[float], final_deposit: float
    ) -> None:
        agg_id = uuid4()
        commands = [
            OpenAccount(account_id=agg_id, owner=owner),
            *(DepositMoney(account_id=agg_id, amount=amt) for amt in deposits),
        ]
        events = [
            AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner=owner),
            *(
                MoneyDeposited(aggregate_id=agg_id, aggregate_version=i + 2, amount=amt)
                for i, amt in enumerate(deposits)
            ),
        ]
        when_cmd = DepositMoney(account_id=agg_id, amount=final_deposit)

        by_commands = DeciderScenario(Account).given_commands(*commands).when(when_cmd)
        by_events = DeciderScenario(Account).given(*events).when(when_cmd)

        assert by_commands.state == by_events.state
        assert [type(e) for e in by_commands.events] == [type(e) for e in by_events.events]
