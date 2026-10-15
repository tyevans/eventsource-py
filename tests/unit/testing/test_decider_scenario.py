"""Tests for DeciderScenario."""

from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.domain.exceptions import CommandRejectedError
from eventsource.testing import DeciderScenario
from eventsource.testing.scenario import Scenario
from tests.unit.domain.test_decider_aggregate import (
    Account,
    AccountOpened,
    AccountState,
    DepositMoney,
    MoneyDeposited,
    OpenAccount,
)


class TestGivenWhenThen:
    def test_then_events_and_failure_modes(self) -> None:
        agg_id = uuid4()
        scenario = (
            DeciderScenario(Account)
            .given(AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner="alice"))
            .when(DepositMoney(account_id=agg_id, amount=5.0))
            .then_events(MoneyDeposited)
        )
        with pytest.raises(AssertionError):
            scenario.then_events(AccountOpened)
        rejected = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            rejected.then_events(MoneyDeposited)

    def test_then_event_types_on_scenario_and_decider_scenario(self) -> None:
        for cls in (DeciderScenario, Scenario):
            agg_id = uuid4()
            res = (
                cls(Account)
                .when_all(
                    OpenAccount(account_id=agg_id, owner="alice"),
                    DepositMoney(account_id=agg_id, amount=5.0),
                )
                .then_event_types(AccountOpened, MoneyDeposited)
            )
            assert isinstance(res, cls)
            with pytest.raises(AssertionError, match="Expected event types .* got"):
                res.then_event_types(AccountOpened)
            rejected = cls(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
            with pytest.raises(AssertionError, match="Expected event types .* but command raised"):
                rejected.then_event_types(MoneyDeposited)

    def test_then_no_events(self) -> None:
        noop = DeciderScenario(
            decide=lambda cmd, st: [], evolve=Account.evolve, initial_state=Account.initial_state
        )
        with pytest.raises(AssertionError, match="when"):
            noop.then_no_events()
        assert noop.when(object()).then_no_events() is noop

        agg_id = uuid4()
        emitted = DeciderScenario(Account).when(OpenAccount(account_id=agg_id, owner="alice"))
        with pytest.raises(AssertionError, match="expected no events, got \\['AccountOpened'\\]"):
            emitted.then_no_events()

        rejected = DeciderScenario(Account).when(DepositMoney(account_id=agg_id, amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            rejected.then_no_events()

    def test_then_event_with_fields_and_chaining(self) -> None:
        agg_id = uuid4()
        with pytest.raises(AssertionError, match="when"):
            DeciderScenario(Account).then_event(AccountOpened, owner="alice")

        scenario = (
            Scenario(Account)
            .given_commands(OpenAccount(account_id=agg_id, owner="alice"))
            .when(DepositMoney(account_id=agg_id, amount=25.0))
            .then_event(MoneyDeposited, aggregate_id=agg_id, amount=25.0)
            .then_state(AccountState(owner="alice", balance=25.0, is_open=True))
        )
        with pytest.raises(AssertionError, match="AccountOpened"):
            scenario.then_event(AccountOpened)
        with pytest.raises(AssertionError, match="MoneyDeposited\\(amount=99.0\\)"):
            scenario.then_event(MoneyDeposited, amount=99.0)

        rejected = Scenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            rejected.then_event(MoneyDeposited, amount=5.0)

    def test_then_rejected_variants(self) -> None:
        rejected = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        rejected.then_rejected(match="not open").then_rejected(CommandRejectedError)
        produced = DeciderScenario(Account).when(OpenAccount(account_id=uuid4(), owner="alice"))
        with pytest.raises(AssertionError):
            produced.then_rejected()

    def test_events_property_and_three_function_form(self) -> None:
        scenario = DeciderScenario(
            decide=Account.decide, evolve=Account.evolve, initial_state=Account.initial_state
        ).when(OpenAccount(account_id=uuid4(), owner="alice"))
        scenario.then_events(AccountOpened)
        assert len(scenario.events) == 1 and isinstance(scenario.events[0], AccountOpened)

    def test_when_before_then_and_clears_stale_error(self) -> None:
        with pytest.raises(AssertionError, match="when"):
            DeciderScenario(Account).then_events(AccountOpened)
        with pytest.raises(AssertionError, match="when"):
            DeciderScenario(Account).then_event_types(AccountOpened)
        scenario = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        scenario.then_rejected()
        scenario.when(OpenAccount(account_id=uuid4(), owner="alice")).then_events(AccountOpened)


class TestMultiAggregateDeciderScenario:
    def test_multi_aggregate_state_isolation(self) -> None:
        id_1, id_2 = uuid4(), uuid4()
        scenario = DeciderScenario(Account).given(
            AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"),
            AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"),
            MoneyDeposited(aggregate_id=id_1, aggregate_version=2, amount=100.0),
            MoneyDeposited(aggregate_id=id_2, aggregate_version=2, amount=50.0),
        )
        assert (scenario.get_state(id_1).owner, scenario.get_state(id_1).balance) == (
            "alice",
            100.0,
        )
        assert (scenario.get_state(id_2).owner, scenario.get_state(id_2).balance) == ("bob", 50.0)

        scenario.when(DepositMoney(account_id=id_1, amount=25.0)).then_events(MoneyDeposited)
        assert (scenario.events[0].aggregate_id, scenario.events[0].amount) == (id_1, 25.0)
        scenario.when(DepositMoney(account_id=id_2, amount=10.0)).then_events(MoneyDeposited)
        assert (scenario.events[0].aggregate_id, scenario.events[0].amount) == (id_2, 10.0)

    def test_multi_aggregate_routing_and_explicit_id(self) -> None:
        id_1, id_2, id_3 = uuid4(), uuid4(), uuid4()
        scenario = DeciderScenario(Account).given(
            AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"),
            AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"),
        )
        scenario.when(DepositMoney(account_id=id_1, amount=15.0), aggregate_id=id_1).then_events(
            MoneyDeposited
        )
        assert scenario.events[0].aggregate_id == id_1
        scenario.when(OpenAccount(account_id=id_3, owner="carol")).then_events(AccountOpened)
        assert scenario.events[0].aggregate_id == id_3

    def test_states_property_and_unresolvable_id_errors(self) -> None:
        id_1, id_2 = uuid4(), uuid4()
        scenario = DeciderScenario(Account)
        assert scenario.state.is_open is False
        scenario.given(AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"))
        assert scenario.state.owner == "alice"
        scenario.given(AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"))
        assert (scenario.states[id_1].owner, scenario.states[id_2].owner) == ("alice", "bob")
        with pytest.raises(ValueError, match="Multiple aggregate states exist"):
            _ = scenario.state
        with pytest.raises(ValueError, match="Multiple aggregates exist"):
            scenario.when(object())

    def test_given_accepts_nested_sequences(self) -> None:
        id_1, id_2 = uuid4(), uuid4()
        scenario = DeciderScenario(Account).given(
            [
                AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"),
                AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob"),
            ]
        )
        assert (scenario.get_state(id_1).owner, scenario.get_state(id_2).owner) == ("alice", "bob")


class TestWhenAllAndThenState:
    def test_when_all_and_then_state(self) -> None:
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
            .then_state(AccountState(owner="alice", balance=55.0, is_open=True))
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

    def test_then_state_failures_and_no_private_shim(self) -> None:
        scenario = DeciderScenario(Account).when(OpenAccount(account_id=uuid4(), owner="alice"))
        with pytest.raises(AssertionError, match="state predicate"):
            scenario.then_state(lambda s: s.balance > 0)
        with pytest.raises(AssertionError, match="expected state"):
            scenario.then_state(AccountState(owner="bob", balance=0.0, is_open=True))
        rejected = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            rejected.then_state(lambda s: True)
        assert not hasattr(DeciderScenario, "_state")


class TestGivenCommands:
    def test_given_commands_and_chaining_in_any_order(self) -> None:
        agg_1, agg_2 = uuid4(), uuid4()
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
