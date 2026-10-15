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

_FINITE_POS = st.floats(min_value=0.0, max_value=1e6, allow_nan=False, allow_infinity=False)


class TestGivenWhenThen:
    def test_then_events_and_failure_modes(self) -> None:
        agg_id = uuid4()
        ev = AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner="alice")
        cmd = DepositMoney(account_id=agg_id, amount=5.0)
        scenario = DeciderScenario(Account).given(ev).when(cmd).then_events(MoneyDeposited)
        with pytest.raises(AssertionError):
            scenario.then_events(AccountOpened)
        rejected = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            rejected.then_events(MoneyDeposited)

    def test_then_event_types_on_scenario_and_decider_scenario(self) -> None:
        for cls in (DeciderScenario, Scenario):
            agg_id = uuid4()
            c1, c2 = OpenAccount(account_id=agg_id, owner="alice"), DepositMoney(account_id=agg_id, amount=5.0)  # fmt: skip
            res = cls(Account).when_all(c1, c2).then_event_types(AccountOpened, MoneyDeposited)
            assert isinstance(res, cls)
            with pytest.raises(AssertionError, match="Expected event types .* got"):
                res.then_event_types(AccountOpened)
            rejected = cls(Account).when(DepositMoney(account_id=uuid4(), amount=5.0))
            with pytest.raises(AssertionError, match="Expected event types .* but command raised"):
                rejected.then_event_types(MoneyDeposited)

    def test_then_no_events(self) -> None:
        noop = DeciderScenario(decide=lambda cmd, st: [], evolve=Account.evolve, initial_state=Account.initial_state)  # fmt: skip
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
        s = Scenario(Account).given_commands(OpenAccount(account_id=agg_id, owner="alice"))
        scenario = s.when(DepositMoney(account_id=agg_id, amount=25.0))
        scenario.then_event(MoneyDeposited, aggregate_id=agg_id, amount=25.0).then_state(AccountState(owner="alice", balance=25.0, is_open=True))  # fmt: skip
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

    def test_events_assertions_len_bool_iter_and_contains(self) -> None:
        s = DeciderScenario(decide=Account.decide, evolve=Account.evolve, initial_state=Account.initial_state)  # fmt: skip
        assert len(s) == 0 and bool(s) is True and list(iter(s)) == [] and AccountOpened not in s
        agg_id = uuid4()
        scenario = s.when(OpenAccount(account_id=agg_id, owner="alice")).then_events(AccountOpened)
        assert len(scenario) == 1 and bool(scenario) is True
        assert list(iter(scenario)) == scenario.events and isinstance(scenario.events[0], AccountOpened)  # fmt: skip
        assert AccountOpened in scenario and MoneyDeposited not in scenario
        assert scenario.events[0] in scenario and "invalid" not in scenario
        scenario.assertions.assert_event_sequence([AccountOpened])
        scenario.assertions.for_aggregate(agg_id).assert_event_published(AccountOpened)

    def test_when_before_then_and_clears_stale_error(self) -> None:
        for fn in (DeciderScenario(Account).then_events, DeciderScenario(Account).then_event_types):
            with pytest.raises(AssertionError, match="when"):
                fn(AccountOpened)
        scenario = DeciderScenario(Account).when(DepositMoney(account_id=uuid4(), amount=5.0)).then_rejected()  # fmt: skip
        scenario.when(OpenAccount(account_id=uuid4(), owner="alice")).then_events(AccountOpened)


class TestMultiAggregateDeciderScenario:
    def test_multi_aggregate_state_isolation(self) -> None:
        id_1, id_2 = uuid4(), uuid4()
        e1, e2 = AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"), AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob")  # fmt: skip
        d1, d2 = MoneyDeposited(aggregate_id=id_1, aggregate_version=2, amount=100.0), MoneyDeposited(aggregate_id=id_2, aggregate_version=2, amount=50.0)  # fmt: skip
        scenario = DeciderScenario(Account).given(e1, e2, d1, d2)
        s1, s2 = scenario.get_state(id_1), scenario.get_state(id_2)
        assert (s1.owner, s1.balance, s2.owner, s2.balance) == ("alice", 100.0, "bob", 50.0)
        scenario.when(DepositMoney(account_id=id_1, amount=25.0)).then_events(MoneyDeposited)
        assert (scenario.events[0].aggregate_id, scenario.events[0].amount) == (id_1, 25.0)
        scenario.when(DepositMoney(account_id=id_2, amount=10.0)).then_events(MoneyDeposited)
        assert (scenario.events[0].aggregate_id, scenario.events[0].amount) == (id_2, 10.0)

    def test_multi_aggregate_routing_and_explicit_id(self) -> None:
        id_1, id_2, id_3 = uuid4(), uuid4(), uuid4()
        e1, e2 = AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"), AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob")  # fmt: skip
        scenario = DeciderScenario(Account).given(e1, e2)
        scenario.when(DepositMoney(account_id=id_1, amount=15.0), aggregate_id=id_1).then_events(MoneyDeposited)  # fmt: skip
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
        ev1, ev2 = AccountOpened(aggregate_id=id_1, aggregate_version=1, owner="alice"), AccountOpened(aggregate_id=id_2, aggregate_version=1, owner="bob")  # fmt: skip
        scenario = DeciderScenario(Account).given([ev1, ev2])
        assert (scenario.get_state(id_1).owner, scenario.get_state(id_2).owner) == ("alice", "bob")


class TestWhenAllAndThenState:
    def test_when_all_and_then_state(self) -> None:
        aid = uuid4()
        c1, c2, c3 = OpenAccount(account_id=aid, owner="alice"), DepositMoney(account_id=aid, amount=40.0), DepositMoney(account_id=aid, amount=15.0)  # fmt: skip
        scenario = DeciderScenario(Account).when_all(c1, c2, c3).then_events(AccountOpened, MoneyDeposited, MoneyDeposited)  # fmt: skip
        scenario.then_state(lambda s: s.is_open and s.owner == "alice" and s.balance == 55.0)
        scenario.then_state(AccountState(owner="alice", balance=55.0, is_open=True))
        assert len(scenario.events) == 3

    def test_when_all_stops_on_first_rejection(self) -> None:
        aid = uuid4()
        c1, c2 = DepositMoney(account_id=aid, amount=10.0), OpenAccount(account_id=aid, owner="a")
        DeciderScenario(Account).when_all(c1, c2).then_rejected(match="not open")

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
        a1, a2 = uuid4(), uuid4()
        ev1 = AccountOpened(aggregate_id=a1, aggregate_version=1, owner="alice")
        c1, c2 = DepositMoney(account_id=a1, amount=5.0), DepositMoney(account_id=a1, amount=25.0)
        s1 = DeciderScenario(Account).given(ev1).given_commands(DepositMoney(account_id=a1, amount=10.0))  # fmt: skip
        s1.when_all(c1, c2).then_events(MoneyDeposited, MoneyDeposited).then_state(AccountState(owner="alice", balance=40.0, is_open=True))  # fmt: skip
        ev2 = MoneyDeposited(aggregate_id=a2, aggregate_version=2, amount=30.0)
        s2 = DeciderScenario(Account).given_commands(OpenAccount(account_id=a2, owner="bob"))
        s2.given(ev2).when(DepositMoney(account_id=a2, amount=12.0)).then_events(MoneyDeposited).then_state(AccountState(owner="bob", balance=42.0, is_open=True))  # fmt: skip

    @given(owner=st.text(min_size=1, max_size=20), deposits=st.lists(_FINITE_POS, max_size=10), final_deposit=_FINITE_POS)  # fmt: skip
    def test_given_commands_equivalent_to_given_emitted_events(self, owner: str, deposits: list[float], final_deposit: float) -> None:  # fmt: skip
        aid = uuid4()
        cmds = [OpenAccount(account_id=aid, owner=owner), *[DepositMoney(account_id=aid, amount=a) for a in deposits]]  # fmt: skip
        evs = [AccountOpened(aggregate_id=aid, aggregate_version=1, owner=owner), *[MoneyDeposited(aggregate_id=aid, aggregate_version=i + 2, amount=a) for i, a in enumerate(deposits)]]  # fmt: skip
        when_cmd = DepositMoney(account_id=aid, amount=final_deposit)
        by_commands = DeciderScenario(Account).given_commands(*cmds).when(when_cmd)
        by_events = DeciderScenario(Account).given(*evs).when(when_cmd)
        assert by_commands.state == by_events.state
        assert [type(e) for e in by_commands.events] == [type(e) for e in by_events.events]
