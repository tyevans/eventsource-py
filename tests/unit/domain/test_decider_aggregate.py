"""Tests for DeciderAggregate."""

import inspect
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.domain import AggregateRoot, DeciderAggregate, DomainCommand, DomainEvent
from eventsource.domain.exceptions import CommandRejectedError
from eventsource.domain.tenant_context import tenant_scope_sync


class AccountOpened(DomainEvent):
    aggregate_type: str = "Account"
    owner: str


class MoneyDeposited(DomainEvent):
    aggregate_type: str = "Account"
    amount: float


class OpenAccount(DomainCommand):
    account_id: UUID
    owner: str


class DepositMoney(DomainCommand):
    account_id: UUID
    amount: float


class AccountState(BaseModel):
    owner: str | None = None
    balance: float = 0.0
    is_open: bool = False


class Account(DeciderAggregate[AccountState]):
    aggregate_type, initial_state = "Account", staticmethod(lambda: AccountState())

    @staticmethod
    def decide(command: object, state: AccountState) -> list[DomainEvent]:
        if isinstance(command, OpenAccount) and not state.is_open:
            return [AccountOpened(aggregate_id=command.account_id, owner=command.owner)]
        if isinstance(command, DepositMoney) and state.is_open:
            return [MoneyDeposited(aggregate_id=command.account_id, amount=command.amount)]
        msg = "account already open" if isinstance(command, OpenAccount) else "account not open"
        raise CommandRejectedError(msg, command=command)

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        if isinstance(event, AccountOpened):
            return state.model_copy(update={"owner": event.owner, "is_open": True})
        if isinstance(event, MoneyDeposited):
            return state.model_copy(update={"balance": state.balance + event.amount})
        return state


def _custom_account(name: str, events_fn) -> type[Account]:
    return type(name, (Account,), {"decide": staticmethod(events_fn)})


class TestEagerState:
    def test_state_is_initial_before_any_event(self) -> None:
        acct = Account(uuid4())
        assert (acct.state.is_open, acct.version) == (False, 0)

    def test_first_command_accepted(self) -> None:
        acct = Account(uuid4())
        acct.execute(OpenAccount(account_id=acct.aggregate_id, owner="alice"))
        assert acct.state.is_open is True


class TestExecuteStamping:
    def test_version_and_type_stamped(self) -> None:
        acct = Account(uuid4())
        events = acct.execute(OpenAccount(account_id=acct.aggregate_id, owner="alice"))
        assert ([e.aggregate_version for e in events], events[0].aggregate_type) == ([1], "Account")
        assert (acct.version, acct.uncommitted_events) == (1, events)

    def test_provenance_from_domain_command(self) -> None:
        acct = Account(aid := uuid4())
        cmd = OpenAccount(account_id=aid, owner="alice", actor_id="user-1", tenant_id=uuid4())
        (ev,) = acct.execute(cmd)
        assert (ev.causation_id, ev.correlation_id) == (cmd.command_id, cmd.correlation_id)
        assert (ev.actor_id, ev.tenant_id) == ("user-1", cmd.tenant_id)

    def test_plain_object_command_gets_version_but_no_provenance(self) -> None:
        ev = AccountOpened(aggregate_id=(aid := uuid4()), owner="alice")
        (event,) = _custom_account("Plain", lambda c, s: [ev])(aid).execute(SimpleNamespace())
        assert (event.aggregate_version, event.causation_id) == (1, None)

    def test_explicit_fields_win_over_stamping(self) -> None:
        ev = AccountOpened(aggregate_id=(aid := uuid4()), owner="a", correlation_id=(c := uuid4()))
        (event,) = _custom_account("Exp", lambda _, s: [ev])(aid).execute(
            OpenAccount(account_id=aid, owner="a")
        )
        assert event.correlation_id == c and event.causation_id is not None

    def test_multi_event_versions_sequential(self) -> None:
        e1 = AccountOpened(aggregate_id=(aid := uuid4()), owner="a")
        e2 = MoneyDeposited(aggregate_id=aid, amount=1.0)
        acct = _custom_account("DoubleAccount", lambda c, s: [e1, e2])(aid)
        events = acct.execute(OpenAccount(account_id=aid, owner="a"))
        assert ([e.aggregate_version for e in events], acct.version) == ([1, 2], 2)


class TestRejectionAtomicity:
    def test_rejection_leaves_aggregate_untouched(self) -> None:
        acct = Account(uuid4())
        with pytest.raises(CommandRejectedError, match="not open"):
            acct.execute(DepositMoney(account_id=acct.aggregate_id, amount=5.0))
        assert (acct.version, acct.uncommitted_events, acct.state.balance) == (0, [], 0.0)


class TestReplayEquivalence:
    def test_load_from_history_equals_folding_evolve(self) -> None:
        acct = Account(agg_id := uuid4())
        acct.execute(OpenAccount(account_id=agg_id, owner="alice"))
        acct.execute(DepositMoney(account_id=agg_id, amount=25.0))
        replayed, folded = Account(agg_id), Account.initial_state()
        replayed.load_from_history(acct.uncommitted_events)
        for event in acct.uncommitted_events:
            folded = Account.evolve(folded, event)
        assert (replayed.state, folded, replayed.version) == (acct.state, acct.state, 2)


class TestSnapshotRoundTrip:
    def test_serialize_and_restore(self) -> None:
        acct = Account(uuid4())
        acct.execute(OpenAccount(account_id=acct.aggregate_id, owner="alice"))
        restored = Account(acct.aggregate_id)
        restored._restore_from_snapshot(acct._serialize_state(), version=1)
        assert (restored.state, restored.version) == (acct.state, 1)


class TestStateInvariant:
    def test_none_state_raises_even_without_assertions(self) -> None:
        cls = type("BrokenDecider", (Account,), {"initial_state": staticmethod(lambda: None)})
        with pytest.raises(RuntimeError, match="initial_state"):
            _ = cls(uuid4()).state


class _ImperativeAccount(AggregateRoot[AccountState]):
    aggregate_type, _get_initial_state = "Account", staticmethod(AccountState)

    def _apply(self, event: DomainEvent) -> None:
        self._state = AccountState(owner=getattr(event, "owner", None), is_open=True)


class TestCreateEventCommandProvenance:
    def test_create_event_stamps_provenance_from_command(self) -> None:
        acct = _ImperativeAccount(uuid4())
        cmd = OpenAccount(account_id=acct.aggregate_id, owner="alice", actor_id="user-1")
        acct.create_event(AccountOpened, command=cmd, owner=cmd.owner)
        (ev,) = acct.uncommitted_events
        assert (ev.causation_id, ev.correlation_id) == (cmd.command_id, cmd.correlation_id)
        assert ev.actor_id == "user-1"

    def test_explicit_kwargs_beat_command_fields(self) -> None:
        acct, corr = _ImperativeAccount(uuid4()), uuid4()
        cmd = OpenAccount(account_id=acct.aggregate_id, owner="alice")
        event = acct.create_event(AccountOpened, command=cmd, owner="alice", correlation_id=corr)
        assert (event.correlation_id, event.causation_id) == (corr, cmd.command_id)


class TestAmbientTenantStamping:
    def test_plain_command_gets_ambient_tenant(self) -> None:
        cls = _custom_account("Ship", lambda c, _: [AccountOpened(aggregate_id=c.id, owner="x")])
        agg = cls(uuid4())
        with tenant_scope_sync(tenant := uuid4()):
            assert agg.execute(SimpleNamespace(id=agg.aggregate_id))[0].tenant_id == tenant


class TestTypedDecider:
    def test_two_param_subscript_works_at_runtime(self) -> None:
        class PingDecider(Account, DeciderAggregate[AccountState, SimpleNamespace]):
            decide = staticmethod(lambda c, s: [])

        assert PingDecider(aid := uuid4()).execute(SimpleNamespace(target=aid)) == []

    def test_single_param_subscript_still_works(self) -> None:
        assert _custom_account("LegacyDecider", lambda c, s: [])(uuid4()).execute(object()) == []

    def test_decider_aggregate_accepts_one_and_two_parameter_subscripts(self) -> None:
        assert (DeciderAggregate[AccountState], DeciderAggregate[AccountState, OpenAccount]) != (
            None,
            None,
        )


class TestInitialStateIsNullary:
    def test_initial_state_takes_no_parameters(self) -> None:
        assert list(inspect.signature(Account.initial_state).parameters) == []

    def test_every_instance_starts_from_the_same_state(self) -> None:
        assert Account(uuid4()).state == Account(uuid4()).state == Account.initial_state()

    def test_decide_takes_the_aggregate_id_from_the_command(self) -> None:
        cmd = OpenAccount(account_id=(target := uuid4()), owner="alice")
        assert Account.decide(cmd, Account.initial_state())[0].aggregate_id == target
