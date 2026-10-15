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
    aggregate_type = "Account"
    initial_state = staticmethod(lambda: AccountState())

    @staticmethod
    def decide(command: object, state: AccountState) -> list[DomainEvent]:
        if isinstance(command, OpenAccount):
            if not state.is_open:
                return [AccountOpened(aggregate_id=command.account_id, owner=command.owner)]
            raise CommandRejectedError("account already open", command=command)
        if isinstance(command, DepositMoney):
            if state.is_open:
                return [MoneyDeposited(aggregate_id=command.account_id, amount=command.amount)]
            raise CommandRejectedError("account not open", command=command)
        raise CommandRejectedError(f"unknown command: {command!r}", command=command)

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        if isinstance(event, AccountOpened):
            return state.model_copy(update={"owner": event.owner, "is_open": True})
        if isinstance(event, MoneyDeposited):
            return state.model_copy(update={"balance": state.balance + event.amount})
        return state


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
        cls = type("PlainAccount", (Account,), {"decide": staticmethod(lambda c, s: [ev])})
        (event,) = cls(aid).execute(SimpleNamespace(account_id=aid, owner="alice"))
        assert (event.aggregate_version, event.causation_id) == (1, None)

    def test_explicit_fields_win_over_stamping(self) -> None:
        aid, corr = uuid4(), uuid4()
        ev = AccountOpened(aggregate_id=aid, owner="alice", correlation_id=corr)
        cls = type("ExplicitAccount", (Account,), {"decide": staticmethod(lambda c, s: [ev])})
        (event,) = cls(aid).execute(OpenAccount(account_id=aid, owner="alice"))
        assert event.correlation_id == corr and event.causation_id is not None

    def test_multi_event_versions_sequential(self) -> None:
        e1 = AccountOpened(aggregate_id=(aid := uuid4()), owner="a")
        e2 = MoneyDeposited(aggregate_id=aid, amount=1.0)
        cls = type("DoubleAccount", (Account,), {"decide": staticmethod(lambda c, s: [e1, e2])})
        acct = cls(aid)
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
        replayed = Account(agg_id)
        replayed.load_from_history(acct.uncommitted_events)
        folded = Account.initial_state()
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
        class BrokenDecider(DeciderAggregate[dict]):
            aggregate_type = "Broken"
            initial_state = staticmethod(lambda: None)  # type: ignore[assignment]
            decide, evolve = staticmethod(lambda c, s: []), staticmethod(lambda s, e: s)

        with pytest.raises(RuntimeError, match="initial_state"):
            _ = BrokenDecider(uuid4()).state


class _ImperativeAccount(AggregateRoot[AccountState]):
    aggregate_type, _get_initial_state = "Account", staticmethod(AccountState)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = AccountState(owner=event.owner, is_open=True)

    def open(self, command: OpenAccount) -> None:
        self.create_event(AccountOpened, command=command, owner=command.owner)


class TestCreateEventCommandProvenance:
    def test_create_event_stamps_provenance_from_command(self) -> None:
        acct = _ImperativeAccount(uuid4())
        cmd = OpenAccount(account_id=acct.aggregate_id, owner="alice", actor_id="user-1")
        acct.open(cmd)
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
        decide = staticmethod(lambda cmd, st: [AccountOpened(aggregate_id=cmd.order_id, owner="x")])
        cls = type("ShipDecider", (Account,), {"decide": decide})
        agg, tenant = cls(uuid4()), uuid4()
        with tenant_scope_sync(tenant):
            assert agg.execute(SimpleNamespace(order_id=agg.aggregate_id))[0].tenant_id == tenant


class TestTypedDecider:
    def test_two_param_subscript_works_at_runtime(self) -> None:
        class PingDecider(DeciderAggregate[AccountState, SimpleNamespace]):
            aggregate_type, initial_state = "Ping", staticmethod(AccountState)
            decide, evolve = staticmethod(lambda c, s: []), staticmethod(lambda s, e: s)

        agg = PingDecider(uuid4())
        assert agg.execute(SimpleNamespace(target=agg.aggregate_id)) == []

    def test_single_param_subscript_still_works(self) -> None:
        class LegacyDecider(DeciderAggregate[AccountState]):
            aggregate_type, initial_state = "Legacy", staticmethod(AccountState)
            decide, evolve = staticmethod(lambda c, s: []), staticmethod(lambda s, e: s)

        assert LegacyDecider(uuid4()).execute(object()) == []

    def test_decider_aggregate_accepts_one_and_two_parameter_subscripts(self) -> None:
        assert DeciderAggregate[AccountState] is not None
        assert DeciderAggregate[AccountState, OpenAccount] is not None


class TestInitialStateIsNullary:
    def test_initial_state_takes_no_parameters(self) -> None:
        assert list(inspect.signature(Account.initial_state).parameters) == []

    def test_every_instance_starts_from_the_same_state(self) -> None:
        assert Account(uuid4()).state == Account(uuid4()).state == Account.initial_state()

    def test_decide_takes_the_aggregate_id_from_the_command(self) -> None:
        cmd = OpenAccount(account_id=(target := uuid4()), owner="alice")
        (event,) = Account.decide(cmd, Account.initial_state())
        assert event.aggregate_id == target
