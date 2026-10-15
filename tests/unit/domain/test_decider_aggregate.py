"""Tests for DeciderAggregate."""

import inspect
from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.event import DomainEvent
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
        match command, state:
            case OpenAccount(account_id=account_id, owner=owner), AccountState(is_open=False):
                return [AccountOpened(aggregate_id=account_id, owner=owner)]
            case OpenAccount(), _:
                raise CommandRejectedError("account already open", command=command)
            case DepositMoney(account_id=account_id, amount=amount), AccountState(is_open=True):
                return [MoneyDeposited(aggregate_id=account_id, amount=amount)]
            case DepositMoney(), _:
                raise CommandRejectedError("account not open", command=command)
            case _:
                raise CommandRejectedError(f"unknown command: {command!r}", command=command)

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        match event:
            case AccountOpened(owner=owner):
                return state.model_copy(update={"owner": owner, "is_open": True})
            case MoneyDeposited(amount=amount):
                return state.model_copy(update={"balance": state.balance + amount})
            case _:
                return state


class TestEagerState:
    def test_state_is_initial_before_any_event(self) -> None:
        acct = Account(uuid4())
        assert acct.state.is_open is False
        assert acct.version == 0

    def test_first_command_accepted(self) -> None:
        acct = Account(uuid4())
        acct.execute(OpenAccount(account_id=acct.aggregate_id, owner="alice"))
        assert acct.state.is_open is True


class TestExecuteStamping:
    def test_version_and_type_stamped(self) -> None:
        acct = Account(uuid4())
        events = acct.execute(OpenAccount(account_id=acct.aggregate_id, owner="alice"))
        assert [e.aggregate_version for e in events] == [1]
        assert events[0].aggregate_type == "Account"
        assert acct.version == 1
        assert acct.uncommitted_events == events

    def test_provenance_from_domain_command(self) -> None:
        acct = Account(aid := uuid4())
        cmd = OpenAccount(account_id=aid, owner="alice", actor_id="user-1", tenant_id=uuid4())
        (event,) = acct.execute(cmd)
        assert event.causation_id == cmd.command_id
        assert event.correlation_id == cmd.correlation_id
        assert event.actor_id == "user-1"
        assert event.tenant_id == cmd.tenant_id

    def test_plain_object_command_gets_version_but_no_provenance(self) -> None:
        @dataclass(frozen=True)
        class PlainOpen:
            account_id: UUID
            owner: str

        ev = AccountOpened(aggregate_id=(aid := uuid4()), owner="alice")

        class PlainAccount(Account):
            decide = staticmethod(lambda c, s: [ev])

        (event,) = PlainAccount(aid).execute(PlainOpen(account_id=aid, owner="alice"))
        assert event.aggregate_version == 1
        assert event.causation_id is None

    def test_explicit_fields_win_over_stamping(self) -> None:
        aid, corr = uuid4(), uuid4()
        ev = AccountOpened(aggregate_id=aid, owner="alice", correlation_id=corr)

        class ExplicitAccount(Account):
            decide = staticmethod(lambda c, s: [ev])

        (event,) = ExplicitAccount(aid).execute(OpenAccount(account_id=aid, owner="alice"))
        assert event.correlation_id == corr
        assert event.causation_id is not None

    def test_multi_event_versions_sequential(self) -> None:
        e1 = AccountOpened(aggregate_id=(aid := uuid4()), owner="a")
        e2 = MoneyDeposited(aggregate_id=aid, amount=1.0)

        class DoubleAccount(Account):
            decide = staticmethod(lambda c, s: [e1, e2])

        acct = DoubleAccount(aid)
        events = acct.execute(OpenAccount(account_id=aid, owner="a"))
        assert [e.aggregate_version for e in events] == [1, 2]
        assert acct.version == 2


class TestRejectionAtomicity:
    def test_rejection_leaves_aggregate_untouched(self) -> None:
        acct = Account(uuid4())
        with pytest.raises(CommandRejectedError, match="not open"):
            acct.execute(DepositMoney(account_id=acct.aggregate_id, amount=5.0))
        assert acct.version == 0
        assert acct.uncommitted_events == []
        assert acct.state.balance == 0.0


class TestReplayEquivalence:
    def test_load_from_history_equals_folding_evolve(self) -> None:
        agg_id = uuid4()
        acct = Account(agg_id)
        acct.execute(OpenAccount(account_id=agg_id, owner="alice"))
        acct.execute(DepositMoney(account_id=agg_id, amount=25.0))
        history = acct.uncommitted_events
        replayed = Account(agg_id)
        replayed.load_from_history(history)
        folded = Account.initial_state()
        for event in history:
            folded = Account.evolve(folded, event)
        assert replayed.state == folded == acct.state
        assert replayed.version == 2


class TestSnapshotRoundTrip:
    def test_serialize_and_restore(self) -> None:
        acct = Account(uuid4())
        acct.execute(OpenAccount(account_id=acct.aggregate_id, owner="alice"))
        snapshot = acct._serialize_state()
        restored = Account(acct.aggregate_id)
        restored._restore_from_snapshot(snapshot, version=1)
        assert restored.state == acct.state
        assert restored.version == 1


class TestStateInvariant:
    def test_none_state_raises_even_without_assertions(self) -> None:
        class BrokenDecider(DeciderAggregate[dict]):
            aggregate_type = "Broken"
            initial_state = staticmethod(lambda: None)  # type: ignore[assignment]
            decide = staticmethod(lambda command, state: [])
            evolve = staticmethod(lambda state, event: state)

        agg = BrokenDecider(uuid4())
        with pytest.raises(RuntimeError, match="initial_state"):
            _ = agg.state


class _ImperativeAccount(AggregateRoot[AccountState]):
    aggregate_type = "Account"
    _get_initial_state = staticmethod(AccountState)

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
        (event,) = acct.uncommitted_events
        assert event.causation_id == cmd.command_id
        assert event.correlation_id == cmd.correlation_id
        assert event.actor_id == "user-1"

    def test_explicit_kwargs_beat_command_fields(self) -> None:
        corr = uuid4()
        acct = _ImperativeAccount(uuid4())
        cmd = OpenAccount(account_id=acct.aggregate_id, owner="alice")
        event = acct.create_event(AccountOpened, command=cmd, owner="alice", correlation_id=corr)
        assert event.correlation_id == corr
        assert event.causation_id == cmd.command_id


class TestAmbientTenantStamping:
    def test_plain_command_gets_ambient_tenant(self) -> None:
        tenant = uuid4()

        @dataclass
        class PlainShip:
            order_id: UUID

        class Shipped(DomainEvent):
            aggregate_type: str = "Order"

        class ShipDecider(DeciderAggregate[dict]):
            aggregate_type = "Order"
            initial_state = staticmethod(dict)
            evolve = staticmethod(lambda state, event: state)
            decide = staticmethod(lambda cmd, st: [Shipped(aggregate_id=cmd.order_id)])

        agg = ShipDecider(uuid4())
        with tenant_scope_sync(tenant):
            events = agg.execute(PlainShip(order_id=agg.aggregate_id))
        assert events[0].tenant_id == tenant


class TestTypedDecider:
    def test_two_param_subscript_works_at_runtime(self) -> None:
        @dataclass(frozen=True)
        class Ping:
            target: UUID

        class PingState(BaseModel):
            pinged: bool = False

        class PingDecider(DeciderAggregate[PingState, Ping]):
            aggregate_type = "Ping"
            initial_state = staticmethod(PingState)
            decide = staticmethod(lambda command, state: [])
            evolve = staticmethod(lambda state, event: state)

        agg = PingDecider(uuid4())
        assert agg.execute(Ping(target=agg.aggregate_id)) == []

    def test_single_param_subscript_still_works(self) -> None:
        class LegacyState(BaseModel):
            seen: int = 0

        class LegacyDecider(DeciderAggregate[LegacyState]):
            aggregate_type = "Legacy"
            initial_state = staticmethod(LegacyState)
            decide = staticmethod(lambda command, state: [])
            evolve = staticmethod(lambda state, event: state)

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
        target = uuid4()
        cmd = OpenAccount(account_id=target, owner="alice")
        (event,) = Account.decide(cmd, Account.initial_state())
        assert event.aggregate_id == target
