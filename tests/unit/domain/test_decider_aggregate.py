"""Tests for DeciderAggregate."""

from dataclasses import dataclass
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import CommandRejectedError


class AccountOpened(DomainEvent):
    event_type: str = "AccountOpened"
    aggregate_type: str = "Account"
    owner: str


class MoneyDeposited(DomainEvent):
    event_type: str = "MoneyDeposited"
    aggregate_type: str = "Account"
    amount: float


class OpenAccount(DomainCommand):
    owner: str


class DepositMoney(DomainCommand):
    amount: float


class AccountState(BaseModel):
    account_id: UUID
    owner: str | None = None
    balance: float = 0.0
    is_open: bool = False


class Account(DeciderAggregate[AccountState]):
    aggregate_type = "Account"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> AccountState:
        return AccountState(account_id=aggregate_id)

    @staticmethod
    def decide(command: object, state: AccountState) -> list[DomainEvent]:
        match command, state:
            case OpenAccount(owner=owner), AccountState(is_open=False):
                return [AccountOpened(aggregate_id=state.account_id, owner=owner)]
            case OpenAccount(), _:
                raise CommandRejectedError("account already open", command=command)
            case DepositMoney(amount=amount), AccountState(is_open=True):
                return [MoneyDeposited(aggregate_id=state.account_id, amount=amount)]
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
        acct.execute(OpenAccount(owner="alice"))
        assert acct.state.is_open is True


class TestExecuteStamping:
    def test_version_and_type_stamped(self) -> None:
        acct = Account(uuid4())
        events = acct.execute(OpenAccount(owner="alice"))
        assert [e.aggregate_version for e in events] == [1]
        assert events[0].aggregate_type == "Account"
        assert acct.version == 1
        assert acct.uncommitted_events == events

    def test_provenance_from_domain_command(self) -> None:
        acct = Account(uuid4())
        cmd = OpenAccount(owner="alice", actor_id="user-1", tenant_id=uuid4())
        (event,) = acct.execute(cmd)
        assert event.causation_id == cmd.command_id
        assert event.correlation_id == cmd.correlation_id
        assert event.actor_id == "user-1"
        assert event.tenant_id == cmd.tenant_id

    def test_plain_object_command_gets_version_but_no_provenance(self) -> None:
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class PlainOpen:
            owner: str

        class PlainAccount(Account):
            @staticmethod
            def decide(command: object, state: AccountState) -> list[DomainEvent]:
                match command:
                    case PlainOpen(owner=owner):
                        return [AccountOpened(aggregate_id=state.account_id, owner=owner)]
                    case _:
                        return Account.decide(command, state)

        acct = PlainAccount(uuid4())
        (event,) = acct.execute(PlainOpen(owner="alice"))
        assert event.aggregate_version == 1
        assert event.causation_id is None

    def test_explicit_fields_win_over_stamping(self) -> None:
        explicit_correlation = uuid4()

        class ExplicitAccount(Account):
            @staticmethod
            def decide(command: object, state: AccountState) -> list[DomainEvent]:
                return [
                    AccountOpened(
                        aggregate_id=state.account_id,
                        owner="alice",
                        correlation_id=explicit_correlation,
                    )
                ]

        acct = ExplicitAccount(uuid4())
        (event,) = acct.execute(OpenAccount(owner="alice"))
        assert event.correlation_id == explicit_correlation
        assert event.causation_id is not None  # not explicit -> still stamped

    def test_multi_event_versions_sequential(self) -> None:
        class DoubleAccount(Account):
            @staticmethod
            def decide(command: object, state: AccountState) -> list[DomainEvent]:
                return [
                    AccountOpened(aggregate_id=state.account_id, owner="a"),
                    MoneyDeposited(aggregate_id=state.account_id, amount=1.0),
                ]

        acct = DoubleAccount(uuid4())
        events = acct.execute(OpenAccount(owner="a"))
        assert [e.aggregate_version for e in events] == [1, 2]
        assert acct.version == 2


class TestRejectionAtomicity:
    def test_rejection_leaves_aggregate_untouched(self) -> None:
        acct = Account(uuid4())
        with pytest.raises(CommandRejectedError, match="not open"):
            acct.execute(DepositMoney(amount=5.0))
        assert acct.version == 0
        assert acct.uncommitted_events == []
        assert acct.state.balance == 0.0


class TestReplayEquivalence:
    def test_load_from_history_equals_folding_evolve(self) -> None:
        agg_id = uuid4()
        acct = Account(agg_id)
        acct.execute(OpenAccount(owner="alice"))
        acct.execute(DepositMoney(amount=25.0))
        history = acct.uncommitted_events

        replayed = Account(agg_id)
        replayed.load_from_history(history)

        folded = Account.initial_state(agg_id)
        for event in history:
            folded = Account.evolve(folded, event)

        assert replayed.state == folded == acct.state
        assert replayed.version == 2


class TestSnapshotRoundTrip:
    def test_serialize_and_restore(self) -> None:
        acct = Account(uuid4())
        acct.execute(OpenAccount(owner="alice"))
        snapshot = acct._serialize_state()

        restored = Account(acct.aggregate_id)
        restored._restore_from_snapshot(snapshot, version=1)
        assert restored.state == acct.state
        assert restored.version == 1


class TestStateInvariant:
    def test_none_state_raises_even_without_assertions(self) -> None:
        """Accessing state when initial_state returns None raises RuntimeError."""

        class BrokenDecider(DeciderAggregate[dict]):
            aggregate_type = "Broken"

            @staticmethod
            def initial_state(aggregate_id: UUID) -> dict:
                return None  # type: ignore[return-value]  # deliberate contract violation

            @staticmethod
            def decide(command: object, state: dict) -> list[DomainEvent]:
                return []

            @staticmethod
            def evolve(state: dict, event: DomainEvent) -> dict:
                return state

        agg = BrokenDecider(uuid4())
        with pytest.raises(RuntimeError, match="initial_state"):
            _ = agg.state


class TestCreateEventCommandProvenance:
    def test_create_event_stamps_provenance_from_command(self) -> None:
        from eventsource.domain.aggregate import AggregateRoot

        class ImperativeAccount(AggregateRoot[AccountState]):
            aggregate_type = "Account"

            def _get_initial_state(self) -> AccountState:
                return AccountState(account_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                if isinstance(event, AccountOpened):
                    self._state = AccountState(
                        account_id=self.aggregate_id, owner=event.owner, is_open=True
                    )

            def open(self, command: OpenAccount) -> None:
                self.create_event(AccountOpened, command=command, owner=command.owner)

        acct = ImperativeAccount(uuid4())
        cmd = OpenAccount(owner="alice", actor_id="user-1")
        acct.open(cmd)
        (event,) = acct.uncommitted_events
        assert event.causation_id == cmd.command_id
        assert event.correlation_id == cmd.correlation_id
        assert event.actor_id == "user-1"

    def test_explicit_kwargs_beat_command_fields(self) -> None:
        from eventsource.domain.aggregate import AggregateRoot

        explicit = uuid4()

        class ImperativeAccount(AggregateRoot[AccountState]):
            aggregate_type = "Account"

            def _get_initial_state(self) -> AccountState:
                return AccountState(account_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        acct = ImperativeAccount(uuid4())
        cmd = OpenAccount(owner="alice")
        event = acct.create_event(
            AccountOpened, command=cmd, owner="alice", correlation_id=explicit
        )
        assert event.correlation_id == explicit
        assert event.causation_id == cmd.command_id


class TestAmbientTenantStamping:
    def test_plain_command_gets_ambient_tenant(self) -> None:
        from eventsource.domain.tenant_context import tenant_scope_sync

        tenant = uuid4()

        @dataclass
        class PlainShip:  # deliberately NOT a DomainCommand
            order_id: UUID

        class ShipDecider(DeciderAggregate[dict]):
            aggregate_type = "Order"

            @staticmethod
            def initial_state(aggregate_id: UUID) -> dict:
                return {"id": aggregate_id}

            @staticmethod
            def decide(command: object, state: dict) -> list[DomainEvent]:
                return [Shipped(aggregate_id=state["id"])]

            @staticmethod
            def evolve(state: dict, event: DomainEvent) -> dict:
                return state

        class Shipped(DomainEvent):
            aggregate_type: str = "Order"

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
            id: UUID

        class PingDecider(DeciderAggregate[PingState, Ping]):
            aggregate_type = "Ping"

            @staticmethod
            def initial_state(aggregate_id: UUID) -> PingState:
                return PingState(id=aggregate_id)

            @staticmethod
            def decide(command: Ping, state: PingState) -> list[DomainEvent]:
                return []

            @staticmethod
            def evolve(state: PingState, event: DomainEvent) -> PingState:
                return state

        agg = PingDecider(uuid4())
        assert agg.execute(Ping(target=agg.aggregate_id)) == []

    def test_single_param_subscript_still_works(self) -> None:
        class LegacyState(BaseModel):
            id: UUID

        class LegacyDecider(DeciderAggregate[LegacyState]):
            aggregate_type = "Legacy"

            @staticmethod
            def initial_state(aggregate_id: UUID) -> LegacyState:
                return LegacyState(id=aggregate_id)

            @staticmethod
            def decide(command: object, state: LegacyState) -> list[DomainEvent]:
                return []

            @staticmethod
            def evolve(state: LegacyState, event: DomainEvent) -> LegacyState:
                return state

        assert LegacyDecider(uuid4()).execute(object()) == []
