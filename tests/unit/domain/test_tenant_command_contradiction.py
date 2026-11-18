"""Command and explicit tenants that contradict the ambient tenant are rejected (PRD-0002).

Both public entry points are exercised: AggregateRoot.create_event() and
DeciderAggregate.execute(). With no ambient tenant the command tenant is used.
"""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.domain import DeciderAggregate, DomainCommand, DomainEvent
from eventsource.domain.exceptions import TenantMismatchError
from eventsource.domain.tenant_context import clear_tenant_context, tenant_scope_sync
from tests.unit.domain.test_create_event._fixtures import OrderAggregate, OrderCreated


class PlaceOrder(DomainCommand):
    aggregate_id: UUID
    customer_id: UUID


class Opened(DomainEvent):
    aggregate_type: str = "Ledger"


class LedgerState(BaseModel):
    opened: bool = False


class Ledger(DeciderAggregate[LedgerState]):
    aggregate_type = "Ledger"

    @staticmethod
    def initial_state() -> LedgerState:
        return LedgerState()

    @staticmethod
    def decide(command: object, state: LedgerState) -> list[DomainEvent]:
        return [Opened(aggregate_id=command.aggregate_id)]  # type: ignore[attr-defined]

    @staticmethod
    def evolve(state: LedgerState, event: DomainEvent) -> LedgerState:
        return LedgerState(opened=True)


class LedgerWithExplicitTenant(Ledger):
    """decide() sets tenant_id itself, as a plain (non-command) caller could."""

    @staticmethod
    def decide(command: object, state: LedgerState) -> list[DomainEvent]:
        return [Opened(aggregate_id=command.aggregate_id, tenant_id=command.explicit_tenant)]  # type: ignore[attr-defined]


class _Cmd:
    """Plain command object (not a DomainCommand)."""

    def __init__(self, aggregate_id: UUID, explicit_tenant: UUID | None = None) -> None:
        self.aggregate_id = aggregate_id
        self.explicit_tenant = explicit_tenant


@pytest.fixture(autouse=True)
def _clean_tenant_context() -> None:
    clear_tenant_context()


class TestCreateEventTenantContradiction:
    def test_command_tenant_differing_from_ambient_raises(self) -> None:
        order = OrderAggregate(uuid4())
        cmd = DomainCommand(tenant_id=uuid4())
        with tenant_scope_sync(uuid4()), pytest.raises(TenantMismatchError):
            order.create_event(OrderCreated, command=cmd, customer_id=uuid4())

    def test_rejected_command_leaves_aggregate_untouched(self) -> None:
        order = OrderAggregate(uuid4())
        with tenant_scope_sync(uuid4()), pytest.raises(TenantMismatchError):
            order.create_event(
                OrderCreated, command=DomainCommand(tenant_id=uuid4()), customer_id=uuid4()
            )
        assert (order.version, order.uncommitted_events) == (0, [])

    def test_command_tenant_equal_to_ambient_is_accepted(self) -> None:
        order = OrderAggregate(uuid4())
        tenant = uuid4()
        with tenant_scope_sync(tenant):
            event = order.create_event(
                OrderCreated, command=DomainCommand(tenant_id=tenant), customer_id=uuid4()
            )
        assert event.tenant_id == tenant

    def test_command_tenant_used_when_no_ambient(self) -> None:
        order = OrderAggregate(uuid4())
        tenant = uuid4()
        event = order.create_event(
            OrderCreated, command=DomainCommand(tenant_id=tenant), customer_id=uuid4()
        )
        assert event.tenant_id == tenant

    def test_ambient_tenant_used_for_command_without_tenant(self) -> None:
        order = OrderAggregate(uuid4())
        tenant = uuid4()
        with tenant_scope_sync(tenant):
            event = order.create_event(OrderCreated, command=DomainCommand(), customer_id=uuid4())
        assert event.tenant_id == tenant

    def test_explicit_tenant_differing_from_ambient_raises(self) -> None:
        order = OrderAggregate(uuid4())
        with tenant_scope_sync(uuid4()), pytest.raises(TenantMismatchError):
            order.create_event(OrderCreated, tenant_id=uuid4(), customer_id=uuid4())


class TestDeciderTenantContradiction:
    def test_command_tenant_differing_from_ambient_raises(self) -> None:
        ledger = Ledger(uuid4())
        with tenant_scope_sync(uuid4()), pytest.raises(TenantMismatchError):
            ledger.execute(
                PlaceOrder(aggregate_id=ledger.aggregate_id, customer_id=uuid4(), tenant_id=uuid4())
            )

    def test_rejected_command_leaves_state_untouched(self) -> None:
        ledger = Ledger(uuid4())
        with tenant_scope_sync(uuid4()), pytest.raises(TenantMismatchError):
            ledger.execute(
                PlaceOrder(aggregate_id=ledger.aggregate_id, customer_id=uuid4(), tenant_id=uuid4())
            )
        assert (ledger.version, ledger.uncommitted_events, ledger.state.opened) == (0, [], False)

    def test_command_tenant_equal_to_ambient_is_accepted(self) -> None:
        ledger, tenant = Ledger(uuid4()), uuid4()
        with tenant_scope_sync(tenant):
            (event,) = ledger.execute(
                PlaceOrder(aggregate_id=ledger.aggregate_id, customer_id=uuid4(), tenant_id=tenant)
            )
        assert event.tenant_id == tenant

    def test_command_tenant_used_when_no_ambient(self) -> None:
        ledger, tenant = Ledger(uuid4()), uuid4()
        (event,) = ledger.execute(
            PlaceOrder(aggregate_id=ledger.aggregate_id, customer_id=uuid4(), tenant_id=tenant)
        )
        assert event.tenant_id == tenant

    def test_event_tenant_set_by_decide_differing_from_ambient_raises(self) -> None:
        agg = LedgerWithExplicitTenant(uuid4())
        with tenant_scope_sync(uuid4()), pytest.raises(TenantMismatchError):
            agg.execute(_Cmd(agg.aggregate_id, explicit_tenant=uuid4()))  # type: ignore[arg-type]
