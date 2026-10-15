"""Tests for AggregateRepository.execute and TenantAwareRepository.execute."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource import EventRegistry, TenantDomainEvent, tenant_scope
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import AggregateNotFoundError, TenantMismatchError
from tests.unit.application.aggregates.test_repository._models import CounterAggregate
from tests.unit.domain.test_decider_aggregate import (
    Account,
    AccountOpened,
    AccountState,
    DepositMoney,
    MoneyDeposited,
    OpenAccount,
)


class TenantAccountOpened(TenantDomainEvent):
    aggregate_type: str = "TenantAccount"
    owner: str


def _make_store() -> InMemoryEventStore:
    registry = EventRegistry()
    registry.register(AccountOpened)
    registry.register(MoneyDeposited)
    registry.register(TenantAccountOpened)
    return InMemoryEventStore(event_registry=registry)


class OpenTenantAccount(DomainCommand):
    account_id: UUID
    owner: str


class TenantAccount(DeciderAggregate[AccountState, OpenTenantAccount]):
    aggregate_type = "TenantAccount"

    @staticmethod
    def initial_state() -> AccountState:
        return AccountState()

    @staticmethod
    def decide(command: OpenTenantAccount, state: AccountState) -> list[DomainEvent]:
        if command.tenant_id is not None:
            return [
                TenantAccountOpened(
                    aggregate_id=command.account_id,
                    tenant_id=command.tenant_id,
                    owner=command.owner,
                )
            ]
        return [
            TenantAccountOpened.with_tenant_context(
                aggregate_id=command.account_id,
                owner=command.owner,
            )
        ]

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        if isinstance(event, TenantAccountOpened):
            return state.model_copy(update={"owner": event.owner, "is_open": True})
        return state


class TestAggregateRepositoryExecute:
    @pytest.mark.asyncio
    async def test_execute_creates_decides_and_saves_new_aggregate(self) -> None:
        store = _make_store()
        repo = AggregateRepository(event_store=store, aggregate_factory=Account)
        agg_id = uuid4()

        agg = await repo.execute(agg_id, OpenAccount(account_id=agg_id, owner="alice"))

        assert agg.state.is_open is True
        assert agg.state.owner == "alice"
        assert agg.version == 1
        assert not agg.has_uncommitted_events

        # Subsequent execute loads existing aggregate and appends next event
        updated = await repo.execute(agg_id, DepositMoney(account_id=agg_id, amount=75.0))
        assert updated.state.balance == 75.0
        assert updated.version == 2

    @pytest.mark.asyncio
    async def test_execute_create_if_missing_false_raises_when_missing(self) -> None:
        store = _make_store()
        repo = AggregateRepository(event_store=store, aggregate_factory=Account)
        agg_id = uuid4()

        with pytest.raises(AggregateNotFoundError):
            await repo.execute(
                agg_id,
                DepositMoney(account_id=agg_id, amount=10.0),
                create_if_missing=False,
            )

    @pytest.mark.asyncio
    async def test_execute_raises_type_error_for_non_executable_aggregate(self) -> None:
        store = _make_store()
        repo = AggregateRepository(event_store=store, aggregate_factory=CounterAggregate)

        with pytest.raises(TypeError, match="DeciderAggregate"):
            await repo.execute(uuid4(), object())


class TestTenantAwareRepositoryExecute:
    @pytest.mark.asyncio
    async def test_tenant_execute_persists_when_tenant_matches(self) -> None:
        store = _make_store()
        base_repo = AggregateRepository(event_store=store, aggregate_factory=TenantAccount)
        tenant_repo = TenantAwareRepository(base_repo, require_tenant_context=True)
        tenant_id = uuid4()
        agg_id = uuid4()

        async with tenant_scope(tenant_id):
            agg = await tenant_repo.execute(
                agg_id,
                OpenTenantAccount(account_id=agg_id, owner="alice", tenant_id=tenant_id),
            )
            assert agg.state.owner == "alice"
            assert agg.version == 1

    @pytest.mark.asyncio
    async def test_tenant_execute_rejects_mismatched_tenant(self) -> None:
        store = _make_store()
        base_repo = AggregateRepository(event_store=store, aggregate_factory=TenantAccount)
        tenant_repo = TenantAwareRepository(base_repo)
        scope_tenant = uuid4()
        wrong_tenant = uuid4()
        agg_id = uuid4()

        async with tenant_scope(scope_tenant):
            with pytest.raises(TenantMismatchError):
                await tenant_repo.execute(
                    agg_id,
                    OpenTenantAccount(account_id=agg_id, owner="alice", tenant_id=wrong_tenant),
                )
