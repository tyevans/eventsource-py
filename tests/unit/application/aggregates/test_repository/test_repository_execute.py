"""Tests for AggregateRepository.execute and TenantAwareRepository.execute."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource import (
    EventRegistry,
    TenantContextNotSetError,
    TenantDomainEvent,
    clear_tenant_context,
    tenant_scope,
)
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.tenant_repository import TenantAwareRepository
from eventsource.domain.command import DomainCommand
from eventsource.domain.decider import DeciderAggregate
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import AggregateNotFoundError, TenantMismatchError
from tests.unit.application.aggregates.test_repository import _models as cm
from tests.unit.domain import test_decider_aggregate as dm

pytestmark = pytest.mark.asyncio


class TenantAccountOpened(TenantDomainEvent):
    aggregate_type: str = "TenantAccount"
    owner: str


def _make_store() -> InMemoryEventStore:
    reg = EventRegistry()
    for evt in (
        dm.AccountOpened,
        dm.MoneyDeposited,
        TenantAccountOpened,
        cm.CounterIncremented,
        cm.CounterDecremented,
    ):
        reg.register(evt)
    return InMemoryEventStore(event_registry=reg)


class OpenTenantAccount(DomainCommand):
    account_id: UUID
    owner: str = "alice"


class TenantAccount(DeciderAggregate[dm.AccountState, OpenTenantAccount]):
    aggregate_type = "TenantAccount"

    @staticmethod
    def initial_state() -> dm.AccountState:
        return dm.AccountState()

    @staticmethod
    def decide(command: OpenTenantAccount, state: dm.AccountState) -> list[DomainEvent]:
        agg_id, owner, tid = command.account_id, command.owner, command.tenant_id
        if tid is not None:
            return [TenantAccountOpened(aggregate_id=agg_id, tenant_id=tid, owner=owner)]
        return [TenantAccountOpened.with_tenant_context(aggregate_id=agg_id, owner=owner)]

    @staticmethod
    def evolve(state: dm.AccountState, event: DomainEvent) -> dm.AccountState:
        if isinstance(event, TenantAccountOpened):
            return state.model_copy(update={"owner": event.owner, "is_open": True})
        return state


def _make_tenant_repo(require_tenant_context: bool = True) -> TenantAwareRepository[TenantAccount]:
    base_repo = AggregateRepository(event_store=_make_store(), aggregate_factory=TenantAccount)
    return TenantAwareRepository(base_repo, require_tenant_context=require_tenant_context)


def _account_repo() -> AggregateRepository[dm.Account]:
    return AggregateRepository(event_store=_make_store(), aggregate_factory=dm.Account)


def _counter_repo() -> AggregateRepository[cm.CounterAggregate]:
    return AggregateRepository(event_store=_make_store(), aggregate_factory=cm.CounterAggregate)


@pytest.fixture(autouse=True)
def _clean_tenant_context() -> None:
    clear_tenant_context()


class TestAggregateRepositoryExecute:
    async def test_execute_creates_decides_and_saves_new_aggregate(self) -> None:
        repo, aid = _account_repo(), uuid4()
        agg = await repo.execute(aid, dm.OpenAccount(account_id=aid, owner="alice"))
        assert (agg.state.is_open, agg.state.owner, agg.version) == (True, "alice", 1)
        assert not agg.has_uncommitted_events
        updated = await repo.execute(aid, dm.DepositMoney(account_id=aid, amount=75.0))
        assert (updated.state.balance, updated.version) == (75.0, 2)

    async def test_execute_variadic_commands_applies_in_order_and_saves_once(self) -> None:
        repo, aid = _account_repo(), uuid4()
        agg = await repo.execute(
            aid,
            dm.OpenAccount(account_id=aid, owner="alice"),
            dm.DepositMoney(account_id=aid, amount=50.0),
            dm.DepositMoney(account_id=aid, amount=25.0),
        )
        assert (agg.state.is_open, agg.state.balance, agg.version) == (True, 75.0, 3)
        assert not agg.has_uncommitted_events

    async def test_execute_create_if_missing_false_raises_when_missing(self) -> None:
        aid, cmd = uuid4(), dm.DepositMoney(account_id=uuid4(), amount=10.0)
        with pytest.raises(AggregateNotFoundError):
            await _account_repo().execute(aid, cmd, create_if_missing=False)

    async def test_execute_callable_creates_and_updates_imperative_aggregate(self) -> None:
        repo, aid = _counter_repo(), uuid4()
        created = await repo.execute(aid, lambda c: c.increment(5))
        assert created.state is not None and (created.state.value, created.version) == (5, 1)
        updated = await repo.execute(aid, lambda c: c.decrement(2), create_if_missing=False)
        assert updated.state is not None and (updated.state.value, updated.version) == (3, 2)

    async def test_execute_callable_create_if_missing_false_raises_when_missing(self) -> None:
        with pytest.raises(AggregateNotFoundError):
            await _counter_repo().execute(uuid4(), lambda c: c.increment(1), create_if_missing=False)  # fmt: skip

    async def test_execute_raises_type_error_for_non_executable_aggregate(self) -> None:
        with pytest.raises(TypeError, match="DeciderAggregate"):
            await _counter_repo().execute(uuid4(), object())


class TestTenantAwareRepositoryExecute:
    async def test_tenant_execute_persists_when_tenant_matches(self) -> None:
        repo, tid, aid = _make_tenant_repo(), uuid4(), uuid4()
        async with tenant_scope(tid):
            agg = await repo.execute(aid, OpenTenantAccount(account_id=aid, tenant_id=tid))
            assert (agg.state.owner, agg.version) == ("alice", 1)

    async def test_tenant_execute_callable_and_variadic_commands(self) -> None:
        repo = TenantAwareRepository(_counter_repo(), require_tenant_context=True)
        async with tenant_scope(uuid4()):
            agg = await repo.execute(uuid4(), lambda c: c.increment(10), lambda c: c.decrement(3))
            assert agg.state is not None and (agg.state.value, agg.version) == (7, 2)

    async def test_tenant_execute_rejects_mismatched_tenant(self) -> None:
        repo, aid = _make_tenant_repo(require_tenant_context=False), uuid4()
        async with tenant_scope(uuid4()):
            with pytest.raises(TenantMismatchError):
                await repo.execute(aid, OpenTenantAccount(account_id=aid, tenant_id=uuid4()))


class TestAggregateRepositoryGetOrNoneAndExists:
    async def test_get_or_none_and_exists_when_present_and_missing(self) -> None:
        repo, aid, missing = _account_repo(), uuid4(), uuid4()
        assert await repo.get_or_none(missing) is None and await repo.exists(missing) is False
        await repo.execute(aid, dm.OpenAccount(account_id=aid, owner="alice"))
        found = await repo.get_or_none(aid)
        assert found is not None and (found.aggregate_id, found.version) == (aid, 1)
        assert found.state.owner == "alice" and await repo.exists(aid) is True


class TestTenantAwareRepositoryGetOrNoneAndExists:
    async def test_get_or_none_and_exists_when_present_and_missing(self) -> None:
        repo, tid, aid, missing = _make_tenant_repo(), uuid4(), uuid4(), uuid4()
        async with tenant_scope(tid):
            assert await repo.get_or_none(missing) is None and await repo.exists(missing) is False
            await repo.execute(aid, OpenTenantAccount(account_id=aid, tenant_id=tid))
            found = await repo.get_or_none(aid)
            assert found is not None and (found.aggregate_id, found.version) == (aid, 1)
            assert found.state.owner == "alice" and await repo.exists(aid) is True

    async def test_get_or_none_and_exists_propagate_missing_tenant_context(self) -> None:
        repo = _make_tenant_repo()
        with pytest.raises(TenantContextNotSetError):
            await repo.get_or_none(uuid4())
        with pytest.raises(TenantContextNotSetError):
            await repo.exists(uuid4())

    async def test_get_or_none_and_exists_propagate_tenant_mismatch_error(self) -> None:
        err = TenantMismatchError(uuid4(), uuid4(), [uuid4()])
        repo = TenantAwareRepository(
            MagicMock(load=AsyncMock(side_effect=err), exists=AsyncMock(side_effect=err))
        )
        with pytest.raises(TenantMismatchError):
            await repo.get_or_none(uuid4())
        with pytest.raises(TenantMismatchError):
            await repo.exists(uuid4())
