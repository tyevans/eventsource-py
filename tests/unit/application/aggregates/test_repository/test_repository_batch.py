"""Tests for batch persistence and hydration helpers save_all and load_many."""

from __future__ import annotations

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
from tests.unit.application.aggregates.test_repository._models import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    MockEventPublisher,
)
from tests.unit.domain.test_decider_aggregate import AccountState


class TenantAccountOpened(TenantDomainEvent):
    aggregate_type: str = "TenantAccount"
    owner: str


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
            evt = TenantAccountOpened(
                aggregate_id=command.account_id, tenant_id=command.tenant_id, owner=command.owner
            )
            return [evt]
        evt = TenantAccountOpened.with_tenant_context(
            aggregate_id=command.account_id, owner=command.owner
        )
        return [evt]

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        if isinstance(event, TenantAccountOpened):
            return state.model_copy(update={"owner": event.owner, "is_open": True})
        return state


def _make_store() -> InMemoryEventStore:
    registry = EventRegistry()
    for evt in (TenantAccountOpened, CounterIncremented, CounterDecremented):
        registry.register(evt)
    return InMemoryEventStore(event_registry=registry)


def _make_counter_repo(
    publisher: MockEventPublisher | None = None,
) -> AggregateRepository[CounterAggregate]:
    return AggregateRepository(
        event_store=_make_store(),
        aggregate_factory=CounterAggregate,
        event_publisher=publisher,
    )


def _make_tenant_repo(require_tenant_context: bool = True) -> TenantAwareRepository[TenantAccount]:
    base_repo = AggregateRepository(event_store=_make_store(), aggregate_factory=TenantAccount)
    return TenantAwareRepository(base_repo, require_tenant_context=require_tenant_context)


def _counter(val: int) -> CounterAggregate:
    c = CounterAggregate(uuid4())
    c.increment(val)
    return c


class TestAggregateRepositoryBatch:
    @pytest.mark.asyncio
    async def test_save_all_variadic_persists_in_order(self) -> None:
        publisher = MockEventPublisher()
        repo = _make_counter_repo(publisher)
        c1, c2 = _counter(10), _counter(20)
        await repo.save_all(c1, c2)

        assert not c1.has_uncommitted_events and not c2.has_uncommitted_events
        published_ids = [evt.aggregate_id for evt in publisher.published_events]
        assert published_ids == [c1.aggregate_id, c2.aggregate_id]
        loaded1, loaded2 = await repo.load(c1.aggregate_id), await repo.load(c2.aggregate_id)
        assert loaded1.state is not None and loaded1.state.value == 10
        assert loaded2.state is not None and loaded2.state.value == 20

    @pytest.mark.asyncio
    async def test_save_all_iterable_persists_in_order(self) -> None:
        publisher = MockEventPublisher()
        repo = _make_counter_repo(publisher)
        c1, c2 = _counter(3), _counter(7)
        await repo.save_all([c1, c2])

        published_ids = [evt.aggregate_id for evt in publisher.published_events]
        assert published_ids == [c1.aggregate_id, c2.aggregate_id]
        loaded = await repo.load_many([c1.aggregate_id, c2.aggregate_id])
        assert list(loaded.keys()) == [c1.aggregate_id, c2.aggregate_id]
        s1, s2 = loaded[c1.aggregate_id].state, loaded[c2.aggregate_id].state
        assert s1 is not None and s1.value == 3
        assert s2 is not None and s2.value == 7

    @pytest.mark.asyncio
    async def test_load_many_raises_when_missing_by_default(self) -> None:
        repo = _make_counter_repo()
        c1 = _counter(1)
        await repo.save(c1)
        with pytest.raises(AggregateNotFoundError):
            await repo.load_many([c1.aggregate_id, uuid4()])

    @pytest.mark.asyncio
    async def test_load_many_ignore_missing_omits_missing_ids(self) -> None:
        repo = _make_counter_repo()
        c1, c2 = _counter(5), _counter(9)
        await repo.save_all(c1, c2)

        missing_id = uuid4()
        loaded = await repo.load_many(
            [c1.aggregate_id, missing_id, c2.aggregate_id], ignore_missing=True
        )
        assert list(loaded.keys()) == [c1.aggregate_id, c2.aggregate_id]
        assert missing_id not in loaded
        s1, s2 = loaded[c1.aggregate_id].state, loaded[c2.aggregate_id].state
        assert s1 is not None and s1.value == 5
        assert s2 is not None and s2.value == 9


class TestTenantAwareRepositoryBatch:
    def setup_method(self) -> None:
        clear_tenant_context()

    def teardown_method(self) -> None:
        clear_tenant_context()

    @pytest.mark.asyncio
    async def test_tenant_save_all_variadic_and_iterable(self) -> None:
        tenant_repo = _make_tenant_repo(require_tenant_context=True)
        tenant_id = uuid4()
        id1, id2, id3 = uuid4(), uuid4(), uuid4()

        async with tenant_scope(tenant_id):
            a1, a2, a3 = (tenant_repo.create_new(i) for i in (id1, id2, id3))
            a1.execute(OpenTenantAccount(account_id=id1, owner="alice", tenant_id=tenant_id))
            a2.execute(OpenTenantAccount(account_id=id2, owner="bob", tenant_id=tenant_id))
            a3.execute(OpenTenantAccount(account_id=id3, owner="carol", tenant_id=tenant_id))
            await tenant_repo.save_all(a1, a2)
            await tenant_repo.save_all([a3])
            loaded = await tenant_repo.load_many([id1, id2, id3])

        assert list(loaded.keys()) == [id1, id2, id3]
        assert loaded[id1].state.owner == "alice"
        assert loaded[id2].state.owner == "bob"
        assert loaded[id3].state.owner == "carol"

    @pytest.mark.asyncio
    async def test_tenant_save_all_validates_tenant_consistency(self) -> None:
        tenant_repo = _make_tenant_repo(require_tenant_context=True)
        tenant_id, wrong_tenant, id1, id2 = uuid4(), uuid4(), uuid4(), uuid4()

        async with tenant_scope(tenant_id):
            a1, a2 = tenant_repo.create_new(id1), tenant_repo.create_new(id2)
            a1.execute(OpenTenantAccount(account_id=id1, owner="alice", tenant_id=tenant_id))
            a2.execute(OpenTenantAccount(account_id=id2, owner="mallory", tenant_id=wrong_tenant))
            with pytest.raises(TenantMismatchError):
                await tenant_repo.save_all([a1, a2])

    @pytest.mark.asyncio
    async def test_tenant_load_many_raises_when_missing_by_default(self) -> None:
        tenant_repo = _make_tenant_repo(require_tenant_context=True)
        tenant_id, id1 = uuid4(), uuid4()

        async with tenant_scope(tenant_id):
            await tenant_repo.execute(
                id1, OpenTenantAccount(account_id=id1, owner="alice", tenant_id=tenant_id)
            )
            with pytest.raises(AggregateNotFoundError):
                await tenant_repo.load_many([id1, uuid4()])

    @pytest.mark.asyncio
    async def test_tenant_load_many_ignore_missing_and_requires_tenant_context(self) -> None:
        tenant_repo = _make_tenant_repo(require_tenant_context=True)
        tenant_id, id1 = uuid4(), uuid4()

        async with tenant_scope(tenant_id):
            await tenant_repo.execute(
                id1, OpenTenantAccount(account_id=id1, owner="alice", tenant_id=tenant_id)
            )
            loaded = await tenant_repo.load_many([id1, uuid4()], ignore_missing=True)
            assert list(loaded.keys()) == [id1] and loaded[id1].state.owner == "alice"

        with pytest.raises(TenantContextNotSetError):
            await tenant_repo.load_many([id1], ignore_missing=True)
