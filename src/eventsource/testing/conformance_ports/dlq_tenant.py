"""Tenant scope cases for the `DLQRepository` port (ADR-0114).

Kept apart from `DLQRepositoryConformance` so a backend adopts the owner column
in its own change. A binding mixes this class in once it stores `tenant_id`.
"""

from uuid import UUID, uuid4

import pytest

from eventsource.domain.exceptions.tenant import TenantScopeOperationError
from eventsource.domain.tenant_context import tenant_scope
from eventsource.ports.dlq import DLQRepository


class DLQTenantScopeConformance:
    """Tenant scope cases for `DLQRepository` (ADR-0114).

    Kept apart from `DLQRepositoryConformance` so a backend can adopt the owner
    column in its own change. A binding mixes this class in once it stores
    `tenant_id`. Bindings use the `store` fixture from the base suite.
    """

    async def _add_for(self, store: DLQRepository, tenant_id: UUID | None) -> UUID:
        eid = uuid4()
        await store.add_failed_event(
            event_id=eid,
            projection_name="P",
            event_type="Created",
            event_data={},
            error=RuntimeError("boom"),
            tenant_id=tenant_id,
        )
        return eid

    async def test_entry_reads_back_with_its_owner_tenant(self, store: DLQRepository) -> None:
        tenant = uuid4()
        await self._add_for(store, tenant)
        (entry,) = await store.get_failed_events()
        assert entry.tenant_id == tenant

    async def test_scoped_list_returns_only_the_owners_entries(self, store: DLQRepository) -> None:
        a, b = uuid4(), uuid4()
        await self._add_for(store, a)
        async with tenant_scope(a):
            assert len(await store.get_failed_events()) == 1
        async with tenant_scope(b):
            assert await store.get_failed_events() == []

    async def test_scoped_get_by_id_and_mark_resolved_skip_foreign_entries(
        self, store: DLQRepository
    ) -> None:
        a, b = uuid4(), uuid4()
        await self._add_for(store, a)
        (entry,) = await store.get_failed_events()
        async with tenant_scope(b):
            assert await store.get_failed_event_by_id(entry.id) is None
            await store.mark_resolved(entry.id, "bob")
        unscoped = await store.get_failed_event_by_id(entry.id)
        assert unscoped is not None
        assert unscoped.status == "failed"

    async def test_scoped_mark_retrying_skips_foreign_entries(self, store: DLQRepository) -> None:
        a, b = uuid4(), uuid4()
        await self._add_for(store, a)
        (entry,) = await store.get_failed_events()
        async with tenant_scope(b):
            await store.mark_retrying(entry.id)
        unscoped = await store.get_failed_event_by_id(entry.id)
        assert unscoped is not None
        assert unscoped.status == "failed"

    async def test_scoped_stats_and_counts_exclude_foreign_entries(
        self, store: DLQRepository
    ) -> None:
        a, b = uuid4(), uuid4()
        await self._add_for(store, a)
        async with tenant_scope(b):
            assert (await store.get_failure_stats()).total_failed == 0
            assert await store.get_projection_failure_counts() == []
        async with tenant_scope(a):
            assert (await store.get_failure_stats()).total_failed == 1

    async def test_entry_without_tenant_is_invisible_under_any_scope(
        self, store: DLQRepository
    ) -> None:
        await self._add_for(store, None)
        async with tenant_scope(uuid4()):
            assert await store.get_failed_events() == []
            assert (await store.get_failure_stats()).total_failed == 0
        assert len(await store.get_failed_events()) == 1

    async def test_unscoped_calls_see_entries_of_every_tenant(self, store: DLQRepository) -> None:
        for tenant in (uuid4(), uuid4(), None):
            await self._add_for(store, tenant)
        assert len(await store.get_failed_events()) == 3
        assert (await store.get_failure_stats()).total_failed == 3

    async def test_delete_resolved_events_is_refused_under_a_scope(
        self, store: DLQRepository
    ) -> None:
        tenant = uuid4()
        await self._add_for(store, tenant)
        (entry,) = await store.get_failed_events()
        await store.mark_resolved(entry.id, "alice")
        async with tenant_scope(tenant):
            with pytest.raises(TenantScopeOperationError):
                await store.delete_resolved_events(older_than_days=0)
        assert await store.get_failed_event_by_id(entry.id) is not None
