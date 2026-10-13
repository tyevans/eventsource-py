"""Tests for write-active tenant first-pass cutover."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import (
    MigrationConfig,
    MigrationPhase,
    TenantMigrationState,
)
from tests.unit.application.migration.test_phase2_integration._events import (
    _REGISTRY,
    OrderCreated,
)
from tests.unit.application.migration.test_phase2_integration._helpers import (
    create_test_events,
    get_all_tenant_events,
)
from tests.unit.application.migration.test_phase2_integration._mocks import GatedFeedStore

if TYPE_CHECKING:
    from eventsource.application.migration.write_pause import WritePauseManager
    from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager
    from tests.unit.application.migration.test_phase2_integration._repositories import (
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestWriteActiveTenantFirstPassCutover:
    """The dual-write interceptor must be installed BEFORE the copy pass.

    With the interceptor installed only after the copy completes, writes
    landing during the copy are mirrored by nobody and never drain: the
    lag anchor stays clamped at the checkpoint, the gap events count as
    lag forever, and a write-active tenant can never cut over on its
    first pass. Installing the interceptor before the pass means every
    event is either in the pass's snapshot (copied) or mirrored
    (dual-written) -- the window never exists.
    """

    @pytest.mark.asyncio
    async def test_write_active_tenant_cuts_over_on_first_pass(
        self,
        tenant_id: UUID,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        lock_manager: MockLockManager,
        write_pause_manager: WritePauseManager,
    ) -> None:
        """Writes landing mid-copy are mirrored, and cutover fires first pass."""
        inner_source = InMemoryEventStore("source", event_registry=_REGISTRY)
        target_store = InMemoryEventStore("target", event_registry=_REGISTRY)
        source = GatedFeedStore(inner_source, gate_after=3)

        router = TenantStoreRouter(
            default_store=source,
            routing_repo=routing_repo,
            stores={"default": source},
            default_store_id="default",
            enable_tracing=False,
            write_pause_manager=write_pause_manager,
        )
        coordinator = MigrationCoordinator(
            source_store=source,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        await create_test_events(inner_source, tenant_id, count=6)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=MigrationConfig(
                cutover_max_lag_events=3,
                batch_size=2,
                verify_consistency=False,
                migrate_subscriptions=False,
            ),
        )

        # The copy pass is provably mid-flight: blocked inside its feed read.
        await asyncio.wait_for(source.mid_pass.wait(), timeout=5.0)

        # Live tenant writes while the copy runs -- more of them than the
        # cutover threshold, so un-mirrored gap events could never drain.
        for _i in range(5):
            aggregate_id = uuid4()
            await router.append(
                StreamId(aggregate_id=aggregate_id, category="Order"),
                [OrderCreated(aggregate_id=aggregate_id, tenant_id=tenant_id)],
                ExpectedVersion.no_stream(),
            )

        source.resume_gate.set()
        await coordinator.wait_for_phase(
            migration.id, MigrationPhase.DUAL_WRITE, timeout=5.0, poll_interval=0.01
        )

        result = await coordinator.trigger_cutover(migration.id)

        assert result.success is True
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.store_id == "dedicated"
        assert routing.migration_state == TenantMigrationState.MIGRATED

        # Nothing was left behind: every source event reached the target.
        source_ids = {e.event_id for e in await get_all_tenant_events(inner_source, tenant_id)}
        target_ids = {e.event_id for e in await get_all_tenant_events(target_store, tenant_id)}
        assert source_ids == target_ids


__all__ = ["TestWriteActiveTenantFirstPassCutover"]
