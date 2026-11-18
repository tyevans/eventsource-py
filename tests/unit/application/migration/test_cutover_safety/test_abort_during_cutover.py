"""Abort is refused while a cutover runs, and cannot race one into ABORTED with MIGRATED routing."""

from __future__ import annotations

import asyncio

import pytest

from eventsource.application.migration.exceptions import MigrationError
from eventsource.ports.locks import migration_lock_key
from eventsource.ports.migration.models import MigrationConfig, MigrationPhase, TenantMigrationState
from tests.unit.application.migration.test_cutover_safety._harness import (
    TARGET_ID,
    build_harness,
    create_dual_write_migration,
    run_to_dual_write,
)


class TestAbortDuringCutover:
    @pytest.mark.asyncio
    async def test_abort_raises_while_cutover_is_running(self) -> None:
        h = build_harness(gate_on="switch_routing")
        config = MigrationConfig(verify_consistency=False, migrate_subscriptions=False)
        migration = await run_to_dual_write(h, config)
        h.routing.armed = True
        cutover = asyncio.create_task(h.coordinator.trigger_cutover(migration.id))
        await asyncio.wait_for(h.routing.reached.wait(), timeout=5)

        with pytest.raises(MigrationError, match="during cutover"):
            await h.coordinator.abort_migration(migration.id, reason="operator")

        h.routing.release.set()
        result = await cutover
        assert result.success is True
        final = await h.migration_repo.get(migration.id)
        assert final is not None and final.phase == MigrationPhase.COMPLETED
        routing = await h.routing.get_routing(h.tenant_id)
        assert routing is not None and routing.store_id == TARGET_ID

    @pytest.mark.asyncio
    async def test_abort_waiting_on_cutover_lock_cannot_interleave(self) -> None:
        """Abort passes the DUAL_WRITE check, then the cutover enters CUTOVER; abort must re-check."""
        h = build_harness()
        migration = await create_dual_write_migration(h)
        key = migration_lock_key(h.tenant_id, "cutover")

        async with h.lock_manager.acquire(key):
            aborting = asyncio.create_task(h.coordinator.abort_migration(migration.id))
            for _ in range(20):
                await asyncio.sleep(0)
            # The cutover starts while abort waits on the lock.
            await h.migration_repo.update_phase(migration.id, MigrationPhase.CUTOVER)

        with pytest.raises(MigrationError, match="during cutover"):
            await aborting
        final = await h.migration_repo.get(migration.id)
        assert final is not None and final.phase == MigrationPhase.CUTOVER
        routing = await h.routing.get_routing(h.tenant_id)
        assert routing is not None and routing.migration_state == TenantMigrationState.DUAL_WRITE

    @pytest.mark.asyncio
    async def test_abort_in_dual_write_still_aborts(self) -> None:
        h = build_harness()
        migration = await create_dual_write_migration(h)
        result = await h.coordinator.abort_migration(migration.id, reason="operator")
        assert result.final_phase == MigrationPhase.ABORTED and result.success is False
        final = await h.migration_repo.get(migration.id)
        assert final is not None and final.phase == MigrationPhase.ABORTED

    @pytest.mark.asyncio
    async def test_abort_in_cutover_refused_without_lock_manager(self) -> None:
        h = build_harness(with_lock=False)
        migration = await create_dual_write_migration(h)
        await h.migration_repo.update_phase(migration.id, MigrationPhase.CUTOVER)
        with pytest.raises(MigrationError, match="during cutover"):
            await h.coordinator.abort_migration(migration.id)
        final = await h.migration_repo.get(migration.id)
        assert final is not None and final.phase == MigrationPhase.CUTOVER
