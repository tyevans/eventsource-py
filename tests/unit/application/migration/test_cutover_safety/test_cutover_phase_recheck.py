"""A cutover stops without switching routing if the migration left CUTOVER before the switch."""

from __future__ import annotations

import asyncio

import pytest

from eventsource.ports.migration.models import MigrationConfig, MigrationPhase, TenantMigrationState
from tests.unit.application.migration.test_cutover_safety._harness import (
    SOURCE_ID,
    TARGET_ID,
    build_harness,
    run_to_dual_write,
)


@pytest.mark.asyncio
async def test_cutover_does_not_switch_routing_after_migration_is_aborted() -> None:
    # Gate the cutover at its first routing read, which runs inside the cutover lock.
    h = build_harness(gate_on="get_routing")
    config = MigrationConfig(verify_consistency=False, migrate_subscriptions=False)
    migration = await run_to_dual_write(h, config)
    h.routing.armed = True

    cutover = asyncio.create_task(h.coordinator.trigger_cutover(migration.id))
    await asyncio.wait_for(h.routing.reached.wait(), timeout=5)
    # A migration that left CUTOVER while the cutover was in flight.
    await h.migration_repo.update_phase(migration.id, MigrationPhase.ABORTED)
    h.routing.release.set()

    result = await cutover

    assert result.success is False and result.rolled_back is False
    assert result.error_message is not None and "CUTOVER" in result.error_message
    routing = await h.routing.get_routing(h.tenant_id)
    assert routing is not None
    assert routing.store_id == SOURCE_ID and routing.store_id != TARGET_ID
    assert routing.migration_state == TenantMigrationState.NORMAL
    final = await h.migration_repo.get(migration.id)
    assert final is not None and final.phase == MigrationPhase.ABORTED
