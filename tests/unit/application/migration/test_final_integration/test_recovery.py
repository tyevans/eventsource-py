"""Tests for recovery after simulated failures in migrations."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.cutover import CutoverManager
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.domain import StreamId
from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationConfig,
    MigrationPhase,
    TenantMigrationState,
)
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.migration.test_final_integration._events import SampleTestEvent
from tests.unit.application.migration.test_final_integration._helpers import (
    copy_events_between_stores,
    create_test_events,
)

pytestmark = pytest.mark.asyncio


async def _start_dedicated(
    src: Any, tgt: Any, mig_repo: Any, rt_repo: Any, router: Any, locks: Any, tenant_id: UUID
) -> tuple[MigrationCoordinator, Any]:
    coord = MigrationCoordinator(
        source_store=src,
        migration_repo=mig_repo,
        routing_repo=rt_repo,
        router=router,
        lock_manager=locks,
        source_store_id="default",
        enable_tracing=False,
    )
    router.register_store("dedicated", tgt)
    mig = await coord.start_migration(
        tenant_id=tenant_id, target_store=tgt, target_store_id="dedicated"
    )
    return coord, mig


def _audit_entry(
    migration_id: UUID,
    event_type: AuditEventType,
    details: dict[str, Any],
    *,
    old_phase: MigrationPhase | None = None,
    new_phase: MigrationPhase | None = None,
    operator: str = "admin",
) -> MigrationAuditEntry:
    return MigrationAuditEntry(
        id=None,
        migration_id=migration_id,
        event_type=event_type,
        old_phase=old_phase,
        new_phase=new_phase,
        details=details,
        operator=operator,
        occurred_at=datetime.now(UTC),
    )


class TestRecoveryAfterSimulatedFailures:
    async def test_recovery_from_bulk_copy_failure(
        self,
        source_store,
        target_store,
        migration_repo,
        routing_repo,
        router,
        lock_manager,
        audit_log_repo,
        tenant_id,
    ):
        await create_test_events(source_store, tenant_id, count=10)
        deps = (source_store, target_store, migration_repo, routing_repo, router, lock_manager)
        _, migration = await _start_dedicated(*deps, tenant_id)
        err_msg = "Simulated bulk copy failure"
        await migration_repo.record_error(migration.id, error=err_msg)
        err_entry = MigrationAuditEntry.error_occurred(
            migration.id, datetime.now(UTC), err_msg, "BulkCopyError"
        )
        await audit_log_repo.record(err_entry)
        migration = await migration_repo.get(migration.id)
        assert migration is not None and migration.error_count == 1
        assert "bulk copy" in migration.last_error.lower() and not migration.phase.is_terminal
        types = [AuditEventType.ERROR_OCCURRED]
        assert len(await audit_log_repo.get_by_migration(migration.id, event_types=types)) == 1

    async def test_recovery_from_cutover_rollback(
        self,
        source_store,
        target_store,
        routing_repo,
        router,
        lock_manager,
        audit_log_repo,
        tenant_id,
        migration_id,
    ):
        await create_test_events(source_store, tenant_id, count=5)
        await copy_events_between_stores(source_store, target_store, tenant_id)
        for i in range(200):
            agg_id = uuid4()
            event = SampleTestEvent(aggregate_id=agg_id, tenant_id=tenant_id, value=f"extra-{i}")
            stream = StreamId(aggregate_id=agg_id, category="TestAggregate")
            await source_store.append(stream, [event], ExpectedVersion.no_stream())
        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)
        cfg = MigrationConfig(cutover_max_lag_events=50)
        lag_tracker = SyncLagTracker(
            source_store, target_store, tenant_id, cfg, enable_tracing=False
        )
        cutover_mgr = CutoverManager(lock_manager, router, routing_repo, enable_tracing=False)
        result = await cutover_mgr.execute_cutover(
            migration_id, tenant_id, lag_tracker, "dedicated", config=cfg, timeout_ms=500.0
        )
        assert result.success is False and result.rolled_back is True
        details = {"reason": result.error_message, "duration_ms": result.duration_ms}
        await audit_log_repo.record(
            _audit_entry(
                migration_id,
                AuditEventType.CUTOVER_ROLLED_BACK,
                details,
                old_phase=MigrationPhase.CUTOVER,
                new_phase=MigrationPhase.DUAL_WRITE,
                operator="system",
            )
        )
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None and routing.migration_state == TenantMigrationState.DUAL_WRITE
        types = [AuditEventType.CUTOVER_ROLLED_BACK]
        assert len(await audit_log_repo.get_by_migration(migration_id, event_types=types)) == 1

    async def test_pause_resume_recovery(
        self,
        source_store,
        target_store,
        migration_repo,
        routing_repo,
        router,
        lock_manager,
        audit_log_repo,
        tenant_id,
    ):
        deps = (source_store, target_store, migration_repo, routing_repo, router, lock_manager)
        coordinator, migration = await _start_dedicated(*deps, tenant_id)
        await coordinator.pause_migration(migration.id)
        await audit_log_repo.record(
            _audit_entry(migration.id, AuditEventType.MIGRATION_PAUSED, {"reason": "Maintenance"})
        )
        assert (await coordinator.get_migration(migration.id)).is_paused
        await coordinator.resume_migration(migration.id)
        await audit_log_repo.record(
            _audit_entry(migration.id, AuditEventType.MIGRATION_RESUMED, {"resumed_by": "admin"})
        )
        assert not (await coordinator.get_migration(migration.id)).is_paused
        types = [AuditEventType.MIGRATION_PAUSED, AuditEventType.MIGRATION_RESUMED]
        assert len(await audit_log_repo.get_by_migration(migration.id, event_types=types)) == 2

    async def test_abort_recovery(
        self,
        source_store,
        target_store,
        migration_repo,
        routing_repo,
        router,
        lock_manager,
        audit_log_repo,
        tenant_id,
    ):
        deps = (source_store, target_store, migration_repo, routing_repo, router, lock_manager)
        coordinator, migration = await _start_dedicated(*deps, tenant_id)
        result = await coordinator.abort_migration(migration.id, reason="Test abort for recovery")
        await audit_log_repo.record(
            _audit_entry(
                migration.id,
                AuditEventType.MIGRATION_ABORTED,
                {"reason": "Test abort for recovery"},
                old_phase=result.final_phase,
                new_phase=MigrationPhase.ABORTED,
            )
        )
        assert result.success is False and result.final_phase == MigrationPhase.ABORTED
        mig = await coordinator.get_migration(result.migration_id)
        assert mig is not None and mig.phase == MigrationPhase.ABORTED
        rt = await routing_repo.get_routing(tenant_id)
        assert rt is not None and rt.migration_state == TenantMigrationState.NORMAL
        new_mig = await coordinator.start_migration(tenant_id, target_store, "dedicated")
        assert new_mig is not None and new_mig.id != migration.id


__all__ = ["TestRecoveryAfterSimulatedFailures"]  # pragma: allowlist secret
