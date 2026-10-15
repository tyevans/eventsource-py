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


def _deps(request: Any) -> tuple[Any, ...]:
    f = request.getfixturevalue
    return (
        f("source_store"),
        f("target_store"),
        f("migration_repo"),
        f("routing_repo"),
        f("router"),
        f("lock_manager"),
    )


class TestRecoveryAfterSimulatedFailures:
    async def test_recovery_from_bulk_copy_failure(
        self, request: Any, audit_log_repo: Any, tenant_id: UUID
    ) -> None:
        src, tgt, mig_repo, rt_repo, router, locks = _deps(request)
        await create_test_events(src, tenant_id, count=10)
        _, mig = await _start_dedicated(src, tgt, mig_repo, rt_repo, router, locks, tenant_id)
        err_msg = "Simulated bulk copy failure"
        await mig_repo.record_error(mig.id, error=err_msg)
        entry = MigrationAuditEntry.error_occurred(
            mig.id, datetime.now(UTC), err_msg, "BulkCopyError"
        )
        await audit_log_repo.record(entry)
        mig = await mig_repo.get(mig.id)
        assert mig is not None and mig.error_count == 1
        assert "bulk copy" in mig.last_error.lower() and not mig.phase.is_terminal
        events = await audit_log_repo.get_by_migration(
            mig.id, event_types=[AuditEventType.ERROR_OCCURRED]
        )
        assert len(events) == 1

    async def test_recovery_from_cutover_rollback(
        self, request: Any, audit_log_repo: Any, tenant_id: UUID, migration_id: UUID
    ) -> None:
        src, tgt, _, rt_repo, router, locks = _deps(request)
        await create_test_events(src, tenant_id, count=5)
        await copy_events_between_stores(src, tgt, tenant_id)
        for i in range(200):
            agg_id = uuid4()
            event = SampleTestEvent(aggregate_id=agg_id, tenant_id=tenant_id, value=f"extra-{i}")
            stream = StreamId(aggregate_id=agg_id, category="TestAggregate")
            await src.append(stream, [event], ExpectedVersion.no_stream())
        await rt_repo.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE, migration_id)
        router.register_store("dedicated", tgt)
        cfg = MigrationConfig(cutover_max_lag_events=50)
        lag_tracker = SyncLagTracker(src, tgt, tenant_id, cfg, enable_tracing=False)
        cutover_mgr = CutoverManager(locks, router, rt_repo, enable_tracing=False)
        res = await cutover_mgr.execute_cutover(
            migration_id, tenant_id, lag_tracker, "dedicated", config=cfg, timeout_ms=500.0
        )
        assert res.success is False and res.rolled_back is True
        await audit_log_repo.record(
            _audit_entry(
                migration_id,
                AuditEventType.CUTOVER_ROLLED_BACK,
                {"reason": res.error_message, "duration_ms": res.duration_ms},
                old_phase=MigrationPhase.CUTOVER,
                new_phase=MigrationPhase.DUAL_WRITE,
                operator="system",
            )
        )
        routing = await rt_repo.get_routing(tenant_id)
        assert routing is not None and routing.migration_state == TenantMigrationState.DUAL_WRITE
        events = await audit_log_repo.get_by_migration(
            migration_id, event_types=[AuditEventType.CUTOVER_ROLLED_BACK]
        )
        assert len(events) == 1

    async def test_pause_resume_recovery(
        self, request: Any, audit_log_repo: Any, tenant_id: UUID
    ) -> None:
        coord, mig = await _start_dedicated(*_deps(request), tenant_id)
        await coord.pause_migration(mig.id)
        await audit_log_repo.record(
            _audit_entry(mig.id, AuditEventType.MIGRATION_PAUSED, {"reason": "Maintenance"})
        )
        assert (await coord.get_migration(mig.id)).is_paused
        await coord.resume_migration(mig.id)
        await audit_log_repo.record(
            _audit_entry(mig.id, AuditEventType.MIGRATION_RESUMED, {"resumed_by": "admin"})
        )
        assert not (await coord.get_migration(mig.id)).is_paused
        types = [AuditEventType.MIGRATION_PAUSED, AuditEventType.MIGRATION_RESUMED]
        assert len(await audit_log_repo.get_by_migration(mig.id, event_types=types)) == 2

    async def test_abort_recovery(self, request: Any, audit_log_repo: Any, tenant_id: UUID) -> None:
        src, tgt, mig_repo, rt_repo, router, locks = _deps(request)
        coord, mig = await _start_dedicated(src, tgt, mig_repo, rt_repo, router, locks, tenant_id)
        res = await coord.abort_migration(mig.id, reason="Test abort for recovery")
        await audit_log_repo.record(
            _audit_entry(
                mig.id,
                AuditEventType.MIGRATION_ABORTED,
                {"reason": "Test abort for recovery"},
                old_phase=res.final_phase,
                new_phase=MigrationPhase.ABORTED,
            )
        )
        assert res.success is False and res.final_phase == MigrationPhase.ABORTED
        aborted = await coord.get_migration(res.migration_id)
        assert aborted is not None and aborted.phase == MigrationPhase.ABORTED
        rt = await rt_repo.get_routing(tenant_id)
        assert rt is not None and rt.migration_state == TenantMigrationState.NORMAL
        new_mig = await coord.start_migration(tenant_id, tgt, "dedicated")
        assert new_mig is not None and new_mig.id != mig.id


__all__ = ["TestRecoveryAfterSimulatedFailures"]  # pragma: allowlist secret
