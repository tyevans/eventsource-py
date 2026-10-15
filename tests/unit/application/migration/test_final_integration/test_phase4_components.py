"""Tests for integration of all Phase 4 components working together."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pytest

from eventsource.application.migration.consistency import ConsistencyVerifier, VerificationLevel
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.metrics import (
    ActiveMigrationsTracker,
    get_migration_metrics,
    release_migration_metrics,
)
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationConfig,
    MigrationPhase,
    TenantMigrationState,
)
from tests.unit.application.migration.test_final_integration._events import src_pos, tgt_pos
from tests.unit.application.migration.test_final_integration._helpers import (
    copy_events_between_stores,
    create_test_events,
)


def _make_coordinator(*args: Any, **kwargs: Any) -> MigrationCoordinator:
    src, mig_repo, rout_repo, router, lock_mgr = args
    return MigrationCoordinator(
        source_store=src,
        migration_repo=mig_repo,
        routing_repo=rout_repo,
        router=router,
        lock_manager=lock_mgr,
        source_store_id="default",
        enable_tracing=False,
        **kwargs,
    )


class TestIntegrationOfAllPhase4Components:
    @pytest.mark.asyncio
    async def test_all_components_work_together(
        self,
        source_store: Any,
        target_store: Any,
        migration_repo: Any,
        routing_repo: Any,
        audit_log_repo: Any,
        position_mapping_repo: Any,
        checkpoint_repo: Any,
        router: Any,
        lock_manager: Any,
        tenant_id: UUID,
    ) -> None:
        pm = PositionMapper(position_mapping_repo, enable_tracing=False)
        await create_test_events(source_store, tenant_id, count=5)
        args = (source_store, migration_repo, routing_repo, router, lock_manager)
        coordinator = _make_coordinator(*args, position_mapper=pm, checkpoint_repo=checkpoint_repo)
        router.register_store("dedicated", target_store)
        cfg = MigrationConfig(batch_size=10, verify_consistency=True, migrate_subscriptions=False)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated", config=cfg
        )
        metrics = get_migration_metrics(migration_id=str(migration.id), tenant_id=str(tenant_id))
        now = datetime.now(UTC)
        await audit_log_repo.record(
            MigrationAuditEntry.migration_started(
                migration_id=migration.id,
                occurred_at=now,
                operator="test",
                details={"config": migration.config.to_dict()},
            )
        )
        with metrics.time_phase("bulk_copy"):
            copied = await copy_events_between_stores(source_store, target_store, tenant_id)
            metrics.record_events_copied(copied, rate_events_per_sec=100.0)
            counter = 1
            async for env in source_store.read_all():
                if env.event.tenant_id == tenant_id:
                    await pm.record_mapping(
                        migration.id, src_pos(counter), tgt_pos(counter), env.event.event_id
                    )
                    counter += 1

        await audit_log_repo.record(
            MigrationAuditEntry.phase_change(
                migration_id=migration.id,
                old_phase=MigrationPhase.BULK_COPY,
                new_phase=MigrationPhase.DUAL_WRITE,
                occurred_at=now,
                operator="system",
            )
        )
        with metrics.time_phase("dual_write"):
            metrics.record_sync_lag(10)
            await asyncio.sleep(0.01)
            metrics.record_sync_lag(0)

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)
        report = await verifier.verify_tenant_consistency(tenant_id, level=VerificationLevel.COUNT)
        details = {
            "is_consistent": report.is_consistent,
            "source_count": report.source_event_count,
            "target_count": report.target_event_count,
        }
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.VERIFICATION_COMPLETED,
                old_phase=None,
                new_phase=None,
                details=details,
                operator="system",
                occurred_at=now,
            )
        )
        snap = metrics.get_snapshot()
        assert snap.events_copied >= 5
        assert "bulk_copy" in snap.phase_durations and "dual_write" in snap.phase_durations
        assert len(await audit_log_repo.get_by_migration(migration.id)) >= 3
        assert await pm.get_mapping_count(migration.id) == 5
        assert report.is_consistent is True
        assert (report.source_event_count, report.target_event_count) == (5, 5)
        release_migration_metrics(str(migration.id))

    @pytest.mark.asyncio
    async def test_end_to_end_migration_smoke_test(
        self,
        source_store: Any,
        target_store: Any,
        migration_repo: Any,
        routing_repo: Any,
        router: Any,
        lock_manager: Any,
        tenant_id: UUID,
    ) -> None:
        await create_test_events(source_store, tenant_id, count=20)
        coordinator = _make_coordinator(
            source_store, migration_repo, routing_repo, router, lock_manager
        )
        router.register_store("dedicated", target_store)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )
        assert migration is not None
        assert migration.phase in (MigrationPhase.PENDING, MigrationPhase.BULK_COPY)
        status = await coordinator.get_status(migration.id)
        assert status is not None and status.migration_id == migration.id
        assert status.tenant_id == tenant_id
        await asyncio.sleep(0.1)
        result = await coordinator.abort_migration(migration.id, reason="Smoke test cleanup")
        assert result.final_phase == MigrationPhase.ABORTED
        routing = await routing_repo.get_routing(tenant_id)
        assert routing.migration_state == TenantMigrationState.NORMAL

    @pytest.mark.asyncio
    async def test_bulk_copy_phase_reports_metrics_through_real_run(
        self,
        source_store: Any,
        target_store: Any,
        migration_repo: Any,
        routing_repo: Any,
        router: Any,
        lock_manager: Any,
        tenant_id: UUID,
    ) -> None:
        await create_test_events(source_store, tenant_id, count=5)
        coordinator = _make_coordinator(
            source_store, migration_repo, routing_repo, router, lock_manager
        )
        router.register_store("dedicated", target_store)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )
        await coordinator.wait_for_phase(migration.id, MigrationPhase.DUAL_WRITE, timeout=5.0)
        tracker = ActiveMigrationsTracker.get_instance()
        assert str(migration.id) in tracker.active_migrations
        snapshot = get_migration_metrics(str(migration.id), str(tenant_id)).get_snapshot()
        assert snapshot.events_copied == 5 and snapshot.phase_durations.get("bulk_copy", 0.0) > 0.0
        await coordinator.abort_migration(migration.id, reason="test cleanup")
        assert str(migration.id) not in tracker.active_migrations


__all__ = ["TestIntegrationOfAllPhase4Components"]  # pragma: allowlist secret
