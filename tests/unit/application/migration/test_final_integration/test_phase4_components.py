"""Tests for integration of all Phase 4 components working together."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from eventsource.application.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
)
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

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_final_integration._mocks import (
        InMemoryAuditLogRepository,
        InMemoryPositionMappingRepository,
        MockLockManager,
    )
    from tests.unit.application.migration.test_final_integration._repositories import (
        InMemoryCheckpointRepository,
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestIntegrationOfAllPhase4Components:
    """Tests for integration of all Phase 4 components working together."""

    @pytest.mark.asyncio
    async def test_all_components_work_together(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        audit_log_repo: InMemoryAuditLogRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        checkpoint_repo: InMemoryCheckpointRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test all Phase 4 components working together in a migration."""
        # Create position mapper and subscription migrator
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)

        # Create historical events
        await create_test_events(source_store, tenant_id, count=5)

        # Create coordinator with all dependencies
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        # 1. Start migration with all operational features
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=MigrationConfig(
                batch_size=10,
                verify_consistency=True,
                migrate_subscriptions=False,
            ),
        )

        # 2. Create and track metrics
        metrics = get_migration_metrics(
            migration_id=str(migration.id),
            tenant_id=str(tenant_id),
        )

        # 3. Record audit events
        await audit_log_repo.record(
            MigrationAuditEntry.migration_started(
                migration_id=migration.id,
                occurred_at=datetime.now(UTC),
                operator="test",
                details={"config": migration.config.to_dict()},
            )
        )

        # 4. Simulate bulk copy with metrics
        with metrics.time_phase("bulk_copy"):
            # Copy events
            copied = await copy_events_between_stores(source_store, target_store, tenant_id)
            metrics.record_events_copied(copied, rate_events_per_sec=100.0)

            # Record position mappings
            source_counter = 1
            target_counter = 1
            async for envelope in source_store.read_all():
                if envelope.event.tenant_id == tenant_id:
                    await position_mapper.record_mapping(
                        migration_id=migration.id,
                        source_position=src_pos(source_counter),
                        target_position=tgt_pos(target_counter),
                        event_id=envelope.event.event_id,
                    )
                    source_counter += 1
                    target_counter += 1

        # 5. Record phase transition
        await audit_log_repo.record(
            MigrationAuditEntry.phase_change(
                migration_id=migration.id,
                old_phase=MigrationPhase.BULK_COPY,
                new_phase=MigrationPhase.DUAL_WRITE,
                occurred_at=datetime.now(UTC),
                operator="system",
            )
        )

        # 6. Simulate dual-write with sync lag tracking
        with metrics.time_phase("dual_write"):
            metrics.record_sync_lag(10)
            await asyncio.sleep(0.01)
            metrics.record_sync_lag(0)

        # 7. Verify consistency
        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)
        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        # Record verification in audit log
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.VERIFICATION_COMPLETED,
                old_phase=None,
                new_phase=None,
                details={
                    "is_consistent": report.is_consistent,
                    "source_count": report.source_event_count,
                    "target_count": report.target_event_count,
                },
                operator="system",
                occurred_at=datetime.now(UTC),
            )
        )

        # 8. Validate all components worked correctly
        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied >= 5
        assert "bulk_copy" in snapshot.phase_durations
        assert "dual_write" in snapshot.phase_durations

        # Audit log entries
        all_entries = await audit_log_repo.get_by_migration(migration.id)
        assert len(all_entries) >= 3  # started, phase_changed, verification

        # Position mappings
        mapping_count = await position_mapper.get_mapping_count(migration.id)
        assert mapping_count == 5

        # Consistency report
        assert report.is_consistent is True
        assert report.source_event_count == 5
        assert report.target_event_count == 5

        # Clean up metrics
        release_migration_metrics(str(migration.id))

    @pytest.mark.asyncio
    async def test_end_to_end_migration_smoke_test(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """End-to-end smoke test for complete migration system."""
        # Setup: Create 20 events in source
        await create_test_events(source_store, tenant_id, count=20)

        # Create coordinator
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        # Start migration
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        assert migration is not None
        assert migration.phase in (MigrationPhase.PENDING, MigrationPhase.BULK_COPY)

        # Get status
        status = await coordinator.get_status(migration.id)
        assert status is not None
        assert status.migration_id == migration.id
        assert status.tenant_id == tenant_id

        # Let it run briefly
        await asyncio.sleep(0.1)

        # Abort to clean up
        result = await coordinator.abort_migration(
            migration.id,
            reason="Smoke test cleanup",
        )
        assert result.final_phase == MigrationPhase.ABORTED

        # Verify clean state
        routing = await routing_repo.get_routing(tenant_id)
        assert routing.migration_state == TenantMigrationState.NORMAL

    @pytest.mark.asyncio
    async def test_bulk_copy_phase_reports_metrics_through_real_run(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """A migration driven end-to-end through the coordinator reports
        bulk_copy phase duration and events copied, and release_migration_metrics()
        at completion drops it from the active-migrations gauge."""
        await create_test_events(source_store, tenant_id, count=5)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )
        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        await coordinator.wait_for_phase(migration.id, MigrationPhase.DUAL_WRITE, timeout=5.0)

        tracker = ActiveMigrationsTracker.get_instance()
        assert str(migration.id) in tracker.active_migrations

        snapshot = get_migration_metrics(str(migration.id), str(tenant_id)).get_snapshot()
        assert snapshot.events_copied == 5
        assert snapshot.phase_durations.get("bulk_copy", 0.0) > 0.0

        await coordinator.abort_migration(migration.id, reason="test cleanup")

        assert str(migration.id) not in tracker.active_migrations


__all__ = [  # pragma: allowlist secret
    "TestIntegrationOfAllPhase4Components",
]
