"""Complete migration lifecycle tests with operational features enabled."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.metrics import (
    get_migration_metrics,
    release_migration_metrics,
)
from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationConfig,
    MigrationPhase,
)
from tests.unit.application.migration.test_final_integration._helpers import (
    copy_events_between_stores,
    create_test_events,
)

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_final_integration._mocks import (
        InMemoryAuditLogRepository,
        MockLockManager,
    )
    from tests.unit.application.migration.test_final_integration._repositories import (
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestCompleteMigrationLifecycleWithOperationalFeatures:
    """Tests for complete migration lifecycle with all operational features enabled.

    These tests serve as smoke tests to validate that all Phase 4 components
    work together correctly during a full migration.
    """

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_audit_logging(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        audit_log_repo: InMemoryAuditLogRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test complete migration lifecycle with audit logging enabled."""
        # Create historical events
        await create_test_events(source_store, tenant_id, count=5)

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
            config=MigrationConfig(batch_size=10),
        )

        assert migration is not None
        assert migration.tenant_id == tenant_id

        # Manually record audit events to simulate what the coordinator does
        await audit_log_repo.record(
            MigrationAuditEntry.migration_started(
                migration_id=migration.id,
                occurred_at=datetime.now(UTC),
                operator="test",
                details={"config": migration.config.to_dict()},
            )
        )

        await audit_log_repo.record(
            MigrationAuditEntry.phase_change(
                migration_id=migration.id,
                old_phase=MigrationPhase.PENDING,
                new_phase=MigrationPhase.BULK_COPY,
                occurred_at=datetime.now(UTC),
                operator="system",
            )
        )

        # Verify audit entries were recorded
        entries = await audit_log_repo.get_by_migration(migration.id)
        assert len(entries) >= 2

        # Verify migration started event
        started_entries = await audit_log_repo.get_by_migration(
            migration.id,
            event_types=[AuditEventType.MIGRATION_STARTED],
        )
        assert len(started_entries) == 1

        # Verify phase change events
        phase_entries = await audit_log_repo.get_by_migration(
            migration.id,
            event_types=[AuditEventType.PHASE_CHANGED],
        )
        assert len(phase_entries) >= 1

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_metrics_collection(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
        migration_id: UUID,
    ) -> None:
        """Test complete migration lifecycle with metrics collection enabled."""
        # Create metrics for the migration
        metrics = get_migration_metrics(
            migration_id=str(migration_id),
            tenant_id=str(tenant_id),
            enable_metrics=True,
        )

        # Simulate bulk copy phase metrics
        with metrics.time_phase("bulk_copy"):
            # Create historical events and copy them
            await create_test_events(source_store, tenant_id, count=10)
            copied = await copy_events_between_stores(source_store, target_store, tenant_id)

            # Record events copied
            metrics.record_events_copied(copied, rate_events_per_sec=100.0)

        # Simulate dual-write phase metrics
        with metrics.time_phase("dual_write"):
            # Simulate sync lag tracking
            metrics.record_sync_lag(50)
            await asyncio.sleep(0.01)
            metrics.record_sync_lag(25)
            await asyncio.sleep(0.01)
            metrics.record_sync_lag(0)

        # Simulate cutover phase metrics
        with metrics.time_cutover() as timer:
            await asyncio.sleep(0.01)
            timer.success = True

        # Get snapshot and verify metrics were recorded
        snapshot = metrics.get_snapshot()

        assert snapshot.events_copied == 10
        assert snapshot.events_copied_rate == 100.0
        assert snapshot.sync_lag_events == 0  # Final lag
        assert "bulk_copy" in snapshot.phase_durations
        assert "dual_write" in snapshot.phase_durations
        assert len(snapshot.cutover_durations) == 1
        assert snapshot.cutover_durations[0] > 0

        # Clean up
        release_migration_metrics(str(migration_id))

    @pytest.mark.asyncio
    async def test_full_lifecycle_phases_tracked(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that all migration phases are properly tracked."""
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

        # Verify migration was created
        assert migration is not None

        # Get status
        status = await coordinator.get_status(migration.id)
        assert status is not None
        assert status.migration_id == migration.id
        assert status.tenant_id == tenant_id

        # Let bulk copy run briefly
        await asyncio.sleep(0.05)

        # Check migration progressed
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        # Should be in BULK_COPY or possibly transitioned further
        assert migration.phase in (
            MigrationPhase.BULK_COPY,
            MigrationPhase.DUAL_WRITE,
        )


__all__ = [  # pragma: allowlist secret
    "TestCompleteMigrationLifecycleWithOperationalFeatures",
]
