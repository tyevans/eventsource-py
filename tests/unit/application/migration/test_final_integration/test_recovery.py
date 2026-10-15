"""Tests for recovery after simulated failures in migrations."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
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


class TestRecoveryAfterSimulatedFailures:
    """Tests for recovery after simulated failures."""

    @pytest.mark.asyncio
    async def test_recovery_from_bulk_copy_failure(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        audit_log_repo: InMemoryAuditLogRepository,
        tenant_id: UUID,
    ) -> None:
        """Test recovery after bulk copy failure."""
        await create_test_events(source_store, tenant_id, count=10)
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
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )

        await migration_repo.record_error(migration.id, error="Simulated bulk copy failure")
        await audit_log_repo.record(
            MigrationAuditEntry.error_occurred(
                migration_id=migration.id,
                occurred_at=datetime.now(UTC),
                error_message="Simulated bulk copy failure",
                error_type="BulkCopyError",
            )
        )

        migration = await migration_repo.get(migration.id)
        assert migration is not None
        assert migration.error_count == 1
        assert "bulk copy" in migration.last_error.lower()
        assert not migration.phase.is_terminal

        errors = await audit_log_repo.get_by_migration(
            migration.id, event_types=[AuditEventType.ERROR_OCCURRED]
        )
        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_recovery_from_cutover_rollback(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        audit_log_repo: InMemoryAuditLogRepository,
        tenant_id: UUID,
        migration_id: UUID,
    ) -> None:
        """Test recovery after cutover rollback."""
        await create_test_events(source_store, tenant_id, count=5)
        await copy_events_between_stores(source_store, target_store, tenant_id)

        for i in range(200):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id, tenant_id=tenant_id, value=f"extra-{i}"
            )
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="TestAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            config=MigrationConfig(cutover_max_lag_events=50),
            enable_tracing=False,
        )
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            config=MigrationConfig(cutover_max_lag_events=50),
            timeout_ms=500.0,
        )
        assert result.success is False
        assert result.rolled_back is True

        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.CUTOVER_ROLLED_BACK,
                old_phase=MigrationPhase.CUTOVER,
                new_phase=MigrationPhase.DUAL_WRITE,
                details={"reason": result.error_message, "duration_ms": result.duration_ms},
                operator="system",
                occurred_at=datetime.now(UTC),
            )
        )

        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.DUAL_WRITE

        rollback_entries = await audit_log_repo.get_by_migration(
            migration_id, event_types=[AuditEventType.CUTOVER_ROLLED_BACK]
        )
        assert len(rollback_entries) == 1

    @pytest.mark.asyncio
    async def test_pause_resume_recovery(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        audit_log_repo: InMemoryAuditLogRepository,
        tenant_id: UUID,
    ) -> None:
        """Test recovery through pause and resume."""
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
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )

        await coordinator.pause_migration(migration.id)
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.MIGRATION_PAUSED,
                old_phase=None,
                new_phase=None,
                details={"reason": "Maintenance"},
                operator="admin",
                occurred_at=datetime.now(UTC),
            )
        )
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        assert migration.is_paused

        await coordinator.resume_migration(migration.id)
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.MIGRATION_RESUMED,
                old_phase=None,
                new_phase=None,
                details={"resumed_by": "admin"},
                operator="admin",
                occurred_at=datetime.now(UTC),
            )
        )
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        assert not migration.is_paused

        entries = await audit_log_repo.get_by_migration(
            migration.id,
            event_types=[AuditEventType.MIGRATION_PAUSED, AuditEventType.MIGRATION_RESUMED],
        )
        assert len(entries) == 2

    @pytest.mark.asyncio
    async def test_abort_recovery(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        audit_log_repo: InMemoryAuditLogRepository,
        tenant_id: UUID,
    ) -> None:
        """Test clean abort and state recovery."""
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
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )

        result = await coordinator.abort_migration(migration.id, reason="Test abort for recovery")
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.MIGRATION_ABORTED,
                old_phase=result.final_phase,
                new_phase=MigrationPhase.ABORTED,
                details={"reason": "Test abort for recovery"},
                operator="admin",
                occurred_at=datetime.now(UTC),
            )
        )

        assert result.success is False
        assert result.final_phase == MigrationPhase.ABORTED

        migration = await coordinator.get_migration(result.migration_id)
        assert migration is not None
        assert migration.phase == MigrationPhase.ABORTED

        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.NORMAL

        new_migration = await coordinator.start_migration(
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )
        assert new_migration is not None
        assert new_migration.id != migration.id


__all__ = ["TestRecoveryAfterSimulatedFailures"]  # pragma: allowlist secret
