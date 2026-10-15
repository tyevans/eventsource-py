"""
Unit tests confirming MigrationCoordinator writes lifecycle audit entries
when an audit_log_repo is configured.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.ports.migration.models import (
    AuditEventType,
    Migration,
    MigrationPhase,
)
from tests.unit.application.migration.test_migration_audit_log._fakes import (
    FakeAuditLogRepository,
)

pytestmark = pytest.mark.asyncio


class TestAuditLogIsOptional:
    async def test_coordinator_works_without_audit_log_repo(self) -> None:
        """Omitting audit_log_repo is not a behavior change -- the
        coordinator still starts a migration exactly as before."""
        tenant_id = uuid4()
        migration_repo = AsyncMock()
        migration_repo.get_by_tenant = AsyncMock(return_value=None)
        migration_repo.create = AsyncMock()
        routing_repo = AsyncMock()
        router = MagicMock()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            enable_tracing=False,
        )
        coordinator._run_bulk_copy = AsyncMock()  # type: ignore[method-assign]

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=AsyncMock(),
            target_store_id="dedicated",
        )

        assert migration is not None


class TestStartMigrationAudit:
    async def test_start_migration_records_migration_started(self) -> None:
        """start_migration() writes a MIGRATION_STARTED audit entry through
        the coordinator's real call path."""
        tenant_id = uuid4()
        migration_repo = AsyncMock()
        migration_repo.get_by_tenant = AsyncMock(return_value=None)
        migration_repo.create = AsyncMock()
        routing_repo = AsyncMock()
        router = MagicMock()
        audit_log = FakeAuditLogRepository()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            audit_log_repo=audit_log,
            enable_tracing=False,
        )
        coordinator._run_bulk_copy = AsyncMock()  # type: ignore[method-assign]

        created: list[Migration] = []
        migration_repo.create.side_effect = lambda m: created.append(m)
        migration_repo.get.side_effect = lambda _id: created[0]

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=AsyncMock(),
            target_store_id="dedicated",
        )

        started = [e for e in audit_log.entries if e.event_type == AuditEventType.MIGRATION_STARTED]
        assert len(started) == 1
        assert started[0].migration_id == migration.id
        assert started[0].details is not None
        assert started[0].details["target_store_id"] == "dedicated"


class TestTransitionToDualWriteAudit:
    async def test_transition_records_phase_changed(self) -> None:
        """_transition_to_dual_write() writes a PHASE_CHANGED audit entry
        (BULK_COPY -> DUAL_WRITE) through the real transition method."""
        migration_id = uuid4()
        tenant_id = uuid4()
        target_store = AsyncMock()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.update_phase = AsyncMock()
        routing_repo = AsyncMock()
        routing_repo.set_migration_state = AsyncMock()
        audit_log = FakeAuditLogRepository()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=MagicMock(),
            audit_log_repo=audit_log,
            enable_tracing=False,
        )

        await coordinator._transition_to_dual_write(migration, target_store)

        phase_changes = [
            e for e in audit_log.entries if e.event_type == AuditEventType.PHASE_CHANGED
        ]
        assert len(phase_changes) == 1
        assert phase_changes[0].old_phase == MigrationPhase.BULK_COPY
        assert phase_changes[0].new_phase == MigrationPhase.DUAL_WRITE
        assert phase_changes[0].migration_id == migration_id


class TestAbortMigrationAudit:
    async def test_abort_records_migration_aborted(self) -> None:
        """abort_migration() writes a MIGRATION_ABORTED audit entry through
        the real abort path."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)
        migration_repo.update_phase = AsyncMock()
        migration_repo.record_error = AsyncMock()
        routing_repo = AsyncMock()
        router = MagicMock()
        audit_log = FakeAuditLogRepository()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            audit_log_repo=audit_log,
            enable_tracing=False,
        )

        await coordinator.abort_migration(migration_id, reason="operator requested")

        aborted = [e for e in audit_log.entries if e.event_type == AuditEventType.MIGRATION_ABORTED]
        assert len(aborted) == 1
        assert aborted[0].migration_id == migration_id
        assert aborted[0].details == {"reason": "operator requested"}


class TestFailMigrationAudit:
    async def test_fail_migration_records_migration_failed(self) -> None:
        """_fail_migration() writes a MIGRATION_FAILED audit entry through
        the real failure path."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.update_phase = AsyncMock()
        migration_repo.record_error = AsyncMock()
        routing_repo = AsyncMock()
        router = MagicMock()
        audit_log = FakeAuditLogRepository()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            audit_log_repo=audit_log,
            enable_tracing=False,
        )

        await coordinator._fail_migration(migration, "store unavailable")

        failed = [e for e in audit_log.entries if e.event_type == AuditEventType.MIGRATION_FAILED]
        assert len(failed) == 1
        assert failed[0].migration_id == migration_id
        assert failed[0].details == {"error": "store unavailable"}


__all__ = [
    "TestAbortMigrationAudit",
    "TestAuditLogIsOptional",
    "TestFailMigrationAudit",
    "TestStartMigrationAudit",
    "TestTransitionToDualWriteAudit",
]
