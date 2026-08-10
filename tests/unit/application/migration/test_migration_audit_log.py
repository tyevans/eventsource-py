"""
Unit tests confirming MigrationCoordinator actually writes audit entries
when an audit_log_repo is configured, through the real code path -- not a
standalone MigrationAuditLogRepository the test constructs and calls
record() on directly (recurring-defects.md #3).
"""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports.migration.models import (
    AuditEventType,
    Migration,
    MigrationAuditEntry,
    MigrationPhase,
)

pytestmark = pytest.mark.asyncio


class FakeAuditLogRepository:
    """In-memory MigrationAuditLogRepository double.

    Not the standalone-only anti-pattern: this is injected into a real
    MigrationCoordinator and exercised through the coordinator's public
    methods below, not called directly by any test.
    """

    def __init__(self) -> None:
        self.entries: list[MigrationAuditEntry] = []

    async def record(self, entry: MigrationAuditEntry) -> int:
        entry_id = len(self.entries) + 1
        self.entries.append(entry)
        return entry_id

    async def get_by_migration(
        self,
        migration_id: UUID,
        event_types: list[AuditEventType] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[MigrationAuditEntry]:
        return [e for e in self.entries if e.migration_id == migration_id]

    async def get_by_id(self, entry_id: int) -> MigrationAuditEntry | None:
        for i, entry in enumerate(self.entries, start=1):
            if i == entry_id:
                return entry
        return None

    async def get_latest(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> MigrationAuditEntry | None:
        matches = [
            e
            for e in self.entries
            if e.migration_id == migration_id and (event_type is None or e.event_type == event_type)
        ]
        return matches[-1] if matches else None

    async def count_by_migration(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> int:
        return len(
            [
                e
                for e in self.entries
                if e.migration_id == migration_id
                and (event_type is None or e.event_type == event_type)
            ]
        )


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

        # start_migration() re-reads the migration after the initial write
        # to pick up server-assigned timestamps; make the mock repo return
        # the same record it was given.
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


class TestCutoverAudit:
    async def test_execute_cutover_success_does_not_touch_audit_log(self) -> None:
        """CutoverManager has no audit_log_repo of its own -- cutover audit
        entries (CUTOVER_INITIATED/COMPLETED/ROLLED_BACK) are the
        coordinator's responsibility, recorded around
        cutover_manager.execute_cutover(), not inside it. This pins that
        boundary so a future change doesn't silently duplicate entries by
        adding logging to both layers."""
        migration_id = uuid4()
        tenant_id = uuid4()

        lock_manager = MagicMock()

        class _AcquireCtx:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *exc: object) -> None:
                return None

        lock_manager.acquire = MagicMock(return_value=_AcquireCtx())
        router = MagicMock()
        router.pause_writes = AsyncMock()
        router.resume_writes = AsyncMock()
        router.get_store = MagicMock(return_value=AsyncMock())
        router.clear_dual_write_interceptor = MagicMock()
        routing_repo = AsyncMock()
        routing_repo.get_routing = AsyncMock(return_value=None)

        lag_tracker = MagicMock()
        lag_tracker.calculate_lag = AsyncMock()
        lag = MagicMock()
        lag.is_within_threshold = MagicMock(return_value=True)
        lag.events = 0
        lag_tracker.current_lag = lag

        manager = CutoverManager(
            router=router,
            routing_repo=routing_repo,
            lock_manager=lock_manager,
            enable_tracing=False,
        )

        result = await manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
        )

        assert result.success is True
        assert not hasattr(manager, "_audit_log_repo")

    async def test_trigger_cutover_records_cutover_initiated_and_completed(self) -> None:
        """trigger_cutover() writes CUTOVER_INITIATED before executing, and
        CUTOVER_COMPLETED after a successful cutover, through the real
        coordinator call path."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)
        migration_repo.update_phase = AsyncMock()
        routing_repo = AsyncMock()
        routing_repo.set_migration_state = AsyncMock()
        router = MagicMock()
        router.clear_dual_write_interceptor = MagicMock()
        lock_manager = AsyncMock()
        audit_log = FakeAuditLogRepository()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            audit_log_repo=audit_log,
            enable_tracing=False,
        )

        lag_tracker = MagicMock()
        coordinator._lag_trackers[migration_id] = lag_tracker

        cutover_result = MagicMock()
        cutover_result.success = True
        cutover_result.duration_ms = 42.0

        cutover_manager = AsyncMock()
        cutover_manager.execute_cutover = AsyncMock(return_value=cutover_result)
        coordinator._cutover_manager = cutover_manager

        await coordinator.trigger_cutover(migration_id)

        event_types = [e.event_type for e in audit_log.entries]
        assert AuditEventType.CUTOVER_INITIATED in event_types
        assert AuditEventType.CUTOVER_COMPLETED in event_types


class TestVerifyConsistencyAudit:
    async def test_verify_consistency_records_verification_failed(self) -> None:
        """verify_consistency() writes a VERIFICATION_FAILED audit entry
        when the report has violations, through the real coordinator
        call path."""
        from unittest.mock import patch

        from eventsource.application.migration.consistency import (
            ConsistencyViolation,
            VerificationLevel,
        )

        migration_id = uuid4()
        tenant_id = uuid4()
        target_store = AsyncMock()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)
        routing_repo = AsyncMock()
        audit_log = FakeAuditLogRepository()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=MagicMock(),
            audit_log_repo=audit_log,
            enable_tracing=False,
        )
        coordinator._target_stores[migration_id] = target_store

        from tests.unit.application.migration.test_coordinator_subscriptions import (
            make_verification_report,
        )

        violations = [
            ConsistencyViolation(
                violation_type="count_mismatch",
                stream_id="test_stream",
                source_value="10",
                target_value="9",
            ),
        ]
        expected_report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
            source_event_count=10,
            target_event_count=9,
            streams_verified=1,
            is_consistent=False,
            violations=violations,
        )

        with patch(
            "eventsource.application.migration.coordinator.ConsistencyVerifier"
        ) as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=expected_report)
            mock_verifier_cls.return_value = mock_verifier

            await coordinator.verify_consistency(migration_id)

        failed = [
            e for e in audit_log.entries if e.event_type == AuditEventType.VERIFICATION_FAILED
        ]
        assert len(failed) == 1
        assert failed[0].details is not None
        assert failed[0].details["violation_count"] == 1


class TestAuditFailureDoesNotFailOperation:
    async def test_audit_write_failure_is_swallowed(self) -> None:
        """A broken audit_log_repo must not fail the migration operation
        that triggered the audit write -- the audit trail is best-effort,
        not a required dependency."""
        tenant_id = uuid4()
        migration_repo = AsyncMock()
        migration_repo.get_by_tenant = AsyncMock(return_value=None)
        migration_repo.create = AsyncMock()
        routing_repo = AsyncMock()
        router = MagicMock()

        broken_audit_log = MagicMock()
        broken_audit_log.record = AsyncMock(side_effect=RuntimeError("db down"))

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            audit_log_repo=broken_audit_log,
            enable_tracing=False,
        )
        coordinator._run_bulk_copy = AsyncMock()  # type: ignore[method-assign]

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=AsyncMock(),
            target_store_id="dedicated",
        )

        assert migration is not None
        broken_audit_log.record.assert_awaited_once()
