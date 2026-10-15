"""Unit tests for MigrationCoordinator cutover, consistency verification, and error resilience audit logging."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.consistency import ConsistencyViolation, VerificationLevel
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.cutover import CutoverManager
from eventsource.ports.migration.models import AuditEventType, Migration, MigrationPhase
from tests.unit.application.migration.test_coordinator_subscriptions import (
    make_verification_report,
)
from tests.unit.application.migration.test_migration_audit_log._fakes import FakeAuditLogRepository

pytestmark = pytest.mark.asyncio


class TestCutoverAudit:
    async def test_execute_cutover_success_does_not_touch_audit_log(self) -> None:
        """CutoverManager has no audit_log_repo of its own."""

        class _AcquireCtx:
            async def __aenter__(self) -> None:
                return None

            async def __aexit__(self, *exc: object) -> None:
                return None

        lock_manager = MagicMock(acquire=MagicMock(return_value=_AcquireCtx()))
        router = MagicMock(
            pause_writes=AsyncMock(),
            resume_writes=AsyncMock(),
            get_store=MagicMock(return_value=AsyncMock()),
            clear_dual_write_interceptor=MagicMock(),
        )
        routing_repo = AsyncMock(get_routing=AsyncMock(return_value=None))
        lag = MagicMock(is_within_threshold=MagicMock(return_value=True), events=0)
        lag_tracker = MagicMock(calculate_lag=AsyncMock(), current_lag=lag)

        manager = CutoverManager(
            router=router,
            routing_repo=routing_repo,
            lock_manager=lock_manager,
            enable_tracing=False,
        )
        result = await manager.execute_cutover(
            migration_id=uuid4(),
            tenant_id=uuid4(),
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
        )
        assert result.success is True
        assert not hasattr(manager, "_audit_log_repo")

    async def test_trigger_cutover_records_cutover_initiated_and_completed(self) -> None:
        """trigger_cutover() writes CUTOVER_INITIATED and CUTOVER_COMPLETED."""
        migration_id, tenant_id = uuid4(), uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration), update_phase=AsyncMock())
        routing_repo = AsyncMock(set_migration_state=AsyncMock())
        router = MagicMock(clear_dual_write_interceptor=MagicMock())
        audit_log = FakeAuditLogRepository()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=AsyncMock(),
            audit_log_repo=audit_log,
            enable_tracing=False,
        )
        coordinator._lag_trackers[migration_id] = MagicMock()
        cutover_result = MagicMock(success=True, duration_ms=42.0)
        coordinator._cutover_manager = AsyncMock(
            execute_cutover=AsyncMock(return_value=cutover_result)
        )

        await coordinator.trigger_cutover(migration_id)
        event_types = [e.event_type for e in audit_log.entries]
        assert AuditEventType.CUTOVER_INITIATED in event_types
        assert AuditEventType.CUTOVER_COMPLETED in event_types


class TestVerifyConsistencyAudit:
    async def test_verify_consistency_records_verification_failed(self) -> None:
        """verify_consistency() writes VERIFICATION_FAILED when report has violations."""
        migration_id, tenant_id = uuid4(), uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        audit_log = FakeAuditLogRepository()

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=AsyncMock(),
            router=MagicMock(),
            audit_log_repo=audit_log,
            enable_tracing=False,
        )
        coordinator._target_stores[migration_id] = AsyncMock()

        violations = [
            ConsistencyViolation(
                violation_type="count_mismatch",
                stream_id="test_stream",
                source_value="10",
                target_value="9",
            )
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
            mock_verifier_cls.return_value = MagicMock(
                verify_tenant_consistency=AsyncMock(return_value=expected_report)
            )
            await coordinator.verify_consistency(migration_id)

        failed = [
            e for e in audit_log.entries if e.event_type == AuditEventType.VERIFICATION_FAILED
        ]
        assert len(failed) == 1
        assert failed[0].details is not None and failed[0].details["violation_count"] == 1


class TestAuditFailureDoesNotFailOperation:
    async def test_audit_write_failure_is_swallowed(self) -> None:
        """A broken audit_log_repo must not fail the migration operation."""
        migration_repo = AsyncMock(get_by_tenant=AsyncMock(return_value=None), create=AsyncMock())
        broken_audit_log = MagicMock(record=AsyncMock(side_effect=RuntimeError("db down")))

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=AsyncMock(),
            router=MagicMock(),
            audit_log_repo=broken_audit_log,
            enable_tracing=False,
        )
        coordinator._run_bulk_copy = AsyncMock()  # type: ignore[method-assign]

        migration = await coordinator.start_migration(
            tenant_id=uuid4(),
            target_store=AsyncMock(),
            target_store_id="dedicated",
        )
        assert migration is not None
        broken_audit_log.record.assert_awaited_once()


__all__ = [
    "TestAuditFailureDoesNotFailOperation",
    "TestCutoverAudit",
    "TestVerifyConsistencyAudit",
]
