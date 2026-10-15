"""Tests for consistency verification in MigrationCoordinator (P3-005)."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.consistency import ConsistencyViolation, VerificationLevel
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.exceptions import MigrationError, MigrationNotFoundError
from eventsource.application.migration.metrics import clear_metrics_registry, get_migration_metrics
from eventsource.ports.migration.models import Migration, MigrationPhase
from tests.unit.application.migration.test_coordinator_subscriptions._fixtures import (
    create_coordinator,
    create_coordinator_deps,
    make_verification_report,
)

_VERIFIER_PATH = "eventsource.application.migration.coordinator.ConsistencyVerifier"


def _mig(migration_id=None, tenant_id=None, phase=MigrationPhase.DUAL_WRITE) -> Migration:
    return Migration(migration_id or uuid4(), tenant_id or uuid4(), "default", "dedicated", phase)


def _setup_coord(deps: dict, migration_id=None, tenant_id=None) -> MigrationCoordinator:
    migration_id = migration_id or uuid4()
    deps["migration_repo"].get = AsyncMock(return_value=_mig(migration_id, tenant_id))
    coordinator = MigrationCoordinator(**deps, enable_tracing=False)
    coordinator._target_stores[migration_id] = AsyncMock()
    return coordinator


class TestVerifyConsistency:
    @pytest.fixture
    def coordinator_deps(self) -> dict:
        return create_coordinator_deps()

    @pytest.mark.asyncio
    async def test_verify_consistency_success(self, coordinator_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = _setup_coord(coordinator_deps, migration_id, tenant_id)
        expected_report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
            source_event_count=1000,
            target_event_count=1000,
            streams_verified=10,
            is_consistent=True,
        )
        with patch(_VERIFIER_PATH) as mock_verifier_cls:
            mock_verifier_cls.return_value = MagicMock(
                verify_tenant_consistency=AsyncMock(return_value=expected_report)
            )
            report = await coordinator.verify_consistency(
                migration_id, level=VerificationLevel.HASH, sample_percentage=100.0
            )
        assert report.is_consistent is True
        assert report.source_event_count == 1000 and report.target_event_count == 1000
        assert coordinator.get_consistency_report(migration_id) is report

    @pytest.mark.asyncio
    async def test_verify_consistency_with_violations(self, coordinator_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = _setup_coord(coordinator_deps, migration_id, tenant_id)
        violations = [ConsistencyViolation("count_mismatch", "test_stream", "1000", "999")]
        expected_report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
            source_event_count=1000,
            target_event_count=999,
            streams_verified=10,
            is_consistent=False,
            violations=violations,
        )
        with patch(_VERIFIER_PATH) as mock_verifier_cls:
            mock_verifier_cls.return_value = MagicMock(
                verify_tenant_consistency=AsyncMock(return_value=expected_report)
            )
            report = await coordinator.verify_consistency(migration_id)
        assert report.is_consistent is False and len(report.violations) == 1
        assert report.violations[0].violation_type == "count_mismatch"

    async def test_verify_consistency_with_violations_records_metric(
        self, coordinator_deps: dict
    ) -> None:
        clear_metrics_registry()
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = _setup_coord(coordinator_deps, migration_id, tenant_id)
        violations = [
            ConsistencyViolation("count_mismatch", "test_stream", "1000", "999"),
            ConsistencyViolation("hash_mismatch", "other_stream", "abc", "def"),
        ]
        expected_report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
            source_event_count=1000,
            target_event_count=999,
            streams_verified=10,
            is_consistent=False,
            violations=violations,
        )
        with patch(_VERIFIER_PATH) as mock_verifier_cls:
            mock_verifier_cls.return_value = MagicMock(
                verify_tenant_consistency=AsyncMock(return_value=expected_report)
            )
            await coordinator.verify_consistency(migration_id)
        snapshot = get_migration_metrics(str(migration_id), str(tenant_id)).get_snapshot()
        assert snapshot.verification_failures == 2
        clear_metrics_registry()

    @pytest.mark.asyncio
    async def test_verify_consistency_migration_not_found(self, coordinator_deps: dict) -> None:
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=None)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        with pytest.raises(MigrationNotFoundError):
            await coordinator.verify_consistency(uuid4())

    @pytest.mark.asyncio
    async def test_verify_consistency_no_target_store(self, coordinator_deps: dict) -> None:
        migration_id = uuid4()
        coordinator_deps["migration_repo"].get = AsyncMock(
            return_value=_mig(migration_id, phase=MigrationPhase.BULK_COPY)
        )
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        with pytest.raises(MigrationError, match="Target store not found"):
            await coordinator.verify_consistency(migration_id)


class TestGetConsistencyReport:
    def test_get_consistency_report_returns_stored_report(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = create_coordinator()
        report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
            source_event_count=500,
            target_event_count=500,
            streams_verified=5,
        )
        coordinator._consistency_reports[migration_id] = report
        assert coordinator.get_consistency_report(migration_id) is report

    def test_get_consistency_report_returns_none_if_not_found(self) -> None:
        assert create_coordinator().get_consistency_report(uuid4()) is None


class TestVerificationLevel:
    @pytest.mark.asyncio
    async def test_verify_consistency_with_count_level(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = create_coordinator(migration_repo=AsyncMock())
        coordinator._migration_repo.get = AsyncMock(return_value=_mig(migration_id, tenant_id))
        coordinator._target_stores[migration_id] = AsyncMock()
        report = make_verification_report(
            tenant_id=tenant_id, level=VerificationLevel.COUNT, streams_verified=5
        )
        with patch(_VERIFIER_PATH) as mock_verifier_cls:
            mock_verifier = MagicMock(verify_tenant_consistency=AsyncMock(return_value=report))
            mock_verifier_cls.return_value = mock_verifier
            await coordinator.verify_consistency(migration_id, level=VerificationLevel.COUNT)
            mock_verifier.verify_tenant_consistency.assert_called_once()
            assert (
                mock_verifier.verify_tenant_consistency.call_args[1]["level"]
                == VerificationLevel.COUNT
            )

    @pytest.mark.asyncio
    async def test_verify_consistency_with_full_level(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = create_coordinator(migration_repo=AsyncMock())
        coordinator._migration_repo.get = AsyncMock(return_value=_mig(migration_id, tenant_id))
        coordinator._target_stores[migration_id] = AsyncMock()
        report = make_verification_report(
            tenant_id=tenant_id, level=VerificationLevel.FULL, streams_verified=5
        )
        with patch(_VERIFIER_PATH) as mock_verifier_cls:
            mock_verifier = MagicMock(verify_tenant_consistency=AsyncMock(return_value=report))
            mock_verifier_cls.return_value = mock_verifier
            await coordinator.verify_consistency(
                migration_id, level=VerificationLevel.FULL, sample_percentage=50.0
            )
            mock_verifier.verify_tenant_consistency.assert_called_once()
            call_kwargs = mock_verifier.verify_tenant_consistency.call_args[1]
            assert call_kwargs["level"] == VerificationLevel.FULL
            assert call_kwargs["sample_percentage"] == 50.0
