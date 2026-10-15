"""Tests for consistency verification in MigrationCoordinator (P3-005)."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.consistency import (
    ConsistencyViolation,
    VerificationLevel,
)
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
)
from eventsource.application.migration.metrics import (
    clear_metrics_registry,
    get_migration_metrics,
)
from eventsource.ports.migration.models import Migration, MigrationPhase
from tests.unit.application.migration.test_coordinator_subscriptions._fixtures import (
    create_coordinator,
    create_coordinator_deps,
    make_verification_report,
)


class TestVerifyConsistency:
    """Tests for MigrationCoordinator.verify_consistency() method."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        """Create coordinator dependencies."""
        return create_coordinator_deps()

    @pytest.mark.asyncio
    async def test_verify_consistency_success(self, coordinator_deps: dict) -> None:
        """Test verify_consistency returns report on success."""
        migration_id, tenant_id = uuid4(), uuid4()
        target_store = AsyncMock()
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration_id] = target_store

        expected_report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
            source_event_count=1000,
            target_event_count=1000,
            streams_verified=10,
            is_consistent=True,
        )

        with patch(
            "eventsource.application.migration.coordinator.ConsistencyVerifier"
        ) as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=expected_report)
            mock_verifier_cls.return_value = mock_verifier

            report = await coordinator.verify_consistency(
                migration_id, level=VerificationLevel.HASH, sample_percentage=100.0
            )

        assert report.is_consistent is True
        assert report.source_event_count == 1000
        assert report.target_event_count == 1000
        stored_report = coordinator.get_consistency_report(migration_id)
        assert stored_report is report

    @pytest.mark.asyncio
    async def test_verify_consistency_with_violations(self, coordinator_deps: dict) -> None:
        """Test verify_consistency handles violations correctly."""
        migration_id, tenant_id = uuid4(), uuid4()
        target_store = AsyncMock()
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration_id] = target_store

        violations = [
            ConsistencyViolation(
                violation_type="count_mismatch",
                stream_id="test_stream",
                source_value="1000",
                target_value="999",
            ),
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

        with patch(
            "eventsource.application.migration.coordinator.ConsistencyVerifier"
        ) as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=expected_report)
            mock_verifier_cls.return_value = mock_verifier

            report = await coordinator.verify_consistency(migration_id)

        assert report.is_consistent is False
        assert len(report.violations) == 1
        assert report.violations[0].violation_type == "count_mismatch"

    async def test_verify_consistency_with_violations_records_metric(
        self, coordinator_deps: dict
    ) -> None:
        """verify_consistency() reports each violation through the real
        MigrationMetrics instance for this migration -- not a standalone
        metrics object the test constructs and calls directly."""
        clear_metrics_registry()
        migration_id, tenant_id = uuid4(), uuid4()
        target_store = AsyncMock()
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration_id] = target_store

        violations = [
            ConsistencyViolation(
                violation_type="count_mismatch",
                stream_id="test_stream",
                source_value="1000",
                target_value="999",
            ),
            ConsistencyViolation(
                violation_type="hash_mismatch",
                stream_id="other_stream",
                source_value="abc",
                target_value="def",
            ),
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

        with patch(
            "eventsource.application.migration.coordinator.ConsistencyVerifier"
        ) as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=expected_report)
            mock_verifier_cls.return_value = mock_verifier

            await coordinator.verify_consistency(migration_id)

        snapshot = get_migration_metrics(str(migration_id), str(tenant_id)).get_snapshot()
        assert snapshot.verification_failures == 2
        clear_metrics_registry()

    @pytest.mark.asyncio
    async def test_verify_consistency_migration_not_found(self, coordinator_deps: dict) -> None:
        """Test verify_consistency raises MigrationNotFoundError."""
        migration_id = uuid4()
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=None)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with pytest.raises(MigrationNotFoundError):
            await coordinator.verify_consistency(migration_id)

    @pytest.mark.asyncio
    async def test_verify_consistency_no_target_store(self, coordinator_deps: dict) -> None:
        """Test verify_consistency raises if no target store found."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.verify_consistency(migration_id)

        assert "Target store not found" in str(exc_info.value)


class TestGetConsistencyReport:
    """Tests for MigrationCoordinator.get_consistency_report() method."""

    def test_get_consistency_report_returns_stored_report(self) -> None:
        """Test get_consistency_report returns stored report."""
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

        result = coordinator.get_consistency_report(migration_id)
        assert result is report

    def test_get_consistency_report_returns_none_if_not_found(self) -> None:
        """Test get_consistency_report returns None if no report exists."""
        coordinator = create_coordinator()
        assert coordinator.get_consistency_report(uuid4()) is None


class TestVerificationLevel:
    """Tests for verification level configuration."""

    @pytest.mark.asyncio
    async def test_verify_consistency_with_count_level(self) -> None:
        """Test verify_consistency with COUNT level."""
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = create_coordinator(migration_repo=AsyncMock())
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )
        coordinator._migration_repo.get = AsyncMock(return_value=migration)
        coordinator._target_stores[migration_id] = AsyncMock()

        report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
            source_event_count=100,
            target_event_count=100,
            streams_verified=5,
        )

        with patch(
            "eventsource.application.migration.coordinator.ConsistencyVerifier"
        ) as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=report)
            mock_verifier_cls.return_value = mock_verifier

            await coordinator.verify_consistency(migration_id, level=VerificationLevel.COUNT)

            mock_verifier.verify_tenant_consistency.assert_called_once()
            call_kwargs = mock_verifier.verify_tenant_consistency.call_args[1]
            assert call_kwargs["level"] == VerificationLevel.COUNT

    @pytest.mark.asyncio
    async def test_verify_consistency_with_full_level(self) -> None:
        """Test verify_consistency with FULL level."""
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = create_coordinator(migration_repo=AsyncMock())
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
        )
        coordinator._migration_repo.get = AsyncMock(return_value=migration)
        coordinator._target_stores[migration_id] = AsyncMock()

        report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.FULL,
            source_event_count=100,
            target_event_count=100,
            streams_verified=5,
        )

        with patch(
            "eventsource.application.migration.coordinator.ConsistencyVerifier"
        ) as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=report)
            mock_verifier_cls.return_value = mock_verifier

            await coordinator.verify_consistency(
                migration_id, level=VerificationLevel.FULL, sample_percentage=50.0
            )

            mock_verifier.verify_tenant_consistency.assert_called_once()
            call_kwargs = mock_verifier.verify_tenant_consistency.call_args[1]
            assert call_kwargs["level"] == VerificationLevel.FULL
            assert call_kwargs["sample_percentage"] == 50.0
