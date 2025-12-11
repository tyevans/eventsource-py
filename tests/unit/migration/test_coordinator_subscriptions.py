"""
Unit tests for MigrationCoordinator consistency and subscription features (P3-005).

Tests cover:
- verify_consistency() method
- migrate_subscriptions() method
- get_consistency_report() method
- get_subscription_summary() method
- _complete_cutover() integration with verification and subscription migration
- _cleanup_migration_resources() including P3-005 resources
- Configuration flags: verify_consistency, migrate_subscriptions
- Error handling for missing dependencies
- Graceful handling of verification/migration failures
"""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.migration.consistency import (
    ConsistencyViolation,
    VerificationLevel,
    VerificationReport,
)
from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
)
from eventsource.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
)
from eventsource.migration.subscription_migrator import (
    MigrationSummary,
    SubscriptionMigrationResult,
)


def make_verification_report(
    tenant_id,
    level: VerificationLevel = VerificationLevel.HASH,
    source_event_count: int = 1000,
    target_event_count: int = 1000,
    streams_verified: int = 10,
    is_consistent: bool = True,
    violations: list = None,
    stream_results: list = None,
    duration_seconds: float = 0.25,
) -> VerificationReport:
    """Helper to create VerificationReport with correct fields."""
    return VerificationReport(
        tenant_id=tenant_id,
        verification_level=level,
        is_consistent=is_consistent,
        source_event_count=source_event_count,
        target_event_count=target_event_count,
        streams_verified=streams_verified,
        streams_consistent=streams_verified if is_consistent else 0,
        streams_inconsistent=0 if is_consistent else streams_verified,
        sample_percentage=100.0,
        violations=violations or [],
        stream_results=stream_results or [],
        duration_seconds=duration_seconds,
        verified_at=datetime.now(UTC),
    )


def make_migration_summary(
    migration_id,
    tenant_id,
    results: list = None,
    successful_count: int = 0,
    failed_count: int = 0,
    skipped_count: int = 0,
) -> MigrationSummary:
    """Helper to create MigrationSummary with correct fields."""
    return MigrationSummary(
        migration_id=migration_id,
        tenant_id=tenant_id,
        results=results or [],
        successful_count=successful_count,
        failed_count=failed_count,
        skipped_count=skipped_count,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        duration_ms=100.0,
    )


class TestVerifyConsistency:
    """Tests for MigrationCoordinator.verify_consistency() method."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        """Create coordinator dependencies."""
        return {
            "source_store": AsyncMock(),
            "migration_repo": AsyncMock(),
            "routing_repo": AsyncMock(),
            "router": MagicMock(),
        }

    @pytest.mark.asyncio
    async def test_verify_consistency_success(self, coordinator_deps: dict) -> None:
        """Test verify_consistency returns report on success."""
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

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        # Store target store
        coordinator._target_stores[migration_id] = target_store

        # Create expected report
        expected_report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
            source_event_count=1000,
            target_event_count=1000,
            streams_verified=10,
            is_consistent=True,
        )

        # Mock ConsistencyVerifier
        with patch("eventsource.migration.coordinator.ConsistencyVerifier") as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=expected_report)
            mock_verifier_cls.return_value = mock_verifier

            report = await coordinator.verify_consistency(
                migration_id,
                level=VerificationLevel.HASH,
                sample_percentage=100.0,
            )

        assert report.is_consistent is True
        assert report.source_event_count == 1000
        assert report.target_event_count == 1000

        # Verify report is stored
        stored_report = coordinator.get_consistency_report(migration_id)
        assert stored_report is report

    @pytest.mark.asyncio
    async def test_verify_consistency_with_violations(self, coordinator_deps: dict) -> None:
        """Test verify_consistency handles violations correctly."""
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

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        coordinator._target_stores[migration_id] = target_store

        # Create report with violations
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

        with patch("eventsource.migration.coordinator.ConsistencyVerifier") as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=expected_report)
            mock_verifier_cls.return_value = mock_verifier

            report = await coordinator.verify_consistency(migration_id)

        assert report.is_consistent is False
        assert len(report.violations) == 1
        assert report.violations[0].violation_type == "count_mismatch"

    @pytest.mark.asyncio
    async def test_verify_consistency_migration_not_found(self, coordinator_deps: dict) -> None:
        """Test verify_consistency raises MigrationNotFoundError."""
        migration_id = uuid4()

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=None)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

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

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        # No target store added

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.verify_consistency(migration_id)

        assert "Target store not found" in str(exc_info.value)


class TestMigrateSubscriptions:
    """Tests for MigrationCoordinator.migrate_subscriptions() method."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        """Create coordinator dependencies."""
        return {
            "source_store": AsyncMock(),
            "migration_repo": AsyncMock(),
            "routing_repo": AsyncMock(),
            "router": MagicMock(),
        }

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_success(self, coordinator_deps: dict) -> None:
        """Test migrate_subscriptions returns summary on success."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        position_mapper = AsyncMock()
        checkpoint_repo = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        # Create expected summary
        expected_summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[
                SubscriptionMigrationResult(
                    subscription_name="Projection1",
                    source_position=100,
                    target_position=50,
                    success=True,
                ),
                SubscriptionMigrationResult(
                    subscription_name="Projection2",
                    source_position=200,
                    target_position=100,
                    success=True,
                ),
                SubscriptionMigrationResult(
                    subscription_name="Projection3",
                    source_position=300,
                    target_position=150,
                    success=True,
                ),
            ],
            successful_count=3,
            failed_count=0,
        )

        with patch("eventsource.migration.coordinator.SubscriptionMigrator") as mock_migrator_cls:
            mock_migrator = MagicMock()
            mock_migrator.migrate_tenant_subscriptions = AsyncMock(return_value=expected_summary)
            mock_migrator_cls.return_value = mock_migrator

            summary = await coordinator.migrate_subscriptions(migration_id)

        assert summary.all_successful is True
        assert summary.successful_count == 3
        assert summary.failed_count == 0

        # Verify summary is stored
        stored_summary = coordinator.get_subscription_summary(migration_id)
        assert stored_summary is summary

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_with_failures(self, coordinator_deps: dict) -> None:
        """Test migrate_subscriptions handles partial failures."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            position_mapper=AsyncMock(),
            checkpoint_repo=AsyncMock(),
            enable_tracing=False,
        )

        expected_summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[
                SubscriptionMigrationResult(
                    subscription_name="Projection1",
                    source_position=100,
                    target_position=50,
                    success=True,
                ),
                SubscriptionMigrationResult(
                    subscription_name="Projection2",
                    source_position=200,
                    target_position=None,
                    success=False,
                    error_message="No mapping found",
                ),
                SubscriptionMigrationResult(
                    subscription_name="Projection3",
                    source_position=300,
                    target_position=150,
                    success=True,
                ),
            ],
            successful_count=2,
            failed_count=1,
        )

        with patch("eventsource.migration.coordinator.SubscriptionMigrator") as mock_migrator_cls:
            mock_migrator = MagicMock()
            mock_migrator.migrate_tenant_subscriptions = AsyncMock(return_value=expected_summary)
            mock_migrator_cls.return_value = mock_migrator

            summary = await coordinator.migrate_subscriptions(migration_id)

        assert summary.all_successful is False
        assert summary.successful_count == 2
        assert summary.failed_count == 1

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_migration_not_found(self, coordinator_deps: dict) -> None:
        """Test migrate_subscriptions raises MigrationNotFoundError."""
        migration_id = uuid4()

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=None)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            position_mapper=AsyncMock(),
            checkpoint_repo=AsyncMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationNotFoundError):
            await coordinator.migrate_subscriptions(migration_id)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_no_position_mapper(self, coordinator_deps: dict) -> None:
        """Test migrate_subscriptions raises if no position_mapper provided."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            # position_mapper=None (default)
            checkpoint_repo=AsyncMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.migrate_subscriptions(migration_id)

        assert "position_mapper not provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_no_checkpoint_repo(self, coordinator_deps: dict) -> None:
        """Test migrate_subscriptions raises if no checkpoint_repo provided."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            position_mapper=AsyncMock(),
            # checkpoint_repo=None (default)
            enable_tracing=False,
        )

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.migrate_subscriptions(migration_id)

        assert "checkpoint_repo not provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_dry_run(self, coordinator_deps: dict) -> None:
        """Test migrate_subscriptions respects dry_run flag."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )

        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            position_mapper=AsyncMock(),
            checkpoint_repo=AsyncMock(),
            enable_tracing=False,
        )

        expected_summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[
                SubscriptionMigrationResult(
                    subscription_name="Projection1",
                    source_position=100,
                    target_position=50,
                    success=True,
                ),
            ],
            successful_count=1,
        )

        with patch("eventsource.migration.coordinator.SubscriptionMigrator") as mock_migrator_cls:
            mock_migrator = MagicMock()
            mock_migrator.migrate_tenant_subscriptions = AsyncMock(return_value=expected_summary)
            mock_migrator_cls.return_value = mock_migrator

            await coordinator.migrate_subscriptions(migration_id, dry_run=True)

            # Verify dry_run was passed
            mock_migrator.migrate_tenant_subscriptions.assert_called_once()
            call_kwargs = mock_migrator.migrate_tenant_subscriptions.call_args[1]
            assert call_kwargs["dry_run"] is True


class TestGetConsistencyReport:
    """Tests for MigrationCoordinator.get_consistency_report() method."""

    def test_get_consistency_report_returns_stored_report(self) -> None:
        """Test get_consistency_report returns stored report."""
        migration_id = uuid4()
        tenant_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

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
        migration_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        result = coordinator.get_consistency_report(migration_id)
        assert result is None


class TestGetSubscriptionSummary:
    """Tests for MigrationCoordinator.get_subscription_summary() method."""

    def test_get_subscription_summary_returns_stored_summary(self) -> None:
        """Test get_subscription_summary returns stored summary."""
        migration_id = uuid4()
        tenant_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            successful_count=2,
        )

        coordinator._subscription_summaries[migration_id] = summary

        result = coordinator.get_subscription_summary(migration_id)
        assert result is summary

    def test_get_subscription_summary_returns_none_if_not_found(self) -> None:
        """Test get_subscription_summary returns None if no summary exists."""
        migration_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        result = coordinator.get_subscription_summary(migration_id)
        assert result is None


class TestCompleteCutoverWithP3005:
    """Tests for _complete_cutover() with P3-005 integration."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        """Create coordinator dependencies."""
        return {
            "source_store": AsyncMock(),
            "migration_repo": AsyncMock(),
            "routing_repo": AsyncMock(),
            "router": MagicMock(),
        }

    @pytest.mark.asyncio
    async def test_complete_cutover_runs_verification_when_enabled(
        self, coordinator_deps: dict
    ) -> None:
        """Test _complete_cutover runs consistency verification when enabled."""
        migration_id = uuid4()
        tenant_id = uuid4()

        config = MigrationConfig(
            verify_consistency=True,
            migrate_subscriptions=False,
        )

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
            config=config,
        )

        coordinator_deps["migration_repo"].update_phase = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        # Add target store
        coordinator._target_stores[migration_id] = AsyncMock()

        # Mock verify_consistency
        expected_report = make_verification_report(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
        )

        with patch.object(coordinator, "verify_consistency", new_callable=AsyncMock) as mock_verify:
            mock_verify.return_value = expected_report

            await coordinator._complete_cutover(migration)

            # Verify verify_consistency was called
            mock_verify.assert_called_once_with(
                migration_id,
                level=VerificationLevel.HASH,
                sample_percentage=100.0,
            )

    @pytest.mark.asyncio
    async def test_complete_cutover_runs_subscription_migration_when_enabled(
        self, coordinator_deps: dict
    ) -> None:
        """Test _complete_cutover runs subscription migration when enabled."""
        migration_id = uuid4()
        tenant_id = uuid4()

        config = MigrationConfig(
            verify_consistency=False,
            migrate_subscriptions=True,
        )

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
            config=config,
        )

        coordinator_deps["migration_repo"].update_phase = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            position_mapper=AsyncMock(),
            checkpoint_repo=AsyncMock(),
            enable_tracing=False,
        )

        expected_summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            successful_count=1,
        )

        with patch.object(
            coordinator, "migrate_subscriptions", new_callable=AsyncMock
        ) as mock_migrate:
            mock_migrate.return_value = expected_summary

            await coordinator._complete_cutover(migration)

            # Verify migrate_subscriptions was called
            mock_migrate.assert_called_once_with(migration_id)

    @pytest.mark.asyncio
    async def test_complete_cutover_skips_verification_when_disabled(
        self, coordinator_deps: dict
    ) -> None:
        """Test _complete_cutover skips verification when disabled."""
        migration_id = uuid4()

        config = MigrationConfig(
            verify_consistency=False,
            migrate_subscriptions=False,
        )

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
            config=config,
        )

        coordinator_deps["migration_repo"].update_phase = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        with patch.object(coordinator, "verify_consistency", new_callable=AsyncMock) as mock_verify:
            await coordinator._complete_cutover(migration)

            # Verify verify_consistency was NOT called
            mock_verify.assert_not_called()

    @pytest.mark.asyncio
    async def test_complete_cutover_handles_verification_error_gracefully(
        self, coordinator_deps: dict
    ) -> None:
        """Test _complete_cutover handles verification errors gracefully."""
        migration_id = uuid4()

        config = MigrationConfig(
            verify_consistency=True,
            migrate_subscriptions=False,
        )

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
            config=config,
        )

        coordinator_deps["migration_repo"].update_phase = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        coordinator._target_stores[migration_id] = AsyncMock()

        with patch.object(coordinator, "verify_consistency", new_callable=AsyncMock) as mock_verify:
            mock_verify.side_effect = Exception("Verification failed")

            # Should not raise - verification errors are non-fatal
            await coordinator._complete_cutover(migration)

            # Phase should still be updated to COMPLETED
            coordinator_deps["migration_repo"].update_phase.assert_called_once_with(
                migration_id, MigrationPhase.COMPLETED
            )

    @pytest.mark.asyncio
    async def test_complete_cutover_handles_subscription_migration_error_gracefully(
        self, coordinator_deps: dict
    ) -> None:
        """Test _complete_cutover handles subscription migration errors gracefully."""
        migration_id = uuid4()

        config = MigrationConfig(
            verify_consistency=False,
            migrate_subscriptions=True,
        )

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
            config=config,
        )

        coordinator_deps["migration_repo"].update_phase = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            position_mapper=AsyncMock(),
            checkpoint_repo=AsyncMock(),
            enable_tracing=False,
        )

        with patch.object(
            coordinator, "migrate_subscriptions", new_callable=AsyncMock
        ) as mock_migrate:
            mock_migrate.side_effect = Exception("Migration failed")

            # Should not raise - subscription errors are non-fatal
            await coordinator._complete_cutover(migration)

            # Phase should still be updated to COMPLETED
            coordinator_deps["migration_repo"].update_phase.assert_called_once_with(
                migration_id, MigrationPhase.COMPLETED
            )


class TestCleanupMigrationResourcesP3005:
    """Tests for _cleanup_migration_resources() including P3-005 resources."""

    def test_cleanup_removes_consistency_report(self) -> None:
        """Test cleanup removes consistency report."""
        migration_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add P3-005 resources
        coordinator._consistency_reports[migration_id] = MagicMock()
        coordinator._subscription_summaries[migration_id] = MagicMock()
        coordinator._lag_trackers[migration_id] = MagicMock()
        coordinator._target_stores[migration_id] = MagicMock()

        coordinator._cleanup_migration_resources(migration_id)

        assert migration_id not in coordinator._consistency_reports
        assert migration_id not in coordinator._subscription_summaries
        assert migration_id not in coordinator._lag_trackers
        assert migration_id not in coordinator._target_stores

    def test_cleanup_handles_missing_resources_gracefully(self) -> None:
        """Test cleanup handles missing P3-005 resources gracefully."""
        migration_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # No resources added
        # Should not raise
        coordinator._cleanup_migration_resources(migration_id)


class TestVerificationLevel:
    """Tests for verification level configuration."""

    @pytest.mark.asyncio
    async def test_verify_consistency_with_count_level(self) -> None:
        """Test verify_consistency with COUNT level."""
        migration_id = uuid4()
        tenant_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=AsyncMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

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

        with patch("eventsource.migration.coordinator.ConsistencyVerifier") as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=report)
            mock_verifier_cls.return_value = mock_verifier

            await coordinator.verify_consistency(
                migration_id,
                level=VerificationLevel.COUNT,
            )

            # Verify level was passed correctly
            mock_verifier.verify_tenant_consistency.assert_called_once()
            call_kwargs = mock_verifier.verify_tenant_consistency.call_args[1]
            assert call_kwargs["level"] == VerificationLevel.COUNT

    @pytest.mark.asyncio
    async def test_verify_consistency_with_full_level(self) -> None:
        """Test verify_consistency with FULL level."""
        migration_id = uuid4()
        tenant_id = uuid4()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=AsyncMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

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

        with patch("eventsource.migration.coordinator.ConsistencyVerifier") as mock_verifier_cls:
            mock_verifier = MagicMock()
            mock_verifier.verify_tenant_consistency = AsyncMock(return_value=report)
            mock_verifier_cls.return_value = mock_verifier

            await coordinator.verify_consistency(
                migration_id,
                level=VerificationLevel.FULL,
                sample_percentage=50.0,
            )

            # Verify level and sample_percentage were passed correctly
            mock_verifier.verify_tenant_consistency.assert_called_once()
            call_kwargs = mock_verifier.verify_tenant_consistency.call_args[1]
            assert call_kwargs["level"] == VerificationLevel.FULL
            assert call_kwargs["sample_percentage"] == 50.0
