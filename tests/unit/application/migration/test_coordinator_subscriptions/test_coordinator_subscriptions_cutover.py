"""Tests for cutover and resource cleanup with P3-005 integration."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.consistency import VerificationLevel
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.ports.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
)
from tests.unit.application.migration.test_coordinator_subscriptions._fixtures import (
    create_coordinator,
    create_coordinator_deps,
    make_migration_summary,
    make_verification_report,
)


class TestCompleteCutoverWithP3005:
    """Tests for _complete_cutover() with P3-005 integration."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        return create_coordinator_deps()

    @pytest.mark.asyncio
    async def test_complete_cutover_runs_verification_when_enabled(
        self, coordinator_deps: dict
    ) -> None:
        migration_id = uuid4()
        tenant_id = uuid4()
        config = MigrationConfig(verify_consistency=True, migrate_subscriptions=False)
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
            config=config,
        )
        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration_id] = AsyncMock()
        expected_report = make_verification_report(
            tenant_id=tenant_id, level=VerificationLevel.HASH
        )

        with patch.object(coordinator, "verify_consistency", new_callable=AsyncMock) as mock_verify:
            mock_verify.return_value = expected_report
            await coordinator._complete_cutover(migration)
            mock_verify.assert_called_once_with(
                migration_id, level=VerificationLevel.HASH, sample_percentage=100.0
            )

    @pytest.mark.asyncio
    async def test_complete_cutover_runs_subscription_migration_when_enabled(
        self, coordinator_deps: dict
    ) -> None:
        migration_id = uuid4()
        tenant_id = uuid4()
        config = MigrationConfig(verify_consistency=False, migrate_subscriptions=True)
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
            migration_id=migration_id, tenant_id=tenant_id, successful_count=1
        )

        with patch.object(
            coordinator, "migrate_subscriptions", new_callable=AsyncMock
        ) as mock_migrate:
            mock_migrate.return_value = expected_summary
            await coordinator._complete_cutover(migration)
            mock_migrate.assert_called_once_with(migration_id)

    @pytest.mark.asyncio
    async def test_complete_cutover_skips_verification_when_disabled(
        self, coordinator_deps: dict
    ) -> None:
        migration_id = uuid4()
        config = MigrationConfig(verify_consistency=False, migrate_subscriptions=False)
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
            config=config,
        )
        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)

        with patch.object(coordinator, "verify_consistency", new_callable=AsyncMock) as mock_verify:
            await coordinator._complete_cutover(migration)
            mock_verify.assert_not_called()

    @pytest.mark.asyncio
    async def test_complete_cutover_handles_verification_error_gracefully(
        self, coordinator_deps: dict
    ) -> None:
        migration_id = uuid4()
        config = MigrationConfig(verify_consistency=True, migrate_subscriptions=False)
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.CUTOVER,
            config=config,
        )
        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator = MigrationCoordinator(**coordinator_deps, enable_tracing=False)
        coordinator._target_stores[migration_id] = AsyncMock()

        with patch.object(coordinator, "verify_consistency", new_callable=AsyncMock) as mock_verify:
            mock_verify.side_effect = Exception("Verification failed")
            await coordinator._complete_cutover(migration)
            coordinator_deps["migration_repo"].update_phase.assert_called_once_with(
                migration_id, MigrationPhase.COMPLETED
            )

    @pytest.mark.asyncio
    async def test_complete_cutover_handles_subscription_migration_error_gracefully(
        self, coordinator_deps: dict
    ) -> None:
        migration_id = uuid4()
        config = MigrationConfig(verify_consistency=False, migrate_subscriptions=True)
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
            await coordinator._complete_cutover(migration)
            coordinator_deps["migration_repo"].update_phase.assert_called_once_with(
                migration_id, MigrationPhase.COMPLETED
            )


class TestCleanupMigrationResourcesP3005:
    """Tests for _cleanup_migration_resources() including P3-005 resources."""

    def test_cleanup_removes_consistency_report(self) -> None:
        migration_id = uuid4()
        coordinator = create_coordinator()
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
        coordinator = create_coordinator()
        coordinator._cleanup_migration_resources(uuid4())
