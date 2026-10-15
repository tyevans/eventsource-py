"""Tests for subscription migration in MigrationCoordinator (P3-005)."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.exceptions import MigrationError, MigrationNotFoundError
from eventsource.application.migration.subscription_migrator import SubscriptionMigrationResult
from eventsource.ports.migration.models import Migration, MigrationPhase
from tests.unit.application.migration.test_coordinator_subscriptions._fixtures import (
    create_coordinator,
    create_coordinator_deps,
    make_migration_summary,
    source_pos,
    target_pos,
)


def _completed_migration(migration_id=None, tenant_id=None) -> Migration:
    return Migration(
        id=migration_id or uuid4(),
        tenant_id=tenant_id or uuid4(),
        source_store_id="default",
        target_store_id="dedicated",
        phase=MigrationPhase.COMPLETED,
    )


def _sub_result(
    name: str, src: int, tgt: int | None, success: bool = True, error: str | None = None
) -> SubscriptionMigrationResult:
    return SubscriptionMigrationResult(
        subscription_name=name,
        source_position=source_pos(src),
        target_position=target_pos(tgt) if tgt is not None else None,
        success=success,
        error_message=error,
    )


class TestMigrateSubscriptions:
    @pytest.fixture
    def coordinator_deps(self) -> dict:
        return create_coordinator_deps()

    def _make_coordinator(self, coordinator_deps: dict, migration) -> MigrationCoordinator:
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=migration)
        return MigrationCoordinator(
            **coordinator_deps,
            position_mapper=AsyncMock(),
            checkpoint_repo=AsyncMock(),
            enable_tracing=False,
        )

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_success(self, coordinator_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = self._make_coordinator(
            coordinator_deps, _completed_migration(migration_id, tenant_id)
        )
        expected_summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[
                _sub_result("Projection1", 100, 50),
                _sub_result("Projection2", 200, 100),
                _sub_result("Projection3", 300, 150),
            ],
            successful_count=3,
            failed_count=0,
        )
        with patch(
            "eventsource.application.migration.coordinator.SubscriptionMigrator"
        ) as mock_cls:
            mock_cls.return_value = MagicMock(
                migrate_tenant_subscriptions=AsyncMock(return_value=expected_summary)
            )
            summary = await coordinator.migrate_subscriptions(migration_id)

        assert (summary.all_successful, summary.successful_count, summary.failed_count) == (
            True,
            3,
            0,
        )
        assert coordinator.get_subscription_summary(migration_id) is summary

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_with_failures(self, coordinator_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = self._make_coordinator(
            coordinator_deps, _completed_migration(migration_id, tenant_id)
        )
        expected_summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[
                _sub_result("Projection1", 100, 50),
                _sub_result("Projection2", 200, None, success=False, error="No mapping found"),
                _sub_result("Projection3", 300, 150),
            ],
            successful_count=2,
            failed_count=1,
        )
        with patch(
            "eventsource.application.migration.coordinator.SubscriptionMigrator"
        ) as mock_cls:
            mock_cls.return_value = MagicMock(
                migrate_tenant_subscriptions=AsyncMock(return_value=expected_summary)
            )
            summary = await coordinator.migrate_subscriptions(migration_id)

        assert (summary.all_successful, summary.successful_count, summary.failed_count) == (
            False,
            2,
            1,
        )

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_migration_not_found(self, coordinator_deps: dict) -> None:
        coordinator = self._make_coordinator(coordinator_deps, None)
        with pytest.raises(MigrationNotFoundError):
            await coordinator.migrate_subscriptions(uuid4())

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_no_position_mapper(self, coordinator_deps: dict) -> None:
        migration_id = uuid4()
        coordinator_deps["migration_repo"].get = AsyncMock(
            return_value=_completed_migration(migration_id)
        )
        coordinator = MigrationCoordinator(
            **coordinator_deps, checkpoint_repo=AsyncMock(), enable_tracing=False
        )
        with pytest.raises(MigrationError) as exc_info:
            await coordinator.migrate_subscriptions(migration_id)
        assert "position_mapper not provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_no_checkpoint_repo(self, coordinator_deps: dict) -> None:
        migration_id = uuid4()
        coordinator_deps["migration_repo"].get = AsyncMock(
            return_value=_completed_migration(migration_id)
        )
        coordinator = MigrationCoordinator(
            **coordinator_deps, position_mapper=AsyncMock(), enable_tracing=False
        )
        with pytest.raises(MigrationError) as exc_info:
            await coordinator.migrate_subscriptions(migration_id)
        assert "checkpoint_repo not provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_dry_run(self, coordinator_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = self._make_coordinator(
            coordinator_deps, _completed_migration(migration_id, tenant_id)
        )
        expected_summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[_sub_result("Projection1", 100, 50)],
            successful_count=1,
        )
        with patch(
            "eventsource.application.migration.coordinator.SubscriptionMigrator"
        ) as mock_cls:
            mock_migrator = MagicMock(
                migrate_tenant_subscriptions=AsyncMock(return_value=expected_summary)
            )
            mock_cls.return_value = mock_migrator
            await coordinator.migrate_subscriptions(migration_id, dry_run=True)
            mock_migrator.migrate_tenant_subscriptions.assert_called_once()
            assert mock_migrator.migrate_tenant_subscriptions.call_args[1]["dry_run"] is True


class TestGetSubscriptionSummary:
    def test_get_subscription_summary_returns_stored_summary(self) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        coordinator = create_coordinator()
        summary = make_migration_summary(
            migration_id=migration_id, tenant_id=tenant_id, successful_count=2
        )
        coordinator._subscription_summaries[migration_id] = summary
        assert coordinator.get_subscription_summary(migration_id) is summary

    def test_get_subscription_summary_returns_none_if_not_found(self) -> None:
        assert create_coordinator().get_subscription_summary(uuid4()) is None
