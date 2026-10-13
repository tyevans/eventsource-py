"""Tests for subscription migration in MigrationCoordinator (P3-005)."""

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
)
from eventsource.application.migration.subscription_migrator import (
    SubscriptionMigrationResult,
)
from eventsource.ports.migration.models import (
    Migration,
    MigrationPhase,
)
from tests.unit.application.migration.test_coordinator_subscriptions._fixtures import (
    create_coordinator,
    create_coordinator_deps,
    make_migration_summary,
    source_pos,
    target_pos,
)


class TestMigrateSubscriptions:
    """Tests for MigrationCoordinator.migrate_subscriptions() method."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        """Create coordinator dependencies."""
        return create_coordinator_deps()

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

        expected_summary = make_migration_summary(
            migration_id=migration_id,
            tenant_id=tenant_id,
            results=[
                SubscriptionMigrationResult(
                    subscription_name="Projection1",
                    source_position=source_pos(100),
                    target_position=target_pos(50),
                    success=True,
                ),
                SubscriptionMigrationResult(
                    subscription_name="Projection2",
                    source_position=source_pos(200),
                    target_position=target_pos(100),
                    success=True,
                ),
                SubscriptionMigrationResult(
                    subscription_name="Projection3",
                    source_position=source_pos(300),
                    target_position=target_pos(150),
                    success=True,
                ),
            ],
            successful_count=3,
            failed_count=0,
        )

        with patch(
            "eventsource.application.migration.coordinator.SubscriptionMigrator"
        ) as mock_migrator_cls:
            mock_migrator = MagicMock()
            mock_migrator.migrate_tenant_subscriptions = AsyncMock(return_value=expected_summary)
            mock_migrator_cls.return_value = mock_migrator

            summary = await coordinator.migrate_subscriptions(migration_id)

        assert summary.all_successful is True
        assert summary.successful_count == 3
        assert summary.failed_count == 0

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
                    source_position=source_pos(100),
                    target_position=target_pos(50),
                    success=True,
                ),
                SubscriptionMigrationResult(
                    subscription_name="Projection2",
                    source_position=source_pos(200),
                    target_position=None,
                    success=False,
                    error_message="No mapping found",
                ),
                SubscriptionMigrationResult(
                    subscription_name="Projection3",
                    source_position=source_pos(300),
                    target_position=target_pos(150),
                    success=True,
                ),
            ],
            successful_count=2,
            failed_count=1,
        )

        with patch(
            "eventsource.application.migration.coordinator.SubscriptionMigrator"
        ) as mock_migrator_cls:
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
                    source_position=source_pos(100),
                    target_position=target_pos(50),
                    success=True,
                ),
            ],
            successful_count=1,
        )

        with patch(
            "eventsource.application.migration.coordinator.SubscriptionMigrator"
        ) as mock_migrator_cls:
            mock_migrator = MagicMock()
            mock_migrator.migrate_tenant_subscriptions = AsyncMock(return_value=expected_summary)
            mock_migrator_cls.return_value = mock_migrator

            await coordinator.migrate_subscriptions(migration_id, dry_run=True)

            mock_migrator.migrate_tenant_subscriptions.assert_called_once()
            call_kwargs = mock_migrator.migrate_tenant_subscriptions.call_args[1]
            assert call_kwargs["dry_run"] is True


class TestGetSubscriptionSummary:
    """Tests for MigrationCoordinator.get_subscription_summary() method."""

    def test_get_subscription_summary_returns_stored_summary(self) -> None:
        """Test get_subscription_summary returns stored summary."""
        migration_id = uuid4()
        tenant_id = uuid4()

        coordinator = create_coordinator()

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
        coordinator = create_coordinator()
        assert coordinator.get_subscription_summary(uuid4()) is None
