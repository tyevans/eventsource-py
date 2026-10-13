"""Tests for SubscriptionMigrator subscription migration and verification."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.position_mapper import TranslationResult
from eventsource.application.migration.subscription_migrator import (
    MigrationSummary,
    SubscriptionMigrationError,
    SubscriptionMigrator,
)
from eventsource.ports.checkpoints import CheckpointData
from eventsource.ports.positions import Position
from tests.unit.application.migration.test_subscription_migrator._fixtures import (
    make_checkpoint_data,
    source_pos,
    target_pos,
)


class TestSubscriptionMigratorMigrateSubscriptions:
    """Tests for SubscriptionMigrator.migrate_subscriptions method."""

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_dry_run(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions in dry_run mode."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=source_pos(1000),
                target_position=target_pos(500),
                is_exact=True,
            )
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=True,
        )

        # Should not call save_position in dry_run mode
        mock_checkpoint_repo.save_position.assert_not_called()

        assert isinstance(summary, MigrationSummary)
        assert summary.migration_id == migration_id
        assert summary.successful_count == 1

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_executes(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions actually migrates checkpoints."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                CheckpointData(
                    projection_name="OrderProjection",
                    last_event_id=event_id,
                    last_event_type="OrderCreated",
                    position=source_pos(1000),
                )
            ]
        )
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=source_pos(1000),
                target_position=target_pos(500),
                is_exact=True,
            )
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=False,
        )

        # Should call save_position
        mock_checkpoint_repo.save_position.assert_called_once_with(
            subscription_id="OrderProjection",
            position=target_pos(500),
            event_id=event_id,
            event_type="OrderCreated",
        )

        assert summary.successful_count == 1
        assert summary.failed_count == 0
        assert summary.all_successful

        # The checkpoint position and its translated target both round-trip
        # through save_position as Position tokens -- no int appears anywhere
        # on the path from checkpoint read to checkpoint write.
        saved_position = mock_checkpoint_repo.save_position.call_args.kwargs["position"]
        assert isinstance(saved_position, Position)
        assert not isinstance(saved_position, int)
        assert summary.results[0].source_position == source_pos(1000)
        assert isinstance(summary.results[0].source_position, Position)
        assert summary.results[0].target_position == target_pos(500)
        assert isinstance(summary.results[0].target_position, Position)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_missing_checkpoint_data(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions skips incomplete checkpoint data."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                make_checkpoint_data(
                    projection_name="OrderProjection",
                    last_event_id=None,
                    last_event_type=None,
                )
            ]
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=False,
        )

        # Should skip due to incomplete data
        assert summary.skipped_count == 1
        assert summary.successful_count == 0

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_translation_error(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions handles position translation errors."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[make_checkpoint_data(last_event_id=event_id)]
        )
        mock_position_mapper.translate_position = AsyncMock(
            side_effect=PositionMappingError(
                "No mapping found",
                migration_id=migration_id,
                source_position=source_pos(1000),
                reason="no_mapping",
            )
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=False,
        )

        assert summary.failed_count == 1
        assert summary.successful_count == 0
        assert not summary.all_successful
        assert summary.results[0].error_message is not None

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_handles_save_error(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_subscriptions handles checkpoint save errors."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[make_checkpoint_data(last_event_id=event_id)]
        )
        mock_checkpoint_repo.save_position = AsyncMock(side_effect=Exception("Database error"))
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=source_pos(1000),
                target_position=target_pos(500),
                is_exact=True,
            )
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
            dry_run=False,
        )

        assert summary.failed_count == 1
        assert summary.results[0].error_message is not None
        assert "Database error" in summary.results[0].error_message

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_raises_on_critical_checkpoint_repo_error(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
    ) -> None:
        """A failure reading checkpoint repo must raise SubscriptionMigrationError."""
        migration_id = uuid4()
        tenant_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(
            side_effect=ConnectionError("checkpoint store unreachable")
        )

        with pytest.raises(SubscriptionMigrationError) as exc_info:
            await migrator.migrate_subscriptions(
                migration_id=migration_id,
                tenant_id=tenant_id,
                subscription_names=["OrderProjection"],
                dry_run=False,
            )

        assert exc_info.value.subscription_name == "OrderProjection"
        assert exc_info.value.migration_id == migration_id
        assert "checkpoint store unreachable" in str(exc_info.value)


class TestSubscriptionMigratorMigrateTenantSubscriptions:
    """Tests for SubscriptionMigrator.migrate_tenant_subscriptions method."""

    @pytest.mark.asyncio
    async def test_migrate_tenant_with_explicit_names(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_tenant_subscriptions with explicit subscription names."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[make_checkpoint_data(last_event_id=event_id)]
        )
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=source_pos(1000),
                target_position=target_pos(500),
                is_exact=True,
            )
        )

        summary = await migrator.migrate_tenant_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert summary.successful_count == 1

    @pytest.mark.asyncio
    async def test_migrate_tenant_discovers_subscriptions(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        """Test migrate_tenant_subscriptions discovers subscriptions from checkpoints."""
        migration_id = uuid4()
        tenant_id = uuid4()
        event_id = uuid4()

        mock_checkpoint_repo.get_all_checkpoints = AsyncMock(
            return_value=[
                make_checkpoint_data("OrderProjection", event_id, "OrderCreated", source_pos(1000)),
                make_checkpoint_data(
                    "InventoryProjection", event_id, "InventoryUpdated", source_pos(2000)
                ),
            ]
        )
        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_checkpoint_repo.save_position = AsyncMock()
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=source_pos(1000),
                target_position=target_pos(500),
                is_exact=True,
            )
        )

        summary = await migrator.migrate_tenant_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=None,
        )

        # Should have discovered both subscriptions
        assert summary.successful_count == 2


class TestSubscriptionMigratorVerifyMigration:
    """Tests for SubscriptionMigrator.verify_migration method."""

    @pytest.mark.asyncio
    async def test_verify_migration_all_present(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
    ) -> None:
        """Test verify_migration returns True for all present checkpoints."""
        migration_id = uuid4()

        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(500))

        results = await migrator.verify_migration(
            migration_id=migration_id,
            subscription_names=["OrderProjection", "InventoryProjection"],
        )

        assert all(results.values())
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_verify_migration_some_missing(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
    ) -> None:
        """Test verify_migration returns False for missing checkpoints."""
        migration_id = uuid4()

        positions = {
            "OrderProjection": source_pos(500),
            "InventoryProjection": None,
        }
        mock_checkpoint_repo.get_position = AsyncMock(side_effect=lambda name: positions.get(name))

        results = await migrator.verify_migration(
            migration_id=migration_id,
            subscription_names=["OrderProjection", "InventoryProjection"],
        )

        assert results["OrderProjection"] is True
        assert results["InventoryProjection"] is False
