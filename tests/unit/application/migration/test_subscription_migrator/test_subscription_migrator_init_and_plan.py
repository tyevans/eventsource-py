"""Tests for SubscriptionMigrator initialization and plan_migration."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.position_mapper import TranslationResult
from eventsource.application.migration.subscription_migrator import (
    MigrationPlan,
    SubscriptionMigrator,
)
from tests.unit.application.migration.test_subscription_migrator._fixtures import (
    source_pos,
    target_pos,
)


class TestSubscriptionMigratorInit:
    """Tests for SubscriptionMigrator initialization."""

    def test_init_with_dependencies(self) -> None:
        mock_position_mapper = MagicMock()
        mock_checkpoint_repo = MagicMock()
        migrator = SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=mock_checkpoint_repo,
        )
        assert migrator._position_mapper == mock_position_mapper
        assert migrator._checkpoint_repo == mock_checkpoint_repo

    def test_init_with_tracing_enabled(self) -> None:
        mock_position_mapper = MagicMock()
        migrator = SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=MagicMock(),
            enable_tracing=True,
        )
        assert migrator._position_mapper == mock_position_mapper

    def test_init_with_tracing_disabled(self) -> None:
        mock_position_mapper = MagicMock()
        migrator = SubscriptionMigrator(
            position_mapper=mock_position_mapper,
            checkpoint_repo=MagicMock(),
            enable_tracing=False,
        )
        assert migrator._position_mapper == mock_position_mapper


class TestSubscriptionMigratorPlanMigration:
    """Tests for SubscriptionMigrator.plan_migration method."""

    @pytest.mark.asyncio
    async def test_plan_migration_returns_plan(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=source_pos(1000),
                target_position=target_pos(500),
                is_exact=True,
            )
        )

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert isinstance(plan, MigrationPlan)
        assert plan.migration_id == migration_id
        assert plan.tenant_id == tenant_id
        assert plan.migratable_count == 1
        assert len(plan.planned_migrations) == 1
        assert plan.planned_migrations[0].subscription_name == "OrderProjection"

    @pytest.mark.asyncio
    async def test_plan_migration_skips_no_checkpoint(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
    ) -> None:
        mock_checkpoint_repo.get_position = AsyncMock(return_value=None)
        plan = await migrator.plan_migration(
            migration_id=uuid4(),
            tenant_id=uuid4(),
            subscription_names=["OrderProjection"],
        )
        assert plan.migratable_count == 0
        assert len(plan.skipped_subscriptions) == 1
        assert "OrderProjection" in plan.skipped_subscriptions

    @pytest.mark.asyncio
    async def test_plan_migration_handles_translation_error(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1000))
        mock_position_mapper.translate_position = AsyncMock(
            side_effect=PositionMappingError(
                "No mapping found",
                migration_id=migration_id,
                source_position=source_pos(1000),
                reason="no_mapping",
            )
        )

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        assert len(plan.planned_migrations) == 1
        assert plan.planned_migrations[0].warning is not None
        assert plan.planned_migrations[0].planned_target_position is None

    @pytest.mark.asyncio
    async def test_plan_migration_with_nearest_translation(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        mock_checkpoint_repo.get_position = AsyncMock(return_value=source_pos(1050))
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=source_pos(1050),
                target_position=target_pos(500),
                is_exact=False,
                nearest_source_position=source_pos(1000),
            )
        )

        plan = await migrator.plan_migration(
            migration_id=uuid4(),
            tenant_id=uuid4(),
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        pm = plan.planned_migrations[0]
        assert pm.is_exact_translation is False
        assert pm.nearest_source_position == source_pos(1000)
        assert pm.warning is not None and "nearest" in pm.warning.lower()

    @pytest.mark.asyncio
    async def test_plan_migration_multiple_subscriptions(
        self,
        migrator: SubscriptionMigrator,
        mock_checkpoint_repo: MagicMock,
        mock_position_mapper: MagicMock,
    ) -> None:
        positions = {
            "OrderProjection": source_pos(1000),
            "InventoryProjection": source_pos(2000),
            "NoCheckpoint": None,
        }
        mock_checkpoint_repo.get_position = AsyncMock(side_effect=lambda name: positions.get(name))
        mock_position_mapper.translate_position = AsyncMock(
            return_value=TranslationResult(
                source_position=source_pos(1000),
                target_position=target_pos(500),
                is_exact=True,
            )
        )

        plan = await migrator.plan_migration(
            migration_id=uuid4(),
            tenant_id=uuid4(),
            subscription_names=["OrderProjection", "InventoryProjection", "NoCheckpoint"],
        )

        assert plan.total_subscriptions == 3
        assert plan.migratable_count == 2
        assert len(plan.skipped_subscriptions) == 1
