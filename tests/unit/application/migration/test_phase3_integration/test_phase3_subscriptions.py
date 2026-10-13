"""Subscription checkpoint migration and dry-run tests for Phase 3 integration."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.application.migration.subscription_migrator import (
    SubscriptionMigrator,
)
from tests.unit.application.migration.test_phase3_integration._events import (
    source_pos,
    target_pos,
)
from tests.unit.application.migration.test_phase3_integration._repositories import (
    InMemoryCheckpointRepository,
)


class TestSubscriptionCheckpointMigration:
    """Tests for subscription checkpoint migration after cutover."""

    @pytest.mark.asyncio
    async def test_migrate_subscription_with_exact_mapping(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test migrating subscription with exact position mapping."""
        # Setup position mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )

        # Setup checkpoint at source position 100
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=source_pos(100),
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert summary.all_successful is True
        assert summary.successful_count == 1
        assert summary.failed_count == 0

        # Verify checkpoint was updated
        new_position = await checkpoint_repo.get_position("OrderProjection")
        assert new_position == target_pos(50)

    @pytest.mark.asyncio
    async def test_migrate_subscription_with_nearest_mapping(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test migrating subscription using nearest position mapping."""
        # Setup position mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(200),
            target_position=target_pos(100),
            event_id=uuid4(),
        )

        # Setup checkpoint at position between mappings
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=source_pos(150),
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert summary.all_successful is True
        assert len(summary.results) == 1
        assert summary.results[0].is_exact_translation is False
        assert summary.results[0].target_position == target_pos(50)  # Nearest lower

    @pytest.mark.asyncio
    async def test_migrate_multiple_subscriptions(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test migrating multiple subscriptions."""
        # Setup position mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(200),
            target_position=target_pos(100),
            event_id=uuid4(),
        )

        # Setup checkpoints
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=source_pos(100),
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        await checkpoint_repo.save_position(
            subscription_id="InventoryProjection",
            position=source_pos(200),
            event_id=uuid4(),
            event_type="InventoryUpdated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection", "InventoryProjection"],
        )

        assert summary.all_successful is True
        assert summary.successful_count == 2

    @pytest.mark.asyncio
    async def test_subscription_migration_skips_no_checkpoint(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test subscription migration skips subscriptions without checkpoints."""
        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        # No checkpoint exists for this subscription
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["NonExistentProjection"],
        )

        assert summary.skipped_count == 1
        assert summary.successful_count == 0


class TestDryRunSubscriptionMigration:
    """Tests for dry-run subscription migration."""

    @pytest.mark.asyncio
    async def test_dry_run_shows_planned_changes(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test dry-run shows planned changes without executing."""
        # Setup mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )

        # Setup checkpoint
        original_position = source_pos(100)
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=original_position,
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        # Run dry-run
        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        assert len(plan.planned_migrations) == 1
        assert plan.planned_migrations[0].current_position == source_pos(100)
        assert plan.planned_migrations[0].planned_target_position == target_pos(50)

        # Verify checkpoint was NOT changed
        current_position = await checkpoint_repo.get_position("OrderProjection")
        assert current_position == original_position

    @pytest.mark.asyncio
    async def test_dry_run_shows_warnings_for_nearest_translation(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test dry-run shows warnings when using nearest translation."""
        # Setup mapping (no exact match for checkpoint position)
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )

        # Setup checkpoint at position without exact mapping
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=source_pos(150),  # No exact mapping exists
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        assert plan.planned_migrations[0].is_exact_translation is False
        assert plan.planned_migrations[0].warning is not None
