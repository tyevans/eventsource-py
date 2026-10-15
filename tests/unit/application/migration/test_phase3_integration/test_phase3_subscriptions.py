"""Subscription checkpoint migration and dry-run tests for Phase 3 integration."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.application.migration.subscription_migrator import SubscriptionMigrator
from tests.unit.application.migration.test_phase3_integration._events import (
    source_pos,
    target_pos,
)
from tests.unit.application.migration.test_phase3_integration._repositories import (
    InMemoryCheckpointRepository,
)


class TestSubscriptionCheckpointMigration:
    @pytest.mark.asyncio
    async def test_migrate_subscription_with_exact_mapping(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
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
        assert summary.results[0].target_position == target_pos(50)

    @pytest.mark.asyncio
    async def test_migrate_multiple_subscriptions(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
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
        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["NonExistentProjection"],
        )

        assert summary.skipped_count == 1
        assert summary.successful_count == 0


class TestDryRunSubscriptionMigration:
    @pytest.mark.asyncio
    async def test_dry_run_shows_planned_changes(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
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
        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        assert len(plan.planned_migrations) == 1
        assert plan.planned_migrations[0].current_position == source_pos(100)
        assert plan.planned_migrations[0].planned_target_position == target_pos(50)
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
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
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
        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        assert plan.planned_migrations[0].is_exact_translation is False
        assert plan.planned_migrations[0].warning is not None
