"""Position mapper edge cases, missing mapping errors, and coordinator error handling."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.exceptions import MigrationError, PositionMappingError
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.application.migration.subscription_migrator import SubscriptionMigrator
from eventsource.ports.migration.models import Migration, MigrationPhase
from tests.unit.application.migration.test_phase3_integration._events import source_pos, target_pos
from tests.unit.application.migration.test_phase3_integration._mocks import (
    InMemoryPositionMappingRepository,
    MockLockManager,
)
from tests.unit.application.migration.test_phase3_integration._repositories import (
    InMemoryCheckpointRepository,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)


class TestPositionMapperEdgeCases:
    @pytest.mark.asyncio
    async def test_empty_batch_mapping(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        count = await position_mapper.record_mappings_batch(migration_id=migration_id, mappings=[])
        assert count == 0

    @pytest.mark.asyncio
    async def test_get_position_bounds_no_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        bounds = await position_mapper.get_position_bounds(migration_id)
        assert bounds is None

    @pytest.mark.asyncio
    async def test_get_position_bounds_with_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(500),
            target_position=target_pos(250),
            event_id=uuid4(),
        )
        bounds = await position_mapper.get_position_bounds(migration_id)
        assert bounds == (source_pos(100), source_pos(500))

    @pytest.mark.asyncio
    async def test_clear_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
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
        count = await position_mapper.clear_mappings(migration_id)
        assert count == 2
        assert await position_mapper.get_mapping_count(migration_id) == 0

    @pytest.mark.asyncio
    async def test_get_mapping_by_event_id(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        event_id = uuid4()
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=event_id,
        )
        mapping = await position_mapper.get_mapping_by_event_id(migration_id, event_id)
        assert mapping is not None
        assert mapping.source_position == source_pos(100)
        assert mapping.target_position == target_pos(50)
        assert mapping.event_id == event_id

    @pytest.mark.asyncio
    async def test_translate_empty_batch(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        results = await position_mapper.translate_positions_batch(
            migration_id=migration_id, source_positions=[]
        )
        assert results == []


class TestMissingMappingErrors:
    @pytest.mark.asyncio
    async def test_translation_fails_with_no_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        with pytest.raises(PositionMappingError):
            await position_mapper.translate_position(
                migration_id=migration_id, source_position=source_pos(100)
            )

    @pytest.mark.asyncio
    async def test_translation_fails_for_position_before_all_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
        with pytest.raises(PositionMappingError):
            await position_mapper.translate_position(
                migration_id=migration_id, source_position=source_pos(50)
            )

    @pytest.mark.asyncio
    async def test_subscription_migration_fails_with_no_mapping(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
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
        assert summary.all_successful is False
        assert summary.failed_count == 1
        assert summary.results[0].error_message is not None


class TestCoordinatorErrorHandling:
    @pytest.mark.asyncio
    async def test_verify_consistency_without_target_store(
        self,
        source_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )
        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )
        await migration_repo.create(migration)
        with pytest.raises(MigrationError) as exc_info:
            await coordinator.verify_consistency(migration.id)
        assert "Target store not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_without_position_mapper(
        self,
        source_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )
        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )
        await migration_repo.create(migration)
        with pytest.raises(MigrationError) as exc_info:
            await coordinator.migrate_subscriptions(migration.id)
        assert "position_mapper not provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_without_checkpoint_repo(
        self,
        source_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            position_mapper=position_mapper,
            source_store_id="default",
            enable_tracing=False,
        )
        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )
        await migration_repo.create(migration)
        with pytest.raises(MigrationError) as exc_info:
            await coordinator.migrate_subscriptions(migration.id)
        assert "checkpoint_repo not provided" in str(exc_info.value)
