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


def _coord(*args: object, position_mapper: PositionMapper | None = None) -> MigrationCoordinator:
    src, mig_repo, route_repo, router, lock_mgr = args
    return MigrationCoordinator(
        src,  # type: ignore[arg-type]
        mig_repo,  # type: ignore[arg-type]
        route_repo,  # type: ignore[arg-type]
        router,  # type: ignore[arg-type]
        lock_manager=lock_mgr,  # type: ignore[arg-type]
        position_mapper=position_mapper,
        source_store_id="default",
        enable_tracing=False,
    )


class TestPositionMapperEdgeCases:
    @pytest.mark.asyncio
    async def test_empty_batch_mapping(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        assert await position_mapper.record_mappings_batch(migration_id, []) == 0

    @pytest.mark.asyncio
    async def test_get_position_bounds_no_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        assert await position_mapper.get_position_bounds(migration_id) is None

    @pytest.mark.asyncio
    async def test_get_position_bounds_with_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        await position_mapper.record_mapping(migration_id, source_pos(100), target_pos(50), uuid4())
        await position_mapper.record_mapping(
            migration_id, source_pos(500), target_pos(250), uuid4()
        )
        assert await position_mapper.get_position_bounds(migration_id) == (
            source_pos(100),
            source_pos(500),
        )

    @pytest.mark.asyncio
    async def test_clear_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        await position_mapper.record_mapping(migration_id, source_pos(100), target_pos(50), uuid4())
        await position_mapper.record_mapping(
            migration_id, source_pos(200), target_pos(100), uuid4()
        )
        assert await position_mapper.clear_mappings(migration_id) == 2
        assert await position_mapper.get_mapping_count(migration_id) == 0

    @pytest.mark.asyncio
    async def test_get_mapping_by_event_id(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        event_id = uuid4()
        await position_mapper.record_mapping(
            migration_id, source_pos(100), target_pos(50), event_id
        )
        mapping = await position_mapper.get_mapping_by_event_id(migration_id, event_id)
        assert mapping is not None and mapping.event_id == event_id
        assert mapping.source_position == source_pos(100)
        assert mapping.target_position == target_pos(50)

    @pytest.mark.asyncio
    async def test_translate_empty_batch(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        assert await position_mapper.translate_positions_batch(migration_id, []) == []


class TestMissingMappingErrors:
    @pytest.mark.asyncio
    async def test_translation_fails_with_no_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        with pytest.raises(PositionMappingError):
            await position_mapper.translate_position(migration_id, source_pos(100))

    @pytest.mark.asyncio
    async def test_translation_fails_for_position_before_all_mappings(
        self, position_mapper: PositionMapper, migration_id: UUID
    ) -> None:
        await position_mapper.record_mapping(migration_id, source_pos(100), target_pos(50), uuid4())
        with pytest.raises(PositionMappingError):
            await position_mapper.translate_position(migration_id, source_pos(50))

    @pytest.mark.asyncio
    async def test_subscription_migration_fails_with_no_mapping(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        await checkpoint_repo.save_position(
            "OrderProjection", source_pos(100), uuid4(), "OrderCreated"
        )
        migrator = SubscriptionMigrator(position_mapper, checkpoint_repo, enable_tracing=False)
        summary = await migrator.migrate_subscriptions(migration_id, tenant_id, ["OrderProjection"])
        assert summary.all_successful is False and summary.failed_count == 1
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
        coordinator = _coord(source_store, migration_repo, routing_repo, router, lock_manager)
        migration = Migration(uuid4(), tenant_id, "default", "dedicated", MigrationPhase.BULK_COPY)
        await migration_repo.create(migration)
        with pytest.raises(MigrationError, match="Target store not found"):
            await coordinator.verify_consistency(migration.id)

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
        coordinator = _coord(source_store, migration_repo, routing_repo, router, lock_manager)
        migration = Migration(uuid4(), tenant_id, "default", "dedicated", MigrationPhase.COMPLETED)
        await migration_repo.create(migration)
        with pytest.raises(MigrationError, match="position_mapper not provided"):
            await coordinator.migrate_subscriptions(migration.id)

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
        mapper = PositionMapper(position_mapping_repo, enable_tracing=False)
        coordinator = _coord(
            source_store, migration_repo, routing_repo, router, lock_manager, position_mapper=mapper
        )
        migration = Migration(uuid4(), tenant_id, "default", "dedicated", MigrationPhase.COMPLETED)
        await migration_repo.create(migration)
        with pytest.raises(MigrationError, match="checkpoint_repo not provided"):
            await coordinator.migrate_subscriptions(migration.id)
