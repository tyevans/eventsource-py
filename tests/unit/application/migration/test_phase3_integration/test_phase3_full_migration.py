"""Full migration lifecycle and summary tests for Phase 3 integration."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.consistency import VerificationLevel
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.application.migration.subscription_migrator import SubscriptionMigrator
from eventsource.domain import StreamId
from eventsource.ports.migration.models import Migration, MigrationConfig, MigrationPhase
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.migration.test_phase3_integration._events import source_pos, target_pos
from tests.unit.application.migration.test_phase3_integration._helpers import create_test_events
from tests.unit.application.migration.test_phase3_integration._mocks import (
    InMemoryPositionMappingRepository,
    MockLockManager,
)
from tests.unit.application.migration.test_phase3_integration._repositories import (
    InMemoryCheckpointRepository,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)


def _setup_coord(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    migration_repo: InMemoryMigrationRepository,
    routing_repo: InMemoryRoutingRepository,
    router: TenantStoreRouter,
    lock_manager: MockLockManager,
    position_mapper: PositionMapper,
    checkpoint_repo: InMemoryCheckpointRepository | None = None,
) -> MigrationCoordinator:
    router.register_store("dedicated", target_store)
    return MigrationCoordinator(
        source_store=source_store,
        migration_repo=migration_repo,
        routing_repo=routing_repo,
        router=router,
        lock_manager=lock_manager,
        position_mapper=position_mapper,
        checkpoint_repo=checkpoint_repo,
        source_store_id="default",
        enable_tracing=False,
    )


class TestFullMigrationWithVerificationAndSubscriptions:
    @pytest.mark.asyncio
    async def test_full_lifecycle_with_verification_enabled(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        checkpoint_repo: InMemoryCheckpointRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        await create_test_events(source_store, tenant_id, count=5)
        mapper = PositionMapper(position_mapping_repo, enable_tracing=False)
        coord_args = (source_store, target_store, migration_repo, routing_repo, router)
        coordinator = _setup_coord(*coord_args, lock_manager, mapper, checkpoint_repo)
        cfg = MigrationConfig(batch_size=10, verify_consistency=True, migrate_subscriptions=False)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated", config=cfg
        )
        assert migration is not None and migration.config.verify_consistency is True
        await asyncio.sleep(0.1)
        assert (await coordinator.get_status(migration.id)).migration_id == migration.id

    @pytest.mark.asyncio
    async def test_verify_consistency_on_coordinator(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        await create_test_events(source_store, tenant_id, count=5)
        async for envelope in source_store.read_all():
            if (e := envelope.event).tenant_id == tenant_id:
                stream = StreamId(aggregate_id=e.aggregate_id, category=e.aggregate_type)
                await target_store.append(stream, [e], ExpectedVersion.any_())
        mapper = PositionMapper(position_mapping_repo, enable_tracing=False)
        coord_args = (source_store, target_store, migration_repo, routing_repo, router)
        coordinator = _setup_coord(*coord_args, lock_manager, mapper)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )
        await asyncio.sleep(0.1)
        report = await coordinator.verify_consistency(migration.id, level=VerificationLevel.COUNT)
        assert report is not None and report.is_consistent is True
        assert coordinator.get_consistency_report(migration.id) is report

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_on_coordinator(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        checkpoint_repo: InMemoryCheckpointRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        mapper = PositionMapper(position_mapping_repo, enable_tracing=False)
        migration_id = uuid4()
        await mapper.record_mapping(migration_id, source_pos(100), target_pos(50), uuid4())
        await checkpoint_repo.save_position("OrderProjection", source_pos(100), uuid4(), "Evt")
        coord_args = (source_store, target_store, migration_repo, routing_repo, router)
        coordinator = _setup_coord(*coord_args, lock_manager, mapper, checkpoint_repo)
        await migration_repo.create(
            Migration(
                id=migration_id,
                tenant_id=tenant_id,
                source_store_id="default",
                target_store_id="dedicated",
                phase=MigrationPhase.PENDING,
            )
        )
        summary = await coordinator.migrate_subscriptions(migration_id, ["OrderProjection"])
        assert summary is not None and summary.successful_count == 1
        assert coordinator.get_subscription_summary(migration_id) is summary


class TestMigrationSummary:
    @pytest.mark.asyncio
    async def test_summary_to_dict_serialization(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        await position_mapper.record_mapping(migration_id, source_pos(100), target_pos(50), uuid4())
        await checkpoint_repo.save_position("TestProjection", source_pos(100), uuid4(), "TestEvent")
        migrator = SubscriptionMigrator(position_mapper, checkpoint_repo, enable_tracing=False)
        summary = await migrator.migrate_subscriptions(migration_id, tenant_id, ["TestProjection"])
        summary_dict = summary.to_dict()
        keys = ("migration_id", "tenant_id", "successful_count", "failed_count", "results")
        assert all(key in summary_dict for key in (*keys, "all_successful"))

    @pytest.mark.asyncio
    async def test_summary_all_successful_property(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        await position_mapper.record_mapping(migration_id, source_pos(100), target_pos(50), uuid4())
        await checkpoint_repo.save_position("Projection1", source_pos(100), uuid4(), "TestEvent")
        await checkpoint_repo.save_position("Projection2", source_pos(50), uuid4(), "TestEvent")
        migrator = SubscriptionMigrator(position_mapper, checkpoint_repo, enable_tracing=False)
        summary = await migrator.migrate_subscriptions(
            migration_id, tenant_id, ["Projection1", "Projection2"]
        )
        assert summary.all_successful is False
        assert (summary.successful_count, summary.failed_count) == (1, 1)
