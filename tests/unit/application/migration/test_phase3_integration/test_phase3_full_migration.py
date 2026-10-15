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
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            position_mapper=PositionMapper(position_mapping_repo, enable_tracing=False),
            checkpoint_repo=checkpoint_repo,
            source_store_id="default",
            enable_tracing=False,
        )
        router.register_store("dedicated", target_store)
        config = MigrationConfig(
            batch_size=10, verify_consistency=True, migrate_subscriptions=False
        )
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=config,
        )
        assert migration is not None
        assert migration.config.verify_consistency is True
        await asyncio.sleep(0.1)
        status = await coordinator.get_status(migration.id)
        assert status.migration_id == migration.id

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
            if envelope.event.tenant_id == tenant_id:
                event = envelope.event
                await target_store.append(
                    StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
                    [event],
                    ExpectedVersion.any_(),
                )
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            position_mapper=PositionMapper(position_mapping_repo, enable_tracing=False),
            source_store_id="default",
            enable_tracing=False,
        )
        router.register_store("dedicated", target_store)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id, target_store=target_store, target_store_id="dedicated"
        )
        await asyncio.sleep(0.1)
        report = await coordinator.verify_consistency(migration.id, level=VerificationLevel.COUNT)
        assert report is not None
        assert report.is_consistent is True
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
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)
        migration_id = uuid4()
        await position_mapper.record_mapping(migration_id, source_pos(100), target_pos(50), uuid4())
        await checkpoint_repo.save_position(
            "OrderProjection", source_pos(100), uuid4(), "OrderCreated"
        )
        coordinator = MigrationCoordinator(
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
        router.register_store("dedicated", target_store)
        await migration_repo.create(
            Migration(
                id=migration_id,
                tenant_id=tenant_id,
                source_store_id="default",
                target_store_id="dedicated",
                phase=MigrationPhase.PENDING,
            )
        )
        summary = await coordinator.migrate_subscriptions(
            migration_id, subscription_names=["OrderProjection"]
        )
        assert summary is not None
        assert summary.successful_count == 1
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
        migrator = SubscriptionMigrator(
            position_mapper=position_mapper, checkpoint_repo=checkpoint_repo, enable_tracing=False
        )
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id, tenant_id=tenant_id, subscription_names=["TestProjection"]
        )
        summary_dict = summary.to_dict()
        for key in (
            "migration_id",
            "tenant_id",
            "successful_count",
            "failed_count",
            "results",
            "all_successful",
        ):
            assert key in summary_dict

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
        migrator = SubscriptionMigrator(
            position_mapper=position_mapper, checkpoint_repo=checkpoint_repo, enable_tracing=False
        )
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["Projection1", "Projection2"],
        )
        assert summary.all_successful is False
        assert summary.successful_count == 1
        assert summary.failed_count == 1
