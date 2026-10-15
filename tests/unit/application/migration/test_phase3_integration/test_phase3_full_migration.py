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
    """Tests for full migration lifecycle with verification and subscription migration."""

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
        """Test full migration lifecycle with consistency verification enabled."""
        await create_test_events(source_store, tenant_id, count=5)
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)

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
        """Test verify_consistency method on coordinator."""
        await create_test_events(source_store, tenant_id, count=5)

        async for envelope in source_store.read_all():
            if envelope.event.tenant_id == tenant_id:
                event = envelope.event
                await target_store.append(
                    StreamId(aggregate_id=event.aggregate_id, category=event.aggregate_type),
                    [event],
                    ExpectedVersion.any_(),
                )

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
        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
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
        """Test migrate_subscriptions method on coordinator."""
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)

        migration_id = uuid4()
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

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
        )
        await migration_repo.create(migration)

        summary = await coordinator.migrate_subscriptions(
            migration_id, subscription_names=["OrderProjection"]
        )

        assert summary is not None
        assert summary.successful_count == 1
        assert coordinator.get_subscription_summary(migration_id) is summary


class TestMigrationSummary:
    """Tests for MigrationSummary functionality."""

    @pytest.mark.asyncio
    async def test_summary_to_dict_serialization(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test summary can be serialized to dictionary."""
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
        await checkpoint_repo.save_position(
            subscription_id="TestProjection",
            position=source_pos(100),
            event_id=uuid4(),
            event_type="TestEvent",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["TestProjection"],
        )

        summary_dict = summary.to_dict()
        assert "migration_id" in summary_dict
        assert "tenant_id" in summary_dict
        assert "successful_count" in summary_dict
        assert "failed_count" in summary_dict
        assert "results" in summary_dict
        assert "all_successful" in summary_dict

    @pytest.mark.asyncio
    async def test_summary_all_successful_property(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test all_successful property works correctly."""
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=source_pos(100),
            target_position=target_pos(50),
            event_id=uuid4(),
        )
        await checkpoint_repo.save_position(
            subscription_id="Projection1",
            position=source_pos(100),
            event_id=uuid4(),
            event_type="TestEvent",
        )
        await checkpoint_repo.save_position(
            subscription_id="Projection2",
            position=source_pos(50),
            event_id=uuid4(),
            event_type="TestEvent",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["Projection1", "Projection2"],
        )

        assert summary.all_successful is False
        assert summary.successful_count == 1
        assert summary.failed_count == 1
