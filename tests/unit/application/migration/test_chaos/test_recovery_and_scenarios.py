"""Tests for recovery after failures and comprehensive multi-failure scenarios."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import MigrationPhase, TenantMigrationState

from ._fixtures import (
    _REGISTRY,
    ChaosTestEvent,
    FailureInjectableStore,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
    MockLockManager,
    count_tenant_events,
)

# =============================================================================
# Recovery After Failure Tests
# =============================================================================


class TestRecoveryAfterFailures:
    """Tests for recovery scenarios after various failure modes."""

    @pytest.mark.asyncio
    async def test_abort_cleans_up_state_properly(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that abort properly cleans up all migration state."""
        router.register_store("dedicated", target_store)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # Start migration
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Wait for background task to start
        await asyncio.sleep(0.1)

        # Abort
        result = await coordinator.abort_migration(
            migration.id,
            reason="Test abort for cleanup verification",
        )

        assert result is not None
        assert result.final_phase == MigrationPhase.ABORTED

        # Verify routing state is cleared
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.NORMAL
        assert routing.active_migration_id is None

        # Verify migration is in terminal state
        aborted_migration = await migration_repo.get(migration.id)
        assert aborted_migration is not None
        assert aborted_migration.phase == MigrationPhase.ABORTED

    @pytest.mark.asyncio
    async def test_can_start_new_migration_after_abort(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that new migration can be started after abort."""
        router.register_store("dedicated", target_store)
        router.register_store("dedicated2", target_store)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # First migration
        migration1 = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Wait for background task
        await asyncio.sleep(0.1)

        # Abort first migration
        await coordinator.abort_migration(migration1.id, reason="Test")

        # Should be able to start new migration
        migration2 = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated2",
        )

        assert migration2 is not None
        assert migration2.id != migration1.id
        assert migration2.tenant_id == tenant_id


# =============================================================================
# Comprehensive Failure Scenario Tests
# =============================================================================


class TestComprehensiveFailureScenarios:
    """Comprehensive tests combining multiple failure scenarios."""

    @pytest.mark.asyncio
    async def test_multiple_failure_types_during_migration(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test handling of multiple failure types during dual-write."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = FailureInjectableStore()

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        events_written = 0

        # Phase 1: Normal operation
        for i in range(5):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            events_written += 1

        assert events_written == 5

        # Phase 2: Network partition on target
        target_store.simulate_network_partition()

        for i in range(5, 10):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            events_written += 1

        assert events_written == 10  # Source writes succeed

        # Phase 3: Heal partition
        target_store.heal_network_partition()

        for i in range(10, 15):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            events_written += 1

        assert events_written == 15

        # Verify source has all events
        source_count = 0
        async for envelope in source_store.read_all():
            if envelope.event.tenant_id == tenant_id:
                source_count += 1
        assert source_count == 15

        # Target has events from phase 1 and 3
        target_count = await count_tenant_events(target_store, tenant_id)
        assert target_count == 10  # 5 from phase 1, 5 from phase 3

    @pytest.mark.asyncio
    async def test_cascading_failures_handled_gracefully(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test that cascading failures are handled gracefully."""
        # Create failing lock manager
        lock_manager = MockLockManager(should_fail=True)

        router.register_store("dedicated", target_store)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # Start migration (should succeed)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        assert migration is not None

        # Verify migration was created
        current = await coordinator.get_migration(migration.id)
        assert current is not None
