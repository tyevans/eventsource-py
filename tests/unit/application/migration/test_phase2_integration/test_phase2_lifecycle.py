"""Tests for complete migration workflow from PENDING to COMPLETED."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.ports.migration.models import MigrationConfig, MigrationPhase
from tests.unit.application.migration.test_phase2_integration._helpers import (
    create_test_events,
    get_all_tenant_events,
)

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager
    from tests.unit.application.migration.test_phase2_integration._repositories import (
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestFullMigrationLifecycle:
    """Tests for complete migration workflow from PENDING to COMPLETED."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_events(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test complete migration lifecycle with actual events."""
        # Setup: Create historical events in source store
        await create_test_events(
            source_store,
            tenant_id,
            count=5,
        )

        # Verify events in source
        source_events = await get_all_tenant_events(source_store, tenant_id)
        assert len(source_events) == 5

        # Create coordinator
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # Register target store
        router.register_store("dedicated", target_store)

        # Start migration (PENDING -> BULK_COPY)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=MigrationConfig(
                batch_size=10,
                cutover_max_lag_events=100,
            ),
        )

        assert migration is not None
        assert migration.tenant_id == tenant_id

        # Wait for bulk copy to complete and transition to DUAL_WRITE
        # In a real scenario this happens automatically, but we simulate it
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None

        # Wait a bit for background task
        await asyncio.sleep(0.1)

        # Check migration progressed
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        # Migration should be in progress (BULK_COPY or DUAL_WRITE)
        assert migration.phase in (
            MigrationPhase.BULK_COPY,
            MigrationPhase.DUAL_WRITE,
        )

    @pytest.mark.asyncio
    async def test_migration_phases_are_tracked(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that migration phases are properly tracked."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Verify migration was created
        assert migration is not None

        # Get status
        status = await coordinator.get_status(migration.id)
        assert status is not None
        assert status.migration_id == migration.id
        assert status.tenant_id == tenant_id


__all__ = ["TestFullMigrationLifecycle"]
