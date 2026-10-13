"""Tests for aborting migration during different phases."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.ports.migration.models import MigrationPhase, TenantMigrationState
from tests.unit.application.migration.test_phase2_integration._helpers import create_test_events

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager
    from tests.unit.application.migration.test_phase2_integration._repositories import (
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestAbortDuringDifferentPhases:
    """Tests for aborting migration during different phases."""

    @pytest.mark.asyncio
    async def test_abort_during_bulk_copy(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test aborting migration during bulk copy phase."""
        # Create some events in source
        await create_test_events(source_store, tenant_id, count=10)

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

        # Abort the migration
        result = await coordinator.abort_migration(
            migration.id,
            reason="Test abort during bulk copy",
        )

        assert result is not None
        assert result.success is False
        assert result.final_phase == MigrationPhase.ABORTED

        # Verify migration state
        migration = await coordinator.get_migration(result.migration_id)
        assert migration is not None
        assert migration.phase == MigrationPhase.ABORTED

    @pytest.mark.asyncio
    async def test_abort_clears_routing_state(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that abort clears routing migration state."""
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

        await coordinator.abort_migration(migration.id, reason="Test abort")

        # Verify routing state was reset
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.NORMAL


__all__ = ["TestAbortDuringDifferentPhases"]
