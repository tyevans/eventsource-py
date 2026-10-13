"""Tests for pause/resume functionality during migration."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager
    from tests.unit.application.migration.test_phase2_integration._repositories import (
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestPauseResumeDuringMigration:
    """Tests for pause/resume functionality during migration."""

    @pytest.mark.asyncio
    async def test_pause_migration(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test pausing a migration."""
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

        # Pause the migration
        await coordinator.pause_migration(migration.id)

        # Verify migration is paused
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        assert migration.is_paused

    @pytest.mark.asyncio
    async def test_resume_migration(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test resuming a paused migration."""
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

        # Pause then resume
        await coordinator.pause_migration(migration.id)
        await coordinator.resume_migration(migration.id)

        # Verify migration is resumed
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        assert migration.is_paused is False


__all__ = ["TestPauseResumeDuringMigration"]
