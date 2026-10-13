"""Tests for error handling and recovery scenarios."""

from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.exceptions import (
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
)
from eventsource.ports.migration.models import MigrationPhase

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager
    from tests.unit.application.migration.test_phase2_integration._repositories import (
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestErrorHandlingAndRecovery:
    """Tests for error handling and recovery scenarios."""

    @pytest.mark.asyncio
    async def test_duplicate_migration_raises_error(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that starting duplicate migration raises error."""
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
        router.register_store("dedicated2", target_store)

        # Start first migration
        await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Try to start second migration for same tenant
        with pytest.raises(MigrationAlreadyExistsError):
            await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=target_store,
                target_store_id="dedicated2",
            )

    @pytest.mark.asyncio
    async def test_get_status_nonexistent_migration(
        self,
        source_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
    ) -> None:
        """Test getting status of nonexistent migration raises error."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        nonexistent_id = uuid4()

        with pytest.raises(MigrationNotFoundError):
            await coordinator.get_status(nonexistent_id)

    @pytest.mark.asyncio
    async def test_abort_completed_migration_raises_error(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that aborting completed migration raises error."""
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

        # Force migration to completed state
        # Note: start_migration already puts it in BULK_COPY phase
        await migration_repo.update_phase(migration.id, MigrationPhase.DUAL_WRITE)
        await migration_repo.update_phase(migration.id, MigrationPhase.CUTOVER)
        await migration_repo.update_phase(migration.id, MigrationPhase.COMPLETED)

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.abort_migration(migration.id)

        assert "completed" in str(exc_info.value).lower()


__all__ = ["TestErrorHandlingAndRecovery"]
