"""Migration lifecycle operations mixin for MigrationCoordinator."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from eventsource.application.migration.exceptions import (
    MigrationAlreadyExistsError,
    MigrationNotFoundError,
)
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_MIGRATION_ID,
    ATTR_MIGRATION_SOURCE_STORE,
    ATTR_MIGRATION_TARGET_STORE,
    ATTR_MIGRATION_TENANT_ID,
)
from eventsource.ports import FullEventStore
from eventsource.ports.migration.models import (
    Migration,
    MigrationAuditEntry,
    MigrationConfig,
    MigrationPhase,
    MigrationStatus,
    TenantMigrationState,
)

if TYPE_CHECKING:
    from eventsource.application.migration.bulk_copier import BulkCopier
    from eventsource.application.migration.router import TenantStoreRouter
    from eventsource.ports.migration.repositories import (
        MigrationRepository,
        TenantRoutingRepository,
    )

logger = logging.getLogger(__name__)


class CoordinatorLifecycleMixin:
    """Mixin providing migration lifecycle operations (start, get_status, list, wait)."""

    _tracer: Tracer
    _source_store_id: str
    _migration_repo: MigrationRepository
    _routing_repo: TenantRoutingRepository
    _router: TenantStoreRouter
    _target_stores: dict[UUID, FullEventStore]
    _active_tasks: dict[UUID, asyncio.Task[None]]
    _active_copiers: dict[UUID, BulkCopier]

    def _build_status(self, migration: Migration) -> MigrationStatus:
        raise NotImplementedError

    async def _record_audit(self, entry: MigrationAuditEntry) -> None:
        raise NotImplementedError

    async def _run_bulk_copy(self, migration: Migration, target_store: FullEventStore) -> None:
        raise NotImplementedError

    async def start_migration(
        self,
        tenant_id: UUID,
        target_store: FullEventStore,
        target_store_id: str,
        config: MigrationConfig | None = None,
        *,
        created_by: str | None = None,
    ) -> Migration:
        """
        Start a new tenant migration.

        Initializes migration state and begins bulk copy phase.
        The bulk copy runs in the background; use get_status() or
        wait_for_phase() to monitor progress.

        Args:
            tenant_id: Tenant UUID to migrate
            target_store: Target FullEventStore instance
            target_store_id: Identifier for target store
            config: Migration configuration (uses defaults if None)
            created_by: Operator identifier for audit

        Returns:
            Migration instance with initial state

        Raises:
            MigrationAlreadyExistsError: If active migration exists for tenant
        """
        with self._tracer.span(
            "eventsource.coordinator.start_migration",
            {
                ATTR_MIGRATION_TENANT_ID: str(tenant_id),
                ATTR_MIGRATION_SOURCE_STORE: self._source_store_id,
                ATTR_MIGRATION_TARGET_STORE: target_store_id,
                "target_store_id": target_store_id,
            },
        ):
            config = config or MigrationConfig()

            # Check for existing active migration
            existing = await self._migration_repo.get_by_tenant(tenant_id)
            if existing is not None:
                raise MigrationAlreadyExistsError(tenant_id, existing.id)

            # Register target store with router
            self._router.register_store(target_store_id, target_store)

            # Create migration record
            migration = Migration(
                id=uuid4(),
                tenant_id=tenant_id,
                source_store_id=self._source_store_id,
                target_store_id=target_store_id,
                phase=MigrationPhase.PENDING,
                config=config,
                created_by=created_by,
            )

            await self._migration_repo.create(migration)

            logger.info(
                "Created migration %s for tenant %s from %s to %s",
                migration.id,
                tenant_id,
                self._source_store_id,
                target_store_id,
            )

            await self._record_audit(
                MigrationAuditEntry.migration_started(
                    migration_id=migration.id,
                    occurred_at=datetime.now(UTC),
                    operator=created_by,
                    details={
                        "tenant_id": str(tenant_id),
                        "source_store_id": self._source_store_id,
                        "target_store_id": target_store_id,
                    },
                )
            )

            # Ensure tenant has routing entry
            await self._routing_repo.get_or_default(tenant_id, self._source_store_id)

            # Update routing state to BULK_COPY
            await self._routing_repo.set_migration_state(
                tenant_id,
                TenantMigrationState.BULK_COPY,
                migration.id,
            )

            # Transition to bulk copy phase
            await self._migration_repo.update_phase(
                migration.id,
                MigrationPhase.BULK_COPY,
            )

            # Refresh migration to get updated timestamps
            migration_id = migration.id
            refreshed_migration = await self._migration_repo.get(migration_id)
            if refreshed_migration is None:
                raise MigrationNotFoundError(migration_id)
            migration = refreshed_migration

            # Store target store reference for later dual-write setup
            self._target_stores[migration.id] = target_store

            # Start bulk copy in background
            task = asyncio.create_task(
                self._run_bulk_copy(migration, target_store),
                name=f"bulk_copy_{migration.id}",
            )
            self._active_tasks[migration.id] = task

            return migration

    async def get_status(self, migration_id: UUID) -> MigrationStatus:
        """
        Get current status of a migration.

        Args:
            migration_id: UUID of the migration

        Returns:
            MigrationStatus with current progress and state

        Raises:
            MigrationNotFoundError: If migration not found
        """
        with self._tracer.span(
            "eventsource.coordinator.get_status",
            {ATTR_MIGRATION_ID: str(migration_id)},
        ):
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            return self._build_status(migration)

    async def list_active_migrations(self) -> list[MigrationStatus]:
        """
        List all active (non-terminal) migrations.

        Returns:
            List of MigrationStatus for active migrations
        """
        with self._tracer.span(
            "eventsource.coordinator.list_active_migrations",
            {},
        ):
            migrations = await self._migration_repo.list_active()
            return [self._build_status(m) for m in migrations]

    async def wait_for_phase(
        self,
        migration_id: UUID,
        phase: MigrationPhase,
        *,
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> Migration:
        """
        Wait for migration to reach a specific phase.

        Args:
            migration_id: UUID of the migration
            phase: Phase to wait for
            timeout: Maximum seconds to wait (None = forever)
            poll_interval: Seconds between status checks

        Returns:
            Migration when phase is reached or terminal state

        Raises:
            MigrationNotFoundError: If migration not found
            TimeoutError: If timeout exceeded
        """
        start = asyncio.get_event_loop().time()

        while True:
            migration = await self._migration_repo.get(migration_id)
            if migration is None:
                raise MigrationNotFoundError(migration_id)

            # Check if we've reached or passed the target phase
            if migration.phase == phase or migration.phase.is_terminal:
                return migration

            # Check timeout
            if timeout is not None:
                elapsed = asyncio.get_event_loop().time() - start
                if elapsed >= timeout:
                    raise TimeoutError(f"Timeout waiting for phase {phase.value}")

            await asyncio.sleep(poll_interval)

    async def get_migration(self, migration_id: UUID) -> Migration | None:
        """
        Get a migration by ID.

        Args:
            migration_id: UUID of the migration

        Returns:
            Migration instance or None if not found
        """
        return await self._migration_repo.get(migration_id)

    async def get_migration_for_tenant(self, tenant_id: UUID) -> Migration | None:
        """
        Get the active migration for a tenant.

        Args:
            tenant_id: Tenant UUID

        Returns:
            Active Migration instance or None if no active migration
        """
        return await self._migration_repo.get_by_tenant(tenant_id)


__all__ = [
    "CoordinatorLifecycleMixin",
]
