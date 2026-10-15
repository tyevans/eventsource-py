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
        """Start a new tenant migration and begin background bulk copy phase."""
        attrs = {
            ATTR_MIGRATION_TENANT_ID: str(tenant_id),
            ATTR_MIGRATION_SOURCE_STORE: self._source_store_id,
            ATTR_MIGRATION_TARGET_STORE: target_store_id,
            "target_store_id": target_store_id,
        }
        with self._tracer.span("eventsource.coordinator.start_migration", attrs):
            if (existing := await self._migration_repo.get_by_tenant(tenant_id)) is not None:
                raise MigrationAlreadyExistsError(tenant_id, existing.id)
            self._router.register_store(target_store_id, target_store)
            migration = Migration(
                id=uuid4(),
                tenant_id=tenant_id,
                source_store_id=self._source_store_id,
                target_store_id=target_store_id,
                phase=MigrationPhase.PENDING,
                config=config or MigrationConfig(),
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
            details = {
                "tenant_id": str(tenant_id),
                "source_store_id": self._source_store_id,
                "target_store_id": target_store_id,
            }
            await self._record_audit(
                MigrationAuditEntry.migration_started(
                    migration_id=migration.id,
                    occurred_at=datetime.now(UTC),
                    operator=created_by,
                    details=details,
                )
            )
            await self._routing_repo.get_or_default(tenant_id, self._source_store_id)
            await self._routing_repo.set_migration_state(
                tenant_id, TenantMigrationState.BULK_COPY, migration.id
            )
            await self._migration_repo.update_phase(migration.id, MigrationPhase.BULK_COPY)
            if (refreshed := await self._migration_repo.get(migration.id)) is None:
                raise MigrationNotFoundError(migration.id)
            self._target_stores[refreshed.id] = target_store
            self._active_tasks[refreshed.id] = asyncio.create_task(
                self._run_bulk_copy(refreshed, target_store), name=f"bulk_copy_{refreshed.id}"
            )
            return refreshed

    async def get_status(self, migration_id: UUID) -> MigrationStatus:
        """Get current status of a migration."""
        with self._tracer.span(
            "eventsource.coordinator.get_status", {ATTR_MIGRATION_ID: str(migration_id)}
        ):
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            return self._build_status(migration)

    async def list_active_migrations(self) -> list[MigrationStatus]:
        """List all active (non-terminal) migrations."""
        with self._tracer.span("eventsource.coordinator.list_active_migrations", {}):
            return [self._build_status(m) for m in await self._migration_repo.list_active()]

    async def wait_for_phase(
        self,
        migration_id: UUID,
        phase: MigrationPhase,
        *,
        timeout: float | None = None,
        poll_interval: float = 1.0,
    ) -> Migration:
        """Wait for migration to reach a specific phase or terminal state."""
        start = asyncio.get_event_loop().time()
        while True:
            if (migration := await self._migration_repo.get(migration_id)) is None:
                raise MigrationNotFoundError(migration_id)
            if migration.phase == phase or migration.phase.is_terminal:
                return migration
            if timeout is not None and (asyncio.get_event_loop().time() - start) >= timeout:
                raise TimeoutError(f"Timeout waiting for phase {phase.value}")
            await asyncio.sleep(poll_interval)

    async def get_migration(self, migration_id: UUID) -> Migration | None:
        return await self._migration_repo.get(migration_id)

    async def get_migration_for_tenant(self, tenant_id: UUID) -> Migration | None:
        return await self._migration_repo.get_by_tenant(tenant_id)


__all__ = ["CoordinatorLifecycleMixin"]
