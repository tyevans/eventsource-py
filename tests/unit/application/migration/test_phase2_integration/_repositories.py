"""In-memory repository implementations for phase 2 migration integration tests."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING
from uuid import UUID

from eventsource.application.migration.exceptions import (
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationNotFoundError,
)
from eventsource.ports.migration.models import (
    Migration,
    MigrationPhase,
    TenantMigrationState,
    TenantRouting,
)

if TYPE_CHECKING:
    from eventsource.ports import Position


class InMemoryMigrationRepository:
    """In-memory implementation of MigrationRepository for testing."""

    def __init__(self) -> None:
        self._migrations: dict[UUID, Migration] = {}
        self._by_tenant: dict[UUID, UUID] = {}

    async def _get_or_raise(self, migration_id: UUID) -> Migration:
        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)
        return migration

    async def create(self, migration: Migration) -> UUID:
        existing = await self.get_by_tenant(migration.tenant_id)
        if existing is not None:
            raise MigrationAlreadyExistsError(migration.tenant_id, existing.id)
        now = datetime.now(UTC)
        migration.created_at = now
        migration.updated_at = now
        self._migrations[migration.id] = migration
        self._by_tenant[migration.tenant_id] = migration.id
        return migration.id

    async def get(self, migration_id: UUID) -> Migration | None:
        return self._migrations.get(migration_id)

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        migration_id = self._by_tenant.get(tenant_id)
        if migration_id is None:
            return None
        migration = self._migrations.get(migration_id)
        return None if migration is None or migration.phase.is_terminal else migration

    async def update_phase(self, migration_id: UUID, new_phase: MigrationPhase) -> None:
        migration = await self._get_or_raise(migration_id)
        if not migration.can_transition_to(new_phase):
            raise InvalidPhaseTransitionError(migration_id, migration.phase, new_phase)
        now = datetime.now(UTC)
        migration.phase = new_phase
        migration.updated_at = now
        if new_phase == MigrationPhase.BULK_COPY:
            migration.started_at = now
            migration.bulk_copy_started_at = now
        elif new_phase == MigrationPhase.DUAL_WRITE:
            migration.bulk_copy_completed_at = now
            migration.dual_write_started_at = now
        elif new_phase == MigrationPhase.CUTOVER:
            migration.cutover_started_at = now
        elif new_phase in (
            MigrationPhase.COMPLETED,
            MigrationPhase.ABORTED,
            MigrationPhase.FAILED,
        ):
            migration.completed_at = now

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: Position | None,
        last_target_position: Position | None = None,
    ) -> None:
        migration = await self._get_or_raise(migration_id)
        migration.events_copied = events_copied
        migration.last_source_position = last_source_position
        if last_target_position is not None:
            migration.last_target_position = last_target_position
        migration.updated_at = datetime.now(UTC)

    async def set_events_total(self, migration_id: UUID, events_total: int) -> None:
        migration = await self._get_or_raise(migration_id)
        migration.events_total = events_total
        migration.updated_at = datetime.now(UTC)

    async def record_error(self, migration_id: UUID, error: str) -> None:
        migration = await self._get_or_raise(migration_id)
        now = datetime.now(UTC)
        migration.error_count += 1
        migration.last_error = error[:1000]
        migration.last_error_at = now
        migration.updated_at = now

    async def set_paused(self, migration_id: UUID, paused: bool, reason: str | None = None) -> None:
        migration = await self._get_or_raise(migration_id)
        now = datetime.now(UTC)
        migration.is_paused = paused
        migration.paused_at = now if paused else None
        migration.pause_reason = reason if paused else None
        migration.updated_at = now

    async def list_active(self) -> list[Migration]:
        return [m for m in self._migrations.values() if not m.phase.is_terminal]


class InMemoryRoutingRepository:
    """In-memory implementation of TenantRoutingRepository for testing."""

    def __init__(self) -> None:
        self._routing: dict[UUID, TenantRouting] = {}

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        return self._routing.get(tenant_id)

    async def get_or_default(self, tenant_id: UUID, default_store_id: str) -> TenantRouting:
        existing = await self.get_routing(tenant_id)
        if existing is not None:
            return existing
        now = datetime.now(UTC)
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id=default_store_id,
            migration_state=TenantMigrationState.NORMAL,
            created_at=now,
            updated_at=now,
        )
        self._routing[tenant_id] = routing
        return routing

    async def set_routing(self, tenant_id: UUID, store_id: str) -> None:
        now = datetime.now(UTC)
        existing = self._routing.get(tenant_id)
        if existing:
            existing.store_id = store_id
            existing.updated_at = now
        else:
            self._routing[tenant_id] = TenantRouting(
                tenant_id=tenant_id,
                store_id=store_id,
                migration_state=TenantMigrationState.NORMAL,
                created_at=now,
                updated_at=now,
            )

    async def set_migration_state(
        self,
        tenant_id: UUID,
        state: TenantMigrationState,
        migration_id: UUID | None = None,
    ) -> None:
        routing = self._routing.get(tenant_id)
        now = datetime.now(UTC)
        if routing is None:
            self._routing[tenant_id] = TenantRouting(
                tenant_id=tenant_id,
                store_id="default",
                migration_state=state,
                active_migration_id=migration_id,
                created_at=now,
                updated_at=now,
            )
        else:
            routing.migration_state = state
            routing.active_migration_id = migration_id
            routing.updated_at = now

    async def switch_routing(
        self,
        tenant_id: UUID,
        store_id: str,
        state: TenantMigrationState = TenantMigrationState.MIGRATED,
        migration_id: UUID | None = None,
    ) -> None:
        await self.set_routing(tenant_id, store_id)
        await self.set_migration_state(tenant_id, state, migration_id=migration_id)

    async def clear_migration_state(self, tenant_id: UUID) -> None:
        await self.set_migration_state(tenant_id, TenantMigrationState.NORMAL, migration_id=None)

    async def list_by_state(self, state: TenantMigrationState) -> list[TenantRouting]:
        return [r for r in self._routing.values() if r.migration_state == state]

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        return [r for r in self._routing.values() if r.store_id == store_id]


__all__ = ["InMemoryMigrationRepository", "InMemoryRoutingRepository"]
