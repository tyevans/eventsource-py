"""In-memory migration, routing, and checkpoint repositories for unit testing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
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


class InMemoryMigrationRepository:
    def __init__(self) -> None:
        self._migrations: dict[UUID, Migration] = {}
        self._by_tenant: dict[UUID, UUID] = {}

    async def _require(self, migration_id: UUID) -> Migration:
        if (migration := await self.get(migration_id)) is None:
            raise MigrationNotFoundError(migration_id)
        return migration

    async def create(self, migration: Migration) -> UUID:
        if (existing := await self.get_by_tenant(migration.tenant_id)) is not None:
            raise MigrationAlreadyExistsError(migration.tenant_id, existing.id)
        migration.created_at = migration.updated_at = datetime.now(UTC)
        self._migrations[migration.id] = migration
        self._by_tenant[migration.tenant_id] = migration.id
        return migration.id

    async def get(self, migration_id: UUID) -> Migration | None:
        return self._migrations.get(migration_id)

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        migration_id = self._by_tenant.get(tenant_id)
        migration = self._migrations.get(migration_id) if migration_id else None
        return None if (migration is None or migration.phase.is_terminal) else migration

    async def update_phase(self, migration_id: UUID, new_phase: MigrationPhase) -> None:
        migration = await self._require(migration_id)
        if not migration.can_transition_to(new_phase):
            raise InvalidPhaseTransitionError(migration_id, migration.phase, new_phase)
        now = datetime.now(UTC)
        migration.phase, migration.updated_at = new_phase, now
        if new_phase == MigrationPhase.BULK_COPY:
            migration.started_at = migration.bulk_copy_started_at = now
        elif new_phase == MigrationPhase.DUAL_WRITE:
            migration.bulk_copy_completed_at = migration.dual_write_started_at = now
        elif new_phase == MigrationPhase.CUTOVER:
            migration.cutover_started_at = now
        elif new_phase.is_terminal:
            migration.completed_at = now

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: int,
        last_target_position: int | None = None,
    ) -> None:
        migration = await self._require(migration_id)
        migration.events_copied = events_copied
        migration.last_source_position = last_source_position
        if last_target_position is not None:
            migration.last_target_position = last_target_position
        migration.updated_at = datetime.now(UTC)

    async def set_events_total(self, migration_id: UUID, events_total: int) -> None:
        migration = await self._require(migration_id)
        migration.events_total, migration.updated_at = events_total, datetime.now(UTC)

    async def record_error(self, migration_id: UUID, error: str) -> None:
        migration = await self._require(migration_id)
        migration.error_count += 1
        migration.last_error = error[:1000]
        migration.last_error_at = migration.updated_at = datetime.now(UTC)

    async def set_paused(self, migration_id: UUID, paused: bool, reason: str | None = None) -> None:
        migration = await self._require(migration_id)
        now = datetime.now(UTC)
        migration.is_paused, migration.updated_at = paused, now
        migration.paused_at, migration.pause_reason = (now, reason) if paused else (None, None)

    async def list_active(self) -> list[Migration]:
        return [m for m in self._migrations.values() if not m.phase.is_terminal]


class InMemoryRoutingRepository:
    def __init__(self) -> None:
        self._routing: dict[UUID, TenantRouting] = {}

    def _new_routing(
        self,
        tenant_id: UUID,
        store_id: str,
        state: TenantMigrationState = TenantMigrationState.NORMAL,
        migration_id: UUID | None = None,
    ) -> TenantRouting:
        now = datetime.now(UTC)
        routing = TenantRouting(
            tenant_id=tenant_id,
            store_id=store_id,
            migration_state=state,
            active_migration_id=migration_id,
            created_at=now,
            updated_at=now,
        )
        self._routing[tenant_id] = routing
        return routing

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        return self._routing.get(tenant_id)

    async def get_or_default(self, tenant_id: UUID, default_store_id: str) -> TenantRouting:
        existing = await self.get_routing(tenant_id)
        return existing if existing is not None else self._new_routing(tenant_id, default_store_id)

    async def set_routing(self, tenant_id: UUID, store_id: str) -> None:
        if existing := self._routing.get(tenant_id):
            existing.store_id, existing.updated_at = store_id, datetime.now(UTC)
        else:
            self._new_routing(tenant_id, store_id)

    async def set_migration_state(
        self, tenant_id: UUID, state: TenantMigrationState, migration_id: UUID | None = None
    ) -> None:
        if (routing := self._routing.get(tenant_id)) is None:
            self._new_routing(tenant_id, "default", state=state, migration_id=migration_id)
        else:
            routing.migration_state, routing.active_migration_id = state, migration_id
            routing.updated_at = datetime.now(UTC)

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


@dataclass
class _CheckpointData:
    projection_name: str
    position: int
    last_event_id: UUID | None = None
    last_event_type: str | None = None


class InMemoryCheckpointRepository:
    def __init__(self) -> None:
        self._checkpoints: dict[str, _CheckpointData] = {}

    async def get_position(self, subscription_id: str) -> int | None:
        return cp.position if (cp := self._checkpoints.get(subscription_id)) else None

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID | None = None,
        event_type: str | None = None,
    ) -> None:
        self._checkpoints[subscription_id] = _CheckpointData(
            subscription_id, position, event_id, event_type
        )

    async def get_all_checkpoints(self) -> list[Any]:
        return list(self._checkpoints.values())
