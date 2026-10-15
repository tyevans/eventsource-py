"""In-memory migration, routing, and checkpoint repositories for Phase 3 integration testing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
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
from eventsource.ports.positions import Position


@dataclass
class _CheckpointData:
    projection_name: str
    position: Position
    last_event_id: UUID | None = None
    last_event_type: str | None = None


class InMemoryCheckpointRepository:
    def __init__(self) -> None:
        self._checkpoints: dict[str, _CheckpointData] = {}

    async def get_position(self, subscription_id: str) -> Position | None:
        cp = self._checkpoints.get(subscription_id)
        return cp.position if cp else None

    async def save_position(
        self,
        subscription_id: str,
        position: Position,
        event_id: UUID | None = None,
        event_type: str | None = None,
    ) -> None:
        self._checkpoints[subscription_id] = _CheckpointData(
            subscription_id, position, event_id, event_type
        )

    async def get_all_checkpoints(self) -> list[_CheckpointData]:
        return list(self._checkpoints.values())


class InMemoryMigrationRepository:
    def __init__(self) -> None:
        self._migrations: dict[UUID, Migration] = {}
        self._by_tenant: dict[UUID, UUID] = {}

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
        m = self._migrations.get(self._by_tenant.get(tenant_id))  # type: ignore[arg-type]
        return None if (m is None or m.phase.is_terminal) else m

    async def _require(self, migration_id: UUID) -> Migration:
        if (migration := await self.get(migration_id)) is None:
            raise MigrationNotFoundError(migration_id)
        return migration

    async def update_phase(self, migration_id: UUID, new_phase: MigrationPhase) -> None:
        m = await self._require(migration_id)
        if not m.can_transition_to(new_phase):
            raise InvalidPhaseTransitionError(migration_id, m.phase, new_phase)
        now = datetime.now(UTC)
        m.phase, m.updated_at = new_phase, now
        if new_phase == MigrationPhase.BULK_COPY:
            m.started_at = m.bulk_copy_started_at = now
        elif new_phase == MigrationPhase.DUAL_WRITE:
            m.bulk_copy_completed_at = m.dual_write_started_at = now
        elif new_phase == MigrationPhase.CUTOVER:
            m.cutover_started_at = now
        elif new_phase.is_terminal:
            m.completed_at = now

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: int,
        last_target_position: int | None = None,
    ) -> None:
        m = await self._require(migration_id)
        m.events_copied, m.last_source_position = events_copied, last_source_position
        if last_target_position is not None:
            m.last_target_position = last_target_position
        m.updated_at = datetime.now(UTC)

    async def set_events_total(self, migration_id: UUID, events_total: int) -> None:
        m = await self._require(migration_id)
        m.events_total, m.updated_at = events_total, datetime.now(UTC)

    async def record_error(self, migration_id: UUID, error: str) -> None:
        m, now = await self._require(migration_id), datetime.now(UTC)
        m.error_count += 1
        m.last_error, m.last_error_at, m.updated_at = error[:1000], now, now

    async def set_paused(self, migration_id: UUID, paused: bool, reason: str | None = None) -> None:
        m, now = await self._require(migration_id), datetime.now(UTC)
        m.is_paused, m.updated_at = paused, now
        m.paused_at, m.pause_reason = (now, reason) if paused else (None, None)

    async def list_active(self) -> list[Migration]:
        return [m for m in self._migrations.values() if not m.phase.is_terminal]


class InMemoryRoutingRepository:
    def __init__(self) -> None:
        self._routing: dict[UUID, TenantRouting] = {}

    def _new(
        self,
        tenant_id: UUID,
        store_id: str,
        state: TenantMigrationState = TenantMigrationState.NORMAL,
        migration_id: UUID | None = None,
    ) -> TenantRouting:
        now = datetime.now(UTC)
        return TenantRouting(tenant_id, store_id, state, migration_id, now, now)

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        return self._routing.get(tenant_id)

    async def get_or_default(self, tenant_id: UUID, default_store_id: str) -> TenantRouting:
        if (existing := await self.get_routing(tenant_id)) is not None:
            return existing
        self._routing[tenant_id] = self._new(tenant_id, default_store_id)
        return self._routing[tenant_id]

    async def set_routing(self, tenant_id: UUID, store_id: str) -> None:
        if existing := self._routing.get(tenant_id):
            existing.store_id, existing.updated_at = store_id, datetime.now(UTC)
        else:
            self._routing[tenant_id] = self._new(tenant_id, store_id)

    async def set_migration_state(
        self, tenant_id: UUID, state: TenantMigrationState, migration_id: UUID | None = None
    ) -> None:
        if (routing := self._routing.get(tenant_id)) is None:
            self._routing[tenant_id] = self._new(tenant_id, "default", state, migration_id)
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


__all__ = [  # pragma: allowlist secret
    "InMemoryCheckpointRepository",
    "InMemoryMigrationRepository",
    "InMemoryRoutingRepository",
]
