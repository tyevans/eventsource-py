"""In-memory repository and lock manager implementations for chaos tests."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

from eventsource.application.migration.exceptions import (
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationNotFoundError,
)
from eventsource.ports import Position
from eventsource.ports.exceptions import LockAcquisitionError
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
        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)
        return migration

    async def create(self, migration: Migration) -> UUID:
        existing = await self.get_by_tenant(migration.tenant_id)
        if existing is not None:
            raise MigrationAlreadyExistsError(migration.tenant_id, existing.id)
        migration.created_at = migration.updated_at = datetime.now(UTC)
        self._migrations[migration.id] = migration
        self._by_tenant[migration.tenant_id] = migration.id
        return migration.id

    async def get(self, migration_id: UUID) -> Migration | None:
        return self._migrations.get(migration_id)

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        mid = self._by_tenant.get(tenant_id)
        m = self._migrations.get(mid) if mid else None
        return None if (m is None or m.phase.is_terminal) else m

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
        last_source_position: Position | None,
        last_target_position: Position | None = None,
    ) -> None:
        m = await self._require(migration_id)
        m.events_copied, m.updated_at = events_copied, datetime.now(UTC)
        m.last_source_position = last_source_position
        if last_target_position is not None:
            m.last_target_position = last_target_position

    async def set_events_total(self, migration_id: UUID, events_total: int) -> None:
        m = await self._require(migration_id)
        m.events_total, m.updated_at = events_total, datetime.now(UTC)

    async def record_error(self, migration_id: UUID, error: str) -> None:
        m = await self._require(migration_id)
        m.error_count, m.last_error = m.error_count + 1, error[:1000]
        m.last_error_at = m.updated_at = datetime.now(UTC)

    async def set_paused(self, migration_id: UUID, paused: bool, reason: str | None = None) -> None:
        m = await self._require(migration_id)
        now = datetime.now(UTC)
        m.is_paused, m.updated_at = paused, now
        m.paused_at, m.pause_reason = (now, reason) if paused else (None, None)

    async def list_active(self) -> list[Migration]:
        return [m for m in self._migrations.values() if not m.phase.is_terminal]


class InMemoryRoutingRepository:
    def __init__(self) -> None:
        self._routing: dict[UUID, TenantRouting] = {}

    def _new_routing(
        self,
        tid: UUID,
        sid: str,
        state: TenantMigrationState = TenantMigrationState.NORMAL,
        mid: UUID | None = None,
    ) -> TenantRouting:
        now = datetime.now(UTC)
        return TenantRouting(
            tenant_id=tid,
            store_id=sid,
            migration_state=state,
            active_migration_id=mid,
            created_at=now,
            updated_at=now,
        )

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        return self._routing.get(tenant_id)

    async def get_or_default(self, tenant_id: UUID, default_store_id: str) -> TenantRouting:
        if tenant_id not in self._routing:
            self._routing[tenant_id] = self._new_routing(tenant_id, default_store_id)
        return self._routing[tenant_id]

    async def set_routing(self, tenant_id: UUID, store_id: str) -> None:
        existing = self._routing.get(tenant_id)
        if existing:
            existing.store_id, existing.updated_at = store_id, datetime.now(UTC)
        else:
            self._routing[tenant_id] = self._new_routing(tenant_id, store_id)

    async def set_migration_state(
        self, tid: UUID, state: TenantMigrationState, migration_id: UUID | None = None
    ) -> None:
        routing = self._routing.get(tid)
        if routing is None:
            self._routing[tid] = self._new_routing(tid, "default", state, migration_id)
        else:
            routing.migration_state, routing.active_migration_id = state, migration_id
            routing.updated_at = datetime.now(UTC)

    async def switch_routing(
        self,
        tid: UUID,
        store_id: str,
        state: TenantMigrationState = TenantMigrationState.MIGRATED,
        migration_id: UUID | None = None,
    ) -> None:
        await self.set_routing(tid, store_id)
        await self.set_migration_state(tid, state, migration_id=migration_id)

    async def clear_migration_state(self, tenant_id: UUID) -> None:
        await self.set_migration_state(tenant_id, TenantMigrationState.NORMAL, migration_id=None)

    async def list_by_state(self, state: TenantMigrationState) -> list[TenantRouting]:
        return [r for r in self._routing.values() if r.migration_state == state]

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        return [r for r in self._routing.values() if r.store_id == store_id]


@dataclass
class MockLockInfo:
    key: str
    acquired_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class MockLockManager:
    def __init__(
        self,
        *,
        should_fail: bool = False,
        fail_after: int | None = None,
        contention_delay: float = 0.0,
    ) -> None:
        self._should_fail, self._fail_after = should_fail, fail_after
        self._acquire_count, self._contention_delay = 0, contention_delay
        self._held_locks: dict[str, MockLockInfo] = {}
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self, key: str, timeout: float | None = None) -> AsyncIterator[MockLockInfo]:
        self._acquire_count += 1
        if self._should_fail:
            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure")
        if self._fail_after is not None and self._acquire_count > self._fail_after:
            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure after threshold")
        if self._contention_delay > 0:
            await asyncio.sleep(self._contention_delay)
        async with self._lock:
            if key in self._held_locks:
                raise LockAcquisitionError(key, timeout or 0.0, "Lock already held")
            lock_info = MockLockInfo(key=key)
            self._held_locks[key] = lock_info
        try:
            yield lock_info
        finally:
            async with self._lock:
                self._held_locks.pop(key, None)

    async def try_acquire(self, key: str) -> MockLockInfo | None:
        if self._should_fail:
            return None
        async with self._lock:
            return None if key in self._held_locks else MockLockInfo(key=key)

    async def release(self, key: str) -> bool:
        async with self._lock:
            return self._held_locks.pop(key, None) is not None

    def is_locked(self, key: str) -> bool:
        return key in self._held_locks
