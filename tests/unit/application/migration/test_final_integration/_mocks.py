"""In-memory audit log and position mapping repositories and mock lock manager."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    PositionMapping,
)
from eventsource.ports.positions import Position


class InMemoryAuditLogRepository:
    """In-memory implementation of MigrationAuditLogRepository for testing."""

    def __init__(self) -> None:
        self._entries: list[MigrationAuditEntry] = []
        self._id_counter: int = 0

    async def record(self, entry: MigrationAuditEntry) -> int:
        """Record an audit log entry."""
        self._id_counter += 1
        new_entry = MigrationAuditEntry(
            id=self._id_counter,
            migration_id=entry.migration_id,
            event_type=entry.event_type,
            old_phase=entry.old_phase,
            new_phase=entry.new_phase,
            details=entry.details,
            operator=entry.operator,
            occurred_at=entry.occurred_at,
        )
        self._entries.append(new_entry)
        return self._id_counter

    async def get_by_migration(
        self,
        migration_id: UUID,
        event_types: list[AuditEventType] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[MigrationAuditEntry]:
        """Get audit entries for a migration."""
        filtered = [e for e in self._entries if e.migration_id == migration_id]

        if event_types:
            filtered = [e for e in filtered if e.event_type in event_types]

        if since:
            filtered = [e for e in filtered if e.occurred_at >= since]

        if until:
            filtered = [e for e in filtered if e.occurred_at <= until]

        filtered.sort(key=lambda e: e.occurred_at)

        if limit:
            filtered = filtered[:limit]

        return filtered

    async def get_by_id(self, entry_id: int) -> MigrationAuditEntry | None:
        """Get an audit entry by ID."""
        for entry in self._entries:
            if entry.id == entry_id:
                return entry
        return None

    async def get_latest(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> MigrationAuditEntry | None:
        """Get the most recent audit entry for a migration."""
        filtered = [e for e in self._entries if e.migration_id == migration_id]

        if event_type:
            filtered = [e for e in filtered if e.event_type == event_type]

        if not filtered:
            return None

        filtered.sort(key=lambda e: e.occurred_at, reverse=True)
        return filtered[0]

    async def count_by_migration(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> int:
        """Count audit entries for a migration."""
        filtered = [e for e in self._entries if e.migration_id == migration_id]

        if event_type:
            filtered = [e for e in filtered if e.event_type == event_type]

        return len(filtered)


class InMemoryPositionMappingRepository:
    """In-memory implementation of PositionMappingRepository for testing."""

    def __init__(self) -> None:
        self._mappings: list[PositionMapping] = []
        self._id_counter: int = 0

    async def create(self, mapping: PositionMapping) -> int:
        """Create a new position mapping."""
        self._id_counter += 1
        self._mappings.append(mapping)
        return self._id_counter

    async def create_batch(self, mappings: list[PositionMapping]) -> int:
        """Create multiple position mappings."""
        for mapping in mappings:
            await self.create(mapping)
        return len(mappings)

    async def find_by_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """Find mapping by exact source position."""
        for mapping in self._mappings:
            if mapping.migration_id == migration_id and mapping.source_position == source_position:
                return mapping
        return None

    async def find_nearest_source_position(
        self,
        migration_id: UUID,
        source_position: Position,
    ) -> PositionMapping | None:
        """Find nearest mapping with source_position <= given position."""
        nearest: PositionMapping | None = None
        for mapping in self._mappings:
            if (
                mapping.migration_id == migration_id
                and mapping.source_position <= source_position
                and (nearest is None or mapping.source_position > nearest.source_position)
            ):
                nearest = mapping
        return nearest

    async def find_by_event_id(
        self,
        migration_id: UUID,
        event_id: UUID,
    ) -> PositionMapping | None:
        """Find mapping by event ID."""
        for mapping in self._mappings:
            if mapping.migration_id == migration_id and mapping.event_id == event_id:
                return mapping
        return None

    async def count_by_migration(self, migration_id: UUID) -> int:
        """Count mappings for a migration."""
        return len([m for m in self._mappings if m.migration_id == migration_id])

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[Position, Position] | None:
        """Get first and last source positions by insertion (id) order."""
        filtered = [m for m in self._mappings if m.migration_id == migration_id]
        if not filtered:
            return None
        return (filtered[0].source_position, filtered[-1].source_position)

    async def delete_by_migration(self, migration_id: UUID) -> int:
        """Delete all mappings for a migration."""
        original_count = len(self._mappings)
        self._mappings = [m for m in self._mappings if m.migration_id != migration_id]
        return original_count - len(self._mappings)

    async def list_by_migration(
        self,
        migration_id: UUID,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PositionMapping]:
        """List mappings for a migration with pagination."""
        filtered = [m for m in self._mappings if m.migration_id == migration_id]
        sorted_mappings = sorted(filtered, key=lambda m: m.source_position)
        return sorted_mappings[offset : offset + limit]


@dataclass
class MockLockInfo:
    """Mock lock info for testing."""

    key: str
    acquired_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class MockLockManager:
    """Mock implementation of LockManager for testing."""

    def __init__(
        self,
        *,
        should_fail: bool = False,
        fail_after: int | None = None,
    ) -> None:
        self._should_fail = should_fail
        self._fail_after = fail_after
        self._acquire_count = 0
        self._held_locks: dict[str, MockLockInfo] = {}

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        timeout: float | None = None,
    ) -> AsyncIterator[MockLockInfo]:
        """Acquire advisory lock."""
        self._acquire_count += 1

        if self._should_fail:
            from eventsource.ports.exceptions import LockAcquisitionError

            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure")

        if self._fail_after is not None and self._acquire_count > self._fail_after:
            from eventsource.ports.exceptions import LockAcquisitionError

            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure after threshold")

        lock_info = MockLockInfo(key=key)
        self._held_locks[key] = lock_info
        try:
            yield lock_info
        finally:
            self._held_locks.pop(key, None)

    async def try_acquire(self, key: str) -> MockLockInfo | None:
        """Try to acquire lock without blocking."""
        if self._should_fail:
            return None

        if key in self._held_locks:
            return None

        return MockLockInfo(key=key)

    async def release(self, key: str) -> bool:
        """Release advisory lock."""
        if key in self._held_locks:
            del self._held_locks[key]
            return True
        return False


__all__ = [  # pragma: allowlist secret
    "InMemoryAuditLogRepository",
    "InMemoryPositionMappingRepository",
    "MockLockInfo",
    "MockLockManager",
]
