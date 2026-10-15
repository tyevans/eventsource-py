"""Mock lock manager and in-memory position mapping repository for Phase 3 tests."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID

from eventsource.ports.exceptions import LockAcquisitionError
from eventsource.ports.migration.models import PositionMapping
from eventsource.ports.positions import Position


@dataclass
class MockLockInfo:
    """Mock lock info for testing."""

    key: str
    acquired_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class MockLockManager:
    """Mock implementation of PostgreSQLLockManager for testing."""

    def __init__(self, *, should_fail: bool = False, fail_after: int | None = None) -> None:
        self._should_fail = should_fail
        self._fail_after = fail_after
        self._acquire_count = 0
        self._held_locks: dict[str, MockLockInfo] = {}

    @asynccontextmanager
    async def acquire(self, key: str, timeout: float | None = None) -> AsyncIterator[MockLockInfo]:
        self._acquire_count += 1
        if self._should_fail:
            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure")
        if self._fail_after is not None and self._acquire_count > self._fail_after:
            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure after threshold")
        lock_info = MockLockInfo(key=key)
        self._held_locks[key] = lock_info
        try:
            yield lock_info
        finally:
            self._held_locks.pop(key, None)

    async def try_acquire(self, key: str) -> MockLockInfo | None:
        if self._should_fail or key in self._held_locks:
            return None
        return MockLockInfo(key=key)

    async def release(self, key: str) -> bool:
        if key in self._held_locks:
            del self._held_locks[key]
            return True
        return False


class InMemoryPositionMappingRepository:
    """In-memory implementation of PositionMappingRepository for testing."""

    def __init__(self) -> None:
        self._mappings: list[PositionMapping] = []
        self._id_counter: int = 0

    async def create(self, mapping: PositionMapping) -> int:
        self._id_counter += 1
        self._mappings.append(mapping)
        return self._id_counter

    async def create_batch(self, mappings: list[PositionMapping]) -> int:
        for mapping in mappings:
            await self.create(mapping)
        return len(mappings)

    async def get(self, mapping_id: int) -> PositionMapping | None:
        if 0 < mapping_id <= len(self._mappings):
            return self._mappings[mapping_id - 1]
        return None

    async def find_by_source_position(
        self, migration_id: UUID, source_position: Position
    ) -> PositionMapping | None:
        for mapping in self._mappings:
            if mapping.migration_id == migration_id and mapping.source_position == source_position:
                return mapping
        return None

    async def find_by_target_position(
        self, migration_id: UUID, target_position: Position
    ) -> PositionMapping | None:
        for mapping in self._mappings:
            if mapping.migration_id == migration_id and mapping.target_position == target_position:
                return mapping
        return None

    async def find_nearest_source_position(
        self, migration_id: UUID, source_position: Position
    ) -> PositionMapping | None:
        nearest: PositionMapping | None = None
        for mapping in self._mappings:
            if (
                mapping.migration_id == migration_id
                and mapping.source_position <= source_position
                and (nearest is None or mapping.source_position > nearest.source_position)
            ):
                nearest = mapping
        return nearest

    async def find_by_event_id(self, migration_id: UUID, event_id: UUID) -> PositionMapping | None:
        for mapping in self._mappings:
            if mapping.migration_id == migration_id and mapping.event_id == event_id:
                return mapping
        return None

    async def list_by_migration(
        self, migration_id: UUID, limit: int = 100, offset: int = 0
    ) -> list[PositionMapping]:
        filtered = [m for m in self._mappings if m.migration_id == migration_id]
        return sorted(filtered, key=lambda m: m.source_position)[offset : offset + limit]

    async def list_in_source_range(
        self, migration_id: UUID, start_position: Position, end_position: Position
    ) -> list[PositionMapping]:
        filtered = [
            m
            for m in self._mappings
            if m.migration_id == migration_id
            and start_position <= m.source_position <= end_position
        ]
        return sorted(filtered, key=lambda m: m.source_position)

    async def count_by_migration(self, migration_id: UUID) -> int:
        return len([m for m in self._mappings if m.migration_id == migration_id])

    async def get_position_bounds(self, migration_id: UUID) -> tuple[Position, Position] | None:
        filtered = [m for m in self._mappings if m.migration_id == migration_id]
        return (filtered[0].source_position, filtered[-1].source_position) if filtered else None

    async def delete_by_migration(self, migration_id: UUID) -> int:
        original_count = len(self._mappings)
        self._mappings = [m for m in self._mappings if m.migration_id != migration_id]
        return original_count - len(self._mappings)


__all__ = [  # pragma: allowlist secret
    "InMemoryPositionMappingRepository",
    "MockLockInfo",
    "MockLockManager",
]
