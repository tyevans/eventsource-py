"""Mock and composition repository implementations for load benchmarks."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import AppendResult, ExpectedVersion
from eventsource.ports.migration.models import Migration, MigrationPhase, PositionMapping
from eventsource.ports.positions import Position
from tests.unit.application.migration.test_load_benchmarks._fixtures import _REGISTRY

_TERMINAL_PHASES = (MigrationPhase.COMPLETED, MigrationPhase.ABORTED, MigrationPhase.FAILED)


class RaisingTargetStore:
    """FullEventStore-shaped wrapper around InMemoryEventStore whose append always raises."""

    __test__ = False

    def __init__(self, inner: InMemoryEventStore | None = None) -> None:
        self._inner = inner or InMemoryEventStore(event_registry=_REGISTRY)

    async def append(
        self, stream: StreamId, events: list[DomainEvent], expected: ExpectedVersion
    ) -> AppendResult:
        raise Exception("Simulated target failure")

    def read_stream(self, stream: StreamId, options=None):
        return self._inner.read_stream(stream, options)

    async def get_stream_version(self, stream: StreamId) -> int:
        return await self._inner.get_stream_version(stream)

    async def event_exists(self, event_id: UUID) -> bool:
        return await self._inner.event_exists(event_id)

    def read_all(self, from_position: Position | None = None, options=None):
        return self._inner.read_all(from_position, options)

    async def current_position(self) -> Position | None:
        return await self._inner.current_position()

    def read_category(self, category: str, options=None):
        return self._inner.read_category(category, options)


class InMemoryMigrationRepository:
    """In-memory migration repository for benchmarking."""

    __test__ = False

    def __init__(self) -> None:
        self._migrations: dict[UUID, Migration] = {}

    async def create(self, migration: Migration) -> None:
        self._migrations[migration.id] = migration

    async def get(self, migration_id: UUID) -> Migration | None:
        return self._migrations.get(migration_id)

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        for m in self._migrations.values():
            if m.tenant_id == tenant_id and m.phase not in _TERMINAL_PHASES:
                return m
        return None

    async def list_active(self) -> list[Migration]:
        return [m for m in self._migrations.values() if m.phase not in _TERMINAL_PHASES]

    async def set_events_total(self, migration_id: UUID, total: int) -> None:
        if migration := self._migrations.get(migration_id):
            migration.events_total = total

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: Position | None,
        last_target_position: Position | None = None,
    ) -> None:
        if migration := self._migrations.get(migration_id):
            migration.events_copied = events_copied
            migration.last_source_position = last_source_position
            migration.last_target_position = last_target_position

    async def update_phase(self, migration_id: UUID, phase: MigrationPhase) -> None:
        if migration := self._migrations.get(migration_id):
            migration.phase = phase

    async def record_error(self, migration_id: UUID, error: str) -> None:
        if migration := self._migrations.get(migration_id):
            migration.error_count += 1
            migration.last_error = error
            migration.last_error_at = datetime.now(UTC)

    async def set_paused(self, migration_id: UUID, paused: bool, reason: str | None = None) -> None:
        if migration := self._migrations.get(migration_id):
            migration.is_paused = paused


class InMemoryPositionMappingRepository:
    """In-memory position mapping repository for benchmarking."""

    __test__ = False

    def __init__(self) -> None:
        self._mappings: dict[UUID, list[PositionMapping]] = {}
        self._by_source: dict[UUID, dict[Position, PositionMapping]] = {}
        self._by_target: dict[UUID, dict[Position, PositionMapping]] = {}
        self._by_event: dict[UUID, dict[UUID, PositionMapping]] = {}
        self._id_counter = 0
        self.nearest_lookup_comparisons = 0

    async def create(self, mapping: PositionMapping) -> int:
        if mapping.migration_id not in self._mappings:
            self._mappings[mapping.migration_id] = []
            self._by_source[mapping.migration_id] = {}
            self._by_target[mapping.migration_id] = {}
            self._by_event[mapping.migration_id] = {}

        self._mappings[mapping.migration_id].append(mapping)
        self._by_source[mapping.migration_id][mapping.source_position] = mapping
        self._by_target[mapping.migration_id][mapping.target_position] = mapping
        self._by_event[mapping.migration_id][mapping.event_id] = mapping

        self._id_counter += 1
        return self._id_counter

    async def create_batch(self, mappings: list[PositionMapping]) -> int:
        for mapping in mappings:
            await self.create(mapping)
        return len(mappings)

    async def get(self, mapping_id: int) -> PositionMapping | None:
        return None

    async def find_by_source_position(
        self, migration_id: UUID, source_position: Position
    ) -> PositionMapping | None:
        return self._by_source.get(migration_id, {}).get(source_position)

    async def find_by_target_position(
        self, migration_id: UUID, target_position: Position
    ) -> PositionMapping | None:
        return self._by_target.get(migration_id, {}).get(target_position)

    async def find_nearest_source_position(
        self, migration_id: UUID, source_position: Position
    ) -> PositionMapping | None:
        """Find nearest mapping with source_position <= given position via binary search."""
        mappings = sorted(self._mappings.get(migration_id, []), key=lambda m: m.source_position)
        if not mappings:
            return None

        lo, hi = 0, len(mappings) - 1
        best: PositionMapping | None = None
        while lo <= hi:
            self.nearest_lookup_comparisons += 1
            mid = (lo + hi) // 2
            candidate = mappings[mid]
            if candidate.source_position <= source_position:
                best = candidate
                lo = mid + 1
            else:
                hi = mid - 1
        return best

    async def find_by_event_id(self, migration_id: UUID, event_id: UUID) -> PositionMapping | None:
        return self._by_event.get(migration_id, {}).get(event_id)

    async def list_by_migration(
        self, migration_id: UUID, limit: int = 100, offset: int = 0
    ) -> list[PositionMapping]:
        mappings = self._mappings.get(migration_id, [])
        return sorted(mappings, key=lambda m: m.source_position)[offset : offset + limit]

    async def list_in_source_range(
        self, migration_id: UUID, start_position: Position, end_position: Position
    ) -> list[PositionMapping]:
        source_map = self._by_source.get(migration_id, {})
        return [m for p, m in source_map.items() if start_position <= p <= end_position]

    async def count_by_migration(self, migration_id: UUID) -> int:
        return len(self._mappings.get(migration_id, []))

    async def get_position_bounds(self, migration_id: UUID) -> tuple[Position, Position] | None:
        if not (source_map := self._by_source.get(migration_id, {})):
            return None
        positions = list(source_map.keys())
        return (min(positions), max(positions))

    async def delete_by_migration(self, migration_id: UUID) -> int:
        count = len(self._mappings.get(migration_id, []))
        self._mappings.pop(migration_id, None)
        self._by_source.pop(migration_id, None)
        self._by_target.pop(migration_id, None)
        self._by_event.pop(migration_id, None)
        return count
