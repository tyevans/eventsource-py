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


class RaisingTargetStore:
    """
    `FullEventStore`-shaped wrapper around `InMemoryEventStore` whose
    `append` always raises.

    Structural conformance only -- delegates the other seven members to
    the inner store and never subclasses the legacy `EventStore` ABC.
    """

    __test__ = False

    def __init__(self, inner: InMemoryEventStore | None = None) -> None:
        self._inner = inner or InMemoryEventStore(event_registry=_REGISTRY)

    async def append(
        self,
        stream: StreamId,
        events: list[DomainEvent],
        expected: ExpectedVersion,
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
    """
    In-memory migration repository for benchmarking.

    This mock implementation stores migrations in memory, enabling
    fast benchmarking without database overhead.
    """

    __test__ = False

    def __init__(self) -> None:
        """Initialize the repository."""
        self._migrations: dict[UUID, Migration] = {}

    async def create(self, migration: Migration) -> None:
        """Create a migration."""
        self._migrations[migration.id] = migration

    async def get(self, migration_id: UUID) -> Migration | None:
        """Get a migration by ID."""
        return self._migrations.get(migration_id)

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        """Get migration by tenant ID."""
        for m in self._migrations.values():
            if m.tenant_id == tenant_id and m.phase not in (
                MigrationPhase.COMPLETED,
                MigrationPhase.ABORTED,
                MigrationPhase.FAILED,
            ):
                return m
        return None

    async def list_active(self) -> list[Migration]:
        """List active migrations."""
        return [
            m
            for m in self._migrations.values()
            if m.phase
            not in (
                MigrationPhase.COMPLETED,
                MigrationPhase.ABORTED,
                MigrationPhase.FAILED,
            )
        ]

    async def set_events_total(self, migration_id: UUID, total: int) -> None:
        """Set total events for migration."""
        migration = self._migrations.get(migration_id)
        if migration:
            migration.events_total = total

    async def update_progress(
        self,
        migration_id: UUID,
        events_copied: int,
        last_source_position: Position | None,
        last_target_position: Position | None = None,
    ) -> None:
        """Update migration progress."""
        migration = self._migrations.get(migration_id)
        if migration:
            migration.events_copied = events_copied
            migration.last_source_position = last_source_position
            migration.last_target_position = last_target_position

    async def update_phase(self, migration_id: UUID, phase: MigrationPhase) -> None:
        """Update migration phase."""
        migration = self._migrations.get(migration_id)
        if migration:
            migration.phase = phase

    async def record_error(self, migration_id: UUID, error: str) -> None:
        """Record an error."""
        migration = self._migrations.get(migration_id)
        if migration:
            migration.error_count += 1
            migration.last_error = error
            migration.last_error_at = datetime.now(UTC)

    async def set_paused(self, migration_id: UUID, paused: bool, reason: str | None = None) -> None:
        """Set paused state."""
        migration = self._migrations.get(migration_id)
        if migration:
            migration.is_paused = paused


class InMemoryPositionMappingRepository:
    """
    In-memory position mapping repository for benchmarking.

    Stores position mappings in memory with efficient lookup structures.
    """

    __test__ = False

    def __init__(self) -> None:
        """Initialize the repository."""
        self._mappings: dict[UUID, list[PositionMapping]] = {}
        self._by_source: dict[UUID, dict[Position, PositionMapping]] = {}
        self._by_target: dict[UUID, dict[Position, PositionMapping]] = {}
        self._by_event: dict[UUID, dict[UUID, PositionMapping]] = {}
        self._id_counter = 0
        # Counts comparisons made by the nearest-position binary search below,
        # mirroring the production PostgreSQL repository's ordinal search
        # (position_mapping.py). Used to assert an O(log n) bound
        # deterministically instead of timing wall-clock latency.
        self.nearest_lookup_comparisons = 0

    async def create(self, mapping: PositionMapping) -> int:
        """Create a position mapping."""
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
        """Create multiple position mappings."""
        count = 0
        for mapping in mappings:
            await self.create(mapping)
            count += 1
        return count

    async def get(self, mapping_id: int) -> PositionMapping | None:
        """Get mapping by ID - not implemented for benchmarks."""
        return None

    async def find_by_source_position(
        self, migration_id: UUID, source_position: Position
    ) -> PositionMapping | None:
        """Find mapping by source position."""
        source_map = self._by_source.get(migration_id, {})
        return source_map.get(source_position)

    async def find_by_target_position(
        self, migration_id: UUID, target_position: Position
    ) -> PositionMapping | None:
        """Find mapping by target position."""
        target_map = self._by_target.get(migration_id, {})
        return target_map.get(target_position)

    async def find_nearest_source_position(
        self, migration_id: UUID, source_position: Position
    ) -> PositionMapping | None:
        """Find nearest mapping with source_position <= given position.

        Binary search over mappings sorted by source_position, mirroring the
        ordinal search the PostgreSQL repository performs against `id` order
        (mappings are recorded in ascending source-position order by a single
        writer). Each iteration counts as one comparison against
        `nearest_lookup_comparisons` so tests can assert an O(log n) bound.
        """
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
        """Find mapping by event ID."""
        event_map = self._by_event.get(migration_id, {})
        return event_map.get(event_id)

    async def list_by_migration(
        self, migration_id: UUID, limit: int = 100, offset: int = 0
    ) -> list[PositionMapping]:
        """List mappings for a migration."""
        mappings = self._mappings.get(migration_id, [])
        return sorted(mappings, key=lambda m: m.source_position)[offset : offset + limit]

    async def list_in_source_range(
        self, migration_id: UUID, start_position: Position, end_position: Position
    ) -> list[PositionMapping]:
        """List mappings in source position range."""
        source_map = self._by_source.get(migration_id, {})
        return [m for p, m in source_map.items() if start_position <= p <= end_position]

    async def count_by_migration(self, migration_id: UUID) -> int:
        """Count mappings for a migration."""
        return len(self._mappings.get(migration_id, []))

    async def get_position_bounds(self, migration_id: UUID) -> tuple[Position, Position] | None:
        """Get min/max source positions."""
        source_map = self._by_source.get(migration_id, {})
        if not source_map:
            return None
        positions = list(source_map.keys())
        return (min(positions), max(positions))

    async def delete_by_migration(self, migration_id: UUID) -> int:
        """Delete all mappings for a migration."""
        count = len(self._mappings.get(migration_id, []))
        self._mappings.pop(migration_id, None)
        self._by_source.pop(migration_id, None)
        self._by_target.pop(migration_id, None)
        self._by_event.pop(migration_id, None)
        return count
