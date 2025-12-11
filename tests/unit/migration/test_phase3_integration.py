"""
Phase 3 Integration Tests for Multi-Tenant Live Migration.

Tests cover:
- Position mapping recording and translation during bulk copy
- Consistency verification at different levels (COUNT, HASH, FULL)
- Consistency verification with sampling
- Subscription checkpoint migration after cutover
- Full migration lifecycle with verification and subscription migration enabled
- Error scenarios: missing mappings, verification failures
- Dry-run subscription migration

These tests use InMemoryEventStore for unit-level integration testing
without requiring PostgreSQL.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
)
from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.exceptions import (
    MigrationError,
    MigrationNotFoundError,
    PositionMappingError,
)
from eventsource.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
    PositionMapping,
    TenantMigrationState,
    TenantRouting,
)
from eventsource.migration.position_mapper import (
    PositionMapper,
)
from eventsource.migration.router import TenantStoreRouter
from eventsource.migration.subscription_migrator import (
    SubscriptionMigrator,
)
from eventsource.migration.write_pause import WritePauseManager
from eventsource.stores.in_memory import InMemoryEventStore

# =============================================================================
# Test Event Classes
# =============================================================================


class SampleTestEvent(DomainEvent):
    """Generic test event."""

    event_type: str = "SampleTestEvent"
    aggregate_type: str = "SampleAggregate"
    value: str = "test"


class OrderCreated(DomainEvent):
    """Test order created event."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str = "customer-123"
    amount: float = 100.0


class OrderUpdated(DomainEvent):
    """Test order updated event."""

    event_type: str = "OrderUpdated"
    aggregate_type: str = "Order"
    new_amount: float = 150.0


# =============================================================================
# In-Memory Repository Implementations
# =============================================================================


class InMemoryPositionMappingRepository:
    """
    In-memory implementation of PositionMappingRepository for testing.

    Stores mappings in a dictionary and implements all protocol methods.
    """

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

    async def get(self, mapping_id: int) -> PositionMapping | None:
        """Get a mapping by ID."""
        if 0 < mapping_id <= len(self._mappings):
            return self._mappings[mapping_id - 1]
        return None

    async def find_by_source_position(
        self,
        migration_id: UUID,
        source_position: int,
    ) -> PositionMapping | None:
        """Find mapping by exact source position."""
        for mapping in self._mappings:
            if mapping.migration_id == migration_id and mapping.source_position == source_position:
                return mapping
        return None

    async def find_by_target_position(
        self,
        migration_id: UUID,
        target_position: int,
    ) -> PositionMapping | None:
        """Find mapping by exact target position."""
        for mapping in self._mappings:
            if mapping.migration_id == migration_id and mapping.target_position == target_position:
                return mapping
        return None

    async def find_nearest_source_position(
        self,
        migration_id: UUID,
        source_position: int,
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

    async def list_in_source_range(
        self,
        migration_id: UUID,
        start_position: int,
        end_position: int,
    ) -> list[PositionMapping]:
        """List mappings within a source position range."""
        filtered = [
            m
            for m in self._mappings
            if m.migration_id == migration_id
            and start_position <= m.source_position <= end_position
        ]
        return sorted(filtered, key=lambda m: m.source_position)

    async def count_by_migration(self, migration_id: UUID) -> int:
        """Count mappings for a migration."""
        return len([m for m in self._mappings if m.migration_id == migration_id])

    async def get_position_bounds(
        self,
        migration_id: UUID,
    ) -> tuple[int, int] | None:
        """Get min and max source positions."""
        filtered = [m for m in self._mappings if m.migration_id == migration_id]
        if not filtered:
            return None
        positions = [m.source_position for m in filtered]
        return (min(positions), max(positions))

    async def delete_by_migration(self, migration_id: UUID) -> int:
        """Delete all mappings for a migration."""
        original_count = len(self._mappings)
        self._mappings = [m for m in self._mappings if m.migration_id != migration_id]
        return original_count - len(self._mappings)


class InMemoryCheckpointRepository:
    """
    In-memory implementation of CheckpointRepository for testing.

    Stores checkpoints in a dictionary.
    """

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict] = {}

    async def get_position(self, subscription_id: str) -> int | None:
        """Get checkpoint position for subscription."""
        checkpoint = self._checkpoints.get(subscription_id)
        if checkpoint:
            return checkpoint.get("position")
        return None

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID | None = None,
        event_type: str | None = None,
    ) -> None:
        """Save checkpoint position."""
        self._checkpoints[subscription_id] = {
            "projection_name": subscription_id,
            "position": position,
            "last_event_id": event_id,
            "last_event_type": event_type,
            "updated_at": datetime.now(UTC),
        }

    async def get_all_checkpoints(self) -> list:
        """Get all checkpoints."""

        @dataclass
        class CheckpointData:
            projection_name: str
            position: int
            last_event_id: UUID | None
            last_event_type: str | None

        return [
            CheckpointData(
                projection_name=data["projection_name"],
                position=data["position"],
                last_event_id=data.get("last_event_id"),
                last_event_type=data.get("last_event_type"),
            )
            for data in self._checkpoints.values()
        ]


class InMemoryMigrationRepository:
    """
    In-memory implementation of MigrationRepository for testing.

    Stores migrations in a dictionary and implements all protocol methods.
    """

    def __init__(self) -> None:
        self._migrations: dict[UUID, Migration] = {}
        self._by_tenant: dict[UUID, UUID] = {}

    async def create(self, migration: Migration) -> UUID:
        """Create a new migration record."""
        from eventsource.migration.exceptions import MigrationAlreadyExistsError

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
        """Get a migration by ID."""
        return self._migrations.get(migration_id)

    async def get_by_tenant(self, tenant_id: UUID) -> Migration | None:
        """Get the active migration for a tenant."""
        migration_id = self._by_tenant.get(tenant_id)
        if migration_id is None:
            return None

        migration = self._migrations.get(migration_id)
        if migration is None:
            return None

        if migration.phase.is_terminal:
            return None

        return migration

    async def update_phase(
        self,
        migration_id: UUID,
        new_phase: MigrationPhase,
    ) -> None:
        """Update migration phase."""
        from eventsource.migration.exceptions import (
            InvalidPhaseTransitionError,
            MigrationNotFoundError,
        )

        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

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
        last_source_position: int,
        last_target_position: int | None = None,
    ) -> None:
        """Update bulk copy progress."""
        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        migration.events_copied = events_copied
        migration.last_source_position = last_source_position
        if last_target_position is not None:
            migration.last_target_position = last_target_position
        migration.updated_at = datetime.now(UTC)

    async def set_events_total(
        self,
        migration_id: UUID,
        events_total: int,
    ) -> None:
        """Set the total events count."""
        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        migration.events_total = events_total
        migration.updated_at = datetime.now(UTC)

    async def record_error(
        self,
        migration_id: UUID,
        error: str,
    ) -> None:
        """Record an error occurrence."""
        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        now = datetime.now(UTC)
        migration.error_count += 1
        migration.last_error = error[:1000]
        migration.last_error_at = now
        migration.updated_at = now

    async def set_paused(
        self,
        migration_id: UUID,
        paused: bool,
        reason: str | None = None,
    ) -> None:
        """Set migration pause state."""
        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        now = datetime.now(UTC)
        migration.is_paused = paused
        if paused:
            migration.paused_at = now
            migration.pause_reason = reason
        else:
            migration.paused_at = None
            migration.pause_reason = None
        migration.updated_at = now

    async def list_active(self) -> list[Migration]:
        """List all active migrations."""
        return [m for m in self._migrations.values() if not m.phase.is_terminal]


class InMemoryRoutingRepository:
    """
    In-memory implementation of TenantRoutingRepository for testing.

    Stores routing entries in a dictionary.
    """

    def __init__(self) -> None:
        self._routing: dict[UUID, TenantRouting] = {}

    async def get_routing(self, tenant_id: UUID) -> TenantRouting | None:
        """Get routing configuration for a tenant."""
        return self._routing.get(tenant_id)

    async def get_or_default(
        self,
        tenant_id: UUID,
        default_store_id: str,
    ) -> TenantRouting:
        """Get routing configuration, creating default if not exists."""
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

    async def set_routing(
        self,
        tenant_id: UUID,
        store_id: str,
    ) -> None:
        """Set or update the store for a tenant."""
        now = datetime.now(UTC)
        existing = self._routing.get(tenant_id)

        if existing:
            existing.store_id = store_id
            existing.updated_at = now
        else:
            routing = TenantRouting(
                tenant_id=tenant_id,
                store_id=store_id,
                migration_state=TenantMigrationState.NORMAL,
                created_at=now,
                updated_at=now,
            )
            self._routing[tenant_id] = routing

    async def set_migration_state(
        self,
        tenant_id: UUID,
        state: TenantMigrationState,
        migration_id: UUID | None = None,
    ) -> None:
        """Update the migration state for routing decisions."""
        routing = self._routing.get(tenant_id)
        if routing is None:
            now = datetime.now(UTC)
            routing = TenantRouting(
                tenant_id=tenant_id,
                store_id="default",
                migration_state=state,
                active_migration_id=migration_id,
                created_at=now,
                updated_at=now,
            )
            self._routing[tenant_id] = routing
        else:
            routing.migration_state = state
            routing.active_migration_id = migration_id
            routing.updated_at = datetime.now(UTC)

    async def clear_migration_state(self, tenant_id: UUID) -> None:
        """Reset migration state to NORMAL."""
        await self.set_migration_state(
            tenant_id,
            TenantMigrationState.NORMAL,
            migration_id=None,
        )

    async def list_by_state(
        self,
        state: TenantMigrationState,
    ) -> list[TenantRouting]:
        """List tenants in a specific migration state."""
        return [r for r in self._routing.values() if r.migration_state == state]

    async def list_by_store(self, store_id: str) -> list[TenantRouting]:
        """List tenants routed to a specific store."""
        return [r for r in self._routing.values() if r.store_id == store_id]


@dataclass
class MockLockInfo:
    """Mock lock info for testing."""

    key: str
    acquired_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class MockLockManager:
    """
    Mock implementation of PostgreSQLLockManager for testing.

    Can be configured to succeed or fail lock acquisition.
    """

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
            from eventsource.locks import LockAcquisitionError

            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure")

        if self._fail_after is not None and self._acquire_count > self._fail_after:
            from eventsource.locks import LockAcquisitionError

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


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def source_store() -> InMemoryEventStore:
    """Create source (shared) event store."""
    return InMemoryEventStore(enable_tracing=False)


@pytest.fixture
def target_store() -> InMemoryEventStore:
    """Create target (dedicated) event store."""
    return InMemoryEventStore(enable_tracing=False)


@pytest.fixture
def migration_repo() -> InMemoryMigrationRepository:
    """Create migration repository."""
    return InMemoryMigrationRepository()


@pytest.fixture
def routing_repo() -> InMemoryRoutingRepository:
    """Create routing repository."""
    return InMemoryRoutingRepository()


@pytest.fixture
def position_mapping_repo() -> InMemoryPositionMappingRepository:
    """Create position mapping repository."""
    return InMemoryPositionMappingRepository()


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """Create checkpoint repository."""
    return InMemoryCheckpointRepository()


@pytest.fixture
def lock_manager() -> MockLockManager:
    """Create mock lock manager."""
    return MockLockManager()


@pytest.fixture
def write_pause_manager() -> WritePauseManager:
    """Create write pause manager."""
    return WritePauseManager(default_timeout=5.0)


@pytest.fixture
def tenant_id() -> UUID:
    """Create a test tenant ID."""
    return uuid4()


@pytest.fixture
def migration_id() -> UUID:
    """Create a test migration ID."""
    return uuid4()


@pytest.fixture
def router(
    source_store: InMemoryEventStore,
    routing_repo: InMemoryRoutingRepository,
    write_pause_manager: WritePauseManager,
) -> TenantStoreRouter:
    """Create tenant store router."""
    return TenantStoreRouter(
        default_store=source_store,
        routing_repo=routing_repo,
        stores={"default": source_store},
        default_store_id="default",
        enable_tracing=False,
        write_pause_manager=write_pause_manager,
    )


@pytest.fixture
def position_mapper(
    position_mapping_repo: InMemoryPositionMappingRepository,
) -> PositionMapper:
    """Create position mapper."""
    return PositionMapper(position_mapping_repo, enable_tracing=False)


# =============================================================================
# Helper Functions
# =============================================================================


async def create_test_events(
    store: InMemoryEventStore,
    tenant_id: UUID,
    count: int = 10,
    aggregate_type: str = "Order",
) -> list[UUID]:
    """Create test events in the store and return aggregate IDs."""
    aggregate_ids = []

    for i in range(count):
        aggregate_id = uuid4()
        aggregate_ids.append(aggregate_id)

        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            customer_id=f"customer-{i}",
            amount=100.0 + i,
        )

        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=[event],
            expected_version=0,
        )

    return aggregate_ids


async def get_all_tenant_events(
    store: InMemoryEventStore,
    tenant_id: UUID,
) -> list[DomainEvent]:
    """Get all events for a tenant from a store."""
    events = []
    async for stored_event in store.read_all():
        if stored_event.event.tenant_id == tenant_id:
            events.append(stored_event.event)
    return events


async def copy_events_with_position_mapping(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    tenant_id: UUID,
    migration_id: UUID,
    position_mapper: PositionMapper,
) -> int:
    """Copy events from source to target and record position mappings."""
    events = []
    async for stored_event in source_store.read_all():
        if stored_event.event.tenant_id == tenant_id:
            events.append(stored_event)

    for stored_event in events:
        event = stored_event.event
        result = await target_store.append_events(
            aggregate_id=event.aggregate_id,
            aggregate_type=event.aggregate_type,
            events=[event],
            expected_version=-1,  # ANY
        )

        # Record position mapping
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=stored_event.global_position,
            target_position=result.new_version,  # Use new version as position
            event_id=event.event_id,
        )

    return len(events)


# =============================================================================
# Position Mapping Integration Tests
# =============================================================================


class TestPositionMappingRecording:
    """Tests for position mapping recording during bulk copy."""

    @pytest.mark.asyncio
    async def test_record_single_mapping(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test recording a single position mapping."""
        event_id = uuid4()

        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=event_id,
        )

        # Verify mapping was recorded
        result = await position_mapper.translate_position(
            migration_id=migration_id,
            source_position=100,
        )

        assert result.target_position == 50
        assert result.is_exact is True
        assert result.source_position == 100

    @pytest.mark.asyncio
    async def test_record_batch_mappings(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test recording multiple position mappings in batch."""
        mappings = [
            (100, 50, uuid4()),
            (200, 100, uuid4()),
            (300, 150, uuid4()),
        ]

        count = await position_mapper.record_mappings_batch(
            migration_id=migration_id,
            mappings=mappings,
        )

        assert count == 3

        # Verify all mappings were recorded
        for source_pos, target_pos, _ in mappings:
            result = await position_mapper.translate_position(
                migration_id=migration_id,
                source_position=source_pos,
            )
            assert result.target_position == target_pos
            assert result.is_exact is True

    @pytest.mark.asyncio
    async def test_position_mapping_during_bulk_copy(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        position_mapper: PositionMapper,
        tenant_id: UUID,
        migration_id: UUID,
    ) -> None:
        """Test position mapping during actual bulk copy operation."""
        # Create events in source
        await create_test_events(source_store, tenant_id, count=5)

        # Copy events with position mapping
        copied_count = await copy_events_with_position_mapping(
            source_store,
            target_store,
            tenant_id,
            migration_id,
            position_mapper,
        )

        assert copied_count == 5

        # Verify mappings were recorded
        count = await position_mapper.get_mapping_count(migration_id)
        assert count == 5


class TestPositionTranslation:
    """Tests for position translation during checkpoint migration."""

    @pytest.mark.asyncio
    async def test_exact_position_translation(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test exact position translation."""
        # Record mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )

        result = await position_mapper.translate_position(
            migration_id=migration_id,
            source_position=100,
        )

        assert result.target_position == 50
        assert result.is_exact is True
        assert result.nearest_source_position is None

    @pytest.mark.asyncio
    async def test_nearest_position_translation(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test nearest position translation when exact match not found."""
        # Record mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=200,
            target_position=100,
            event_id=uuid4(),
        )

        # Query position between recorded mappings
        result = await position_mapper.translate_position(
            migration_id=migration_id,
            source_position=150,
        )

        assert result.target_position == 50  # Nearest lower position
        assert result.is_exact is False
        assert result.nearest_source_position == 100

    @pytest.mark.asyncio
    async def test_translation_without_nearest_fails(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test that translation fails when no mapping exists and use_nearest=False."""
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )

        # Query different position without nearest fallback
        with pytest.raises(PositionMappingError):
            await position_mapper.translate_position(
                migration_id=migration_id,
                source_position=150,
                use_nearest=False,
            )

    @pytest.mark.asyncio
    async def test_batch_position_translation(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test batch position translation."""
        # Record mappings
        mappings = [
            (100, 50, uuid4()),
            (200, 100, uuid4()),
            (300, 150, uuid4()),
        ]
        await position_mapper.record_mappings_batch(migration_id, mappings)

        # Batch translate
        results = await position_mapper.translate_positions_batch(
            migration_id=migration_id,
            source_positions=[100, 200, 300],
        )

        assert len(results) == 3
        assert results[0].target_position == 50
        assert results[1].target_position == 100
        assert results[2].target_position == 150
        assert all(r.is_exact for r in results)


# =============================================================================
# Consistency Verification Integration Tests
# =============================================================================


class TestConsistencyVerificationLevels:
    """Tests for consistency verification at different levels."""

    @pytest.mark.asyncio
    async def test_count_level_verification_passes(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test COUNT level verification passes when counts match."""
        # Create same events in both stores
        await create_test_events(source_store, tenant_id, count=5)

        # Copy to target (same events)
        async for stored_event in source_store.read_all():
            if stored_event.event.tenant_id == tenant_id:
                event = stored_event.event
                await target_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    events=[event],
                    expected_version=-1,
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.is_consistent is True
        assert report.source_event_count == 5
        assert report.target_event_count == 5
        assert report.streams_verified == 5

    @pytest.mark.asyncio
    async def test_count_level_verification_detects_mismatch(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test COUNT level verification detects count mismatch."""
        # Create events in source
        await create_test_events(source_store, tenant_id, count=5)

        # Create fewer events in target (only 3)
        async for stored_event in source_store.read_all():
            if stored_event.event.tenant_id == tenant_id and stored_event.global_position <= 3:
                event = stored_event.event
                await target_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    events=[event],
                    expected_version=-1,
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.is_consistent is False
        assert report.source_event_count == 5
        assert report.target_event_count == 3

    @pytest.mark.asyncio
    async def test_hash_level_verification_passes(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test HASH level verification passes with identical data."""
        # Create same events in both stores
        await create_test_events(source_store, tenant_id, count=5)

        # Copy to target
        async for stored_event in source_store.read_all():
            if stored_event.event.tenant_id == tenant_id:
                event = stored_event.event
                await target_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    events=[event],
                    expected_version=-1,
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
        )

        assert report.is_consistent is True
        assert report.verification_level == VerificationLevel.HASH

    @pytest.mark.asyncio
    async def test_full_level_verification_passes(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test FULL level verification passes with identical data."""
        await create_test_events(source_store, tenant_id, count=3)

        async for stored_event in source_store.read_all():
            if stored_event.event.tenant_id == tenant_id:
                event = stored_event.event
                await target_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    events=[event],
                    expected_version=-1,
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.FULL,
        )

        assert report.is_consistent is True
        assert report.verification_level == VerificationLevel.FULL


class TestConsistencyVerificationWithSampling:
    """Tests for consistency verification with sampling."""

    @pytest.mark.asyncio
    async def test_verification_with_50_percent_sampling(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test verification with 50% sampling."""
        # Create events in both stores
        await create_test_events(source_store, tenant_id, count=10)

        async for stored_event in source_store.read_all():
            if stored_event.event.tenant_id == tenant_id:
                event = stored_event.event
                await target_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    events=[event],
                    expected_version=-1,
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
            sample_percentage=50.0,
        )

        assert report.is_consistent is True
        assert report.sample_percentage == 50.0

    @pytest.mark.asyncio
    async def test_invalid_sample_percentage_raises(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that invalid sample percentage raises ValueError."""
        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        with pytest.raises(ValueError):
            await verifier.verify_tenant_consistency(
                tenant_id=tenant_id,
                level=VerificationLevel.COUNT,
                sample_percentage=0.0,  # Invalid
            )

        with pytest.raises(ValueError):
            await verifier.verify_tenant_consistency(
                tenant_id=tenant_id,
                level=VerificationLevel.COUNT,
                sample_percentage=150.0,  # Invalid
            )


# =============================================================================
# Subscription Migration Integration Tests
# =============================================================================


class TestSubscriptionCheckpointMigration:
    """Tests for subscription checkpoint migration after cutover."""

    @pytest.mark.asyncio
    async def test_migrate_subscription_with_exact_mapping(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test migrating subscription with exact position mapping."""
        # Setup position mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )

        # Setup checkpoint at source position 100
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=100,
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert summary.all_successful is True
        assert summary.successful_count == 1
        assert summary.failed_count == 0

        # Verify checkpoint was updated
        new_position = await checkpoint_repo.get_position("OrderProjection")
        assert new_position == 50

    @pytest.mark.asyncio
    async def test_migrate_subscription_with_nearest_mapping(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test migrating subscription using nearest position mapping."""
        # Setup position mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=200,
            target_position=100,
            event_id=uuid4(),
        )

        # Setup checkpoint at position between mappings
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=150,
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert summary.all_successful is True
        assert len(summary.results) == 1
        assert summary.results[0].is_exact_translation is False
        assert summary.results[0].target_position == 50  # Nearest lower

    @pytest.mark.asyncio
    async def test_migrate_multiple_subscriptions(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test migrating multiple subscriptions."""
        # Setup position mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=200,
            target_position=100,
            event_id=uuid4(),
        )

        # Setup checkpoints
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=100,
            event_id=uuid4(),
            event_type="OrderCreated",
        )
        await checkpoint_repo.save_position(
            subscription_id="InventoryProjection",
            position=200,
            event_id=uuid4(),
            event_type="InventoryUpdated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection", "InventoryProjection"],
        )

        assert summary.all_successful is True
        assert summary.successful_count == 2

    @pytest.mark.asyncio
    async def test_subscription_migration_skips_no_checkpoint(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test subscription migration skips subscriptions without checkpoints."""
        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        # No checkpoint exists for this subscription
        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["NonExistentProjection"],
        )

        assert summary.skipped_count == 1
        assert summary.successful_count == 0


class TestDryRunSubscriptionMigration:
    """Tests for dry-run subscription migration."""

    @pytest.mark.asyncio
    async def test_dry_run_shows_planned_changes(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test dry-run shows planned changes without executing."""
        # Setup mappings
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )

        # Setup checkpoint
        original_position = 100
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=original_position,
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        # Run dry-run
        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        assert len(plan.planned_migrations) == 1
        assert plan.planned_migrations[0].current_position == 100
        assert plan.planned_migrations[0].planned_target_position == 50

        # Verify checkpoint was NOT changed
        current_position = await checkpoint_repo.get_position("OrderProjection")
        assert current_position == original_position

    @pytest.mark.asyncio
    async def test_dry_run_shows_warnings_for_nearest_translation(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test dry-run shows warnings when using nearest translation."""
        # Setup mapping (no exact match for checkpoint position)
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )

        # Setup checkpoint at position without exact mapping
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=150,  # No exact mapping exists
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        plan = await migrator.plan_migration(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert plan.migratable_count == 1
        assert plan.planned_migrations[0].is_exact_translation is False
        assert plan.planned_migrations[0].warning is not None


# =============================================================================
# Full Migration Lifecycle Tests
# =============================================================================


class TestFullMigrationWithVerificationAndSubscriptions:
    """Tests for full migration lifecycle with verification and subscription migration."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_verification_enabled(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        checkpoint_repo: InMemoryCheckpointRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test full migration lifecycle with consistency verification enabled."""
        # Create historical events
        await create_test_events(source_store, tenant_id, count=5)

        # Create position mapper
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)

        # Create coordinator with Phase 3 dependencies
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        # Start migration with verification enabled
        config = MigrationConfig(
            batch_size=10,
            verify_consistency=True,
            migrate_subscriptions=False,
        )

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=config,
        )

        assert migration is not None
        assert migration.config.verify_consistency is True

        # Let bulk copy run briefly
        await asyncio.sleep(0.1)

        # Verify migration started
        status = await coordinator.get_status(migration.id)
        assert status.migration_id == migration.id

    @pytest.mark.asyncio
    async def test_verify_consistency_on_coordinator(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test verify_consistency method on coordinator."""
        # Create events in both stores (simulating completed bulk copy)
        await create_test_events(source_store, tenant_id, count=5)

        async for stored_event in source_store.read_all():
            if stored_event.event.tenant_id == tenant_id:
                event = stored_event.event
                await target_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    events=[event],
                    expected_version=-1,
                )

        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            position_mapper=position_mapper,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        # Start migration and manually add target store reference
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Let bulk copy start
        await asyncio.sleep(0.1)

        # Call verify_consistency directly
        report = await coordinator.verify_consistency(
            migration.id,
            level=VerificationLevel.COUNT,
        )

        assert report is not None
        assert report.is_consistent is True

        # Verify report is stored
        stored_report = coordinator.get_consistency_report(migration.id)
        assert stored_report is report

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_on_coordinator(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        checkpoint_repo: InMemoryCheckpointRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test migrate_subscriptions method on coordinator."""
        # Create position mapper with mappings
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)

        # Record some position mappings
        migration_id = uuid4()
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )

        # Setup checkpoint
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=100,
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        # Create migration manually to test migrate_subscriptions
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
        )
        await migration_repo.create(migration)

        # Call migrate_subscriptions
        summary = await coordinator.migrate_subscriptions(
            migration_id,
            subscription_names=["OrderProjection"],
        )

        assert summary is not None
        assert summary.successful_count == 1

        # Verify summary is stored
        stored_summary = coordinator.get_subscription_summary(migration_id)
        assert stored_summary is summary


# =============================================================================
# Error Scenario Tests
# =============================================================================


class TestMissingMappingErrors:
    """Tests for error scenarios with missing position mappings."""

    @pytest.mark.asyncio
    async def test_translation_fails_with_no_mappings(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test translation fails when no mappings exist."""
        with pytest.raises(PositionMappingError):
            await position_mapper.translate_position(
                migration_id=migration_id,
                source_position=100,
            )

    @pytest.mark.asyncio
    async def test_translation_fails_for_position_before_all_mappings(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test translation fails when position is before all mappings."""
        # Record mapping at position 100
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )

        # Try to translate position before any mapping
        with pytest.raises(PositionMappingError):
            await position_mapper.translate_position(
                migration_id=migration_id,
                source_position=50,  # Before position 100
            )

    @pytest.mark.asyncio
    async def test_subscription_migration_fails_with_no_mapping(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test subscription migration records failure when no mapping exists."""
        # Setup checkpoint without any position mappings
        await checkpoint_repo.save_position(
            subscription_id="OrderProjection",
            position=100,
            event_id=uuid4(),
            event_type="OrderCreated",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["OrderProjection"],
        )

        assert summary.all_successful is False
        assert summary.failed_count == 1
        assert summary.results[0].error_message is not None


class TestVerificationFailures:
    """Tests for consistency verification failure scenarios."""

    @pytest.mark.asyncio
    async def test_verification_detects_missing_events(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test verification detects missing events in target."""
        # Create events in source only
        await create_test_events(source_store, tenant_id, count=5)

        # Target is empty

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.is_consistent is False
        assert report.source_event_count == 5
        assert report.target_event_count == 0
        assert len(report.violations) > 0

    @pytest.mark.asyncio
    async def test_verification_detects_extra_events_in_target(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test verification detects extra events in target."""
        # Create more events in target than source
        await create_test_events(target_store, tenant_id, count=5)
        # Source has no events

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.is_consistent is False
        assert report.source_event_count == 0
        assert report.target_event_count == 5

    @pytest.mark.asyncio
    async def test_verification_passes_with_empty_stores(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test verification passes when both stores are empty."""
        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
        )

        assert report.is_consistent is True
        assert report.source_event_count == 0
        assert report.target_event_count == 0


class TestCoordinatorErrorHandling:
    """Tests for coordinator error handling with Phase 3 features."""

    @pytest.mark.asyncio
    async def test_verify_consistency_without_target_store(
        self,
        source_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test verify_consistency fails gracefully without target store."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # Create migration but don't add target store to coordinator
        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )
        await migration_repo.create(migration)

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.verify_consistency(migration.id)

        assert "Target store not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_without_position_mapper(
        self,
        source_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test migrate_subscriptions fails without position_mapper."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            # No position_mapper provided
            source_store_id="default",
            enable_tracing=False,
        )

        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )
        await migration_repo.create(migration)

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.migrate_subscriptions(migration.id)

        assert "position_mapper not provided" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_migrate_subscriptions_without_checkpoint_repo(
        self,
        source_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test migrate_subscriptions fails without checkpoint_repo."""
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            position_mapper=position_mapper,
            # No checkpoint_repo provided
            source_store_id="default",
            enable_tracing=False,
        )

        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )
        await migration_repo.create(migration)

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.migrate_subscriptions(migration.id)

        assert "checkpoint_repo not provided" in str(exc_info.value)


# =============================================================================
# Position Mapper Edge Cases
# =============================================================================


class TestPositionMapperEdgeCases:
    """Tests for position mapper edge cases."""

    @pytest.mark.asyncio
    async def test_empty_batch_mapping(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test recording empty batch returns 0."""
        count = await position_mapper.record_mappings_batch(
            migration_id=migration_id,
            mappings=[],
        )
        assert count == 0

    @pytest.mark.asyncio
    async def test_get_position_bounds_no_mappings(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test get_position_bounds returns None when no mappings."""
        bounds = await position_mapper.get_position_bounds(migration_id)
        assert bounds is None

    @pytest.mark.asyncio
    async def test_get_position_bounds_with_mappings(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test get_position_bounds returns correct bounds."""
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=500,
            target_position=250,
            event_id=uuid4(),
        )

        bounds = await position_mapper.get_position_bounds(migration_id)
        assert bounds == (100, 500)

    @pytest.mark.asyncio
    async def test_clear_mappings(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test clearing all mappings for a migration."""
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=200,
            target_position=100,
            event_id=uuid4(),
        )

        count = await position_mapper.clear_mappings(migration_id)
        assert count == 2

        # Verify mappings were deleted
        remaining = await position_mapper.get_mapping_count(migration_id)
        assert remaining == 0

    @pytest.mark.asyncio
    async def test_get_mapping_by_event_id(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test getting mapping by event ID."""
        event_id = uuid4()
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=event_id,
        )

        mapping = await position_mapper.get_mapping_by_event_id(migration_id, event_id)
        assert mapping is not None
        assert mapping.source_position == 100
        assert mapping.target_position == 50
        assert mapping.event_id == event_id

    @pytest.mark.asyncio
    async def test_translate_empty_batch(
        self,
        position_mapper: PositionMapper,
        migration_id: UUID,
    ) -> None:
        """Test translating empty batch returns empty list."""
        results = await position_mapper.translate_positions_batch(
            migration_id=migration_id,
            source_positions=[],
        )
        assert results == []


# =============================================================================
# Verification Report Tests
# =============================================================================


class TestVerificationReport:
    """Tests for VerificationReport functionality."""

    @pytest.mark.asyncio
    async def test_report_to_dict_serialization(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test report can be serialized to dictionary."""
        await create_test_events(source_store, tenant_id, count=3)
        async for stored_event in source_store.read_all():
            if stored_event.event.tenant_id == tenant_id:
                event = stored_event.event
                await target_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    events=[event],
                    expected_version=-1,
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
        )

        report_dict = report.to_dict()

        assert "tenant_id" in report_dict
        assert "is_consistent" in report_dict
        assert "source_event_count" in report_dict
        assert "target_event_count" in report_dict
        assert "verification_level" in report_dict
        assert report_dict["verification_level"] == "hash"

    @pytest.mark.asyncio
    async def test_report_consistency_percentage(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test consistency percentage calculation."""
        # Create events that will pass verification
        await create_test_events(source_store, tenant_id, count=3)
        async for stored_event in source_store.read_all():
            if stored_event.event.tenant_id == tenant_id:
                event = stored_event.event
                await target_store.append_events(
                    aggregate_id=event.aggregate_id,
                    aggregate_type=event.aggregate_type,
                    events=[event],
                    expected_version=-1,
                )

        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)

        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        assert report.consistency_percentage == 100.0


# =============================================================================
# Migration Summary Tests
# =============================================================================


class TestMigrationSummary:
    """Tests for MigrationSummary functionality."""

    @pytest.mark.asyncio
    async def test_summary_to_dict_serialization(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test summary can be serialized to dictionary."""
        # Setup
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )
        await checkpoint_repo.save_position(
            subscription_id="TestProjection",
            position=100,
            event_id=uuid4(),
            event_type="TestEvent",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["TestProjection"],
        )

        summary_dict = summary.to_dict()

        assert "migration_id" in summary_dict
        assert "tenant_id" in summary_dict
        assert "successful_count" in summary_dict
        assert "failed_count" in summary_dict
        assert "results" in summary_dict
        assert "all_successful" in summary_dict

    @pytest.mark.asyncio
    async def test_summary_all_successful_property(
        self,
        position_mapper: PositionMapper,
        checkpoint_repo: InMemoryCheckpointRepository,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test all_successful property works correctly."""
        # Setup mappings - only mapping at position 100
        await position_mapper.record_mapping(
            migration_id=migration_id,
            source_position=100,
            target_position=50,
            event_id=uuid4(),
        )

        await checkpoint_repo.save_position(
            subscription_id="Projection1",
            position=100,
            event_id=uuid4(),
            event_type="TestEvent",
        )
        # Position 50 is BEFORE the first mapping (100), so no nearest mapping exists
        await checkpoint_repo.save_position(
            subscription_id="Projection2",
            position=50,  # Before any mapping, will fail
            event_id=uuid4(),
            event_type="TestEvent",
        )

        migrator = SubscriptionMigrator(
            position_mapper=position_mapper,
            checkpoint_repo=checkpoint_repo,
            enable_tracing=False,
        )

        summary = await migrator.migrate_subscriptions(
            migration_id=migration_id,
            tenant_id=tenant_id,
            subscription_names=["Projection1", "Projection2"],
        )

        # One succeeds, one fails (no mapping for position 50 - before any mappings)
        assert summary.all_successful is False
        assert summary.successful_count == 1
        assert summary.failed_count == 1
