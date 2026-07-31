"""
Phase 2 Integration Tests for Multi-Tenant Live Migration.

Tests cover:
- Full migration lifecycle: PENDING -> BULK_COPY -> DUAL_WRITE -> CUTOVER -> COMPLETED
- Dual-write behavior: writes go to both stores during DUAL_WRITE phase
- Sync lag tracking during dual-write
- Cutover success scenario
- Cutover failure and rollback to DUAL_WRITE
- Abort during different phases
- Pause/resume during different phases
- Error handling and recovery
- Concurrent operations during migration

These tests use InMemoryEventStore for unit-level integration testing
without requiring PostgreSQL.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.migration.bulk_copier import BulkCopier
from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.cutover import CutoverManager
from eventsource.migration.dual_write import DualWriteInterceptor
from eventsource.migration.exceptions import (
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
)
from eventsource.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
    TenantMigrationState,
    TenantRouting,
)
from eventsource.migration.router import TenantStoreRouter
from eventsource.migration.sync_lag_tracker import SyncLagTracker
from eventsource.migration.write_pause import WritePauseManager
from eventsource.ports import ExpectedVersion, FeedReadOptions, Position

# =============================================================================
# Test Event Classes
# =============================================================================


class SampleTestEvent(DomainEvent):
    """Sample event for integration tests."""

    event_type: str = "SampleTestEvent"
    aggregate_type: str = "SampleAggregate"
    value: str = "test"


class OrderCreated(DomainEvent):
    """Test order created event."""

    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str = "customer-123"
    amount: float = 100.0


class OrderConfirmed(DomainEvent):
    """Test order confirmed event."""

    event_type: str = "OrderConfirmed"
    aggregate_type: str = "Order"


# =============================================================================
# In-Memory Repository Implementations
# =============================================================================


class InMemoryMigrationRepository:
    """
    In-memory implementation of MigrationRepository for testing.

    Stores migrations in a dictionary and implements all protocol methods.
    """

    def __init__(self) -> None:
        self._migrations: dict[UUID, Migration] = {}
        self._by_tenant: dict[UUID, UUID] = {}  # tenant_id -> migration_id

    async def create(self, migration: Migration) -> UUID:
        """Create a new migration record."""
        # Check for existing active migration
        existing = await self.get_by_tenant(migration.tenant_id)
        if existing is not None:
            raise MigrationAlreadyExistsError(
                migration.tenant_id,
                existing.id,
            )

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

        # Only return if not terminal
        if migration.phase.is_terminal:
            return None

        return migration

    async def update_phase(
        self,
        migration_id: UUID,
        new_phase: MigrationPhase,
    ) -> None:
        """Update migration phase with validation."""
        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        if not migration.can_transition_to(new_phase):
            raise InvalidPhaseTransitionError(
                migration_id,
                migration.phase,
                new_phase,
            )

        now = datetime.now(UTC)
        migration.phase = new_phase
        migration.updated_at = now

        # Update phase-specific timestamps
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
        migration.last_error = error[:1000]  # Truncate
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
        """List all active (non-terminal) migrations."""
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
    return InMemoryEventStore("source")


@pytest.fixture
def target_store() -> InMemoryEventStore:
    """Create target (dedicated) event store."""
    return InMemoryEventStore("target")


@pytest.fixture
def migration_repo() -> InMemoryMigrationRepository:
    """Create migration repository."""
    return InMemoryMigrationRepository()


@pytest.fixture
def routing_repo() -> InMemoryRoutingRepository:
    """Create routing repository."""
    return InMemoryRoutingRepository()


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
def coordinator(
    source_store: InMemoryEventStore,
    migration_repo: InMemoryMigrationRepository,
    routing_repo: InMemoryRoutingRepository,
    router: TenantStoreRouter,
    lock_manager: MockLockManager,
) -> MigrationCoordinator:
    """Create migration coordinator."""
    return MigrationCoordinator(
        source_store=source_store,
        migration_repo=migration_repo,
        routing_repo=routing_repo,
        router=router,
        lock_manager=lock_manager,
        source_store_id="default",
        enable_tracing=False,
    )


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

        await store.append(
            StreamId(aggregate_id=aggregate_id, category=aggregate_type),
            [event],
            ExpectedVersion.no_stream(),
        )

    return aggregate_ids


async def get_all_tenant_events(
    store: InMemoryEventStore,
    tenant_id: UUID,
) -> list[DomainEvent]:
    """Get all events for a tenant from a store."""
    events = []
    async for envelope in store.read_all():
        if envelope.event.tenant_id == tenant_id:
            events.append(envelope.event)
    return events


# =============================================================================
# Full Migration Lifecycle Tests
# =============================================================================


class TestFullMigrationLifecycle:
    """Tests for complete migration workflow from PENDING to COMPLETED."""

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_events(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test complete migration lifecycle with actual events."""
        # Setup: Create historical events in source store
        await create_test_events(
            source_store,
            tenant_id,
            count=5,
        )

        # Verify events in source
        source_events = await get_all_tenant_events(source_store, tenant_id)
        assert len(source_events) == 5

        # Create coordinator
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # Register target store
        router.register_store("dedicated", target_store)

        # Start migration (PENDING -> BULK_COPY)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=MigrationConfig(
                batch_size=10,
                cutover_max_lag_events=100,
            ),
        )

        assert migration is not None
        assert migration.tenant_id == tenant_id

        # Wait for bulk copy to complete and transition to DUAL_WRITE
        # In a real scenario this happens automatically, but we simulate it
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None

        # Wait a bit for background task
        await asyncio.sleep(0.1)

        # Check migration progressed
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        # Migration should be in progress (BULK_COPY or DUAL_WRITE)
        assert migration.phase in (
            MigrationPhase.BULK_COPY,
            MigrationPhase.DUAL_WRITE,
        )

    @pytest.mark.asyncio
    async def test_migration_phases_are_tracked(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that migration phases are properly tracked."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Verify migration was created
        assert migration is not None

        # Get status
        status = await coordinator.get_status(migration.id)
        assert status is not None
        assert status.migration_id == migration.id
        assert status.tenant_id == tenant_id


# =============================================================================
# Dual-Write Behavior Tests
# =============================================================================


class TestDualWriteBehavior:
    """Tests for dual-write interceptor behavior."""

    @pytest.mark.asyncio
    async def test_dual_write_writes_to_both_stores(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that dual-write interceptor writes to both stores."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        stream = StreamId(aggregate_id=aggregate_id, category="Order")

        # Write through interceptor
        result = await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        assert result.new_version == 1

        # Verify event in source
        source_events = [e async for e in source_store.read_stream(stream)]
        assert len(source_events) == 1
        assert source_events[0].event.event_id == event.event_id

        # Verify event in target
        target_events = [e async for e in target_store.read_stream(stream)]
        assert len(target_events) == 1
        assert target_events[0].event.event_id == event.event_id

    @pytest.mark.asyncio
    async def test_dual_write_reads_from_source(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that reads during dual-write come from source store."""
        # Pre-populate source with events
        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        stream = StreamId(aggregate_id=aggregate_id, category="Order")

        await source_store.append(stream, [event], ExpectedVersion.no_stream())

        # Target is empty

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Read through interceptor
        events = [e async for e in interceptor.read_stream(stream)]

        # Should get event from source
        assert len(events) == 1
        assert events[0].event.event_id == event.event_id

    @pytest.mark.asyncio
    async def test_dual_write_handles_target_failure(
        self,
        source_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that dual-write handles target store failure gracefully."""

        # Create a target store that will fail on append
        class FailingStore(InMemoryEventStore):
            async def append(self, *args, **kwargs):
                raise RuntimeError("Target store failure")

        failing_target = FailingStore("failing-target")

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=failing_target,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        stream = StreamId(aggregate_id=aggregate_id, category="Order")

        # Write should succeed (source is authoritative)
        result = await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        assert result.new_version == 1

        # Verify event in source
        source_events = [e async for e in source_store.read_stream(stream)]
        assert len(source_events) == 1

        # Verify failure was recorded
        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 1

    @pytest.mark.asyncio
    async def test_dual_write_multiple_events_same_aggregate(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test dual-write with multiple events on same aggregate."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="Order")

        # Write first event
        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        await interceptor.append(stream, [event1], ExpectedVersion.no_stream())

        # Write second event
        event2 = OrderConfirmed(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        await interceptor.append(stream, [event2], ExpectedVersion.exact(1))

        # Verify both events in both stores
        source_events = [e async for e in source_store.read_stream(stream)]
        assert len(source_events) == 2

        target_events = [e async for e in target_store.read_stream(stream)]
        assert len(target_events) == 2


# =============================================================================
# Sync Lag Tracking Tests
# =============================================================================


class TestSyncLagTracking:
    """Tests for sync lag tracking during dual-write."""

    @pytest.mark.asyncio
    async def test_sync_lag_calculation(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test sync lag is calculated correctly."""
        # Pre-populate source with more events than target
        for _i in range(10):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Target has only 5 events
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await target_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=MigrationConfig(cutover_max_lag_events=100),  # exact count, not clamped
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # No copying has happened yet (since=None): every source event is
        # behind, regardless of how many events the target happens to hold.
        lag = await tracker.calculate_lag()

        assert lag is not None
        assert lag.source_position is not None
        assert lag.target_position is not None
        assert lag.events == 10  # nothing copied yet -> all 10 source events are behind

    @pytest.mark.asyncio
    async def test_sync_lag_converged_when_equal(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test sync lag shows converged when the target has caught up."""
        last_source_position: Position | None = None

        # Same events in both stores
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            stream = StreamId(aggregate_id=aggregate_id, category="SampleAggregate")
            result = await source_store.append(stream, [event], ExpectedVersion.no_stream())
            last_source_position = result.position
            await target_store.append(stream, [event], ExpectedVersion.no_stream())

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # `since` is the last position the target has copied; passing the
        # final source position means nothing is behind.
        lag = await tracker.calculate_lag(since=last_source_position)

        assert lag is not None
        assert lag.events == 0
        assert lag.is_converged

    @pytest.mark.asyncio
    async def test_sync_ready_within_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test is_sync_ready returns True when lag is within threshold."""
        config = MigrationConfig(cutover_max_lag_events=100)

        last_source_position: Position | None = None

        # Copy the first 10 events' worth of position, leave the rest (10
        # more) as lag -- 10 events behind, well within the 100 threshold.
        for i in range(20):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            result = await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            if i == 9:
                last_source_position = result.position

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        await tracker.calculate_lag(since=last_source_position)

        # 10 events lag is within 100 event threshold
        assert tracker.is_sync_ready()

    @pytest.mark.asyncio
    async def test_lag_statistics_tracking(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test lag statistics are properly tracked over time."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=MigrationConfig(cutover_max_lag_events=100),  # exact count, not clamped
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Take multiple lag samples, always measured from the start
        # (since=None) -- each new event increases the count behind by one.
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

            await tracker.calculate_lag()

        stats = tracker.get_lag_stats()

        assert stats.sample_count == 5
        assert stats.max_lag == 5  # Final lag after 5 events
        assert stats.min_lag == 1  # First lag was 1


class FlakyTarget:
    """`FullEventStore` wrapper that rejects the first `reject_first` appends.

    Composition, not inheritance -- it stands in wherever a
    `FullEventStore` is expected and lets the wrapped store see only the
    appends it does not reject.
    """

    max_append_batch: int | None = None

    def __init__(self, inner: InMemoryEventStore, *, reject_first: int = 0) -> None:
        self._inner = inner
        self._reject_first = reject_first
        self.append_calls = 0

    async def append(self, stream, events, expected):  # type: ignore[no-untyped-def]
        self.append_calls += 1
        if self.append_calls <= self._reject_first:
            raise RuntimeError("target unavailable")
        return await self._inner.append(stream, events, expected)

    def read_stream(self, stream, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_stream(stream, options)

    async def get_stream_version(self, stream):  # type: ignore[no-untyped-def]
        return await self._inner.get_stream_version(stream)

    async def event_exists(self, event_id):  # type: ignore[no-untyped-def]
        return await self._inner.event_exists(event_id)

    def read_all(self, from_position=None, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_all(from_position, options)

    async def current_position(self):  # type: ignore[no-untyped-def]
        return await self._inner.current_position()

    def read_category(self, category, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_category(category, options)


async def drive_dual_writes(
    interceptor: DualWriteInterceptor,
    tenant_id: UUID,
    count: int,
) -> None:
    """Push `count` single-event appends through the interceptor."""
    for _i in range(count):
        aggregate_id = uuid4()
        event = SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        await interceptor.append(
            StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )


async def seed_copied_prefix(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    tenant_id: UUID,
    count: int,
) -> Position | None:
    """Write `count` events to BOTH stores; return the last source position.

    Stands in for a completed bulk-copy pass.
    """
    last_source_position: Position | None = None
    for _i in range(count):
        aggregate_id = uuid4()
        event = SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="SampleAggregate")
        result = await source_store.append(stream, [event], ExpectedVersion.no_stream())
        last_source_position = result.position
        await target_store.append(stream, [event], ExpectedVersion.no_stream())
    return last_source_position


class TestSyncLagAnchorOnWriteActiveTenant:
    """Lag must converge on a live tenant WITHOUT ever hiding missing events.

    The count-behind reads the source feed after its anchor, which
    includes everything the `DualWriteInterceptor` has already mirrored.
    The anchor therefore advances to the interceptor's synced watermark --
    but never past its first failure, because events at or after that
    point may be missing from the target.
    """

    @pytest.mark.asyncio
    async def test_healthy_tenant_converges_unbounded(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """All writes mirrored and the checkpoint covers the install window:
        lag 0, and not a bounded guess.

        Without the coordinator's completed-pass attestation the checkpoint
        must have reached the interceptor's coverage for the anchor to
        advance at all -- see `test_install_window_gap_blocks_cutover` for
        why. `copied_through` stands for the checkpoint of a copy pass that
        ran with the interceptor installed, which since the phase reorder
        is every coordinator-run pass.
        """
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 3)

        # A copy pass ran with the interceptor installed, so the
        # checkpoint now covers everything up to here.
        copied_through = await source_store.current_position()
        assert copied_through is not None
        assert interceptor.first_seen_source_position is not None
        assert copied_through >= interceptor.first_seen_source_position

        await drive_dual_writes(interceptor, tenant_id, 2)

        assert interceptor.first_failed_source_position is None
        assert interceptor.last_synced_source_position is not None

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=MigrationConfig(cutover_max_lag_events=3),
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        anchor = interceptor.safe_lag_anchor(copied_through)
        assert anchor == interceptor.last_synced_source_position

        lag = await tracker.calculate_lag(since=anchor)

        assert lag.events == 0
        assert lag.count_is_bounded is False
        assert tracker.is_sync_ready() is True

    @pytest.mark.asyncio
    async def test_failed_prefix_never_reads_as_converged(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
    ) -> None:
        """Failures followed by successes must not hide the missing events.

        This is the data-loss case: the target was down for the first
        three writes and recovered for the next five. The successes must
        NOT advance the anchor past the failure, or the missing three
        vanish from the count and cutover proceeds over a hole.
        """
        migration_id = uuid4()
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 4)

        flaky = FlakyTarget(target_store, reject_first=3)
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=flaky,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 8)

        # Three events never reached the target.
        assert interceptor.first_failed_source_position is not None

        config = MigrationConfig(cutover_max_lag_events=3)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        anchor = interceptor.safe_lag_anchor(copied_through)
        lag = await tracker.calculate_lag(since=anchor)

        # At least the three missing events are still counted.
        assert lag.events >= 3
        assert tracker.is_sync_ready() is False

        # And cutover refuses.
        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=tracker,
            target_store_id="dedicated",
            config=config,
            timeout_ms=500.0,
            since=anchor,
        )

        assert result.success is False
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.store_id != "dedicated"

    @pytest.mark.asyncio
    async def test_first_failure_watermark_does_not_move_on_recovery(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Later successes never clear or advance the first-failure mark."""
        flaky = FlakyTarget(target_store, reject_first=1)
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=flaky,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        await drive_dual_writes(interceptor, tenant_id, 1)
        first_failed = interceptor.first_failed_source_position
        assert first_failed is not None

        await drive_dual_writes(interceptor, tenant_id, 5)

        assert interceptor.first_failed_source_position == first_failed
        # Successes after the failure DO advance the synced watermark...
        assert interceptor.last_synced_source_position is not None
        assert interceptor.last_synced_source_position > first_failed
        # ...but the anchor refuses to move past the failure.
        assert interceptor.safe_lag_anchor(None) is None

    @pytest.mark.asyncio
    async def test_install_window_gap_blocks_cutover(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
    ) -> None:
        """The install-window clamp is dead-man defense: writes mirrored by
        nobody never let a later successful mirror advance the anchor past
        them.

        The coordinator now installs the interceptor BEFORE the copy pass,
        so this window is unreachable-by-construction in the in-tree flow
        (see TestWriteActiveTenantFirstPassCutover). The clamp stays for
        every path WITHOUT the coordinator's completed-pass attestation
        (`mark_copy_pass_complete`): a restarted orchestrator's fresh
        interceptor, hand-driven components as here, or a pass that never
        completed. In those, writes may predate the interceptor, go to the
        source alone, and must keep counting as lag.
        """
        migration_id = uuid4()
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 4)

        # The install window: source-only writes, mirrored by nobody.
        for _i in range(3):
            aggregate_id = uuid4()
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)],
                ExpectedVersion.no_stream(),
            )

        # Interceptor installed only now, and mirroring cleanly.
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 2)

        assert interceptor.first_failed_source_position is None
        assert interceptor.last_synced_source_position is not None
        assert interceptor.first_seen_source_position is not None

        # The anchor must not advance over the gap.
        anchor = interceptor.safe_lag_anchor(copied_through)
        assert anchor == copied_through

        # NOTE: the threshold has to be tighter than the backlog for
        # cutover to refuse. Gap events counted but sitting UNDER a
        # generous threshold would still permit cutover, because the
        # threshold assumes the remainder drains during the pause -- and
        # gap events never drain, since nobody is mirroring them.
        config = MigrationConfig(cutover_max_lag_events=3)
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        lag = await tracker.calculate_lag(since=anchor)

        # The 3 gap events are genuinely missing from the target.
        assert lag.events >= 3
        assert tracker.is_sync_ready() is False

        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=tracker,
            target_store_id="dedicated",
            config=config,
            timeout_ms=500.0,
            since=anchor,
        )

        assert result.success is False
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.store_id != "dedicated"

    @pytest.mark.asyncio
    async def test_anchor_advances_when_interceptor_covers_the_checkpoint(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """The install-window clamp is a gate, not a veto.

        Once the checkpoint reaches the interceptor's coverage the anchor
        advances normally -- otherwise the clamp would pin every
        migration at its checkpoint forever.
        """
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 1)

        # Checkpoint exactly at the start of coverage: the boundary case.
        checkpoint = interceptor.first_seen_source_position
        assert checkpoint is not None

        await drive_dual_writes(interceptor, tenant_id, 3)

        anchor = interceptor.safe_lag_anchor(checkpoint)
        assert anchor == interceptor.last_synced_source_position
        assert anchor != checkpoint

    @pytest.mark.asyncio
    async def test_restart_refuses_until_a_fresh_copy_pass(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """A restarted orchestrator has no watermarks, so it refuses cutover.

        The watermarks live in the interceptor's memory. After a restart
        the anchor falls back to the bulk-copy checkpoint, so already
        mirrored events count as lag and cutover refuses until a fresh
        copy pass advances the checkpoint.
        """
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 4)

        pre_restart = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(pre_restart, tenant_id, 5)

        # The process restarts: a brand-new interceptor, no watermarks.
        restarted = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        assert restarted.last_synced_source_position is None
        assert restarted.safe_lag_anchor(copied_through) == copied_through

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=MigrationConfig(cutover_max_lag_events=3),
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        lag = await tracker.calculate_lag(since=restarted.safe_lag_anchor(copied_through))

        # The five already-mirrored events read as lag -- conservative,
        # and the documented consequence of not persisting the watermark.
        assert lag.events > 0
        assert tracker.is_sync_ready() is False

    @pytest.mark.asyncio
    async def test_overlap_mirror_conflict_is_absorbed_by_the_copy_pass(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """A mirror refused on a behind stream never leapfrogs the copier.

        The mirror appends at the exact pre-append source version, so on a
        stream the copier has not caught up it fails -- even for an `any`
        writer that would previously have landed out of order. The refusal
        is recorded, the copy pass delivers the event in source order, and
        the completed pass absorbs the recorded failure so the anchor can
        advance again.
        """
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="SampleAggregate")
        await source_store.append(
            stream,
            [SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)],
            ExpectedVersion.no_stream(),
        )
        await source_store.append(
            stream,
            [SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)],
            ExpectedVersion.exact(1),
        )

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # A live `any` writer appends while the target holds none of the
        # stream yet: the source takes it, the mirror must refuse.
        live = SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        result = await interceptor.append(stream, [live], ExpectedVersion.any_())
        assert result.new_version == 3
        assert await target_store.get_stream_version(stream) == 0
        assert interceptor.first_failed_source_position == result.position

        # The copy pass (which runs with the interceptor installed) then
        # delivers everything, in source stream order.
        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )
        envelopes = [
            env async for env in source_store.read_all(None, FeedReadOptions(tenant_id=tenant_id))
        ]
        await copier._write_batch(uuid4(), tenant_id, envelopes)
        checkpoint = envelopes[-1].position
        assert checkpoint is not None

        source_ids = [env.event.event_id async for env in source_store.read_stream(stream)]
        target_ids = [env.event.event_id async for env in target_store.read_stream(stream)]
        assert target_ids == source_ids

        # The completed covered pass absorbs the failure...
        assert interceptor.mark_copy_pass_complete(checkpoint) == 0
        assert interceptor.first_failed_source_position is None

        # ...so the next mirrored write advances the anchor again.
        follow_up = await interceptor.append(
            stream,
            [SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)],
            ExpectedVersion.exact(3),
        )
        assert interceptor.safe_lag_anchor(checkpoint) == follow_up.position

    @pytest.mark.asyncio
    async def test_failures_beyond_the_checkpoint_stay_unabsorbed(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """`mark_copy_pass_complete` only absorbs what the checkpoint covers.

        A failure past the pass's checkpoint was not re-copied by that
        pass, so it keeps clamping the anchor until a later pass reaches
        it.
        """
        flaky = FlakyTarget(target_store, reject_first=2)
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=flaky,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 2)
        failures = [fw.source_position for fw in interceptor.get_failed_writes()]
        assert len(failures) == 2
        first_failure, second_failure = failures
        assert first_failure is not None and second_failure is not None

        # A pass that only reached the first failure absorbs only it.
        assert interceptor.mark_copy_pass_complete(first_failure) == 1
        assert interceptor.first_failed_source_position == second_failure

        # A later mirror success cannot advance past the remaining hole.
        await drive_dual_writes(interceptor, tenant_id, 1)
        assert interceptor.safe_lag_anchor(first_failure) == first_failure

        # The next pass's checkpoint covers it and the anchor is free.
        assert interceptor.mark_copy_pass_complete(second_failure) == 0
        await drive_dual_writes(interceptor, tenant_id, 1)
        anchor = interceptor.safe_lag_anchor(second_failure)
        assert anchor == interceptor.last_synced_source_position
        assert anchor is not None and anchor > second_failure

    @pytest.mark.asyncio
    async def test_attested_pass_releases_the_install_window_clamp(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """The coordinator's completed-pass attestation replaces the
        first_seen proxy: mirrored writes beyond the checkpoint advance the
        anchor even though the checkpoint never reached first_seen."""
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 2)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 2)

        # Without attestation the proxy clamp holds (checkpoint < first_seen).
        assert interceptor.safe_lag_anchor(copied_through) == copied_through

        # With it, the window is provably empty and the anchor advances.
        assert interceptor.mark_copy_pass_complete(copied_through) == 0
        anchor = interceptor.safe_lag_anchor(copied_through)
        assert anchor == interceptor.last_synced_source_position
        assert anchor is not None and copied_through is not None
        assert anchor > copied_through

    @pytest.mark.asyncio
    async def test_saturated_failure_history_never_reports_clean(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Once tracked failures exceed `max_failure_history`, the fail-closed
        guard must pin, not just slow, recovery.

        Dropping the oldest unabsorbed failure position to bound memory
        makes a future "fully absorbed" claim unsound -- there is no way
        to prove the dropped position was re-copied. So once saturated,
        `safe_lag_anchor` must clamp to the checkpoint forever, and
        `mark_copy_pass_complete` must never again report zero remaining
        failures, even when its checkpoint covers every position still on
        record.
        """
        always_fails = FlakyTarget(target_store, reject_first=1_000_000)
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=always_fails,
            tenant_id=tenant_id,
            enable_tracing=False,
            max_failure_history=2,
        )

        # Drive more failing mirrors than the cap so the guard saturates.
        await drive_dual_writes(interceptor, tenant_id, 5)
        checkpoint = await source_store.current_position()
        assert checkpoint is not None

        # A checkpoint that covers every recorded failure would normally
        # absorb them all and report clean -- saturation must override that.
        remaining = interceptor.mark_copy_pass_complete(checkpoint)
        assert remaining >= 1

        # Calling it again (another completed pass at the same checkpoint)
        # must still refuse to report clean.
        assert interceptor.mark_copy_pass_complete(checkpoint) >= 1

        # The anchor stays pinned at the checkpoint regardless of how far
        # writes have otherwise synced.
        assert interceptor.safe_lag_anchor(checkpoint) == checkpoint


class GatedFeedStore:
    """`FullEventStore` wrapper whose second `read_all` call blocks mid-stream.

    The bulk copier's first `read_all` is the counting pass and its second
    is the copy pass; gating the second lets a test write live events while
    the copy pass is provably mid-flight, then release it. Every other
    operation passes straight through.
    """

    max_append_batch: int | None = None

    def __init__(self, inner: InMemoryEventStore, *, gate_after: int = 3) -> None:
        self._inner = inner
        self._gate_after = gate_after
        self._read_all_calls = 0
        self.mid_pass = asyncio.Event()
        self.resume_gate = asyncio.Event()

    async def append(self, stream, events, expected):  # type: ignore[no-untyped-def]
        return await self._inner.append(stream, events, expected)

    def read_stream(self, stream, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_stream(stream, options)

    async def get_stream_version(self, stream):  # type: ignore[no-untyped-def]
        return await self._inner.get_stream_version(stream)

    async def event_exists(self, event_id):  # type: ignore[no-untyped-def]
        return await self._inner.event_exists(event_id)

    def read_all(self, from_position=None, options=None):  # type: ignore[no-untyped-def]
        self._read_all_calls += 1
        if self._read_all_calls == 2:
            return self._gated_read(from_position, options)
        return self._inner.read_all(from_position, options)

    async def _gated_read(self, from_position, options):  # type: ignore[no-untyped-def]
        yielded = 0
        async for envelope in self._inner.read_all(from_position, options):
            yield envelope
            yielded += 1
            if yielded == self._gate_after:
                self.mid_pass.set()
                await self.resume_gate.wait()

    async def current_position(self):  # type: ignore[no-untyped-def]
        return await self._inner.current_position()

    def read_category(self, category, options=None):  # type: ignore[no-untyped-def]
        return self._inner.read_category(category, options)


class TestWriteActiveTenantFirstPassCutover:
    """The dual-write interceptor must be installed BEFORE the copy pass.

    With the interceptor installed only after the copy completes, writes
    landing during the copy are mirrored by nobody and never drain: the
    lag anchor stays clamped at the checkpoint, the gap events count as
    lag forever, and a write-active tenant can never cut over on its
    first pass. Installing the interceptor before the pass means every
    event is either in the pass's snapshot (copied) or mirrored
    (dual-written) -- the window never exists.
    """

    @pytest.mark.asyncio
    async def test_write_active_tenant_cuts_over_on_first_pass(
        self,
        tenant_id: UUID,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        lock_manager: MockLockManager,
        write_pause_manager: WritePauseManager,
    ) -> None:
        """Writes landing mid-copy are mirrored, and cutover fires first pass."""
        inner_source = InMemoryEventStore("source")
        target_store = InMemoryEventStore("target")
        source = GatedFeedStore(inner_source, gate_after=3)

        router = TenantStoreRouter(
            default_store=source,
            routing_repo=routing_repo,
            stores={"default": source},
            default_store_id="default",
            enable_tracing=False,
            write_pause_manager=write_pause_manager,
        )
        coordinator = MigrationCoordinator(
            source_store=source,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        await create_test_events(inner_source, tenant_id, count=6)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=MigrationConfig(
                cutover_max_lag_events=3,
                batch_size=2,
                verify_consistency=False,
                migrate_subscriptions=False,
            ),
        )

        # The copy pass is provably mid-flight: blocked inside its feed read.
        await asyncio.wait_for(source.mid_pass.wait(), timeout=5.0)

        # Live tenant writes while the copy runs -- more of them than the
        # cutover threshold, so un-mirrored gap events could never drain.
        for _i in range(5):
            aggregate_id = uuid4()
            await router.append(
                StreamId(aggregate_id=aggregate_id, category="Order"),
                [OrderCreated(aggregate_id=aggregate_id, tenant_id=tenant_id)],
                ExpectedVersion.no_stream(),
            )

        source.resume_gate.set()
        await coordinator.wait_for_phase(
            migration.id, MigrationPhase.DUAL_WRITE, timeout=5.0, poll_interval=0.01
        )

        result = await coordinator.trigger_cutover(migration.id)

        assert result.success is True
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.store_id == "dedicated"
        assert routing.migration_state == TenantMigrationState.MIGRATED

        # Nothing was left behind: every source event reached the target.
        source_ids = {e.event_id for e in await get_all_tenant_events(inner_source, tenant_id)}
        target_ids = {e.event_id for e in await get_all_tenant_events(target_store, tenant_id)}
        assert source_ids == target_ids


class TestCatchUpRoundsCap:
    """The post-copy catch-up loop must be bounded, not spin forever.

    A mirror that never drains (every mirrored write keeps failing) would
    otherwise make the `while completed` loop in `_run_bulk_copy` run
    forever waiting for `remaining == 0`. `_MAX_CATCHUP_ROUNDS` caps that:
    once hit, the migration still completes its copy and transitions to
    DUAL_WRITE (fail-open on progress), but the lag anchor -- driven by
    the same unabsorbed-failure bookkeeping exercised in
    `TestSyncLagAnchorOnWriteActiveTenant` -- stays clamped at the
    checkpoint, so cutover keeps refusing until a later pass absorbs the
    remainder.
    """

    @pytest.mark.asyncio
    async def test_cap_hit_still_transitions_with_anchor_clamped(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Force every catch-up round to report failures still outstanding.

        Rather than fighting the copier into a genuine sustained mirror
        failure (which the retry/duplicate handling in `BulkCopier` is
        specifically designed to resolve), fake the always-failing mirror
        at the interceptor's public attestation boundary:
        `mark_copy_pass_complete` never reports zero remaining, and
        `safe_lag_anchor` never advances past the checkpoint -- exactly
        the observable contract a permanently-saturated interceptor
        already has (see `test_saturated_failure_history_never_reports_clean`).
        This isolates the coordinator's bounded-loop control flow from the
        interceptor's own bookkeeping, which is covered elsewhere.
        """
        monkeypatch.setattr(
            DualWriteInterceptor,
            "mark_copy_pass_complete",
            lambda self, checkpoint: 1,
        )
        monkeypatch.setattr(
            DualWriteInterceptor,
            "safe_lag_anchor",
            lambda self, checkpoint: checkpoint,
        )

        await create_test_events(source_store, tenant_id, count=3)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )
        router.register_store("dedicated", target_store)

        with caplog.at_level(logging.WARNING, logger="eventsource.migration.coordinator"):
            migration = await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=target_store,
                target_store_id="dedicated",
                config=MigrationConfig(
                    cutover_max_lag_events=2,
                    verify_consistency=False,
                    migrate_subscriptions=False,
                ),
            )

            # The loop terminates (does not hang) and still reaches DUAL_WRITE.
            migration = await coordinator.wait_for_phase(
                migration.id, MigrationPhase.DUAL_WRITE, timeout=5.0, poll_interval=0.01
            )

        assert migration.phase == MigrationPhase.DUAL_WRITE
        assert any(
            "mirror failures remain unabsorbed" in record.message for record in caplog.records
        )

        # More source events land after the checkpoint. A working anchor
        # would still be safe to advance on a healthy mirror, but with the
        # cap hit and the mirror faked as permanently saturated, the
        # anchor is pinned at the checkpoint -- these count as lag.
        await create_test_events(source_store, tenant_id, count=5)

        # The anchor never advanced past the checkpoint, so cutover refuses.
        result = await coordinator.trigger_cutover(migration.id)
        assert result.success is False


# =============================================================================
# Cutover Tests
# =============================================================================


class TestCutoverSuccess:
    """Tests for successful cutover scenarios."""

    @pytest.mark.asyncio
    async def test_cutover_success_when_synced(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test successful cutover when stores are synchronized."""
        migration_id = uuid4()
        last_source_position: Position | None = None

        # Setup: Same events in both stores (fully synced)
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            stream = StreamId(aggregate_id=aggregate_id, category="SampleAggregate")
            result = await source_store.append(stream, [event], ExpectedVersion.no_stream())
            last_source_position = result.position
            await target_store.append(stream, [event], ExpectedVersion.no_stream())

        # Setup routing state
        await routing_repo.set_migration_state(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id,
        )

        # Register target store
        router.register_store("dedicated", target_store)

        # Create lag tracker
        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            config=MigrationConfig(cutover_max_lag_events=100),
            enable_tracing=False,
        )

        # Create cutover manager
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        # Execute cutover -- `since` the last position already copied, so
        # the lag tracker sees zero events behind.
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            timeout_ms=500.0,
            since=last_source_position,
        )

        assert result.success
        assert result.duration_ms > 0

        # Verify routing was updated
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.store_id == "dedicated"
        assert routing.migration_state == TenantMigrationState.MIGRATED


class TestCutoverFailureAndRollback:
    """Tests for cutover failure and rollback scenarios."""

    @pytest.mark.asyncio
    async def test_cutover_fails_when_lag_too_high(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test cutover fails when sync lag exceeds threshold."""
        migration_id = uuid4()

        # Setup: Source has many more events than target, and nothing has
        # been copied yet (since=None), so every source event counts as lag.
        for _i in range(200):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Target has only 10 events
        for _i in range(10):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await target_store.append(
                StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Setup routing state
        await routing_repo.set_migration_state(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id,
        )

        router.register_store("dedicated", target_store)

        # Create lag tracker with strict threshold
        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            config=MigrationConfig(cutover_max_lag_events=50),  # Strict threshold
            enable_tracing=False,
        )

        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            config=MigrationConfig(cutover_max_lag_events=50),
            timeout_ms=500.0,
        )

        assert result.success is False
        assert result.rolled_back is True
        assert "lag" in result.error_message.lower()

        # Verify routing was rolled back to DUAL_WRITE
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.DUAL_WRITE

    @pytest.mark.asyncio
    async def test_cutover_fails_on_lock_acquisition_failure(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test cutover fails when lock cannot be acquired."""
        migration_id = uuid4()

        # Use failing lock manager
        failing_lock_manager = MockLockManager(should_fail=True)

        await routing_repo.set_migration_state(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id,
        )

        router.register_store("dedicated", target_store)

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        cutover_manager = CutoverManager(
            lock_manager=failing_lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            timeout_ms=500.0,
        )

        assert result.success is False
        assert "lock" in result.error_message.lower()


# =============================================================================
# Abort Tests
# =============================================================================


class TestAbortDuringDifferentPhases:
    """Tests for aborting migration during different phases."""

    @pytest.mark.asyncio
    async def test_abort_during_bulk_copy(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test aborting migration during bulk copy phase."""
        # Create some events in source
        await create_test_events(source_store, tenant_id, count=10)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Abort the migration
        result = await coordinator.abort_migration(
            migration.id,
            reason="Test abort during bulk copy",
        )

        assert result is not None
        assert result.success is False
        assert result.final_phase == MigrationPhase.ABORTED

        # Verify migration state
        migration = await coordinator.get_migration(result.migration_id)
        assert migration is not None
        assert migration.phase == MigrationPhase.ABORTED

    @pytest.mark.asyncio
    async def test_abort_clears_routing_state(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that abort clears routing migration state."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        await coordinator.abort_migration(migration.id, reason="Test abort")

        # Verify routing state was reset
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.NORMAL


# =============================================================================
# Pause/Resume Tests
# =============================================================================


class TestPauseResumeDuringMigration:
    """Tests for pause/resume functionality during migration."""

    @pytest.mark.asyncio
    async def test_pause_migration(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test pausing a migration."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Pause the migration
        await coordinator.pause_migration(migration.id)

        # Verify migration is paused
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        assert migration.is_paused

    @pytest.mark.asyncio
    async def test_resume_migration(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test resuming a paused migration."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Pause then resume
        await coordinator.pause_migration(migration.id)
        await coordinator.resume_migration(migration.id)

        # Verify migration is resumed
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        assert migration.is_paused is False


# =============================================================================
# Error Handling Tests
# =============================================================================


class TestErrorHandlingAndRecovery:
    """Tests for error handling and recovery scenarios."""

    @pytest.mark.asyncio
    async def test_duplicate_migration_raises_error(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that starting duplicate migration raises error."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)
        router.register_store("dedicated2", target_store)

        # Start first migration
        await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Try to start second migration for same tenant
        with pytest.raises(MigrationAlreadyExistsError):
            await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=target_store,
                target_store_id="dedicated2",
            )

    @pytest.mark.asyncio
    async def test_get_status_nonexistent_migration(
        self,
        source_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
    ) -> None:
        """Test getting status of nonexistent migration raises error."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        nonexistent_id = uuid4()

        with pytest.raises(MigrationNotFoundError):
            await coordinator.get_status(nonexistent_id)

    @pytest.mark.asyncio
    async def test_abort_completed_migration_raises_error(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that aborting completed migration raises error."""
        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        router.register_store("dedicated", target_store)

        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Force migration to completed state
        # Note: start_migration already puts it in BULK_COPY phase
        await migration_repo.update_phase(migration.id, MigrationPhase.DUAL_WRITE)
        await migration_repo.update_phase(migration.id, MigrationPhase.CUTOVER)
        await migration_repo.update_phase(migration.id, MigrationPhase.COMPLETED)

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.abort_migration(migration.id)

        assert "completed" in str(exc_info.value).lower()


# =============================================================================
# Concurrent Operations Tests
# =============================================================================


class TestConcurrentOperations:
    """Tests for concurrent operations during migration."""

    @pytest.mark.asyncio
    async def test_concurrent_writes_during_dual_write(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test concurrent writes during dual-write phase."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        async def write_events(count: int, prefix: str) -> list[UUID]:
            """Write multiple events concurrently."""
            ids = []
            for i in range(count):
                aggregate_id = uuid4()
                ids.append(aggregate_id)

                event = SampleTestEvent(
                    aggregate_id=aggregate_id,
                    tenant_id=tenant_id,
                    value=f"{prefix}-{i}",
                )

                await interceptor.append(
                    StreamId(aggregate_id=aggregate_id, category="SampleAggregate"),
                    [event],
                    ExpectedVersion.no_stream(),
                )

            return ids

        # Run concurrent writes
        results = await asyncio.gather(
            write_events(5, "writer-1"),
            write_events(5, "writer-2"),
            write_events(5, "writer-3"),
        )

        total_ids = sum(len(r) for r in results)
        assert total_ids == 15

        # Verify all events in source
        source_events = [e async for e in source_store.read_all()]
        assert len(source_events) == 15

        # Verify all events in target
        target_events = [e async for e in target_store.read_all()]
        assert len(target_events) == 15

    @pytest.mark.asyncio
    async def test_write_pause_blocks_concurrent_writers(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test that write pause blocks concurrent writers."""
        write_started = asyncio.Event()
        write_completed = asyncio.Event()

        async def writer_task():
            """Task that waits if paused."""
            write_started.set()
            wait_time = await write_pause_manager.wait_if_paused(
                tenant_id,
                timeout=5.0,
            )
            write_completed.set()
            return wait_time

        # Pause writes
        await write_pause_manager.pause_writes(tenant_id)

        # Start writer task
        task = asyncio.create_task(writer_task())

        # Wait for writer to start waiting
        await write_started.wait()
        await asyncio.sleep(0.05)  # Give it time to block

        # Writer should not have completed yet
        assert not write_completed.is_set()

        # Resume writes
        await write_pause_manager.resume_writes(tenant_id)

        # Writer should complete
        wait_time = await task
        assert wait_time > 0
        assert write_completed.is_set()

    @pytest.mark.asyncio
    async def test_multiple_tenants_independent(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
    ) -> None:
        """Test that multiple tenants operate independently."""
        tenant1 = uuid4()
        tenant2 = uuid4()

        interceptor1 = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant1,
            enable_tracing=False,
        )

        interceptor2 = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant2,
            enable_tracing=False,
        )

        # Write events for tenant1
        agg1 = uuid4()
        event1 = SampleTestEvent(aggregate_id=agg1, tenant_id=tenant1, value="tenant1")
        stream1 = StreamId(aggregate_id=agg1, category="SampleAggregate")
        await interceptor1.append(stream1, [event1], ExpectedVersion.no_stream())

        # Write events for tenant2
        agg2 = uuid4()
        event2 = SampleTestEvent(aggregate_id=agg2, tenant_id=tenant2, value="tenant2")
        stream2 = StreamId(aggregate_id=agg2, category="SampleAggregate")
        await interceptor2.append(stream2, [event2], ExpectedVersion.no_stream())

        # Verify both tenants have their events
        events1 = [e async for e in source_store.read_stream(stream1)]
        assert len(events1) == 1
        assert events1[0].event.tenant_id == tenant1

        events2 = [e async for e in source_store.read_stream(stream2)]
        assert len(events2) == 1
        assert events2[0].event.tenant_id == tenant2


# =============================================================================
# Write Pause Manager Tests
# =============================================================================


class TestWritePauseManager:
    """Tests for WritePauseManager functionality."""

    @pytest.mark.asyncio
    async def test_pause_and_resume_flow(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test basic pause and resume flow."""
        # Initially not paused
        assert not write_pause_manager.is_paused(tenant_id)

        # Pause
        created = await write_pause_manager.pause_writes(tenant_id)
        assert created is True
        assert write_pause_manager.is_paused(tenant_id)

        # Idempotent pause
        created_again = await write_pause_manager.pause_writes(tenant_id)
        assert created_again is False

        # Resume
        metrics = await write_pause_manager.resume_writes(tenant_id)
        assert metrics is not None
        assert metrics.duration_ms > 0
        assert not write_pause_manager.is_paused(tenant_id)

    @pytest.mark.asyncio
    async def test_wait_if_not_paused_returns_immediately(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test that wait returns immediately if not paused."""
        wait_time = await write_pause_manager.wait_if_paused(tenant_id)
        assert wait_time == 0.0

    @pytest.mark.asyncio
    async def test_pause_metrics_tracking(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test that pause metrics are properly tracked."""
        await write_pause_manager.pause_writes(tenant_id)
        await asyncio.sleep(0.05)  # Brief pause
        metrics = await write_pause_manager.resume_writes(tenant_id)

        assert metrics is not None
        assert metrics.tenant_id == tenant_id
        assert metrics.duration_ms >= 50  # At least 50ms
        assert metrics.started_at < metrics.ended_at

        # Check history
        history = write_pause_manager.get_metrics_history()
        assert len(history) == 1
        assert history[0].tenant_id == tenant_id
