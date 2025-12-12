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
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
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
from eventsource.stores.in_memory import InMemoryEventStore

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

        # Write through interceptor
        result = await interceptor.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event],
            expected_version=0,
        )

        assert result.success

        # Verify event in source
        source_stream = await source_store.get_events(aggregate_id, "Order")
        assert len(source_stream.events) == 1
        assert source_stream.events[0].event_id == event.event_id

        # Verify event in target
        target_stream = await target_store.get_events(aggregate_id, "Order")
        assert len(target_stream.events) == 1
        assert target_stream.events[0].event_id == event.event_id

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

        await source_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event],
            expected_version=0,
        )

        # Target is empty

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Read through interceptor
        stream = await interceptor.get_events(aggregate_id, "Order")

        # Should get event from source
        assert len(stream.events) == 1
        assert stream.events[0].event_id == event.event_id

    @pytest.mark.asyncio
    async def test_dual_write_handles_target_failure(
        self,
        source_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test that dual-write handles target store failure gracefully."""

        # Create a target store that will fail on append
        class FailingStore(InMemoryEventStore):
            async def append_events(self, *args, **kwargs):
                raise RuntimeError("Target store failure")

        failing_target = FailingStore(enable_tracing=False)

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

        # Write should succeed (source is authoritative)
        result = await interceptor.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event],
            expected_version=0,
        )

        assert result.success

        # Verify event in source
        source_stream = await source_store.get_events(aggregate_id, "Order")
        assert len(source_stream.events) == 1

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

        # Write first event
        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        await interceptor.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event1],
            expected_version=0,
        )

        # Write second event
        event2 = OrderConfirmed(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )
        await interceptor.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event2],
            expected_version=1,
        )

        # Verify both events in both stores
        source_stream = await source_store.get_events(aggregate_id, "Order")
        assert len(source_stream.events) == 2

        target_stream = await target_store.get_events(aggregate_id, "Order")
        assert len(target_stream.events) == 2


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
            await source_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )

        # Target has only 5 events
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await target_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

        assert lag is not None
        assert lag.source_position == 10
        assert lag.target_position == 5
        assert lag.events == 5  # 10 - 5 = 5 events behind

    @pytest.mark.asyncio
    async def test_sync_lag_converged_when_equal(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Test sync lag shows converged when stores are in sync."""
        # Same events in both stores
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )
            await target_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

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

        # Create small lag (10 events)
        for _i in range(20):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )

        for _i in range(10):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await target_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        await tracker.calculate_lag()

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
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Take multiple lag samples
        for _i in range(5):
            # Add event to source to increase lag
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )

            await tracker.calculate_lag()

        stats = tracker.get_lag_stats()

        assert stats.sample_count == 5
        assert stats.max_lag == 5  # Final lag after 5 events
        assert stats.min_lag == 1  # First lag was 1


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

        # Setup: Same events in both stores (fully synced)
        for _i in range(5):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )
            await target_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )

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

        # Execute cutover
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            timeout_ms=500.0,
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

        # Setup: Source has many more events than target
        for _i in range(200):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await source_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
            )

        # Target has only 10 events
        for _i in range(10):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
            )
            await target_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="SampleAggregate",
                events=[event],
                expected_version=0,
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

                await interceptor.append_events(
                    aggregate_id=aggregate_id,
                    aggregate_type="SampleAggregate",
                    events=[event],
                    expected_version=0,
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
        source_count = await source_store.get_event_count()
        assert source_count == 15

        # Verify all events in target
        target_count = await target_store.get_event_count()
        assert target_count == 15

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
        await interceptor1.append_events(
            aggregate_id=agg1,
            aggregate_type="SampleAggregate",
            events=[event1],
            expected_version=0,
        )

        # Write events for tenant2
        agg2 = uuid4()
        event2 = SampleTestEvent(aggregate_id=agg2, tenant_id=tenant2, value="tenant2")
        await interceptor2.append_events(
            aggregate_id=agg2,
            aggregate_type="SampleAggregate",
            events=[event2],
            expected_version=0,
        )

        # Verify both tenants have their events
        stream1 = await source_store.get_events(agg1, "SampleAggregate")
        assert len(stream1.events) == 1
        assert stream1.events[0].tenant_id == tenant1

        stream2 = await source_store.get_events(agg2, "SampleAggregate")
        assert len(stream2.events) == 1
        assert stream2.events[0].tenant_id == tenant2


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
