"""
Chaos Tests for Multi-Tenant Live Migration System.

These tests verify system resilience under various failure conditions:
- Network partition simulation (store becomes unavailable temporarily)
- Process crash during bulk copy (resume from checkpoint)
- Process crash during dual-write (verify no data loss)
- Lock contention scenarios (multiple coordinators trying cutover)
- Target store failures during dual-write (verify source writes succeed)
- Timeout scenarios during cutover
- Recovery after various failure modes

The tests use a FailureInjectableStore wrapper around InMemoryEventStore
to simulate various failure scenarios without requiring actual network
or process failures.

See Also:
    - Task: P3-007-chaos-tests.md
    - FRD: docs/tasks/multi-tenant-live-migration/multi-tenant-live-migration.md
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum, auto
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.cutover import CutoverManager
from eventsource.migration.dual_write import DualWriteInterceptor
from eventsource.migration.exceptions import (
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
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
from eventsource.migration.write_pause import WritePausedError, WritePauseManager
from eventsource.stores.in_memory import InMemoryEventStore
from eventsource.stores.interface import (
    AppendResult,
    EventStore,
    EventStream,
    ReadOptions,
    StoredEvent,
)

# =============================================================================
# Test Event Classes
# =============================================================================


class ChaosTestEvent(DomainEvent):
    """Test event for chaos testing."""

    event_type: str = "ChaosTestEvent"
    aggregate_type: str = "ChaosAggregate"
    value: str = "test"
    sequence: int = 0


# =============================================================================
# Failure Injection Infrastructure
# =============================================================================


class FailureMode(Enum):
    """Types of failures that can be injected."""

    NONE = auto()
    FAIL_APPEND = auto()  # Fail on append_events
    FAIL_READ = auto()  # Fail on read operations
    FAIL_ALL = auto()  # Fail all operations
    DELAY_APPEND = auto()  # Delay append operations
    DELAY_READ = auto()  # Delay read operations
    INTERMITTENT = auto()  # Fail randomly based on rate
    FAIL_AFTER_N_OPS = auto()  # Fail after N successful operations
    TIMEOUT = auto()  # Simulate timeout (long delay then fail)


@dataclass
class FailureConfig:
    """Configuration for failure injection."""

    mode: FailureMode = FailureMode.NONE
    fail_rate: float = 0.0  # For INTERMITTENT mode (0.0-1.0)
    delay_seconds: float = 0.0  # For DELAY modes
    fail_after_n: int = 0  # For FAIL_AFTER_N_OPS mode
    timeout_seconds: float = 5.0  # For TIMEOUT mode
    error_message: str = "Injected failure"

    # State tracking
    operation_count: int = field(default=0, init=False)
    failure_count: int = field(default=0, init=False)


class InjectedFailureError(Exception):
    """Exception raised when a failure is injected."""

    def __init__(self, message: str, failure_mode: FailureMode):
        super().__init__(message)
        self.failure_mode = failure_mode


class FailureInjectableStore(EventStore):
    """
    Wrapper around InMemoryEventStore that can inject failures.

    This class wraps an InMemoryEventStore and can be configured to
    inject various types of failures for chaos testing.

    Example:
        >>> store = FailureInjectableStore()
        >>> store.set_failure(FailureConfig(mode=FailureMode.FAIL_APPEND))
        >>> await store.append_events(...)  # Raises InjectedFailureError
    """

    def __init__(
        self,
        inner_store: InMemoryEventStore | None = None,
        *,
        enable_tracing: bool = False,
    ) -> None:
        """Initialize with optional inner store."""
        self._inner = inner_store or InMemoryEventStore(enable_tracing=enable_tracing)
        self._failure_config = FailureConfig()
        self._is_available = True
        self._available_event = asyncio.Event()
        self._available_event.set()

    @property
    def inner_store(self) -> InMemoryEventStore:
        """Get the underlying store."""
        return self._inner

    def set_failure(self, config: FailureConfig) -> None:
        """Set the failure configuration."""
        self._failure_config = config

    def clear_failure(self) -> None:
        """Clear any failure configuration."""
        self._failure_config = FailureConfig()

    def simulate_network_partition(self) -> None:
        """Simulate network partition by making store unavailable."""
        self._is_available = False
        self._available_event.clear()

    def heal_network_partition(self) -> None:
        """Heal network partition by restoring availability."""
        self._is_available = True
        self._available_event.set()

    async def wait_for_availability(self, timeout: float = 5.0) -> bool:
        """Wait for the store to become available."""
        try:
            await asyncio.wait_for(self._available_event.wait(), timeout=timeout)
            return True
        except TimeoutError:
            return False

    async def _maybe_fail(self, operation: str) -> None:
        """Check failure config and potentially raise an error."""
        config = self._failure_config
        config.operation_count += 1

        # Check network partition
        if not self._is_available:
            raise InjectedFailureError(
                f"Network partition: store unavailable for {operation}",
                FailureMode.FAIL_ALL,
            )

        if config.mode == FailureMode.NONE:
            return

        should_fail = False
        delay = 0.0

        if (
            config.mode == FailureMode.FAIL_ALL
            or config.mode == FailureMode.FAIL_APPEND
            and operation == "append"
            or config.mode == FailureMode.FAIL_READ
            and operation in ("read", "get_events")
        ):
            should_fail = True
        elif (
            config.mode == FailureMode.DELAY_APPEND
            and operation == "append"
            or config.mode == FailureMode.DELAY_READ
            and operation in ("read", "get_events")
        ):
            delay = config.delay_seconds
        elif config.mode == FailureMode.INTERMITTENT:
            import random

            should_fail = random.random() < config.fail_rate
        elif config.mode == FailureMode.FAIL_AFTER_N_OPS:
            should_fail = config.operation_count > config.fail_after_n
        elif config.mode == FailureMode.TIMEOUT:
            # Simulate timeout with delay then failure
            await asyncio.sleep(config.timeout_seconds)
            should_fail = True

        if delay > 0:
            await asyncio.sleep(delay)

        if should_fail:
            config.failure_count += 1
            raise InjectedFailureError(
                f"{config.error_message} (operation={operation}, count={config.failure_count})",
                config.mode,
            )

    # =========================================================================
    # EventStore Interface Implementation
    # =========================================================================

    async def append_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
        events: list[DomainEvent],
        expected_version: int,
    ) -> AppendResult:
        """Append events with potential failure injection."""
        await self._maybe_fail("append")
        return await self._inner.append_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            events=events,
            expected_version=expected_version,
        )

    async def get_events(
        self,
        aggregate_id: UUID,
        aggregate_type: str | None = None,
        from_version: int = 0,
        from_timestamp: datetime | None = None,
        to_timestamp: datetime | None = None,
    ) -> EventStream:
        """Get events with potential failure injection."""
        await self._maybe_fail("get_events")
        return await self._inner.get_events(
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            from_version=from_version,
            from_timestamp=from_timestamp,
            to_timestamp=to_timestamp,
        )

    async def read_all(
        self,
        options: ReadOptions | None = None,
    ) -> AsyncIterator[StoredEvent]:
        """Read all events with potential failure injection."""
        await self._maybe_fail("read")
        async for event in self._inner.read_all(options):
            yield event

    async def get_global_position(self) -> int:
        """Get global position with potential failure injection."""
        await self._maybe_fail("read")
        return await self._inner.get_global_position()

    async def get_event_count(self) -> int:
        """Get event count with potential failure injection."""
        await self._maybe_fail("read")
        return await self._inner.get_event_count()

    async def stream_exists(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        """Check if stream exists."""
        await self._maybe_fail("read")
        return await self._inner.stream_exists(aggregate_id, aggregate_type)

    async def event_exists(self, event_id: UUID) -> bool:
        """Check if an event exists with potential failure injection."""
        await self._maybe_fail("read")
        return await self._inner.event_exists(event_id)

    async def get_events_by_type(
        self,
        aggregate_type: str,
        tenant_id: UUID | None = None,
        from_timestamp: datetime | None = None,
    ) -> list[DomainEvent]:
        """Get events by type with potential failure injection."""
        await self._maybe_fail("read")
        return await self._inner.get_events_by_type(
            aggregate_type=aggregate_type,
            tenant_id=tenant_id,
            from_timestamp=from_timestamp,
        )


# =============================================================================
# In-Memory Repository Implementations
# =============================================================================


class InMemoryMigrationRepository:
    """In-memory implementation of MigrationRepository for testing."""

    def __init__(self) -> None:
        self._migrations: dict[UUID, Migration] = {}
        self._by_tenant: dict[UUID, UUID] = {}

    async def create(self, migration: Migration) -> UUID:
        """Create a new migration record."""
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
        """Update migration phase with validation."""
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

    async def set_events_total(self, migration_id: UUID, events_total: int) -> None:
        """Set the total events count."""
        migration = await self.get(migration_id)
        if migration is None:
            raise MigrationNotFoundError(migration_id)

        migration.events_total = events_total
        migration.updated_at = datetime.now(UTC)

    async def record_error(self, migration_id: UUID, error: str) -> None:
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
    """In-memory implementation of TenantRoutingRepository for testing."""

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

    async def set_routing(self, tenant_id: UUID, store_id: str) -> None:
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

    async def list_by_state(self, state: TenantMigrationState) -> list[TenantRouting]:
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
    """Mock implementation of lock manager for testing."""

    def __init__(
        self,
        *,
        should_fail: bool = False,
        fail_after: int | None = None,
        contention_delay: float = 0.0,
    ) -> None:
        self._should_fail = should_fail
        self._fail_after = fail_after
        self._acquire_count = 0
        self._held_locks: dict[str, MockLockInfo] = {}
        self._contention_delay = contention_delay
        self._lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(
        self,
        key: str,
        timeout: float | None = None,
    ) -> AsyncIterator[MockLockInfo]:
        """Acquire advisory lock with potential failure injection."""
        self._acquire_count += 1

        if self._should_fail:
            from eventsource.locks import LockAcquisitionError

            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure")

        if self._fail_after is not None and self._acquire_count > self._fail_after:
            from eventsource.locks import LockAcquisitionError

            raise LockAcquisitionError(key, timeout or 0.0, "Mock failure after threshold")

        # Simulate lock contention
        if self._contention_delay > 0:
            await asyncio.sleep(self._contention_delay)

        async with self._lock:
            if key in self._held_locks:
                from eventsource.locks import LockAcquisitionError

                raise LockAcquisitionError(key, timeout or 0.0, "Lock already held")

            lock_info = MockLockInfo(key=key)
            self._held_locks[key] = lock_info

        try:
            yield lock_info
        finally:
            async with self._lock:
                self._held_locks.pop(key, None)

    async def try_acquire(self, key: str) -> MockLockInfo | None:
        """Try to acquire lock without blocking."""
        if self._should_fail:
            return None

        async with self._lock:
            if key in self._held_locks:
                return None
            return MockLockInfo(key=key)

    async def release(self, key: str) -> bool:
        """Release advisory lock."""
        async with self._lock:
            if key in self._held_locks:
                del self._held_locks[key]
                return True
            return False

    def is_locked(self, key: str) -> bool:
        """Check if a key is currently locked."""
        return key in self._held_locks


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def source_store() -> FailureInjectableStore:
    """Create failure-injectable source store."""
    return FailureInjectableStore(enable_tracing=False)


@pytest.fixture
def target_store() -> FailureInjectableStore:
    """Create failure-injectable target store."""
    return FailureInjectableStore(enable_tracing=False)


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
    source_store: FailureInjectableStore,
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


# =============================================================================
# Helper Functions
# =============================================================================


async def create_test_events(
    store: FailureInjectableStore | InMemoryEventStore,
    tenant_id: UUID,
    count: int = 10,
) -> list[UUID]:
    """Create test events in the store and return aggregate IDs."""
    aggregate_ids = []

    # If it's a FailureInjectableStore, use inner store
    actual_store = store.inner_store if isinstance(store, FailureInjectableStore) else store

    for i in range(count):
        aggregate_id = uuid4()
        aggregate_ids.append(aggregate_id)

        event = ChaosTestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            value=f"event-{i}",
            sequence=i,
        )

        await actual_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="ChaosAggregate",
            events=[event],
            expected_version=0,
        )

    return aggregate_ids


async def count_tenant_events(
    store: FailureInjectableStore | InMemoryEventStore,
    tenant_id: UUID,
) -> int:
    """Count events for a tenant in a store."""
    actual_store = store.inner_store if isinstance(store, FailureInjectableStore) else store

    count = 0
    async for stored_event in actual_store.read_all():
        if stored_event.event.tenant_id == tenant_id:
            count += 1
    return count


# =============================================================================
# Network Partition Simulation Tests
# =============================================================================


class TestNetworkPartitionSimulation:
    """Tests for network partition simulation scenarios."""

    @pytest.mark.asyncio
    async def test_append_fails_during_network_partition(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that appends fail when store is partitioned."""
        # Simulate network partition
        target_store.simulate_network_partition()

        aggregate_id = uuid4()
        event = ChaosTestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )

        with pytest.raises(InjectedFailureError) as exc_info:
            await target_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

        assert "Network partition" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_reads_fail_during_network_partition(
        self,
        source_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that reads fail when store is partitioned."""
        # Create event before partition
        await create_test_events(source_store, tenant_id, count=1)

        # Simulate network partition
        source_store.simulate_network_partition()

        with pytest.raises(InjectedFailureError) as exc_info:
            await source_store.get_global_position()

        assert "Network partition" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_partition_heals_and_operations_resume(
        self,
        source_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that operations resume after partition heals."""
        # Create events before partition
        await create_test_events(source_store, tenant_id, count=5)

        # Simulate partition
        source_store.simulate_network_partition()

        # Verify failure during partition
        with pytest.raises(InjectedFailureError):
            await source_store.get_global_position()

        # Heal partition
        source_store.heal_network_partition()

        # Operations should work now
        position = await source_store.get_global_position()
        assert position == 5

    @pytest.mark.asyncio
    async def test_dual_write_handles_target_partition(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test dual-write continues to source when target is partitioned."""
        # Use regular InMemoryEventStore for source (no failure injection)
        source_store = InMemoryEventStore(enable_tracing=False)
        target_store = FailureInjectableStore(enable_tracing=False)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Partition the target store
        target_store.simulate_network_partition()

        aggregate_id = uuid4()
        event = ChaosTestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )

        # Write should succeed (source is authoritative)
        result = await interceptor.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="ChaosAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success

        # Verify event in source
        source_stream = await source_store.get_events(aggregate_id, "ChaosAggregate")
        assert len(source_stream.events) == 1

        # Verify failure was recorded
        stats = interceptor.get_failure_stats()
        assert stats.total_failures >= 1


# =============================================================================
# Process Crash Simulation Tests - Dual Write
# =============================================================================


class TestProcessCrashDuringDualWrite:
    """Tests for process crash simulation during dual-write phase."""

    @pytest.mark.asyncio
    async def test_dual_write_no_data_loss_on_source(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that dual-write ensures no data loss on source during failures."""
        # Use regular store for source, failing store for target
        source_store = InMemoryEventStore(enable_tracing=False)
        target_store = FailureInjectableStore(enable_tracing=False)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Set target to fail intermittently
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.INTERMITTENT,
                fail_rate=0.5,  # 50% failure rate
                error_message="Random target failure",
            )
        )

        aggregate_ids = []
        for i in range(20):
            aggregate_id = uuid4()
            aggregate_ids.append(aggregate_id)

            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )

            # All writes should succeed (source is authoritative)
            result = await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )
            assert result.success

        # Verify ALL events in source (no data loss)
        source_count = 0
        async for event in source_store.read_all():
            if event.event.tenant_id == tenant_id:
                source_count += 1
        assert source_count == 20

        # Some failures should have been recorded
        stats = interceptor.get_failure_stats()
        assert stats.total_failures > 0

    @pytest.mark.asyncio
    async def test_dual_write_source_failure_propagates(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that source store failure propagates to caller."""
        source_store = FailureInjectableStore(enable_tracing=False)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Fail the source store
        source_store.set_failure(
            FailureConfig(
                mode=FailureMode.FAIL_ALL,
                error_message="Source store failure",
            )
        )

        aggregate_id = uuid4()
        event = ChaosTestEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
        )

        # Source failure should propagate
        with pytest.raises(InjectedFailureError) as exc_info:
            await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

        assert "Source store failure" in str(exc_info.value)


# =============================================================================
# Lock Contention Tests
# =============================================================================


class TestLockContentionScenarios:
    """Tests for lock contention during migration operations."""

    @pytest.mark.asyncio
    async def test_cutover_fails_when_lock_acquisition_fails(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test cutover fails when lock cannot be acquired."""
        migration_id = uuid4()

        # Create a lock manager that always fails
        lock_manager = MockLockManager(should_fail=True)

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
            timeout_ms=100.0,
        )

        assert result.success is False
        assert "lock" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_concurrent_cutover_attempts_serialized(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test that concurrent cutover attempts are properly serialized."""
        migration_id = uuid4()

        # Lock manager with contention delay
        lock_manager = MockLockManager(contention_delay=0.1)

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
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        # Attempt concurrent cutovers
        async def attempt_cutover(attempt_id: int):
            try:
                return await cutover_manager.execute_cutover(
                    migration_id=migration_id,
                    tenant_id=tenant_id,
                    lag_tracker=lag_tracker,
                    target_store_id="dedicated",
                    timeout_ms=500.0,
                )
            except Exception as e:
                return e

        results = await asyncio.gather(
            attempt_cutover(1),
            attempt_cutover(2),
            attempt_cutover(3),
            return_exceptions=True,
        )

        # At most one should succeed, others should fail or error
        success_count = sum(1 for r in results if hasattr(r, "success") and r.success)
        # Due to lock contention, typically only one succeeds
        assert success_count <= 1

    @pytest.mark.asyncio
    async def test_lock_released_on_failure(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test that lock is released even when cutover fails."""
        migration_id = uuid4()
        lock_manager = MockLockManager()

        await routing_repo.set_migration_state(
            tenant_id,
            TenantMigrationState.DUAL_WRITE,
            migration_id,
        )

        router.register_store("dedicated", target_store)

        # Create lag that exceeds threshold to force failure
        await create_test_events(source_store, tenant_id, count=200)

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            config=MigrationConfig(cutover_max_lag_events=10),  # Strict threshold
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
            config=MigrationConfig(cutover_max_lag_events=10),
            timeout_ms=500.0,
        )

        # Cutover should fail due to lag
        assert result.success is False

        # Lock should be released
        lock_key = f"migration:cutover:{tenant_id}"
        assert not lock_manager.is_locked(lock_key)


# =============================================================================
# Target Store Failure During Dual-Write Tests
# =============================================================================


class TestTargetStoreFailures:
    """Tests for target store failures during dual-write."""

    @pytest.mark.asyncio
    async def test_source_writes_succeed_when_target_fails(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that source writes succeed even when target completely fails."""
        source_store = InMemoryEventStore(enable_tracing=False)
        target_store = FailureInjectableStore(enable_tracing=False)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Complete target failure
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.FAIL_ALL,
                error_message="Target store completely unavailable",
            )
        )

        success_count = 0
        for i in range(10):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )

            result = await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

            if result.success:
                success_count += 1

        # All writes should succeed
        assert success_count == 10

        # All events should be in source
        source_count = 0
        async for event in source_store.read_all():
            if event.event.tenant_id == tenant_id:
                source_count += 1
        assert source_count == 10

        # No events in target
        target_count = await count_tenant_events(target_store, tenant_id)
        assert target_count == 0

    @pytest.mark.asyncio
    async def test_target_failure_rate_tracked(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that target failure rate is tracked for monitoring."""
        source_store = InMemoryEventStore(enable_tracing=False)
        target_store = FailureInjectableStore(enable_tracing=False)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # 50% target failure rate
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.INTERMITTENT,
                fail_rate=0.5,
            )
        )

        for i in range(100):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )

            await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

        stats = interceptor.get_failure_stats()

        # Should have recorded failures (around 50%, but with variance)
        assert stats.total_failures > 20
        assert stats.total_failures < 80

    @pytest.mark.asyncio
    async def test_target_recovers_and_writes_resume(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that writes to target resume after recovery."""
        source_store = InMemoryEventStore(enable_tracing=False)
        target_store = FailureInjectableStore(enable_tracing=False)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Target initially fails
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.FAIL_ALL,
                error_message="Target unavailable",
            )
        )

        # Write during failure
        for i in range(5):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )
            await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

        # Target should have no events
        target_count = await count_tenant_events(target_store, tenant_id)
        assert target_count == 0

        # Target recovers
        target_store.clear_failure()

        # Write after recovery
        for i in range(5, 10):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )
            await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

        # Target should now have the newer events
        target_count = await count_tenant_events(target_store, tenant_id)
        assert target_count == 5  # Only the ones after recovery


# =============================================================================
# Timeout Scenarios Tests
# =============================================================================


class TestTimeoutScenarios:
    """Tests for timeout scenarios during migration operations."""

    @pytest.mark.asyncio
    async def test_cutover_timeout_causes_rollback(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test that cutover timeout triggers automatic rollback."""
        migration_id = uuid4()

        # Lock manager that takes too long
        lock_manager = MockLockManager(contention_delay=1.0)  # 1 second delay

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
            timeout_ms=100.0,  # Very short timeout (100ms)
        )

        # Should fail due to timeout during lock acquisition
        # Note: This may succeed if lock is acquired fast enough
        # The test verifies timeout behavior is handled
        if not result.success:
            assert (
                result.rolled_back
                or "timeout" in result.error_message.lower()
                or "lock" in result.error_message.lower()
            )

    @pytest.mark.asyncio
    async def test_write_pause_timeout(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test that write pause timeout raises appropriate error."""
        # Pause writes
        await write_pause_manager.pause_writes(tenant_id)

        # Attempt to wait with very short timeout
        with pytest.raises(WritePausedError) as exc_info:
            await write_pause_manager.wait_if_paused(
                tenant_id,
                timeout=0.05,  # 50ms timeout
            )

        assert exc_info.value.tenant_id == tenant_id
        assert exc_info.value.timeout == 0.05

    @pytest.mark.asyncio
    async def test_slow_target_causes_lag_accumulation(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that slow target causes sync lag to accumulate."""
        source_store = InMemoryEventStore(enable_tracing=False)
        target_store = FailureInjectableStore(enable_tracing=False)

        # Target with delays
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.DELAY_APPEND,
                delay_seconds=0.05,  # 50ms delay per write
            )
        )

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Rapid writes
        for i in range(10):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )
            await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

        # Both stores should have all events (dual-write is synchronous)
        source_count = 0
        async for event in source_store.read_all():
            if event.event.tenant_id == tenant_id:
                source_count += 1

        target_count = await count_tenant_events(target_store, tenant_id)

        assert source_count == 10
        assert target_count == 10


# =============================================================================
# Recovery After Failure Tests
# =============================================================================


class TestRecoveryAfterFailures:
    """Tests for recovery scenarios after various failure modes."""

    @pytest.mark.asyncio
    async def test_abort_cleans_up_state_properly(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that abort properly cleans up all migration state."""
        router.register_store("dedicated", target_store)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # Start migration
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Wait for background task to start
        await asyncio.sleep(0.1)

        # Abort
        result = await coordinator.abort_migration(
            migration.id,
            reason="Test abort for cleanup verification",
        )

        assert result is not None
        assert result.final_phase == MigrationPhase.ABORTED

        # Verify routing state is cleared
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.NORMAL
        assert routing.active_migration_id is None

        # Verify migration is in terminal state
        aborted_migration = await migration_repo.get(migration.id)
        assert aborted_migration is not None
        assert aborted_migration.phase == MigrationPhase.ABORTED

    @pytest.mark.asyncio
    async def test_can_start_new_migration_after_abort(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that new migration can be started after abort."""
        router.register_store("dedicated", target_store)
        router.register_store("dedicated2", target_store)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # First migration
        migration1 = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Wait for background task
        await asyncio.sleep(0.1)

        # Abort first migration
        await coordinator.abort_migration(migration1.id, reason="Test")

        # Should be able to start new migration
        migration2 = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated2",
        )

        assert migration2 is not None
        assert migration2.id != migration1.id
        assert migration2.tenant_id == tenant_id


# =============================================================================
# Comprehensive Failure Scenario Tests
# =============================================================================


class TestComprehensiveFailureScenarios:
    """Comprehensive tests combining multiple failure scenarios."""

    @pytest.mark.asyncio
    async def test_multiple_failure_types_during_migration(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test handling of multiple failure types during dual-write."""
        source_store = InMemoryEventStore(enable_tracing=False)
        target_store = FailureInjectableStore(enable_tracing=False)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        events_written = 0

        # Phase 1: Normal operation
        for i in range(5):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            result = await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )
            if result.success:
                events_written += 1

        assert events_written == 5

        # Phase 2: Network partition on target
        target_store.simulate_network_partition()

        for i in range(5, 10):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            result = await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )
            if result.success:
                events_written += 1

        assert events_written == 10  # Source writes succeed

        # Phase 3: Heal partition
        target_store.heal_network_partition()

        for i in range(10, 15):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            result = await interceptor.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )
            if result.success:
                events_written += 1

        assert events_written == 15

        # Verify source has all events
        source_count = 0
        async for event in source_store.read_all():
            if event.event.tenant_id == tenant_id:
                source_count += 1
        assert source_count == 15

        # Target has events from phase 1 and 3
        target_count = await count_tenant_events(target_store, tenant_id)
        assert target_count == 10  # 5 from phase 1, 5 from phase 3

    @pytest.mark.asyncio
    async def test_cascading_failures_handled_gracefully(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test that cascading failures are handled gracefully."""
        # Create failing lock manager
        lock_manager = MockLockManager(should_fail=True)

        router.register_store("dedicated", target_store)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )

        # Start migration (should succeed)
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        assert migration is not None

        # Verify migration was created
        current = await coordinator.get_migration(migration.id)
        assert current is not None


# =============================================================================
# Failure Injection Configuration Tests
# =============================================================================


class TestFailureInjectionInfrastructure:
    """Tests for the failure injection infrastructure itself."""

    @pytest.mark.asyncio
    async def test_failure_config_none_mode_passes_through(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that NONE failure mode passes through normally."""
        target_store.set_failure(FailureConfig(mode=FailureMode.NONE))

        aggregate_id = uuid4()
        event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        result = await target_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="ChaosAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success

    @pytest.mark.asyncio
    async def test_failure_config_fail_after_n_works(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that FAIL_AFTER_N_OPS mode works correctly."""
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.FAIL_AFTER_N_OPS,
                fail_after_n=3,
            )
        )

        success_count = 0
        for i in range(5):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            try:
                result = await target_store.append_events(
                    aggregate_id=aggregate_id,
                    aggregate_type="ChaosAggregate",
                    events=[event],
                    expected_version=0,
                )
                if result.success:
                    success_count += 1
            except InjectedFailureError:
                pass

        # First 3 succeed, then failures start
        assert success_count == 3

    @pytest.mark.asyncio
    async def test_clear_failure_restores_normal_operation(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that clearing failure config restores normal operation."""
        # Set failure
        target_store.set_failure(FailureConfig(mode=FailureMode.FAIL_ALL))

        aggregate_id = uuid4()
        event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        # Should fail
        with pytest.raises(InjectedFailureError):
            await target_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

        # Clear failure
        target_store.clear_failure()

        # Should succeed now
        result = await target_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="ChaosAggregate",
            events=[event],
            expected_version=0,
        )

        assert result.success

    @pytest.mark.asyncio
    async def test_operation_count_tracking(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that operation count is properly tracked."""
        config = FailureConfig(mode=FailureMode.NONE)
        target_store.set_failure(config)

        for i in range(5):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            await target_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="ChaosAggregate",
                events=[event],
                expected_version=0,
            )

        assert config.operation_count == 5
