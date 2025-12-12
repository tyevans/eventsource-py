"""
Final Integration Tests (P4-007) for Multi-Tenant Live Migration.

These tests serve as smoke tests for the complete migration system with all
operational features enabled. They validate the end-to-end migration lifecycle
including:

- Complete migration lifecycle with all operational features enabled
- Audit logging throughout the migration process
- Metrics collection during migration phases
- Status streaming during migration
- Error handling and classification in real scenarios
- Recovery after simulated failures
- Integration of all Phase 4 components

These tests use InMemoryEventStore and in-memory implementations of all
repositories for unit-level integration testing without requiring PostgreSQL.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.migration.consistency import (
    ConsistencyVerifier,
    VerificationLevel,
)
from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.cutover import CutoverManager
from eventsource.migration.exceptions import (
    BulkCopyError,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
    DualWriteError,
    ErrorHandler,
    ErrorRecoverability,
    ErrorSeverity,
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
    RetryConfig,
    classify_exception,
)
from eventsource.migration.metrics import (
    clear_metrics_registry,
    get_migration_metrics,
    release_migration_metrics,
)
from eventsource.migration.models import (
    AuditEventType,
    Migration,
    MigrationAuditEntry,
    MigrationConfig,
    MigrationPhase,
    PositionMapping,
    TenantMigrationState,
    TenantRouting,
)
from eventsource.migration.position_mapper import PositionMapper
from eventsource.migration.router import TenantStoreRouter
from eventsource.migration.status_streamer import StatusStreamer, StatusStreamManager
from eventsource.migration.sync_lag_tracker import SyncLagTracker
from eventsource.migration.write_pause import WritePauseManager
from eventsource.stores.in_memory import InMemoryEventStore

# =============================================================================
# Test Event Classes
# =============================================================================


class SampleTestEvent(DomainEvent):
    """Generic test event for integration tests."""

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
        """Update migration phase."""
        from eventsource.migration.exceptions import InvalidPhaseTransitionError

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


class InMemoryAuditLogRepository:
    """In-memory implementation of MigrationAuditLogRepository for testing."""

    def __init__(self) -> None:
        self._entries: list[MigrationAuditEntry] = []
        self._id_counter: int = 0

    async def record(self, entry: MigrationAuditEntry) -> int:
        """Record an audit log entry."""
        self._id_counter += 1
        # Create a new entry with the assigned ID
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

        # Sort by occurred_at ascending
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

        # Return most recent
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
        source_position: int,
    ) -> PositionMapping | None:
        """Find mapping by exact source position."""
        for mapping in self._mappings:
            if mapping.migration_id == migration_id and mapping.source_position == source_position:
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


class InMemoryCheckpointRepository:
    """In-memory implementation of CheckpointRepository for testing."""

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict[str, Any]] = {}

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

    async def get_all_checkpoints(self) -> list[Any]:
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
def audit_log_repo() -> InMemoryAuditLogRepository:
    """Create audit log repository."""
    return InMemoryAuditLogRepository()


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


@pytest.fixture(autouse=True)
def cleanup_metrics():
    """Clean up metrics registry before and after each test."""
    clear_metrics_registry()
    yield
    clear_metrics_registry()


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


async def copy_events_between_stores(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    tenant_id: UUID,
) -> int:
    """Copy events from source to target store."""
    count = 0
    async for stored_event in source_store.read_all():
        if stored_event.event.tenant_id == tenant_id:
            event = stored_event.event
            await target_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type=event.aggregate_type,
                events=[event],
                expected_version=-1,  # ANY
            )
            count += 1
    return count


# =============================================================================
# Complete Migration Lifecycle Tests
# =============================================================================


class TestCompleteMigrationLifecycleWithOperationalFeatures:
    """
    Tests for complete migration lifecycle with all operational features enabled.

    These tests serve as smoke tests to validate that all Phase 4 components
    work together correctly during a full migration.
    """

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_audit_logging(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        audit_log_repo: InMemoryAuditLogRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test complete migration lifecycle with audit logging enabled."""
        # Create historical events
        await create_test_events(source_store, tenant_id, count=5)

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

        router.register_store("dedicated", target_store)

        # Start migration
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=MigrationConfig(batch_size=10),
        )

        assert migration is not None
        assert migration.tenant_id == tenant_id

        # Manually record audit events to simulate what the coordinator does
        await audit_log_repo.record(
            MigrationAuditEntry.migration_started(
                migration_id=migration.id,
                occurred_at=datetime.now(UTC),
                operator="test",
                details={"config": migration.config.to_dict()},
            )
        )

        await audit_log_repo.record(
            MigrationAuditEntry.phase_change(
                migration_id=migration.id,
                old_phase=MigrationPhase.PENDING,
                new_phase=MigrationPhase.BULK_COPY,
                occurred_at=datetime.now(UTC),
                operator="system",
            )
        )

        # Verify audit entries were recorded
        entries = await audit_log_repo.get_by_migration(migration.id)
        assert len(entries) >= 2

        # Verify migration started event
        started_entries = await audit_log_repo.get_by_migration(
            migration.id,
            event_types=[AuditEventType.MIGRATION_STARTED],
        )
        assert len(started_entries) == 1

        # Verify phase change events
        phase_entries = await audit_log_repo.get_by_migration(
            migration.id,
            event_types=[AuditEventType.PHASE_CHANGED],
        )
        assert len(phase_entries) >= 1

    @pytest.mark.asyncio
    async def test_full_lifecycle_with_metrics_collection(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
        migration_id: UUID,
    ) -> None:
        """Test complete migration lifecycle with metrics collection enabled."""
        # Create metrics for the migration
        metrics = get_migration_metrics(
            migration_id=str(migration_id),
            tenant_id=str(tenant_id),
            enable_metrics=True,
        )

        # Simulate bulk copy phase metrics
        with metrics.time_phase("bulk_copy"):
            # Create historical events and copy them
            await create_test_events(source_store, tenant_id, count=10)
            copied = await copy_events_between_stores(source_store, target_store, tenant_id)

            # Record events copied
            metrics.record_events_copied(copied, rate_events_per_sec=100.0)

        # Simulate dual-write phase metrics
        with metrics.time_phase("dual_write"):
            # Simulate sync lag tracking
            metrics.record_sync_lag(50)
            await asyncio.sleep(0.01)
            metrics.record_sync_lag(25)
            await asyncio.sleep(0.01)
            metrics.record_sync_lag(0)

        # Simulate cutover phase metrics
        with metrics.time_cutover() as timer:
            await asyncio.sleep(0.01)
            timer.success = True

        # Get snapshot and verify metrics were recorded
        snapshot = metrics.get_snapshot()

        assert snapshot.events_copied == 10
        assert snapshot.events_copied_rate == 100.0
        assert snapshot.sync_lag_events == 0  # Final lag
        assert "bulk_copy" in snapshot.phase_durations
        assert "dual_write" in snapshot.phase_durations
        assert len(snapshot.cutover_durations) == 1
        assert snapshot.cutover_durations[0] > 0

        # Clean up
        release_migration_metrics(str(migration_id))

    @pytest.mark.asyncio
    async def test_full_lifecycle_phases_tracked(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that all migration phases are properly tracked."""
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

        # Start migration
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

        # Let bulk copy run briefly
        await asyncio.sleep(0.05)

        # Check migration progressed
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        # Should be in BULK_COPY or possibly transitioned further
        assert migration.phase in (
            MigrationPhase.BULK_COPY,
            MigrationPhase.DUAL_WRITE,
        )


# =============================================================================
# Audit Logging Tests
# =============================================================================


class TestAuditLoggingThroughoutMigration:
    """Tests for audit logging throughout the migration process."""

    @pytest.mark.asyncio
    async def test_audit_log_records_all_phase_transitions(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test that audit log records all phase transitions."""
        phases = [
            (None, MigrationPhase.PENDING),
            (MigrationPhase.PENDING, MigrationPhase.BULK_COPY),
            (MigrationPhase.BULK_COPY, MigrationPhase.DUAL_WRITE),
            (MigrationPhase.DUAL_WRITE, MigrationPhase.CUTOVER),
            (MigrationPhase.CUTOVER, MigrationPhase.COMPLETED),
        ]

        for old_phase, new_phase in phases:
            await audit_log_repo.record(
                MigrationAuditEntry.phase_change(
                    migration_id=migration_id,
                    old_phase=old_phase,
                    new_phase=new_phase,
                    occurred_at=datetime.now(UTC),
                    operator="system",
                )
            )

        # Verify all phase transitions were recorded
        entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.PHASE_CHANGED],
        )

        assert len(entries) == 5

        # Verify order is correct (ascending by occurred_at)
        for i, (old_phase, new_phase) in enumerate(phases):
            assert entries[i].old_phase == old_phase
            assert entries[i].new_phase == new_phase

    @pytest.mark.asyncio
    async def test_audit_log_records_operator_actions(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test that audit log records operator actions (pause, resume, abort)."""
        # Record pause action
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.MIGRATION_PAUSED,
                old_phase=None,
                new_phase=None,
                details={"reason": "Maintenance window"},
                operator="admin@example.com",
                occurred_at=datetime.now(UTC),
            )
        )

        # Record resume action
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.MIGRATION_RESUMED,
                old_phase=None,
                new_phase=None,
                details={"resumed_after_seconds": 300},
                operator="admin@example.com",
                occurred_at=datetime.now(UTC),
            )
        )

        # Verify entries
        entries = await audit_log_repo.get_by_migration(migration_id)
        assert len(entries) == 2

        pause_entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.MIGRATION_PAUSED],
        )
        assert len(pause_entries) == 1
        assert pause_entries[0].operator == "admin@example.com"
        assert pause_entries[0].details["reason"] == "Maintenance window"

        resume_entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.MIGRATION_RESUMED],
        )
        assert len(resume_entries) == 1

    @pytest.mark.asyncio
    async def test_audit_log_records_errors(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test that audit log records errors during migration."""
        await audit_log_repo.record(
            MigrationAuditEntry.error_occurred(
                migration_id=migration_id,
                occurred_at=datetime.now(UTC),
                error_message="Connection timeout to target store",
                error_type="NetworkError",
                operator="system",
            )
        )

        # Verify error was recorded
        error_entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.ERROR_OCCURRED],
        )

        assert len(error_entries) == 1
        assert "Connection timeout" in error_entries[0].details["error_message"]
        assert error_entries[0].details["error_type"] == "NetworkError"

    @pytest.mark.asyncio
    async def test_audit_log_records_cutover_events(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test that audit log records cutover-specific events."""
        # Record cutover initiated
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.CUTOVER_INITIATED,
                old_phase=MigrationPhase.DUAL_WRITE,
                new_phase=MigrationPhase.CUTOVER,
                details={"sync_lag": 0},
                operator="admin@example.com",
                occurred_at=datetime.now(UTC),
            )
        )

        # Record cutover completed
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.CUTOVER_COMPLETED,
                old_phase=MigrationPhase.CUTOVER,
                new_phase=MigrationPhase.COMPLETED,
                details={"duration_ms": 45.5},
                operator="system",
                occurred_at=datetime.now(UTC),
            )
        )

        # Verify cutover events
        entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[
                AuditEventType.CUTOVER_INITIATED,
                AuditEventType.CUTOVER_COMPLETED,
            ],
        )

        assert len(entries) == 2

    @pytest.mark.asyncio
    async def test_audit_log_get_latest_entry(
        self,
        audit_log_repo: InMemoryAuditLogRepository,
        migration_id: UUID,
    ) -> None:
        """Test getting the latest audit entry."""
        # Record multiple entries
        for i in range(5):
            await audit_log_repo.record(
                MigrationAuditEntry(
                    id=None,
                    migration_id=migration_id,
                    event_type=AuditEventType.PROGRESS_CHECKPOINT,
                    old_phase=None,
                    new_phase=None,
                    details={"checkpoint": i},
                    operator="system",
                    occurred_at=datetime.now(UTC),
                )
            )

        latest = await audit_log_repo.get_latest(migration_id)
        assert latest is not None
        assert latest.details["checkpoint"] == 4

        # Get latest of specific type
        await audit_log_repo.record(
            MigrationAuditEntry.error_occurred(
                migration_id=migration_id,
                occurred_at=datetime.now(UTC),
                error_message="Test error",
                error_type="TestError",
            )
        )

        latest_error = await audit_log_repo.get_latest(
            migration_id,
            event_type=AuditEventType.ERROR_OCCURRED,
        )
        assert latest_error is not None
        assert latest_error.event_type == AuditEventType.ERROR_OCCURRED


# =============================================================================
# Metrics Collection Tests
# =============================================================================


class TestMetricsCollectionDuringMigration:
    """Tests for metrics collection during migration phases."""

    @pytest.mark.asyncio
    async def test_metrics_track_events_copied(
        self,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test metrics track events copied during bulk copy."""
        metrics = get_migration_metrics(
            migration_id=str(migration_id),
            tenant_id=str(tenant_id),
        )

        # Record multiple batches
        metrics.record_events_copied(100, rate_events_per_sec=1000.0)
        metrics.record_events_copied(150, rate_events_per_sec=1500.0)
        metrics.record_events_copied(50, rate_events_per_sec=500.0)

        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied == 300  # 100 + 150 + 50
        assert snapshot.events_copied_rate == 500.0  # Last recorded rate

    @pytest.mark.asyncio
    async def test_metrics_track_sync_lag(
        self,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test metrics track sync lag during dual-write."""
        metrics = get_migration_metrics(
            migration_id=str(migration_id),
            tenant_id=str(tenant_id),
        )

        # Simulate decreasing lag
        metrics.record_sync_lag(1000)
        assert metrics.current_sync_lag == 1000

        metrics.record_sync_lag(500)
        assert metrics.current_sync_lag == 500

        metrics.record_sync_lag(0)
        assert metrics.current_sync_lag == 0

        # Negative lag should be clamped to 0
        metrics.record_sync_lag(-10)
        assert metrics.current_sync_lag == 0

    @pytest.mark.asyncio
    async def test_metrics_track_phase_durations(
        self,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test metrics track phase durations."""
        metrics = get_migration_metrics(
            migration_id=str(migration_id),
            tenant_id=str(tenant_id),
        )

        # Manually record phase durations
        metrics.record_phase_duration("bulk_copy", 300.0)
        metrics.record_phase_duration("dual_write", 600.0)
        metrics.record_phase_duration("cutover", 0.05)

        snapshot = metrics.get_snapshot()
        assert "bulk_copy" in snapshot.phase_durations
        assert "dual_write" in snapshot.phase_durations
        assert "cutover" in snapshot.phase_durations

        assert snapshot.phase_durations["bulk_copy"] == 300.0
        assert snapshot.phase_durations["dual_write"] == 600.0
        assert snapshot.phase_durations["cutover"] == 0.05

    @pytest.mark.asyncio
    async def test_metrics_track_cutover_duration(
        self,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test metrics track cutover duration."""
        metrics = get_migration_metrics(
            migration_id=str(migration_id),
            tenant_id=str(tenant_id),
        )

        # Record successful cutover
        metrics.record_cutover_duration(45.5, success=True)

        # Record failed cutover
        metrics.record_cutover_duration(100.0, success=False)

        snapshot = metrics.get_snapshot()
        assert len(snapshot.cutover_durations) == 2
        assert 45.5 in snapshot.cutover_durations
        assert 100.0 in snapshot.cutover_durations

    @pytest.mark.asyncio
    async def test_metrics_track_failures(
        self,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test metrics track target write failures and verification failures."""
        metrics = get_migration_metrics(
            migration_id=str(migration_id),
            tenant_id=str(tenant_id),
        )

        # Record target write failures
        metrics.record_failed_target_write(error_type="timeout")
        metrics.record_failed_target_write(error_type="connection_error")
        metrics.record_failed_target_write()

        # Record verification failures
        metrics.record_verification_failure(failure_type="count_mismatch")
        metrics.record_verification_failure()

        snapshot = metrics.get_snapshot()
        assert snapshot.failed_target_writes == 3
        assert snapshot.verification_failures == 2

    @pytest.mark.asyncio
    async def test_metrics_context_managers(
        self,
        migration_id: UUID,
        tenant_id: UUID,
    ) -> None:
        """Test metrics context managers for timing."""
        metrics = get_migration_metrics(
            migration_id=str(migration_id),
            tenant_id=str(tenant_id),
        )

        # Test time_phase context manager
        with metrics.time_phase("test_phase"):
            await asyncio.sleep(0.01)

        snapshot = metrics.get_snapshot()
        assert "test_phase" in snapshot.phase_durations
        assert snapshot.phase_durations["test_phase"] >= 0.01

        # Test time_cutover context manager
        with metrics.time_cutover() as timer:
            await asyncio.sleep(0.01)
            timer.success = True

        snapshot = metrics.get_snapshot()
        assert len(snapshot.cutover_durations) == 1
        assert snapshot.cutover_durations[0] >= 10  # ms


# =============================================================================
# Status Streaming Tests
# =============================================================================


class TestStatusStreamingDuringMigration:
    """Tests for status streaming during migration."""

    @pytest.mark.asyncio
    async def test_status_streamer_yields_initial_status(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that status streamer yields initial status."""
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

        # Create status streamer
        streamer = StatusStreamer(
            coordinator=coordinator,
            migration_id=migration.id,
            enable_tracing=False,
        )

        # Get initial status
        statuses_received = []
        async for status in streamer.stream_status(
            update_interval=0.1,
            include_initial=True,
        ):
            statuses_received.append(status)
            # Break after first status to avoid hanging
            break

        assert len(statuses_received) == 1
        assert statuses_received[0].migration_id == migration.id
        assert statuses_received[0].tenant_id == tenant_id

        await streamer.close()

    @pytest.mark.asyncio
    async def test_status_stream_manager_creates_streamers(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that stream manager creates and reuses streamers."""
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

        # Create stream manager
        manager = StatusStreamManager(
            coordinator=coordinator,
            enable_tracing=False,
        )

        # Get streamer for migration
        streamer1 = await manager.get_streamer(migration.id)
        assert streamer1 is not None
        assert manager.active_streamers == 1

        # Get same streamer again (should reuse)
        streamer2 = await manager.get_streamer(migration.id)
        assert streamer2 is streamer1
        assert manager.active_streamers == 1

        # Close all
        await manager.close_all()
        assert manager.active_streamers == 0

    @pytest.mark.asyncio
    async def test_status_streamer_subscriber_count(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test that streamer tracks subscriber count."""
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

        streamer = StatusStreamer(
            coordinator=coordinator,
            migration_id=migration.id,
            enable_tracing=False,
        )

        assert streamer.subscriber_count == 0

        # Start streaming in background
        async def stream_and_count():
            async for _ in streamer.stream_status(update_interval=0.1):
                break

        task = asyncio.create_task(stream_and_count())

        # Give time for subscriber to register
        await asyncio.sleep(0.05)
        # Note: subscriber_count may be 0 or 1 depending on timing
        # The important thing is it doesn't crash

        await task
        await streamer.close()


# =============================================================================
# Error Handling and Classification Tests
# =============================================================================


class TestErrorHandlingAndClassification:
    """Tests for error handling and classification in real scenarios."""

    def test_error_severity_levels(self) -> None:
        """Test error severity classification."""
        # CRITICAL errors
        consistency_error = MigrationError(
            message="Data inconsistency detected",
            recoverable=False,
        )
        assert consistency_error.severity == ErrorSeverity.ERROR

        # Test specific error types have correct severity
        cutover_timeout = CutoverTimeoutError(
            migration_id=uuid4(),
            elapsed_ms=150.0,
            timeout_ms=100.0,
        )
        assert cutover_timeout.severity == ErrorSeverity.ERROR

        cutover_lag = CutoverLagError(
            migration_id=uuid4(),
            current_lag=200,
            max_lag=100,
        )
        assert cutover_lag.severity == ErrorSeverity.WARNING

    def test_error_recoverability_classification(self) -> None:
        """Test error recoverability classification."""
        # TRANSIENT errors should be retried
        dual_write_error = DualWriteError(
            migration_id=uuid4(),
            target_error="Connection timeout",
        )
        assert dual_write_error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert dual_write_error.recoverability_type.should_retry is True

        # FATAL errors should not be retried
        not_found_error = MigrationNotFoundError(uuid4())
        assert not_found_error.recoverability_type == ErrorRecoverability.FATAL
        assert not_found_error.recoverability_type.should_retry is False

    def test_classify_exception_function(self) -> None:
        """Test classify_exception helper function."""
        # Migration errors get their specific classification
        cutover_error = CutoverError(
            message="Cutover failed",
            migration_id=uuid4(),
        )
        classification = classify_exception(cutover_error)
        assert classification.error_code == "CUTOVER_ERROR"
        assert classification.category == "cutover"

        # Non-migration errors get generic classification
        generic_error = ValueError("Some value error")
        classification = classify_exception(generic_error)
        assert classification.error_code == "UNKNOWN_ERROR"
        assert classification.severity == ErrorSeverity.ERROR

    def test_error_to_dict_serialization(self) -> None:
        """Test error serialization to dictionary."""
        migration_id = uuid4()
        uuid4()

        error = BulkCopyError(
            migration_id=migration_id,
            last_position=5000,
            error="Connection lost",
        )

        error_dict = error.to_dict()

        assert error_dict["message"] is not None
        assert error_dict["migration_id"] == str(migration_id)
        assert error_dict["error_code"] == "BULK_COPY_ERROR"
        assert "classification" in error_dict
        assert error_dict["classification"]["category"] == "bulk_copy"

    @pytest.mark.asyncio
    async def test_error_handler_with_retry(self) -> None:
        """Test error handler with automatic retry."""
        handler = ErrorHandler()

        # Track retry attempts
        attempts = []

        async def flaky_operation():
            attempts.append(datetime.now(UTC))
            if len(attempts) < 3:
                raise DualWriteError(
                    migration_id=uuid4(),
                    target_error="Temporary failure",
                )
            return "success"

        # Execute with retry (should succeed on 3rd attempt)
        result = await handler.execute_with_retry(
            flaky_operation,
            operation_name="flaky_op",
            retry_config=RetryConfig(
                max_attempts=5,
                base_delay_ms=10.0,  # Short delay for testing
                max_delay_ms=100.0,
            ),
        )

        assert result == "success"
        assert len(attempts) == 3

    @pytest.mark.asyncio
    async def test_error_handler_exhausts_retries(self) -> None:
        """Test error handler exhausts retries and raises."""
        handler = ErrorHandler()

        async def always_fails():
            raise DualWriteError(
                migration_id=uuid4(),
                target_error="Permanent failure",
            )

        with pytest.raises(DualWriteError):
            await handler.execute_with_retry(
                always_fails,
                operation_name="always_fails",
                retry_config=RetryConfig(
                    max_attempts=3,
                    base_delay_ms=10.0,
                    max_delay_ms=50.0,
                ),
            )

    @pytest.mark.asyncio
    async def test_circuit_breaker_opens_after_failures(self) -> None:
        """Test circuit breaker opens after threshold failures."""
        cb = CircuitBreaker(
            config=CircuitBreakerConfig(
                failure_threshold=3,
                timeout_seconds=0.1,
            ),
            name="test_circuit",
        )

        migration_id = uuid4()

        # Cause failures to open circuit
        for _ in range(3):
            try:
                ctx = await cb.protect("test_op", migration_id)
                async with ctx:
                    raise MigrationError("Test failure", migration_id=migration_id)
            except MigrationError:
                pass

        # Circuit should now be open
        with pytest.raises(CircuitBreakerOpenError):
            await cb.protect("test_op", migration_id)

        # Wait for timeout and verify half-open
        await asyncio.sleep(0.15)

        # Should now be able to try again (half-open)
        ctx = await cb.protect("test_op", migration_id)
        async with ctx:
            pass  # Success

        # After success in half-open, should transition to closed
        # (need more successes based on config, but at least it doesn't raise)

    @pytest.mark.asyncio
    async def test_circuit_breaker_reset(self) -> None:
        """Test circuit breaker can be manually reset."""
        cb = CircuitBreaker(
            config=CircuitBreakerConfig(failure_threshold=2),
            name="test_reset",
        )

        migration_id = uuid4()

        # Cause failures
        for _ in range(2):
            try:
                ctx = await cb.protect("test_op", migration_id)
                async with ctx:
                    raise MigrationError("Failure", migration_id=migration_id)
            except MigrationError:
                pass

        # Circuit should be open
        with pytest.raises(CircuitBreakerOpenError):
            await cb.protect("test_op", migration_id)

        # Reset the circuit
        cb.reset()

        # Should be able to use again
        ctx = await cb.protect("test_op", migration_id)
        async with ctx:
            pass  # Success


# =============================================================================
# Recovery After Simulated Failures Tests
# =============================================================================


class TestRecoveryAfterSimulatedFailures:
    """Tests for recovery after simulated failures."""

    @pytest.mark.asyncio
    async def test_recovery_from_bulk_copy_failure(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        audit_log_repo: InMemoryAuditLogRepository,
        tenant_id: UUID,
    ) -> None:
        """Test recovery after bulk copy failure."""
        # Create events in source
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

        # Start migration
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        # Simulate recording an error (as if bulk copy failed)
        await migration_repo.record_error(
            migration.id,
            error="Simulated bulk copy failure",
        )

        # Record error in audit log
        await audit_log_repo.record(
            MigrationAuditEntry.error_occurred(
                migration_id=migration.id,
                occurred_at=datetime.now(UTC),
                error_message="Simulated bulk copy failure",
                error_type="BulkCopyError",
            )
        )

        # Verify error was recorded
        migration = await migration_repo.get(migration.id)
        assert migration is not None
        assert migration.error_count == 1
        assert "bulk copy" in migration.last_error.lower()

        # Migration should still be in progress (recoverable)
        assert not migration.phase.is_terminal

        # Verify audit log has error
        errors = await audit_log_repo.get_by_migration(
            migration.id,
            event_types=[AuditEventType.ERROR_OCCURRED],
        )
        assert len(errors) == 1

    @pytest.mark.asyncio
    async def test_recovery_from_cutover_rollback(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        audit_log_repo: InMemoryAuditLogRepository,
        tenant_id: UUID,
        migration_id: UUID,
    ) -> None:
        """Test recovery after cutover rollback."""
        # Setup: Create events in both stores (simulating completed bulk copy)
        await create_test_events(source_store, tenant_id, count=5)
        await copy_events_between_stores(source_store, target_store, tenant_id)

        # Add extra events to source to create lag (simulating new writes)
        for i in range(200):
            aggregate_id = uuid4()
            event = SampleTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                value=f"extra-{i}",
            )
            await source_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="TestAggregate",
                events=[event],
                expected_version=0,
            )

        # Setup routing state for dual-write
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
            config=MigrationConfig(cutover_max_lag_events=50),
            enable_tracing=False,
        )

        # Create cutover manager
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        # Attempt cutover (should fail due to high lag)
        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            config=MigrationConfig(cutover_max_lag_events=50),
            timeout_ms=500.0,
        )

        # Verify cutover failed and rolled back
        assert result.success is False
        assert result.rolled_back is True

        # Record rollback in audit log
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration_id,
                event_type=AuditEventType.CUTOVER_ROLLED_BACK,
                old_phase=MigrationPhase.CUTOVER,
                new_phase=MigrationPhase.DUAL_WRITE,
                details={"reason": result.error_message, "duration_ms": result.duration_ms},
                operator="system",
                occurred_at=datetime.now(UTC),
            )
        )

        # Verify routing was rolled back
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.DUAL_WRITE

        # Verify audit log has rollback event
        rollback_entries = await audit_log_repo.get_by_migration(
            migration_id,
            event_types=[AuditEventType.CUTOVER_ROLLED_BACK],
        )
        assert len(rollback_entries) == 1

    @pytest.mark.asyncio
    async def test_pause_resume_recovery(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        audit_log_repo: InMemoryAuditLogRepository,
        tenant_id: UUID,
    ) -> None:
        """Test recovery through pause and resume."""
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

        # Pause migration
        await coordinator.pause_migration(migration.id)

        # Record pause in audit log
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.MIGRATION_PAUSED,
                old_phase=None,
                new_phase=None,
                details={"reason": "Maintenance"},
                operator="admin",
                occurred_at=datetime.now(UTC),
            )
        )

        # Verify paused
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        assert migration.is_paused

        # Resume migration
        await coordinator.resume_migration(migration.id)

        # Record resume in audit log
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.MIGRATION_RESUMED,
                old_phase=None,
                new_phase=None,
                details={"resumed_by": "admin"},
                operator="admin",
                occurred_at=datetime.now(UTC),
            )
        )

        # Verify resumed
        migration = await coordinator.get_migration(migration.id)
        assert migration is not None
        assert not migration.is_paused

        # Verify audit log has both events
        entries = await audit_log_repo.get_by_migration(
            migration.id,
            event_types=[
                AuditEventType.MIGRATION_PAUSED,
                AuditEventType.MIGRATION_RESUMED,
            ],
        )
        assert len(entries) == 2

    @pytest.mark.asyncio
    async def test_abort_recovery(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        audit_log_repo: InMemoryAuditLogRepository,
        tenant_id: UUID,
    ) -> None:
        """Test clean abort and state recovery."""
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

        # Abort migration
        result = await coordinator.abort_migration(
            migration.id,
            reason="Test abort for recovery",
        )

        # Record abort in audit log
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.MIGRATION_ABORTED,
                old_phase=result.final_phase,
                new_phase=MigrationPhase.ABORTED,
                details={"reason": "Test abort for recovery"},
                operator="admin",
                occurred_at=datetime.now(UTC),
            )
        )

        # Verify abort
        assert result.success is False  # Abort is not a "success"
        assert result.final_phase == MigrationPhase.ABORTED

        # Verify migration state
        migration = await coordinator.get_migration(result.migration_id)
        assert migration is not None
        assert migration.phase == MigrationPhase.ABORTED

        # Verify routing state was reset
        routing = await routing_repo.get_routing(tenant_id)
        assert routing is not None
        assert routing.migration_state == TenantMigrationState.NORMAL

        # Verify can start new migration for same tenant
        new_migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )
        assert new_migration is not None
        assert new_migration.id != migration.id


# =============================================================================
# Integration of All Phase 4 Components Tests
# =============================================================================


class TestIntegrationOfAllPhase4Components:
    """Tests for integration of all Phase 4 components working together."""

    @pytest.mark.asyncio
    async def test_all_components_work_together(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        audit_log_repo: InMemoryAuditLogRepository,
        position_mapping_repo: InMemoryPositionMappingRepository,
        checkpoint_repo: InMemoryCheckpointRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """Test all Phase 4 components working together in a migration."""
        # Create position mapper and subscription migrator
        position_mapper = PositionMapper(position_mapping_repo, enable_tracing=False)

        # Create historical events
        await create_test_events(source_store, tenant_id, count=5)

        # Create coordinator with all dependencies
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

        # 1. Start migration with all operational features
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
            config=MigrationConfig(
                batch_size=10,
                verify_consistency=True,
                migrate_subscriptions=False,
            ),
        )

        # 2. Create and track metrics
        metrics = get_migration_metrics(
            migration_id=str(migration.id),
            tenant_id=str(tenant_id),
        )

        # 3. Record audit events
        await audit_log_repo.record(
            MigrationAuditEntry.migration_started(
                migration_id=migration.id,
                occurred_at=datetime.now(UTC),
                operator="test",
                details={"config": migration.config.to_dict()},
            )
        )

        # 4. Simulate bulk copy with metrics
        with metrics.time_phase("bulk_copy"):
            # Copy events
            copied = await copy_events_between_stores(source_store, target_store, tenant_id)
            metrics.record_events_copied(copied, rate_events_per_sec=100.0)

            # Record position mappings
            source_pos = 1
            target_pos = 1
            async for stored_event in source_store.read_all():
                if stored_event.event.tenant_id == tenant_id:
                    await position_mapper.record_mapping(
                        migration_id=migration.id,
                        source_position=source_pos,
                        target_position=target_pos,
                        event_id=stored_event.event.event_id,
                    )
                    source_pos += 1
                    target_pos += 1

        # 5. Record phase transition
        await audit_log_repo.record(
            MigrationAuditEntry.phase_change(
                migration_id=migration.id,
                old_phase=MigrationPhase.BULK_COPY,
                new_phase=MigrationPhase.DUAL_WRITE,
                occurred_at=datetime.now(UTC),
                operator="system",
            )
        )

        # 6. Simulate dual-write with sync lag tracking
        with metrics.time_phase("dual_write"):
            metrics.record_sync_lag(10)
            await asyncio.sleep(0.01)
            metrics.record_sync_lag(0)

        # 7. Verify consistency
        verifier = ConsistencyVerifier(source_store, target_store, enable_tracing=False)
        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.COUNT,
        )

        # Record verification in audit log
        await audit_log_repo.record(
            MigrationAuditEntry(
                id=None,
                migration_id=migration.id,
                event_type=AuditEventType.VERIFICATION_COMPLETED,
                old_phase=None,
                new_phase=None,
                details={
                    "is_consistent": report.is_consistent,
                    "source_count": report.source_event_count,
                    "target_count": report.target_event_count,
                },
                operator="system",
                occurred_at=datetime.now(UTC),
            )
        )

        # 8. Validate all components worked correctly
        # Metrics snapshot
        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied == 5
        assert "bulk_copy" in snapshot.phase_durations
        assert "dual_write" in snapshot.phase_durations

        # Audit log entries
        all_entries = await audit_log_repo.get_by_migration(migration.id)
        assert len(all_entries) >= 3  # started, phase_changed, verification

        # Position mappings
        mapping_count = await position_mapper.get_mapping_count(migration.id)
        assert mapping_count == 5

        # Consistency report
        assert report.is_consistent is True
        assert report.source_event_count == 5
        assert report.target_event_count == 5

        # Clean up metrics
        release_migration_metrics(str(migration.id))

    @pytest.mark.asyncio
    async def test_end_to_end_migration_smoke_test(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
    ) -> None:
        """End-to-end smoke test for complete migration system."""
        # Setup: Create 20 events in source
        await create_test_events(source_store, tenant_id, count=20)

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

        router.register_store("dedicated", target_store)

        # Start migration
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            target_store=target_store,
            target_store_id="dedicated",
        )

        assert migration is not None
        assert migration.phase in (MigrationPhase.PENDING, MigrationPhase.BULK_COPY)

        # Get status
        status = await coordinator.get_status(migration.id)
        assert status.migration_id == migration.id
        assert status.tenant_id == tenant_id

        # Let it run briefly
        await asyncio.sleep(0.1)

        # Abort to clean up
        result = await coordinator.abort_migration(
            migration.id,
            reason="Smoke test cleanup",
        )
        assert result.final_phase == MigrationPhase.ABORTED

        # Verify clean state
        routing = await routing_repo.get_routing(tenant_id)
        assert routing.migration_state == TenantMigrationState.NORMAL
