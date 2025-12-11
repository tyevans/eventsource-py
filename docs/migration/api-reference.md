# Migration API Reference

This document provides API reference for the key classes in the migration system.

## MigrationCoordinator

The central orchestrator for tenant migrations.

### Initialization

```python
from eventsource.migration import MigrationCoordinator, MigrationConfig

coordinator = MigrationCoordinator(
    migration_repo=migration_repository,
    routing_repo=routing_repository,
    router=tenant_store_router,
    config=MigrationConfig(
        batch_size=1000,
        sync_lag_threshold=100,
        cutover_timeout_seconds=5.0,
        verification_level=VerificationLevel.HASH,
    ),
)
```

### Key Methods

#### `start_migration()`
Start a new migration for a tenant.

```python
async def start_migration(
    self,
    tenant_id: UUID,
    source_store: EventStore,
    target_store: EventStore,
    source_store_id: str = "source",
    target_store_id: str = "target",
) -> Migration:
    """
    Start a new migration from source to target store.

    Args:
        tenant_id: The tenant to migrate.
        source_store: Source EventStore instance.
        target_store: Target EventStore instance.
        source_store_id: Identifier for the source store.
        target_store_id: Identifier for the target store.

    Returns:
        Migration object with id and initial state.

    Raises:
        MigrationAlreadyExistsError: If migration already in progress.
    """
```

**Example:**
```python
migration = await coordinator.start_migration(
    tenant_id=tenant_uuid,
    source_store=shared_store,
    target_store=dedicated_store,
    source_store_id="shared-postgresql",
    target_store_id="dedicated-tenant-a",
)
print(f"Started migration: {migration.id}")
```

#### `get_migration()`
Get current migration state.

```python
async def get_migration(self, migration_id: UUID) -> Migration | None:
    """
    Get migration by ID.

    Args:
        migration_id: The migration identifier.

    Returns:
        Migration object or None if not found.
    """
```

#### `pause_migration()`
Pause an active migration.

```python
async def pause_migration(self, migration_id: UUID) -> Migration:
    """
    Pause an active migration.

    The migration can be resumed later. Safe to call during
    BULK_COPY or DUAL_WRITE phases.

    Args:
        migration_id: The migration to pause.

    Returns:
        Updated Migration object.

    Raises:
        MigrationNotFoundError: If migration not found.
        InvalidStateTransitionError: If migration cannot be paused.
    """
```

#### `resume_migration()`
Resume a paused migration.

```python
async def resume_migration(self, migration_id: UUID) -> Migration:
    """
    Resume a paused migration.

    Continues from where it left off.

    Args:
        migration_id: The migration to resume.

    Returns:
        Updated Migration object.

    Raises:
        MigrationNotFoundError: If migration not found.
        InvalidStateTransitionError: If migration cannot be resumed.
    """
```

#### `abort_migration()`
Abort a migration and rollback.

```python
async def abort_migration(
    self,
    migration_id: UUID,
    reason: str = "Manual abort",
) -> Migration:
    """
    Abort migration and rollback to source store.

    Cleans up target store and resets routing to source.
    Safe to call from any state except MIGRATED.

    Args:
        migration_id: The migration to abort.
        reason: Reason for aborting (logged to audit).

    Returns:
        Updated Migration object with ROLLED_BACK state.

    Raises:
        MigrationNotFoundError: If migration not found.
        InvalidStateTransitionError: If migration already completed.
    """
```

#### `initiate_cutover()`
Start the cutover process.

```python
async def initiate_cutover(
    self,
    migration_id: UUID,
    force: bool = False,
) -> Migration:
    """
    Initiate cutover from source to target store.

    Will pause writes, verify consistency, and switch routing.
    Automatically rolls back if verification fails or timeout.

    Args:
        migration_id: The migration to cut over.
        force: Skip verification (use with caution).

    Returns:
        Updated Migration object with MIGRATED state.

    Raises:
        MigrationNotFoundError: If migration not found.
        InvalidStateTransitionError: If not in DUAL_WRITE phase.
        CutoverTimeoutError: If cutover exceeds timeout.
        ConsistencyError: If verification fails.
    """
```

---

## MigrationConfig

Configuration for migration behavior.

```python
from eventsource.migration import MigrationConfig
from eventsource.migration.consistency import VerificationLevel

config = MigrationConfig(
    # Bulk copy settings
    batch_size=1000,              # Events per batch

    # Sync settings
    sync_lag_threshold=100,       # Max lag before pausing cutover
    sync_check_interval=1.0,      # Seconds between lag checks

    # Cutover settings
    cutover_timeout_seconds=5.0,  # Max pause duration

    # Verification settings
    verification_level=VerificationLevel.HASH,
    sample_percentage=100.0,      # Percentage to verify

    # Observability
    enable_tracing=True,
    enable_metrics=True,
)
```

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `batch_size` | int | 1000 | Events per bulk copy batch |
| `sync_lag_threshold` | int | 100 | Max events behind before blocking cutover |
| `sync_check_interval` | float | 1.0 | Seconds between sync lag checks |
| `cutover_timeout_seconds` | float | 5.0 | Maximum cutover pause duration |
| `verification_level` | VerificationLevel | HASH | Consistency check thoroughness |
| `sample_percentage` | float | 100.0 | Percentage of events to verify |
| `enable_tracing` | bool | True | Enable OpenTelemetry tracing |
| `enable_metrics` | bool | True | Enable metrics collection |

---

## TenantStoreRouter

Routes operations based on tenant migration state.

### Initialization

```python
from eventsource.migration import TenantStoreRouter

router = TenantStoreRouter(
    default_store=shared_store,
    routing_repo=routing_repository,
    stores={
        "shared": shared_store,
        "dedicated-a": dedicated_store_a,
        "dedicated-b": dedicated_store_b,
    },
    default_store_id="shared",
    write_pause_timeout=5.0,
)
```

### Key Methods

#### `register_store()`
Register a store for routing.

```python
def register_store(self, store_id: str, store: EventStore) -> None:
    """
    Register a store for routing.

    Args:
        store_id: Unique identifier for the store.
        store: EventStore instance.
    """
```

#### `append_events()`
Write events, routed by tenant.

```python
async def append_events(
    self,
    aggregate_id: UUID,
    aggregate_type: str,
    events: list[DomainEvent],
    expected_version: int,
) -> AppendResult:
    """
    Append events to the appropriate store.

    Routes based on tenant_id from events and migration state:
    - NORMAL: Routes to configured store
    - BULK_COPY: Routes to source store
    - DUAL_WRITE: Routes through DualWriteInterceptor
    - CUTOVER_PAUSED: Blocks until cutover completes
    - MIGRATED: Routes to target store

    Args:
        aggregate_id: Aggregate identifier.
        aggregate_type: Type of aggregate.
        events: Events to append.
        expected_version: Expected version for optimistic locking.

    Returns:
        AppendResult with success status.

    Raises:
        WritePausedError: If writes blocked during cutover timeout.
    """
```

#### `get_events()`
Read events, routed by tenant.

```python
async def get_events(
    self,
    aggregate_id: UUID,
    aggregate_type: str | None = None,
    from_version: int = 0,
) -> EventStream:
    """
    Get events from the appropriate store.

    Routes based on tenant migration state.

    Args:
        aggregate_id: Aggregate identifier.
        aggregate_type: Type of aggregate (optional).
        from_version: Start from this version.

    Returns:
        EventStream with aggregate events.
    """
```

#### `pause_writes()` / `resume_writes()`
Control write pause during cutover.

```python
async def pause_writes(self, tenant_id: UUID) -> bool:
    """
    Pause writes for a tenant during cutover.

    Returns:
        True if pause was initiated, False if already paused.
    """

async def resume_writes(self, tenant_id: UUID) -> PauseMetrics | None:
    """
    Resume writes after cutover.

    Returns:
        PauseMetrics with duration and blocked writer count.
    """
```

---

## BulkCopier

Copies historical events from source to target store.

### Initialization

```python
from eventsource.migration import BulkCopier

copier = BulkCopier(
    source_store=source,
    target_store=target,
    batch_size=1000,
    enable_tracing=True,
)
```

### Key Methods

#### `copy_tenant_events()`
Copy all events for a tenant.

```python
async def copy_tenant_events(
    self,
    tenant_id: UUID,
    migration_id: UUID,
    progress_callback: Callable[[int, int], None] | None = None,
) -> CopyResult:
    """
    Copy all events for a tenant from source to target.

    Args:
        tenant_id: Tenant to copy.
        migration_id: Associated migration ID.
        progress_callback: Called with (copied, total) during copy.

    Returns:
        CopyResult with statistics.
    """
```

**Example:**
```python
def on_progress(copied: int, total: int):
    print(f"Progress: {copied}/{total} ({100*copied/total:.1f}%)")

result = await copier.copy_tenant_events(
    tenant_id=tenant_uuid,
    migration_id=migration.id,
    progress_callback=on_progress,
)
print(f"Copied {result.events_copied} events in {result.duration_seconds}s")
```

---

## ConsistencyVerifier

Verifies data integrity between stores.

### Initialization

```python
from eventsource.migration import ConsistencyVerifier
from eventsource.migration.consistency import VerificationLevel

verifier = ConsistencyVerifier(
    source_store=source,
    target_store=target,
    enable_tracing=True,
)
```

### Key Methods

#### `verify_tenant_consistency()`
Comprehensive consistency verification.

```python
async def verify_tenant_consistency(
    self,
    tenant_id: UUID,
    level: VerificationLevel = VerificationLevel.HASH,
    sample_percentage: float = 100.0,
) -> VerificationReport:
    """
    Verify consistency for all tenant data.

    Args:
        tenant_id: Tenant to verify.
        level: Verification thoroughness (COUNT, HASH, FULL).
        sample_percentage: Percentage to verify (1-100).

    Returns:
        VerificationReport with detailed results.
    """
```

**Example:**
```python
report = await verifier.verify_tenant_consistency(
    tenant_id=tenant_uuid,
    level=VerificationLevel.HASH,
    sample_percentage=100.0,
)

if report.is_consistent:
    print("Verification passed")
else:
    print(f"Found {len(report.violations)} violations:")
    for v in report.violations:
        print(f"  - {v}")
```

### Verification Levels

| Level | Speed | Thoroughness | Use Case |
|-------|-------|--------------|----------|
| `COUNT` | Fast | Basic | Quick sanity check |
| `HASH` | Medium | Good | Standard verification |
| `FULL` | Slow | Complete | Critical data |

---

## DualWriteInterceptor

Intercepts writes during dual-write phase.

### Initialization

```python
from eventsource.migration import DualWriteInterceptor

interceptor = DualWriteInterceptor(
    source_store=source,
    target_store=target,
    tenant_id=tenant_uuid,
    max_failure_history=1000,
)
```

### Key Methods

#### `get_failure_stats()`
Get statistics about target write failures.

```python
def get_failure_stats(self) -> FailureStats:
    """
    Get aggregate failure statistics.

    Returns:
        FailureStats with:
        - total_failures: Total failed target writes
        - total_events_failed: Total events affected
        - first_failure_at: Timestamp of first failure
        - last_failure_at: Timestamp of last failure
        - unique_aggregates_affected: Distinct aggregates
    """
```

---

## SubscriptionMigrator

Migrates subscription checkpoints.

### Initialization

```python
from eventsource.migration import SubscriptionMigrator

migrator = SubscriptionMigrator(
    position_mapper=position_mapper,
    checkpoint_repo=checkpoint_repository,
)
```

### Key Methods

#### `plan_migration()`
Preview migration without executing.

```python
async def plan_migration(
    self,
    migration_id: UUID,
    tenant_id: UUID,
    subscription_names: list[str],
) -> MigrationPlan:
    """
    Create a migration plan (dry-run).

    Args:
        migration_id: Associated migration.
        tenant_id: Tenant being migrated.
        subscription_names: Subscriptions to migrate.

    Returns:
        MigrationPlan with planned position translations.
    """
```

#### `migrate_subscriptions()`
Execute subscription migrations.

```python
async def migrate_subscriptions(
    self,
    migration_id: UUID,
    tenant_id: UUID,
    subscription_names: list[str],
) -> MigrationSummary:
    """
    Migrate subscription checkpoints.

    Translates positions and updates checkpoints atomically.

    Args:
        migration_id: Associated migration.
        tenant_id: Tenant being migrated.
        subscription_names: Subscriptions to migrate.

    Returns:
        MigrationSummary with results for each subscription.
    """
```

**Example:**
```python
# Preview first
plan = await migrator.plan_migration(
    migration_id=migration.id,
    tenant_id=tenant_uuid,
    subscription_names=["OrderProjection", "InventoryProjection"],
)

for p in plan.planned_migrations:
    print(f"{p.subscription_name}: {p.current_position} -> {p.planned_target_position}")

# Execute if plan looks good
summary = await migrator.migrate_subscriptions(
    migration_id=migration.id,
    tenant_id=tenant_uuid,
    subscription_names=["OrderProjection", "InventoryProjection"],
)

print(f"Migrated {summary.successful_count} subscriptions")
```

---

## Migration Data Models

### Migration

```python
@dataclass
class Migration:
    id: UUID
    tenant_id: UUID
    source_store_id: str
    target_store_id: str
    state: MigrationState
    progress_percentage: float
    events_copied: int
    total_events: int
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
```

### MigrationState

```python
class MigrationState(Enum):
    PENDING = "pending"
    BULK_COPY = "bulk_copy"
    DUAL_WRITE = "dual_write"
    CUTOVER_PAUSED = "cutover_paused"
    MIGRATED = "migrated"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"
    PAUSED = "paused"
```

### TenantMigrationState

```python
class TenantMigrationState(Enum):
    NORMAL = "normal"
    BULK_COPY = "bulk_copy"
    DUAL_WRITE = "dual_write"
    CUTOVER_PAUSED = "cutover_paused"
    MIGRATED = "migrated"
```

---

## Exception Classes

### MigrationError (Base)
```python
class MigrationError(Exception):
    migration_id: UUID
    recoverable: bool
    suggested_action: str | None
```

### MigrationNotFoundError
```python
class MigrationNotFoundError(MigrationError):
    """Raised when migration ID not found."""
```

### InvalidStateTransitionError
```python
class InvalidStateTransitionError(MigrationError):
    """Raised when state transition is invalid."""
    current_state: str
    attempted_state: str
    allowed_transitions: list[str]
```

### CutoverTimeoutError
```python
class CutoverTimeoutError(MigrationError):
    """Raised when cutover exceeds timeout."""
    timeout_seconds: float
    blocked_writers: int
```

### ConsistencyError
```python
class ConsistencyError(MigrationError):
    """Raised when consistency verification fails."""
    details: str
```

### WritePausedError
```python
class WritePausedError(MigrationError):
    """Raised when write attempted during pause."""
    tenant_id: UUID
    timeout_seconds: float
```

---

## Next Steps

- [Migration Guide](./migration-guide.md): Step-by-step instructions
- [Runbook](./runbook.md): Operational procedures
- [Troubleshooting](./troubleshooting.md): Common issues
