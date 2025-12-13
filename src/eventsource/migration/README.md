# eventsource.migration

A zero-downtime live migration system for multi-tenant event stores. This module enables seamless backend migrations (e.g., shared PostgreSQL to dedicated PostgreSQL) without stopping your application.

## Overview

The migration system provides:

- **Zero data loss**: Source store remains authoritative throughout migration
- **Minimal downtime**: Sub-100ms cutover pause with automatic rollback
- **Transparent operation**: Application code doesn't need to change
- **Full observability**: Real-time status streaming, metrics, and audit logging
- **Subscription continuity**: Automatic checkpoint translation for subscriptions

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              MigrationCoordinator                   │
│          (Orchestrates migration lifecycle)         │
└────────────────────────┬────────────────────────────┘
                         │
     ┌───────────────────┼───────────────────┐
     │                   │                   │
     ▼                   ▼                   ▼
┌──────────┐      ┌─────────────┐      ┌──────────┐
│BulkCopier│      │DualWrite    │      │Cutover   │
│          │      │Interceptor  │      │Manager   │
└──────────┘      └─────────────┘      └──────────┘
     │                   │                   │
     └───────────────────┼───────────────────┘
                         │
                         ▼
            ┌─────────────────────────┐
            │   TenantStoreRouter     │
            │   (Transparent proxy)   │
            └─────────────────────────┘
                    │         │
                    ▼         ▼
               ┌───────┐ ┌───────┐
               │Source │ │Target │
               │Store  │ │Store  │
               └───────┘ └───────┘
```

## Migration Phases

```
PENDING ──► BULK_COPY ──► DUAL_WRITE ──► CUTOVER ──► COMPLETED
               │              │             │
               └──────────────┴─────────────┴──► ABORTED/FAILED
```

| Phase | Description |
|-------|-------------|
| `PENDING` | Migration created but not started |
| `BULK_COPY` | Historical events being copied to target store |
| `DUAL_WRITE` | New events written to both stores simultaneously |
| `CUTOVER` | Brief pause (~100ms) while routing switches to target |
| `COMPLETED` | Migration finished successfully |
| `ABORTED` | Migration cancelled by operator |
| `FAILED` | Unrecoverable error occurred |

## Quick Start

### Basic Migration

```python
from eventsource.migration import (
    MigrationCoordinator,
    MigrationConfig,
    TenantStoreRouter,
)

# 1. Set up the router (use this as your event store)
router = TenantStoreRouter(
    default_store=shared_store,
    routing_repo=routing_repo,
)

# 2. Create the coordinator
coordinator = MigrationCoordinator(
    source_store=shared_store,
    migration_repo=migration_repo,
    routing_repo=routing_repo,
    router=router,
    lock_manager=lock_manager,
    position_mapper=position_mapper,
    checkpoint_repo=checkpoint_repo,
)

# 3. Start the migration
migration = await coordinator.start_migration(
    tenant_id="tenant-123",
    target_store=dedicated_store,
    target_store_id="dedicated-tenant-123",
)

# 4. Monitor progress
async for status in coordinator.stream_status(migration.id):
    print(f"Phase: {status.phase.value}")
    print(f"Progress: {status.progress_percent:.1f}%")

    if status.phase == MigrationPhase.DUAL_WRITE:
        print(f"Sync lag: {status.sync_lag_events} events")

    if status.phase.is_terminal:
        break

# 5. Trigger cutover when ready
ready, error = await coordinator.is_cutover_ready(migration.id)
if ready:
    result = await coordinator.trigger_cutover(migration.id)
    if result.success:
        print(f"Cutover completed in {result.duration_ms:.2f}ms")
```

### Configuration Options

```python
config = MigrationConfig(
    # Bulk copy settings
    batch_size=1000,              # Events per batch
    max_bulk_copy_rate=10000,     # Max events/second

    # Dual-write settings
    dual_write_timeout_minutes=30,  # Max time in dual-write

    # Cutover settings
    cutover_max_lag_events=100,   # Max lag before cutover allowed
    cutover_timeout_ms=500,       # Hard timeout for cutover

    # Post-cutover options
    position_mapping_enabled=True,  # Record position mappings
    verify_consistency=True,        # Verify data after migration
    migrate_subscriptions=True,     # Migrate subscription checkpoints
)

migration = await coordinator.start_migration(
    tenant_id="tenant-123",
    target_store=dedicated_store,
    target_store_id="dedicated-tenant-123",
    config=config,
)
```

## Key Components

### MigrationCoordinator

The main entry point for managing migrations. Orchestrates all components and manages the migration lifecycle.

```python
# Start a migration
migration = await coordinator.start_migration(tenant_id, target_store, target_store_id)

# Get current status
status = await coordinator.get_status(migration.id)

# Stream real-time status updates
async for status in coordinator.stream_status(migration.id):
    print(status)

# Wait for a specific phase
await coordinator.wait_for_phase(migration.id, MigrationPhase.DUAL_WRITE)

# Check if cutover is ready
ready, error = await coordinator.is_cutover_ready(migration.id)

# Trigger the cutover
result = await coordinator.trigger_cutover(migration.id)

# Pause/resume migration
await coordinator.pause_migration(migration.id)
await coordinator.resume_migration(migration.id)

# Abort migration
await coordinator.abort_migration(migration.id, reason="No longer needed")
```

### TenantStoreRouter

A transparent proxy that routes operations to the correct store based on tenant migration state. Use this as your event store during migrations.

```python
router = TenantStoreRouter(
    default_store=shared_store,
    routing_repo=routing_repo,
)

# The router implements the EventStore protocol
# Use it exactly like a regular event store
events = await router.read_events(stream_id, tenant_id="tenant-123")
await router.append_events(stream_id, events, tenant_id="tenant-123")
```

### BulkCopier

Streams historical events from source to target store during the `BULK_COPY` phase.

- Configurable batch size and rate limiting
- Pause/resume support
- Progress tracking with estimated completion time
- Position mapping recording for subscription continuity

### DualWriteInterceptor

Handles transparent dual-write during the `DUAL_WRITE` phase.

- **Source-first semantics**: Source write must succeed; target is best-effort
- **Async target writes**: Minimal latency impact on application
- **Failure tracking**: Failed target writes logged for recovery

### CutoverManager

Performs the atomic switch from source to target store.

- **Sub-100ms pause**: Configurable timeout (default 500ms)
- **Advisory locks**: Distributed coordination via PostgreSQL
- **Automatic rollback**: Reverts to `DUAL_WRITE` on failure

### SyncLagTracker

Monitors synchronization lag between stores during dual-write.

```python
# Check current lag
lag = await sync_lag_tracker.get_current_lag(migration.id)
print(f"Current lag: {lag.events_behind} events")
print(f"Converging: {lag.is_converging}")
```

### ConsistencyVerifier

Validates data consistency between stores after migration.

```python
from eventsource.migration import VerificationLevel

# Verify consistency with different levels
report = await verifier.verify(
    migration_id=migration.id,
    level=VerificationLevel.HASH,  # COUNT, HASH, or FULL
)

for stream_id, consistency in report.streams.items():
    print(f"Stream {stream_id}: {consistency.status}")
```

### SubscriptionMigrator

Migrates subscription checkpoints from source to target store positions.

```python
# Plan the migration
plan = await migrator.plan_migration(migration.id)
print(f"Will migrate {len(plan.subscriptions)} subscriptions")

# Execute the migration
summary = await migrator.execute_migration(plan)
print(f"Migrated: {summary.successful}/{summary.total}")
```

## Error Handling

The migration system includes sophisticated error handling with classification and recovery.

### Error Severity

- `CRITICAL`: System failure requiring immediate attention
- `ERROR`: Significant issue affecting migration
- `WARNING`: Potential issue to monitor
- `INFO`: Informational message

### Error Recoverability

- `RECOVERABLE`: Can be fixed with operator action
- `TRANSIENT`: Temporary issue, will be auto-retried
- `FATAL`: Requires migration abort

### Circuit Breaker

Built-in circuit breaker for transient failures:

```python
from eventsource.migration import CircuitBreaker, CircuitBreakerConfig

breaker = CircuitBreaker(CircuitBreakerConfig(
    failure_threshold=5,
    recovery_timeout_seconds=30,
    half_open_max_calls=3,
))

# The coordinator uses circuit breakers internally
# for operations like target store writes
```

### Common Errors

```python
from eventsource.migration import (
    MigrationNotFoundError,
    MigrationStateError,
    CutoverTimeoutError,
    ConsistencyError,
)

try:
    await coordinator.trigger_cutover(migration_id)
except CutoverTimeoutError:
    # Cutover exceeded pause SLA - automatically rolled back
    print("Cutover timed out, rolled back to DUAL_WRITE")
except MigrationStateError as e:
    # Invalid state for operation
    print(f"Cannot cutover: {e}")
```

## Monitoring and Observability

### Status Streaming

```python
async for status in coordinator.stream_status(migration.id):
    print(f"Phase: {status.phase.value}")
    print(f"Progress: {status.progress_percent:.1f}%")
    print(f"Events copied: {status.events_copied}")
    print(f"Sync lag: {status.sync_lag_events}")
    print(f"Rate: {status.current_rate_events_per_sec:.0f} events/sec")
    print(f"ETA: {status.estimated_completion_time}")
```

### Metrics

```python
from eventsource.migration import get_migration_metrics

metrics = get_migration_metrics(migration.id)
snapshot = metrics.snapshot()

print(f"Bulk copy duration: {snapshot.bulk_copy_duration}")
print(f"Dual-write duration: {snapshot.dual_write_duration}")
print(f"Cutover duration: {snapshot.cutover_duration}")
print(f"Total events: {snapshot.total_events_copied}")
```

### Audit Logging

All significant migration events are recorded:

```python
from eventsource.migration import AuditEventType

# Events logged include:
# - MIGRATION_STARTED
# - PHASE_CHANGED
# - CUTOVER_INITIATED
# - CUTOVER_COMPLETED
# - CUTOVER_ROLLED_BACK
# - MIGRATION_COMPLETED
# - ERROR_OCCURRED
```

## Complete Example

```python
import asyncio
from eventsource.migration import (
    MigrationCoordinator,
    MigrationConfig,
    MigrationPhase,
    TenantStoreRouter,
    VerificationLevel,
)

async def migrate_tenant(
    tenant_id: str,
    source_store,
    target_store,
    target_store_id: str,
):
    """Migrate a tenant from source to target store."""

    # Set up infrastructure (repositories, etc. would be created earlier)
    router = TenantStoreRouter(source_store, routing_repo)

    coordinator = MigrationCoordinator(
        source_store=source_store,
        migration_repo=migration_repo,
        routing_repo=routing_repo,
        router=router,
        lock_manager=lock_manager,
        position_mapper=position_mapper,
        checkpoint_repo=checkpoint_repo,
    )

    # Configure migration
    config = MigrationConfig(
        batch_size=500,
        cutover_max_lag_events=50,
        verify_consistency=True,
        migrate_subscriptions=True,
    )

    # Start migration
    migration = await coordinator.start_migration(
        tenant_id=tenant_id,
        target_store=target_store,
        target_store_id=target_store_id,
        config=config,
    )
    print(f"Started migration {migration.id}")

    # Monitor bulk copy phase
    print("Copying historical events...")
    await coordinator.wait_for_phase(
        migration.id,
        MigrationPhase.DUAL_WRITE,
        timeout=3600,  # 1 hour max for bulk copy
    )
    print("Bulk copy complete, now in dual-write mode")

    # Wait for sync lag to converge
    print("Waiting for sync lag to converge...")
    while True:
        ready, error = await coordinator.is_cutover_ready(migration.id)
        if ready:
            break

        status = await coordinator.get_status(migration.id)
        print(f"Sync lag: {status.sync_lag_events} events")
        await asyncio.sleep(5)

    # Execute cutover
    print("Executing cutover...")
    result = await coordinator.trigger_cutover(migration.id)

    if result.success:
        print(f"Cutover successful in {result.duration_ms:.2f}ms")

        # Verify consistency
        report = coordinator.get_consistency_report(migration.id)
        if report:
            print(f"Consistency verified: {report.is_consistent}")

        # Check subscription migration
        summary = coordinator.get_subscription_summary(migration.id)
        if summary:
            print(f"Subscriptions migrated: {summary.successful}/{summary.total}")

        print("Migration complete!")
    else:
        print(f"Cutover failed: {result.error_message}")
        print("Rolled back to dual-write mode")
        # Handle failure - could retry or abort

# Run the migration
asyncio.run(migrate_tenant(
    tenant_id="tenant-123",
    source_store=shared_store,
    target_store=dedicated_store,
    target_store_id="dedicated-tenant-123",
))
```

## Module Structure

```
eventsource/migration/
├── __init__.py              # Public API exports
├── coordinator.py           # MigrationCoordinator - main orchestrator
├── router.py                # TenantStoreRouter - transparent routing
├── bulk_copier.py           # BulkCopier - historical event streaming
├── dual_write.py            # DualWriteInterceptor - transparent dual-write
├── cutover.py               # CutoverManager - atomic switch
├── sync_lag_tracker.py      # SyncLagTracker - convergence monitoring
├── position_mapper.py       # PositionMapper - position translation
├── subscription_migrator.py # SubscriptionMigrator - checkpoint migration
├── consistency.py           # ConsistencyVerifier - data validation
├── write_pause.py           # WritePauseManager - pause coordination
├── status_streamer.py       # StatusStreamer - real-time updates
├── models.py                # Data models and enums
├── exceptions.py            # Error types and classification
├── metrics.py               # Migration metrics
└── repositories/            # Data persistence
    ├── migration.py         # Migration state repository
    ├── routing.py           # Tenant routing repository
    ├── position_mapping.py  # Position mapping repository
    └── audit_log.py         # Audit log repository
```

## Performance Characteristics

| Phase | Duration | Impact |
|-------|----------|--------|
| Bulk Copy | Hours to days | Minimal - background streaming |
| Dual-Write | Minutes to hours | Low - async target writes |
| Cutover | <100ms | Brief pause (writes blocked) |
| Verification | Seconds to minutes | None - post-migration |

## Best Practices

1. **Test in staging first**: Run full migration cycles in staging before production
2. **Monitor sync lag**: Ensure lag converges before triggering cutover
3. **Use appropriate batch sizes**: Larger batches = faster, but more memory
4. **Set realistic timeouts**: Cutover timeout should account for network latency
5. **Enable consistency verification**: Catch issues before they affect users
6. **Keep audit logs**: Essential for debugging and compliance
7. **Plan for rollback**: Dual-write allows reverting if issues are found
