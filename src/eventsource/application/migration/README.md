# eventsource.application.migration

A zero-downtime live migration system for multi-tenant event stores. This module enables seamless backend migrations (e.g., shared PostgreSQL to dedicated PostgreSQL) without stopping your application.

## Overview

The migration system provides:

- **Zero data loss**: Source store remains authoritative throughout migration
- **Short cutover pause**: Target pause under 100 ms, with a hard timeout (`cutover_timeout_ms`). Failures before the routing switch roll back automatically
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
               │              │             ├──► DUAL_WRITE  (failure before the switch)
               │              │             └──► FAILED      (failed check after the switch)
               │              │
               └──────────────┴──► ABORTED  (operator abort; not allowed in CUTOVER)
```

| Phase | Description |
|-------|-------------|
| `PENDING` | Migration created but not started |
| `BULK_COPY` | Historical events being copied to target store; the dual-write interceptor is already installed, so new writes are mirrored while the copy runs |
| `DUAL_WRITE` | Copy complete; new events keep being written to both stores |
| `CUTOVER` | Brief pause while routing switches to target |
| `COMPLETED` | Migration finished successfully, including any enabled post-switch checks |
| `ABORTED` | Migration cancelled by operator |
| `FAILED` | Unrecoverable error occurred. Also used when a post-switch check fails: the tenant stays on the target store |

## Quick Start

### Basic Migration

```python
from eventsource.application.migration import (
    MigrationCoordinator,
    MigrationConfig,
    MigrationPhase,
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
    tenant_id=tenant_id,              # a UUID
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
    else:
        # rolled_back=True: the tenant is back on the source (DUAL_WRITE).
        # rolled_back=False: the tenant stays on the target and the migration is FAILED.
        print(f"Cutover did not complete: {result.error_message} (rolled_back={result.rolled_back})")
```

### Configuration Options

```python
config = MigrationConfig(
    # Bulk copy settings
    batch_size=1000,              # Events per batch
    max_bulk_copy_rate=10000,     # Max events/second

    # Cutover settings
    cutover_max_lag_events=0,     # strict (the default): no cutover over missing events
    cutover_timeout_ms=500,       # Hard timeout for cutover (minimum 100)

    # Post-cutover options
    position_mapping_enabled=True,  # Record position mappings
    verify_consistency=True,        # Verify data after the switch; a failed check ends in FAILED
    migrate_subscriptions=True,     # Migrate subscription checkpoints; a failure ends in FAILED
)

migration = await coordinator.start_migration(
    tenant_id=tenant_id,
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

# Pause/resume migration (the write pause applies to this process only)
await coordinator.pause_migration(migration.id)
await coordinator.resume_migration(migration.id)

# Abort migration (raises MigrationError when the migration is terminal or in CUTOVER)
await coordinator.abort_migration(migration.id, reason="No longer needed")
```

### TenantStoreRouter

A transparent proxy that routes operations to the correct store based on tenant migration state. Use this as your event store during migrations.

```python
router = TenantStoreRouter(
    default_store=shared_store,
    routing_repo=routing_repo,
)

# The router satisfies the FullEventStore port
# Use it exactly like a regular event store
async for envelope in router.read_stream(stream):
    ...
await router.append(stream, events, ExpectedVersion.exact(0))
```

### BulkCopier

Streams historical events from source to target store during the `BULK_COPY` phase.

- Configurable batch size and rate limiting
- Pause/resume support
- Progress tracking with estimated completion time
- Position mapping recording for subscription continuity
- **Duplicate events**: An event that already exists in the target counts as copied only when the target holds the same event at the same stream version, with the same event ID, type, and payload. Any other difference raises `BulkCopyError`. The coordinator then ends the migration in `FAILED`.

### DualWriteInterceptor

Handles transparent dual-write. Installed at the start of the bulk-copy
phase -- before the copy pass -- so there is never a window in which a
write is mirrored by nobody.

- **Source-first semantics**: Source write must succeed; target is best-effort
- **Converged-only mirroring**: The mirror appends at the exact pre-append
  source version, so it only ever extends a target stream that has fully
  caught up -- it can never put events out of order while the copy runs
- **Failure tracking**: Failed target writes logged for recovery; the
  coordinator's catch-up rounds re-copy and absorb them

### CutoverManager

Performs the atomic switch from source to target store.

- **Short pause**: The target pause is under 100 ms (`CutoverResult.within_timeout`). The hard timeout is `cutover_timeout_ms` (default 500 ms). A timeout is returned as a failed `CutoverResult`; it is not raised.
- **Cutover lock**: The coordinator takes a cutover lock from the lock manager (for example `PostgreSQLLockManager`). Abort takes the same lock, so it cannot interleave with a running cutover.
- **Rollback before the switch**: A failure before the routing switch reverts the migration to `DUAL_WRITE`. `CutoverResult.rolled_back` is `True`.
- **Failure after the switch**: Routing already points to the target. A failed consistency check or subscription migration ends the migration in `FAILED`. The tenant stays on the target store. `CutoverResult.success` is `False`, `CutoverResult.consistency_verified` shows the check outcome, and `CutoverResult.error_message` describes the failure.
- **Write pause is per process**: The pause blocks writers in the process that runs the cutover only. Other processes see `CUTOVER_PAUSED` in the routing record and reject their writes, but they do not wait for the pause. A write that passed the routing check before the pause can still reach the source. Run one writer per tenant during cutover.

### SyncLagTracker

Monitors synchronization lag between stores during dual-write.

```python
# Most recent lag measurement (None until the first measurement)
lag = sync_lag_tracker.current_lag
if lag is not None:
    print(f"Current lag: {lag.events} events")

# Aggregated statistics over the sample window
stats = sync_lag_tracker.get_lag_stats()
print(f"Converging: {stats.is_converging}")
```

### ConsistencyVerifier

Validates data consistency between stores. The coordinator runs this check after the switch when `verify_consistency=True`. Read the result with `coordinator.get_consistency_report(migration.id)`.

```python
from eventsource.application.migration import VerificationLevel

# Verify consistency with different levels
report = await verifier.verify_tenant_consistency(
    tenant_id,
    level=VerificationLevel.HASH,  # COUNT, HASH, or FULL
    sample_percentage=100.0,
)

print(f"Consistent: {report.is_consistent}")
for stream in report.stream_results:
    print(f"Stream {stream.stream_id}: consistent={stream.is_consistent}")
```

### SubscriptionMigrator

Migrates subscription checkpoints from source to target store positions. The coordinator runs this step after the switch when `migrate_subscriptions=True`. A failed subscription ends the migration in `FAILED`.

```python
# Plan the migration
plan = await migrator.plan_migration(migration.id, tenant_id, subscription_names)
print(f"Will migrate {plan.migratable_count} of {plan.total_subscriptions} subscriptions")

# Execute the migration (dry_run=True reports without writing checkpoints)
summary = await migrator.migrate_subscriptions(
    migration.id, tenant_id, subscription_names, dry_run=False
)
print(f"Migrated: {summary.successful_count} ok, {summary.failed_count} failed")
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

`CircuitBreaker` is a standalone utility. It is exported from this package. The coordinator does not create one. To use it, pass a breaker to `ErrorHandler`:

```python
from eventsource.application.migration import CircuitBreaker, CircuitBreakerConfig

breaker = CircuitBreaker(CircuitBreakerConfig(
    failure_threshold=5,     # failures before the circuit opens
    success_threshold=2,     # successes in half-open state before it closes
    timeout_seconds=30.0,    # time the circuit stays open before a trial call
))
```

### Common Errors

`trigger_cutover` raises for invalid requests. It does not raise for a failed cutover. A failed cutover is returned as a `CutoverResult` with `success=False`.

```python
from eventsource.application.migration import (
    MigrationNotFoundError,
    MigrationStateError,
)

try:
    result = await coordinator.trigger_cutover(migration_id)
except MigrationNotFoundError:
    print("Unknown migration")
except MigrationStateError as e:
    # Not in DUAL_WRITE, so no cutover started
    print(f"Cannot cutover: {e}")
else:
    if not result.success:
        if result.rolled_back:
            print(f"Cutover timed out or failed, rolled back to DUAL_WRITE: {result.error_message}")
        else:
            print(f"Cutover switched routing, but a post-switch check failed. Migration is FAILED: {result.error_message}")
```

## Monitoring and Observability

### Status Streaming

```python
async for status in coordinator.stream_status(migration.id, update_interval=1.0):
    print(f"Phase: {status.phase.value}")
    print(f"Progress: {status.progress_percent:.1f}%")
    print(f"Events copied: {status.events_copied}")
    print(f"Sync lag: {status.sync_lag_events}")
    print(f"Rate: {status.current_rate_events_per_sec:.0f} events/sec")
    print(f"ETA: {status.estimated_completion}")
    print(f"Paused: {status.is_paused}")
```

- `update_interval` must be at least 0.05 seconds. A smaller value raises `ValueError`.
- A streamer accepts at most 100 subscribers. A new subscriber beyond that limit raises `RuntimeError`.
- The stream stops when the migration reaches a terminal phase.

### Metrics

```python
from eventsource.application.migration import get_migration_metrics

metrics = get_migration_metrics(str(migration.id), str(migration.tenant_id))
snapshot = metrics.get_snapshot()

print(f"Events copied: {snapshot.events_copied}")
print(f"Copy rate: {snapshot.events_copied_rate:.0f} events/sec")
print(f"Sync lag: {snapshot.sync_lag_events}")
print(f"Phase durations (seconds): {snapshot.phase_durations}")
print(f"Cutover durations (ms): {snapshot.cutover_durations}")
print(f"Cutover failures: {snapshot.cutover_failures}")
print(f"Verification failures: {snapshot.verification_failures}")
```

`get_migration_metrics` takes string IDs. It creates the metrics entry when none exists. The coordinator releases the entry when the migration reaches a terminal phase.

### Audit Logging

All significant migration events are recorded:

```python
from eventsource.application.migration import AuditEventType

# Events logged include:
# - MIGRATION_STARTED
# - PHASE_CHANGED
# - MIGRATION_PAUSED / MIGRATION_RESUMED
# - CUTOVER_INITIATED
# - CUTOVER_COMPLETED
# - CUTOVER_ROLLED_BACK
# - VERIFICATION_STARTED / VERIFICATION_COMPLETED / VERIFICATION_FAILED
# - MIGRATION_COMPLETED
# - MIGRATION_ABORTED
# - MIGRATION_FAILED
# - ERROR_OCCURRED
```

## Complete Example

```python
import asyncio
from eventsource.application.migration import (
    MigrationCoordinator,
    MigrationConfig,
    MigrationPhase,
    TenantStoreRouter,
    VerificationLevel,
)

async def migrate_tenant(
    tenant_id,
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

        # Verification report (None when verify_consistency=False)
        report = coordinator.get_consistency_report(migration.id)
        if report:
            print(f"Consistency verified: {report.is_consistent}")

        # Subscription summary (None when migrate_subscriptions=False)
        summary = coordinator.get_subscription_summary(migration.id)
        if summary:
            print(f"Subscriptions migrated: {summary.successful_count}/{len(summary.results)}")

        print("Migration complete!")
    elif result.rolled_back:
        print(f"Cutover failed: {result.error_message}")
        print("Rolled back to dual-write mode")
        # Handle failure - could retry or abort
    else:
        print(f"Post-switch check failed: {result.error_message}")
        print("Migration is FAILED. The tenant stays on the target store.")

# Run the migration
asyncio.run(migrate_tenant(
    tenant_id=tenant_id,
    source_store=shared_store,
    target_store=dedicated_store,
    target_store_id="dedicated-tenant-123",
))
```

## Module Structure

The public API comes from `eventsource.application.migration`. The data types
(models, enums, and repository ports) live in `eventsource.ports.migration`.

```
application/migration/
├── __init__.py                     # Public API exports
├── coordinator.py                  # MigrationCoordinator (mixins: coordinator_*.py)
├── router.py                       # TenantStoreRouter (helpers: router_*.py)
├── bulk_copier.py                  # BulkCopier (helpers: bulk_copier_*.py)
├── dual_write.py                   # DualWriteInterceptor (helpers: dual_write_*.py)
├── cutover.py                      # CutoverManager (helpers: cutover_*.py)
├── sync_lag_tracker.py             # SyncLagTracker (helpers: sync_lag_tracker_*.py)
├── position_mapper.py              # PositionMapper (helpers: position_mapper_*.py)
├── subscription_migrator.py        # SubscriptionMigrator (helpers: subscription_migrator_*.py)
├── consistency.py                  # ConsistencyVerifier (helpers: consistency_*.py)
├── write_pause.py                  # WritePauseManager (helpers: write_pause_*.py)
├── status_streamer.py              # StatusStreamer (helpers: status_streamer_*.py)
├── metrics_registry.py             # get_migration_metrics and active-migration tracking
├── metrics_recorder.py             # MigrationMetrics (mixin: metrics_timers.py)
├── metrics_types.py                # Metric instruments and MigrationMetricSnapshot
├── circuit_breaker.py              # CircuitBreaker
├── error_handling.py               # ErrorHandler
├── error_classification.py         # Error severity and recoverability
└── exceptions*.py                  # Error types

ports/migration/
├── models/                         # Migration, MigrationConfig, MigrationPhase,
│                                   # MigrationStatus, CutoverResult, audit types
└── repositories/                   # Migration state, routing, position mapping, audit log
```

## Performance Characteristics

| Phase | Duration | Impact |
|-------|----------|--------|
| Bulk Copy | Hours to days | Minimal - background streaming |
| Dual-Write | Minutes to hours | Low - async target writes |
| Cutover | Target under 100ms; hard timeout `cutover_timeout_ms` (default 500ms) | Brief pause (writes blocked in the cutover process) |
| Verification | Seconds to minutes | Runs after the switch; a failure ends in `FAILED` |

## Best Practices

1. **Test in staging first**: Run full migration cycles in staging before production
2. **Monitor sync lag**: Ensure lag converges before triggering cutover
3. **Use appropriate batch sizes**: Larger batches = faster, but more memory
4. **Set realistic timeouts**: Cutover timeout should account for network latency
5. **Enable consistency verification**: Catch issues before they affect users
6. **Keep audit logs**: Essential for debugging and compliance
7. **Plan for rollback**: Before the switch, a failed cutover returns to `DUAL_WRITE`. After the switch, the tenant stays on the target store. A failed check ends in `FAILED`, so rehearse this path in staging
8. **Run one writer per tenant during cutover**: The write pause does not apply across processes
