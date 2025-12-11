# Migration Troubleshooting Guide

This guide helps diagnose and resolve common issues during tenant migrations.

## Quick Diagnosis

| Symptom | Likely Cause | Section |
|---------|--------------|---------|
| Migration stuck in bulk_copy | Target store slow/unavailable | [Slow Bulk Copy](#slow-bulk-copy) |
| High sync lag in dual_write | Target write failures | [High Sync Lag](#high-sync-lag) |
| Cutover timeout | Too many concurrent writes | [Cutover Timeout](#cutover-timeout) |
| Consistency check fails | Missing events in target | [Consistency Failures](#consistency-failures) |
| WritePausedError | Cutover taking too long | [Write Paused Errors](#write-paused-errors) |
| Migration not found | Wrong ID or not started | [Migration Not Found](#migration-not-found) |

---

## Error: MigrationNotFoundError

### Symptoms
```
MigrationNotFoundError: Migration 12345678-... not found
```

### Causes
1. Migration ID is incorrect
2. Migration was never started
3. Database connection issue

### Resolution

```python
# Check all migrations for tenant
migrations = await migration_repo.get_migrations_by_tenant(tenant_id)
for m in migrations:
    print(f"{m.id}: {m.state.value}")

# Check if migration exists
exists = await migration_repo.migration_exists(migration_id)
if not exists:
    print("Migration does not exist - check ID")
```

---

## Error: InvalidStateTransitionError

### Symptoms
```
InvalidStateTransitionError: Cannot transition from 'migrated' to 'paused'
```

### Causes
1. Attempting operation not valid for current state
2. Race condition between operations
3. Migration already completed/failed

### Resolution

```python
# Check current state
migration = await coordinator.get_migration(migration_id)
print(f"Current state: {migration.state.value}")

# Valid transitions by state:
# PENDING -> BULK_COPY, FAILED
# BULK_COPY -> DUAL_WRITE, PAUSED, FAILED, ROLLED_BACK
# DUAL_WRITE -> CUTOVER_PAUSED, PAUSED, FAILED, ROLLED_BACK
# PAUSED -> BULK_COPY, DUAL_WRITE, ROLLED_BACK
# CUTOVER_PAUSED -> MIGRATED, FAILED, ROLLED_BACK
# MIGRATED -> (none - terminal state)
# FAILED -> ROLLED_BACK
# ROLLED_BACK -> (none - terminal state)
```

### If Migration Already Completed
- Cannot undo a completed migration through the API
- Must perform a reverse migration (new migration from target to source)

---

## Error: CutoverTimeoutError

### Symptoms
```
CutoverTimeoutError: Cutover timed out after 5.0 seconds
  Blocked writers: 15
```

### Causes
1. Too many concurrent write operations
2. Long-running transactions blocking
3. Timeout configured too short

### Resolution

**Step 1: Check for long-running transactions**
```sql
SELECT pid, now() - xact_start as duration, query
FROM pg_stat_activity
WHERE state = 'active'
  AND xact_start < now() - interval '5 seconds'
ORDER BY xact_start;
```

**Step 2: Increase timeout**
```python
config = MigrationConfig(
    cutover_timeout_seconds=30.0,  # Increase from 5s
)
coordinator = MigrationCoordinator(..., config=config)
```

**Step 3: Retry during low-traffic period**
```python
# Schedule cutover during off-peak hours
await coordinator.initiate_cutover(migration_id)
```

**Step 4: Check for write storms**
```python
# Monitor write rate before retry
stats = await router.write_pause_manager.get_pause_stats(tenant_id)
print(f"Blocked writers: {stats.blocked_count}")
```

---

## Error: ConsistencyError

### Symptoms
```
ConsistencyError: Verification failed: 5 violations found
```

### Causes
1. Events missing from target store
2. Events duplicated in target store
3. Event content corruption
4. Target store write failures during dual-write

### Resolution

**Step 1: Get detailed report**
```python
report = await verifier.verify_tenant_consistency(
    tenant_id=tenant_id,
    level=VerificationLevel.FULL,
)

print(f"Source events: {report.source_event_count}")
print(f"Target events: {report.target_event_count}")

for v in report.violations:
    print(f"Violation: {v}")
```

**Step 2: Identify affected streams**
```python
for sr in report.stream_results:
    if not sr.is_consistent:
        print(f"Stream {sr.stream_id}:")
        print(f"  Source: {sr.source_count} events")
        print(f"  Target: {sr.target_count} events")
        print(f"  Difference: {sr.count_mismatch}")
```

**Step 3: Re-copy missing events**
```python
# Abort current migration
await coordinator.abort_migration(migration_id)

# Start fresh migration (will re-copy all events)
new_migration = await coordinator.start_migration(
    tenant_id=tenant_id,
    source_store=source,
    target_store=target,
)
```

**Step 4: If specific streams affected**
```python
# Copy specific stream manually
copier = BulkCopier(source, target)
await copier.copy_stream(
    aggregate_id=affected_aggregate_id,
    aggregate_type=affected_type,
)
```

---

## Error: WritePausedError

### Symptoms
```
WritePausedError: Writes paused for tenant 12345678-...
  Timeout: 5.0 seconds
```

### Causes
1. Application trying to write during cutover pause
2. Cutover taking longer than expected
3. Cutover stuck

### Resolution

**Step 1: Check if cutover is in progress**
```python
migration = await coordinator.get_migration(migration_id)
if migration.state.value == "cutover_paused":
    print("Cutover in progress - writes will resume shortly")
```

**Step 2: If cutover is stuck**
```python
# Force resume writes
metrics = await router.resume_writes(tenant_id)
print(f"Resumed after {metrics.pause_duration_ms}ms")
print(f"Blocked writers: {metrics.blocked_count}")
```

**Step 3: Increase client timeout**
```python
# In application code, handle pause gracefully
try:
    await router.append_events(...)
except WritePausedError:
    # Retry after short delay
    await asyncio.sleep(1)
    await router.append_events(...)
```

---

## Error: PositionMappingError

### Symptoms
```
PositionMappingError: No mapping found for source position 12345
```

### Causes
1. Position mapping not recorded during bulk copy
2. Gap in position mappings
3. Subscription checkpoint position is before migration started

### Resolution

**Step 1: Check position mappings**
```sql
SELECT source_position, target_position, event_id
FROM migration_position_mappings
WHERE migration_id = '...'
ORDER BY source_position;
```

**Step 2: Use nearest position**
```python
# Configure position mapper to use nearest
translation = await mapper.translate_position(
    migration_id=migration_id,
    source_position=position,
    use_nearest=True,  # Will find closest mapping
)
```

**Step 3: If mappings missing**
```python
# Re-record mappings during bulk copy
# This requires restarting the migration
await coordinator.abort_migration(migration_id)
migration = await coordinator.start_migration(...)
```

---

## Problem: Slow Bulk Copy

### Symptoms
- Progress percentage increases slowly
- Events copied counter moves slowly
- Migration taking hours/days

### Causes
1. Large tenant with millions of events
2. Slow target store
3. Network latency
4. Batch size too small

### Resolution

**Step 1: Check progress rate**
```python
import time

start = time.time()
start_count = migration.events_copied

await asyncio.sleep(60)  # Wait 1 minute

migration = await coordinator.get_migration(migration_id)
rate = (migration.events_copied - start_count) / 60
print(f"Copy rate: {rate:.0f} events/second")
```

**Step 2: Increase batch size**
```python
config = MigrationConfig(
    batch_size=5000,  # Increase from default 1000
)
```

**Step 3: Check target store performance**
```sql
-- Check for slow queries on target
SELECT query, calls, mean_exec_time
FROM pg_stat_statements
WHERE query LIKE '%INSERT%events%'
ORDER BY mean_exec_time DESC;
```

**Step 4: Optimize target store**
```sql
-- Disable indexes during bulk load
ALTER INDEX events_tenant_idx ON events_target SET (indisvalid = false);

-- Re-enable after bulk copy
REINDEX INDEX events_tenant_idx;
```

---

## Problem: High Sync Lag

### Symptoms
- Sync lag stays high during dual-write
- Lag increases over time
- Cannot proceed to cutover

### Causes
1. Target write failures
2. Target store slower than source
3. High write volume

### Resolution

**Step 1: Check dual-write failures**
```python
interceptor = router._dual_write_interceptors.get(tenant_id)
if interceptor:
    stats = interceptor.get_failure_stats()
    print(f"Total failures: {stats.total_failures}")
    print(f"Events failed: {stats.total_events_failed}")
    print(f"First failure: {stats.first_failure_at}")
    print(f"Last failure: {stats.last_failure_at}")
```

**Step 2: Check target store health**
```sql
-- Check connection count
SELECT count(*) FROM pg_stat_activity
WHERE datname = 'events_target';

-- Check for locks
SELECT relation::regclass, mode, granted
FROM pg_locks
WHERE NOT granted;
```

**Step 3: Reduce write volume temporarily**
- Scale down write-heavy services
- Defer batch operations
- Rate limit API writes

**Step 4: Recover failed writes**
```python
# Background catch-up will handle this automatically
# But you can trigger manual catch-up:
copier = BulkCopier(source, target)
await copier.catch_up_from_position(
    tenant_id=tenant_id,
    from_position=last_known_target_position,
)
```

---

## Problem: Target Store Connection Issues

### Symptoms
- Dual-write failures
- Target unreachable errors
- Intermittent failures

### Resolution

**Step 1: Check connectivity**
```python
try:
    await target_store.get_global_position()
    print("Target store reachable")
except Exception as e:
    print(f"Target store error: {e}")
```

**Step 2: Check connection pool**
```python
# If using asyncpg
print(f"Pool size: {pool.get_size()}")
print(f"Free connections: {pool.get_idle_size()}")
print(f"Used connections: {pool.get_size() - pool.get_idle_size()}")
```

**Step 3: Increase pool size**
```python
pool = await asyncpg.create_pool(
    dsn=target_dsn,
    min_size=10,
    max_size=50,  # Increase max connections
)
```

---

## Problem: Memory Issues During Bulk Copy

### Symptoms
- Process memory grows during copy
- Out of memory errors
- System swap usage increases

### Causes
1. Batch size too large
2. Events contain large data
3. Memory leak in event processing

### Resolution

**Step 1: Reduce batch size**
```python
config = MigrationConfig(
    batch_size=100,  # Reduce from default 1000
)
```

**Step 2: Monitor memory**
```python
import psutil

process = psutil.Process()
print(f"Memory: {process.memory_info().rss / 1024 / 1024:.0f} MB")
```

**Step 3: Use streaming instead of batch loading**
```python
# BulkCopier already streams - verify it's being used correctly
async for batch in copier._stream_batches(tenant_id):
    await copier._write_batch(batch)
    # Memory should stay constant here
```

---

## Problem: Audit Log Gaps

### Symptoms
- Missing entries in migration_audit_log
- Cannot trace migration history

### Resolution

**Step 1: Check audit log**
```sql
SELECT event_type, details, created_at
FROM migration_audit_log
WHERE migration_id = '...'
ORDER BY created_at;
```

**Step 2: Verify audit repository is configured**
```python
coordinator = MigrationCoordinator(
    audit_log=audit_repository,  # Must be provided
    ...
)
```

---

## Diagnostic Queries

### Check All Active Migrations
```sql
SELECT id, tenant_id, state, progress_percentage,
       events_copied, total_events, created_at
FROM tenant_migrations
WHERE state NOT IN ('migrated', 'failed', 'rolled_back')
ORDER BY created_at DESC;
```

### Check Tenant Routing State
```sql
SELECT tenant_id, store_id, migration_state, target_store_id
FROM tenant_routing
WHERE migration_state != 'normal';
```

### Check Position Mapping Coverage
```sql
SELECT migration_id,
       min(source_position) as min_pos,
       max(source_position) as max_pos,
       count(*) as mapping_count
FROM migration_position_mappings
GROUP BY migration_id;
```

### Check Recent Audit Events
```sql
SELECT m.tenant_id, a.event_type, a.details, a.created_at
FROM migration_audit_log a
JOIN tenant_migrations m ON a.migration_id = m.id
ORDER BY a.created_at DESC
LIMIT 50;
```

---

## Recovery Procedures

### Complete Recovery from Failed Migration

```python
async def recover_failed_migration(
    coordinator: MigrationCoordinator,
    migration_id: UUID,
    tenant_id: UUID
):
    """Recover from a failed migration state."""

    migration = await coordinator.get_migration(migration_id)

    if migration.state.value != "failed":
        print(f"Migration not in failed state: {migration.state.value}")
        return

    print(f"Recovery for migration {migration_id}")
    print(f"Error: {migration.error_message}")

    # Step 1: Abort and rollback
    print("\n1. Rolling back to source store...")
    await coordinator.abort_migration(migration_id, reason="Recovery from failure")

    # Step 2: Verify routing
    routing = await routing_repo.get_routing(tenant_id)
    print(f"2. Routing state: {routing.store_id} ({routing.migration_state.value})")

    # Step 3: Verify tenant can write
    print("3. Testing write capability...")
    # ... test write ...

    # Step 4: Option to restart
    restart = input("Restart migration? (y/n): ")
    if restart.lower() == "y":
        new_migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            source_store=source_store,
            target_store=target_store,
        )
        print(f"New migration started: {new_migration.id}")
```

---

## Getting Help

If you cannot resolve an issue:

1. **Collect diagnostics**:
   - Migration ID and state
   - Error messages
   - Relevant logs
   - Database query results

2. **Check logs**:
   ```bash
   # Filter logs for migration
   grep "migration_id=12345678" app.log
   ```

3. **Enable debug logging**:
   ```python
   import logging
   logging.getLogger("eventsource.migration").setLevel(logging.DEBUG)
   ```

4. **File an issue** with:
   - eventsource-py version
   - Python version
   - PostgreSQL version
   - Steps to reproduce
   - Full error traceback
