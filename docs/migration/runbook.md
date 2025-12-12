# Migration Operations Runbook

This runbook provides procedures for common operational scenarios during tenant migrations.

## Quick Reference

| Scenario | Action | Command |
|----------|--------|---------|
| Check status | Get current state | `await coordinator.get_migration(migration_id)` |
| Pause migration | Stop without losing progress | `await coordinator.pause_migration(migration_id)` |
| Resume migration | Continue from pause | `await coordinator.resume_migration(migration_id)` |
| Abort and rollback | Cancel and revert | `await coordinator.abort_migration(migration_id)` |
| Force cutover | Skip verification | `await coordinator.initiate_cutover(migration_id, force=True)` |

---

## Procedure 1: Check Migration Status

### When to Use
- Regular monitoring during migration
- Investigating issues
- Before any operational action

### Steps

```python
from eventsource.migration import MigrationCoordinator

async def check_migration_status(coordinator: MigrationCoordinator, migration_id: UUID):
    """Check and display migration status."""

    migration = await coordinator.get_migration(migration_id)

    if migration is None:
        print(f"Migration {migration_id} not found")
        return

    print(f"Migration ID: {migration.id}")
    print(f"Tenant ID: {migration.tenant_id}")
    print(f"State: {migration.state.value}")
    print(f"Source Store: {migration.source_store_id}")
    print(f"Target Store: {migration.target_store_id}")

    if migration.state.value == "bulk_copy":
        print(f"Progress: {migration.progress_percentage:.1f}%")
        print(f"Events Copied: {migration.events_copied}/{migration.total_events}")

    if migration.error_message:
        print(f"Error: {migration.error_message}")

    print(f"Created: {migration.created_at}")
    print(f"Started: {migration.started_at}")
    print(f"Completed: {migration.completed_at}")
```

### Expected Output

```
Migration ID: 12345678-1234-1234-1234-123456789abc
Tenant ID: 87654321-4321-4321-4321-cba987654321
State: dual_write
Source Store: shared
Target Store: dedicated-a
Created: 2024-01-15T10:30:00Z
Started: 2024-01-15T10:30:01Z
Completed: None
```

---

## Procedure 2: Pause Migration

### When to Use
- Need to perform maintenance on source/target store
- High system load affecting migration
- Investigation needed before proceeding
- Planning to resume during a maintenance window

### Prerequisites
- Migration must be in `bulk_copy` or `dual_write` state

### Steps

```python
async def pause_migration(coordinator: MigrationCoordinator, migration_id: UUID):
    """Pause an active migration."""

    # Check current state
    migration = await coordinator.get_migration(migration_id)

    if migration is None:
        print(f"ERROR: Migration {migration_id} not found")
        return

    if migration.state.value not in ("bulk_copy", "dual_write"):
        print(f"ERROR: Cannot pause migration in state {migration.state.value}")
        return

    print(f"Current state: {migration.state.value}")
    print("Pausing migration...")

    try:
        migration = await coordinator.pause_migration(migration_id)
        print(f"Migration paused successfully")
        print(f"New state: {migration.state.value}")
    except Exception as e:
        print(f"ERROR: Failed to pause: {e}")
```

### What Happens When Paused

1. **During Bulk Copy**:
   - Copying stops at next batch boundary
   - Progress is saved
   - Routing unchanged (source store)

2. **During Dual-Write**:
   - New writes continue to source only
   - Target sync stops
   - Routing unchanged (source store)

### Verification

```python
migration = await coordinator.get_migration(migration_id)
assert migration.state.value == "paused"
```

---

## Procedure 3: Resume Migration

### When to Use
- After resolving issues that caused pause
- During a maintenance window
- Ready to continue after planned pause

### Prerequisites
- Migration must be in `paused` state

### Steps

```python
async def resume_migration(coordinator: MigrationCoordinator, migration_id: UUID):
    """Resume a paused migration."""

    migration = await coordinator.get_migration(migration_id)

    if migration is None:
        print(f"ERROR: Migration {migration_id} not found")
        return

    if migration.state.value != "paused":
        print(f"ERROR: Migration not paused (state: {migration.state.value})")
        return

    print("Resuming migration...")

    try:
        migration = await coordinator.resume_migration(migration_id)
        print(f"Migration resumed")
        print(f"New state: {migration.state.value}")
    except Exception as e:
        print(f"ERROR: Failed to resume: {e}")
```

### What Happens When Resumed

1. **If Was in Bulk Copy**:
   - Copying continues from last checkpoint
   - No events are duplicated or missed

2. **If Was in Dual-Write**:
   - Sync catches up from source
   - Dual-writing resumes

### Verification

```python
migration = await coordinator.get_migration(migration_id)
assert migration.state.value in ("bulk_copy", "dual_write")
```

---

## Procedure 4: Abort and Rollback

### When to Use
- Critical issues discovered during migration
- Need to abandon migration entirely
- Target store has issues
- Business decision to cancel

### Prerequisites
- Migration must NOT be in `migrated` state
- Rollback is destructive - data in target may be lost

### Steps

```python
async def abort_migration(coordinator: MigrationCoordinator, migration_id: UUID, reason: str):
    """Abort migration and rollback to source store."""

    migration = await coordinator.get_migration(migration_id)

    if migration is None:
        print(f"ERROR: Migration {migration_id} not found")
        return

    if migration.state.value == "migrated":
        print("ERROR: Cannot abort completed migration")
        print("Data is already on target store")
        return

    print(f"Current state: {migration.state.value}")
    print(f"Reason for abort: {reason}")
    print("")
    print("WARNING: This will:")
    print("  - Stop the migration")
    print("  - Reset routing to source store")
    print("  - Mark migration as rolled back")
    print("")

    # Confirm in production
    # confirmation = input("Type 'ABORT' to confirm: ")
    # if confirmation != "ABORT":
    #     print("Abort cancelled")
    #     return

    try:
        migration = await coordinator.abort_migration(migration_id, reason=reason)
        print(f"Migration aborted successfully")
        print(f"New state: {migration.state.value}")
    except Exception as e:
        print(f"ERROR: Failed to abort: {e}")
```

### What Happens During Abort

1. **Routing Reset**: Tenant routes back to source store immediately
2. **Dual-Write Stopped**: If active, dual-write interceptor removed
3. **State Updated**: Migration marked as `rolled_back`
4. **Audit Logged**: Abort reason recorded in audit log

### Verification

```python
# Check migration state
migration = await coordinator.get_migration(migration_id)
assert migration.state.value == "rolled_back"

# Check routing
routing = await routing_repo.get_routing(tenant_id)
assert routing.store_id == "shared"  # Back to source
assert routing.migration_state.value == "normal"
```

---

## Procedure 5: Initiate Cutover

### When to Use
- Migration in dual-write state
- Sync lag is zero or acceptable
- Consistency verified
- Ready to complete migration

### Prerequisites
- Migration must be in `dual_write` state
- Sync lag should be zero (or use force)

### Steps

```python
async def initiate_cutover(
    coordinator: MigrationCoordinator,
    migration_id: UUID,
    force: bool = False
):
    """Initiate cutover to target store."""

    migration = await coordinator.get_migration(migration_id)

    if migration is None:
        print(f"ERROR: Migration {migration_id} not found")
        return

    if migration.state.value != "dual_write":
        print(f"ERROR: Must be in dual_write state (current: {migration.state.value})")
        return

    # Check sync lag
    lag = await coordinator.get_sync_lag(migration_id)
    print(f"Current sync lag: {lag} events")

    if lag > 0 and not force:
        print("WARNING: Sync lag > 0. Wait for sync or use force=True")
        return

    print("Initiating cutover...")
    print("This will:")
    print("  1. Pause writes briefly")
    print("  2. Verify consistency")
    print("  3. Switch routing to target")
    print("  4. Resume writes")

    try:
        migration = await coordinator.initiate_cutover(migration_id, force=force)
        print(f"Cutover successful!")
        print(f"State: {migration.state.value}")
        print(f"Completed at: {migration.completed_at}")
    except CutoverTimeoutError as e:
        print(f"ERROR: Cutover timed out ({e.timeout_seconds}s)")
        print(f"Blocked writers: {e.blocked_writers}")
        print("Migration has been rolled back automatically")
    except ConsistencyError as e:
        print(f"ERROR: Consistency check failed: {e}")
        print("Migration has been rolled back automatically")
    except Exception as e:
        print(f"ERROR: Cutover failed: {e}")
```

### Force Cutover

Use `force=True` only when:
- You're confident data is consistent
- Sync lag is acceptable (will be lost)
- You understand the risks

```python
# Force cutover - USE WITH CAUTION
await coordinator.initiate_cutover(migration_id, force=True)
```

### Verification After Cutover

```python
# Check migration complete
migration = await coordinator.get_migration(migration_id)
assert migration.state.value == "migrated"

# Check routing updated
routing = await routing_repo.get_routing(tenant_id)
assert routing.store_id == "dedicated-a"
assert routing.migration_state.value == "normal"

# Verify reads work
events = await router.get_events(aggregate_id, "Order")
assert len(events.events) > 0
```

---

## Procedure 6: Handle Cutover Timeout

### When to Use
- Cutover exceeded timeout
- Migration automatically rolled back
- Need to investigate and retry

### Steps

```python
async def handle_cutover_timeout(
    coordinator: MigrationCoordinator,
    migration_id: UUID
):
    """Handle a cutover timeout and retry."""

    migration = await coordinator.get_migration(migration_id)

    if migration.state.value != "dual_write":
        print(f"Migration not in expected state: {migration.state.value}")
        return

    print("Investigating cutover timeout...")

    # Check what caused the timeout
    lag = await coordinator.get_sync_lag(migration_id)
    print(f"Sync lag: {lag} events")

    # Check for pending writes
    interceptor = router._dual_write_interceptors.get(tenant_id)
    if interceptor:
        stats = interceptor.get_failure_stats()
        print(f"Target write failures: {stats.total_failures}")

    # Options:
    print("\nOptions:")
    print("1. Wait for sync lag to clear and retry")
    print("2. Increase cutover timeout and retry")
    print("3. Force cutover (skip verification)")
    print("4. Abort migration")
```

### Retry with Increased Timeout

```python
# Create coordinator with longer timeout
coordinator_retry = MigrationCoordinator(
    migration_repo=migration_repo,
    routing_repo=routing_repo,
    router=router,
    config=MigrationConfig(
        cutover_timeout_seconds=30.0,  # Increased from 5s
    ),
)

await coordinator_retry.initiate_cutover(migration_id)
```

---

## Procedure 7: Handle Consistency Failure

### When to Use
- Consistency verification failed during cutover
- Need to investigate discrepancies
- Decide whether to fix or abort

### Steps

```python
async def handle_consistency_failure(
    source_store: EventStore,
    target_store: EventStore,
    tenant_id: UUID
):
    """Investigate and handle consistency failure."""

    verifier = ConsistencyVerifier(source_store, target_store)

    # Run detailed verification
    report = await verifier.verify_tenant_consistency(
        tenant_id=tenant_id,
        level=VerificationLevel.FULL,
        sample_percentage=100.0,
    )

    print(f"Consistency Report")
    print(f"==================")
    print(f"Source events: {report.source_event_count}")
    print(f"Target events: {report.target_event_count}")
    print(f"Streams verified: {report.streams_verified}")
    print(f"Streams consistent: {report.streams_consistent}")
    print(f"Violations: {len(report.violations)}")

    if not report.is_consistent:
        print("\nViolations:")
        for v in report.violations[:10]:  # Show first 10
            print(f"  - {v}")

        if len(report.violations) > 10:
            print(f"  ... and {len(report.violations) - 10} more")

    # Check specific streams
    print("\nInconsistent streams:")
    for sr in report.stream_results:
        if not sr.is_consistent:
            print(f"  {sr.stream_id}:")
            print(f"    Source: {sr.source_count} events, version {sr.source_version}")
            print(f"    Target: {sr.target_count} events, version {sr.target_version}")
```

### Recovery Options

1. **Fix and Retry**: If missing events, re-run bulk copy for affected streams
2. **Force Cutover**: If discrepancy is acceptable, force cutover
3. **Abort**: If data integrity is critical, abort and investigate

---

## Procedure 8: Emergency Stop

### When to Use
- System is unstable
- Need to stop all migration activity immediately
- Critical production issue

### Steps

```python
async def emergency_stop(router: TenantStoreRouter, tenant_ids: list[UUID]):
    """Emergency stop all migrations for tenants."""

    print("EMERGENCY STOP - Disabling migrations")

    for tenant_id in tenant_ids:
        # Force resume writes (in case paused)
        await router.resume_writes(tenant_id)

        # Clear dual-write interceptor
        router.clear_dual_write_interceptor(tenant_id)

        print(f"  Tenant {tenant_id}: Writes enabled, dual-write cleared")

    print("\nAll tenants back to normal operation")
    print("Migrations may need manual cleanup")
```

### Database-Level Emergency

If code-level stop isn't possible:

```sql
-- Emergency: Reset all tenant routing to source
UPDATE tenant_routing
SET migration_state = 'normal',
    target_store_id = NULL,
    updated_at = NOW()
WHERE migration_state != 'normal';

-- Emergency: Mark all active migrations as failed
UPDATE tenant_migrations
SET state = 'failed',
    error_message = 'Emergency stop - manual intervention'
WHERE state NOT IN ('migrated', 'failed', 'rolled_back');
```

---

## Procedure 9: Verify Post-Migration

### When to Use
- After migration completes
- Before declaring migration success
- Regular health check

### Steps

```python
async def verify_post_migration(
    coordinator: MigrationCoordinator,
    router: TenantStoreRouter,
    routing_repo: TenantRoutingRepository,
    migration_id: UUID,
    tenant_id: UUID,
    test_aggregate_id: UUID
):
    """Verify migration completed successfully."""

    print("Post-Migration Verification")
    print("===========================")

    # 1. Check migration state
    migration = await coordinator.get_migration(migration_id)
    print(f"Migration state: {migration.state.value}")
    assert migration.state.value == "migrated", "Migration not complete"

    # 2. Check routing
    routing = await routing_repo.get_routing(tenant_id)
    print(f"Tenant routing: {routing.store_id}")
    print(f"Migration state in routing: {routing.migration_state.value}")
    assert routing.migration_state.value == "normal", "Routing not normal"

    # 3. Test read operation
    print("\nTesting read...")
    events = await router.get_events(test_aggregate_id, "Order")
    print(f"  Read {len(events.events)} events")

    # 4. Test write operation
    print("\nTesting write...")
    from eventsource.events.base import DomainEvent
    test_event = DomainEvent(
        event_type="TestEvent",
        tenant_id=tenant_id,
    )
    result = await router.append_events(
        test_aggregate_id,
        "Order",
        [test_event],
        len(events.events),
    )
    print(f"  Write successful: {result.success}")

    # 5. Check no dual-write interceptor
    has_interceptor = router.has_dual_write_interceptor(tenant_id)
    print(f"\nDual-write interceptor: {'Active (ERROR!)' if has_interceptor else 'Cleared'}")
    assert not has_interceptor, "Interceptor still active"

    print("\nVerification PASSED")
```

---

## Runbook Checklist

Before any migration operation, verify:

- [ ] Current migration state is known
- [ ] Tenant ID is correct
- [ ] You have the migration ID
- [ ] Monitoring is active
- [ ] You know the rollback procedure
- [ ] Stakeholders are notified

After any migration operation, verify:

- [ ] Operation completed as expected
- [ ] No error messages in logs
- [ ] Tenant can read/write normally
- [ ] Metrics look healthy
- [ ] Audit log entry created

---

## Next Steps

- [Troubleshooting](./troubleshooting.md): Detailed error resolution
- [Monitoring](./monitoring.md): What metrics to watch
