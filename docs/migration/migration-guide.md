# Migration Guide

This guide walks you through migrating a tenant from a shared event store to a dedicated store.

## Prerequisites

Before starting a migration, ensure you have:

1. **PostgreSQL Database**: The migration system requires PostgreSQL for advisory locks
2. **Target Store Ready**: The dedicated store must be set up and accessible
3. **Schema Deployed**: Migration tables must exist in your database
4. **Sufficient Resources**: Monitor disk space, memory, and CPU during migration
5. **Backup**: Have a recent backup of both source and target stores

### Database Schema Setup

Apply the migration schema before your first migration:

```sql
-- Create migration tables
-- See: src/eventsource/migration/schema.sql

CREATE TABLE IF NOT EXISTS tenant_migrations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    source_store_id VARCHAR(255) NOT NULL,
    target_store_id VARCHAR(255) NOT NULL,
    state VARCHAR(50) NOT NULL,
    progress_percentage DECIMAL(5,2) DEFAULT 0,
    events_copied BIGINT DEFAULT 0,
    total_events BIGINT DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    CONSTRAINT unique_active_migration
        UNIQUE (tenant_id) WHERE state NOT IN ('migrated', 'rolled_back', 'failed')
);

CREATE TABLE IF NOT EXISTS tenant_routing (
    tenant_id UUID PRIMARY KEY,
    store_id VARCHAR(255) NOT NULL,
    migration_state VARCHAR(50) NOT NULL DEFAULT 'normal',
    target_store_id VARCHAR(255),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS migration_position_mappings (
    id SERIAL PRIMARY KEY,
    migration_id UUID NOT NULL REFERENCES tenant_migrations(id),
    source_position BIGINT NOT NULL,
    target_position BIGINT NOT NULL,
    event_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (migration_id, source_position)
);

CREATE TABLE IF NOT EXISTS migration_audit_log (
    id SERIAL PRIMARY KEY,
    migration_id UUID NOT NULL REFERENCES tenant_migrations(id),
    event_type VARCHAR(100) NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create indexes for performance
CREATE INDEX idx_migrations_tenant ON tenant_migrations(tenant_id);
CREATE INDEX idx_migrations_state ON tenant_migrations(state);
CREATE INDEX idx_position_mappings_migration ON migration_position_mappings(migration_id);
CREATE INDEX idx_audit_log_migration ON migration_audit_log(migration_id);
```

## Step-by-Step Migration

### Step 1: Set Up the Migration Infrastructure

```python
from uuid import UUID
import asyncpg

from eventsource.migration import (
    MigrationCoordinator,
    MigrationConfig,
    TenantStoreRouter,
)
from eventsource.migration.repositories import (
    PostgreSQLMigrationRepository,
    PostgreSQLRoutingRepository,
)
from eventsource.migration.consistency import VerificationLevel
from eventsource.stores.postgresql import PostgreSQLEventStore

# Create database connection pool
pool = await asyncpg.create_pool(
    host="localhost",
    database="events",
    user="postgres",
    password="password",
)

# Create stores
shared_store = PostgreSQLEventStore(pool, table_name="events_shared")
dedicated_store = PostgreSQLEventStore(pool, table_name="events_dedicated_a")

# Create repositories
migration_repo = PostgreSQLMigrationRepository(pool)
routing_repo = PostgreSQLRoutingRepository(pool)

# Create router
router = TenantStoreRouter(
    default_store=shared_store,
    routing_repo=routing_repo,
    stores={
        "shared": shared_store,
        "dedicated-a": dedicated_store,
    },
)

# Create coordinator
coordinator = MigrationCoordinator(
    migration_repo=migration_repo,
    routing_repo=routing_repo,
    router=router,
    config=MigrationConfig(
        batch_size=1000,
        sync_lag_threshold=100,
        cutover_timeout_seconds=5.0,
        verification_level=VerificationLevel.HASH,
    ),
)
```

### Step 2: Start the Migration

```python
# Define the tenant to migrate
tenant_id = UUID("12345678-1234-1234-1234-123456789abc")

# Start migration
migration = await coordinator.start_migration(
    tenant_id=tenant_id,
    source_store=shared_store,
    target_store=dedicated_store,
    source_store_id="shared",
    target_store_id="dedicated-a",
)

print(f"Migration started: {migration.id}")
print(f"State: {migration.state}")
```

### Step 3: Monitor Bulk Copy Progress

The bulk copy phase copies all historical events. Monitor progress:

```python
import asyncio

while True:
    migration = await coordinator.get_migration(migration.id)

    if migration.state.value == "bulk_copy":
        print(
            f"Copying: {migration.progress_percentage:.1f}% "
            f"({migration.events_copied}/{migration.total_events})"
        )
    elif migration.state.value == "dual_write":
        print("Bulk copy complete, entering dual-write phase")
        break
    elif migration.state.value == "failed":
        print(f"Migration failed: {migration.error_message}")
        break

    await asyncio.sleep(5)  # Check every 5 seconds
```

### Step 4: Monitor Dual-Write Phase

During dual-write, new events go to both stores:

```python
# Check sync lag
lag = await coordinator.get_sync_lag(migration.id)
print(f"Sync lag: {lag} events")

# Check for dual-write failures
interceptor = router._dual_write_interceptors.get(tenant_id)
if interceptor:
    stats = interceptor.get_failure_stats()
    if stats.total_failures > 0:
        print(f"Warning: {stats.total_failures} target write failures")
```

### Step 5: Verify Data Before Cutover

Before cutting over, verify consistency:

```python
from eventsource.migration import ConsistencyVerifier
from eventsource.migration.consistency import VerificationLevel

verifier = ConsistencyVerifier(
    source_store=shared_store,
    target_store=dedicated_store,
)

report = await verifier.verify_tenant_consistency(
    tenant_id=tenant_id,
    level=VerificationLevel.HASH,
)

if report.is_consistent:
    print("Verification passed!")
    print(f"  Events verified: {report.source_event_count}")
    print(f"  Streams verified: {report.streams_verified}")
else:
    print("Verification FAILED!")
    for v in report.violations:
        print(f"  - {v}")
```

### Step 6: Initiate Cutover

When ready, initiate the cutover:

```python
try:
    migration = await coordinator.initiate_cutover(migration.id)
    print(f"Cutover successful!")
    print(f"Migration state: {migration.state}")
except CutoverTimeoutError as e:
    print(f"Cutover timed out: {e}")
    print("Migration automatically rolled back")
except ConsistencyError as e:
    print(f"Consistency check failed: {e}")
```

### Step 7: Migrate Subscriptions (If Applicable)

If you have subscriptions that need position translation:

```python
from eventsource.migration import SubscriptionMigrator

migrator = SubscriptionMigrator(
    position_mapper=position_mapper,
    checkpoint_repo=checkpoint_repo,
)

# Preview first
plan = await migrator.plan_migration(
    migration_id=migration.id,
    tenant_id=tenant_id,
    subscription_names=["OrderProjection", "InventoryProjection"],
)

print("Migration plan:")
for p in plan.planned_migrations:
    print(f"  {p.subscription_name}: {p.current_position} -> {p.planned_target_position}")

# Execute
summary = await migrator.migrate_subscriptions(
    migration_id=migration.id,
    tenant_id=tenant_id,
    subscription_names=["OrderProjection", "InventoryProjection"],
)

if summary.all_successful:
    print("All subscriptions migrated successfully")
```

### Step 8: Verify Migration Complete

```python
migration = await coordinator.get_migration(migration.id)
assert migration.state.value == "migrated"

# Verify routing updated
routing = await routing_repo.get_routing(tenant_id)
assert routing.store_id == "dedicated-a"
assert routing.migration_state.value == "normal"

print("Migration complete!")
print(f"Tenant {tenant_id} is now on dedicated store")
```

## Complete Example

Here's a complete migration script:

```python
#!/usr/bin/env python3
"""
Tenant Migration Script

Usage:
    python migrate_tenant.py <tenant_id> --target-store dedicated-a
"""

import asyncio
import argparse
from uuid import UUID

import asyncpg

from eventsource.migration import (
    MigrationCoordinator,
    MigrationConfig,
    TenantStoreRouter,
    ConsistencyVerifier,
)
from eventsource.migration.repositories import (
    PostgreSQLMigrationRepository,
    PostgreSQLRoutingRepository,
)
from eventsource.migration.consistency import VerificationLevel
from eventsource.stores.postgresql import PostgreSQLEventStore


async def migrate_tenant(tenant_id: UUID, target_store_id: str):
    """Migrate a tenant to a dedicated store."""

    # Setup
    pool = await asyncpg.create_pool(dsn="postgresql://...")

    try:
        # Create stores and infrastructure
        shared_store = PostgreSQLEventStore(pool, table_name="events_shared")
        dedicated_store = PostgreSQLEventStore(pool, table_name=f"events_{target_store_id}")

        migration_repo = PostgreSQLMigrationRepository(pool)
        routing_repo = PostgreSQLRoutingRepository(pool)

        router = TenantStoreRouter(
            default_store=shared_store,
            routing_repo=routing_repo,
            stores={
                "shared": shared_store,
                target_store_id: dedicated_store,
            },
        )

        coordinator = MigrationCoordinator(
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            config=MigrationConfig(
                batch_size=1000,
                cutover_timeout_seconds=5.0,
            ),
        )

        # Start migration
        print(f"Starting migration for tenant {tenant_id}")
        migration = await coordinator.start_migration(
            tenant_id=tenant_id,
            source_store=shared_store,
            target_store=dedicated_store,
            source_store_id="shared",
            target_store_id=target_store_id,
        )

        # Wait for bulk copy
        while True:
            migration = await coordinator.get_migration(migration.id)

            if migration.state.value == "bulk_copy":
                print(f"  Progress: {migration.progress_percentage:.1f}%")
            elif migration.state.value == "dual_write":
                print("Bulk copy complete")
                break
            elif migration.state.value in ("failed", "rolled_back"):
                raise RuntimeError(f"Migration failed: {migration.error_message}")

            await asyncio.sleep(2)

        # Wait for sync to complete
        print("Waiting for sync lag to clear...")
        while True:
            lag = await coordinator.get_sync_lag(migration.id)
            if lag == 0:
                break
            print(f"  Sync lag: {lag} events")
            await asyncio.sleep(1)

        # Verify consistency
        print("Verifying consistency...")
        verifier = ConsistencyVerifier(shared_store, dedicated_store)
        report = await verifier.verify_tenant_consistency(
            tenant_id=tenant_id,
            level=VerificationLevel.HASH,
        )

        if not report.is_consistent:
            raise RuntimeError(f"Consistency check failed: {len(report.violations)} violations")

        print(f"  Verified {report.source_event_count} events")

        # Cutover
        print("Initiating cutover...")
        migration = await coordinator.initiate_cutover(migration.id)

        print(f"Migration complete! Tenant {tenant_id} is now on {target_store_id}")

    finally:
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("tenant_id", type=UUID)
    parser.add_argument("--target-store", required=True)
    args = parser.parse_args()

    asyncio.run(migrate_tenant(args.tenant_id, args.target_store))
```

## Best Practices

### Planning

1. **Test in staging first** - Always run migrations in a staging environment before production
2. **Choose low-traffic windows** - While migration is zero-downtime, cutover is faster with less traffic
3. **Alert stakeholders** - Let teams know a migration is happening
4. **Have a rollback plan** - Know how to abort and rollback if needed

### Execution

1. **Monitor throughout** - Watch metrics and logs during the entire migration
2. **Don't rush cutover** - Ensure sync lag is zero before cutting over
3. **Verify before cutover** - Always run consistency verification
4. **Keep the migration ID** - Record it for audit and troubleshooting

### After Migration

1. **Verify routing** - Confirm tenant is reading/writing to new store
2. **Monitor for errors** - Watch for any issues in the first hours
3. **Clean up source** - Plan when to archive/delete source data
4. **Document completion** - Record the migration for future reference

## Common Scenarios

### Migrating Multiple Tenants

```python
# Sequential migration (safer)
for tenant_id in tenant_ids:
    await migrate_tenant(tenant_id, target_store_id)
    await asyncio.sleep(60)  # Cool-down between migrations

# Parallel migration (faster, more resource intensive)
tasks = [migrate_tenant(tid, "dedicated") for tid in tenant_ids]
await asyncio.gather(*tasks)
```

### Handling Large Tenants

For tenants with millions of events:

```python
config = MigrationConfig(
    batch_size=5000,           # Larger batches
    cutover_timeout_seconds=10.0,  # More time for cutover
    sample_percentage=10.0,    # Sample verification for speed
)
```

### Weekend Maintenance Window

```python
# Start bulk copy during the week
migration = await coordinator.start_migration(...)

# Pause before weekend cutover
await coordinator.pause_migration(migration.id)

# Resume during maintenance window
await coordinator.resume_migration(migration.id)

# Wait for sync and cut over
await coordinator.initiate_cutover(migration.id)
```

## Next Steps

- [Runbook](./runbook.md): Operational procedures for pause/resume/abort
- [Troubleshooting](./troubleshooting.md): Common issues and solutions
- [Monitoring](./monitoring.md): What to watch during migration
