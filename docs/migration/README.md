# Multi-Tenant Live Migration Documentation

Zero-downtime tenant migration for eventsource-py event stores.

## Overview

The migration system enables moving tenant data from a shared event store to a dedicated store without service interruption. Key features include:

- **Zero-downtime migration** using bulk-copy, dual-write, and cutover phases
- **Sub-100ms cutover** with automatic rollback on timeout
- **Full rollback capability** at any point before completion
- **Subscription continuity** with checkpoint position translation
- **Comprehensive observability** with metrics, tracing, and audit logs

## Documentation Index

| Document | Description |
|----------|-------------|
| [Architecture](./architecture.md) | System design, components, and data flow |
| [API Reference](./api-reference.md) | Detailed API documentation for key classes |
| [Migration Guide](./migration-guide.md) | Step-by-step migration instructions |
| [Runbook](./runbook.md) | Operational procedures (pause, resume, abort, rollback) |
| [Troubleshooting](./troubleshooting.md) | Common errors and solutions |
| [Monitoring](./monitoring.md) | Metrics, alerting, and health checks |

## Quick Start

```python
from eventsource.migration import (
    MigrationCoordinator,
    MigrationConfig,
    TenantStoreRouter,
)

# Set up router and coordinator
router = TenantStoreRouter(default_store=shared_store, routing_repo=routing_repo)
coordinator = MigrationCoordinator(
    migration_repo=migration_repo,
    routing_repo=routing_repo,
    router=router,
)

# Start migration
migration = await coordinator.start_migration(
    tenant_id=tenant_uuid,
    source_store=shared_store,
    target_store=dedicated_store,
)

# Wait for bulk copy and dual-write phases...

# Complete migration
await coordinator.initiate_cutover(migration.id)
```

## Migration Phases

```
NORMAL  -->  BULK_COPY  -->  DUAL_WRITE  -->  CUTOVER  -->  MIGRATED
             Copy all        Write to         Brief          Done!
             events          both stores      pause
```

1. **Bulk Copy**: Copy historical events to target store
2. **Dual-Write**: New events written to both stores
3. **Cutover**: Brief pause, verify consistency, switch routing
4. **Migrated**: All operations now use target store

## Key Classes

| Class | Purpose |
|-------|---------|
| `MigrationCoordinator` | Orchestrates entire migration lifecycle |
| `TenantStoreRouter` | Routes operations based on migration state |
| `DualWriteInterceptor` | Writes to both stores during sync |
| `ConsistencyVerifier` | Validates data integrity before cutover |
| `BulkCopier` | Efficiently copies historical events |
| `SubscriptionMigrator` | Translates subscription checkpoints |

## Requirements

- Python 3.13+
- PostgreSQL (required for advisory locks)
- eventsource-py library

## Common Operations

### Start a Migration
```python
migration = await coordinator.start_migration(tenant_id, source, target)
```

### Check Status
```python
migration = await coordinator.get_migration(migration_id)
print(f"State: {migration.state}, Progress: {migration.progress_percentage}%")
```

### Pause Migration
```python
await coordinator.pause_migration(migration_id)
```

### Resume Migration
```python
await coordinator.resume_migration(migration_id)
```

### Abort and Rollback
```python
await coordinator.abort_migration(migration_id, reason="Issue discovered")
```

### Complete Migration
```python
await coordinator.initiate_cutover(migration_id)
```

## Support

For issues or questions:
1. Check the [Troubleshooting Guide](./troubleshooting.md)
2. Review logs with migration_id filter
