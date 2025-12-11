# Multi-Tenant Live Migration Architecture

This document describes the architecture of the eventsource-py multi-tenant live migration system, which enables zero-downtime migration of tenant data between event stores.

## Overview

The migration system enables moving a tenant's event data from a shared event store to a dedicated store (or between any two stores) without service interruption. It achieves this through a three-phase approach:

1. **Bulk Copy Phase**: Copy historical events from source to target
2. **Dual-Write Phase**: Write new events to both stores simultaneously
3. **Cutover Phase**: Switch reads and writes to the target store

```
                                Migration Flow
    +--------------------------------------------------------------------------+
    |                                                                          |
    |   NORMAL          BULK_COPY         DUAL_WRITE         MIGRATED          |
    |   State           State             State              State             |
    |                                                                          |
    |   +------+        +------+          +------+           +------+          |
    |   |Source| -----> |Copy  | -------> |Sync  | --------> |Target|          |
    |   |Store |        |Events|          |Events|           |Store |          |
    |   +------+        +------+          +------+           +------+          |
    |                                                                          |
    |   Reads/Writes    Reads: Source     Reads: Source      Reads/Writes      |
    |   to Source       Writes: Source    Writes: Both       to Target         |
    |                                                                          |
    +--------------------------------------------------------------------------+
```

## Key Design Principles

### Zero Downtime
- Application continues serving requests throughout migration
- Sub-100ms pause during cutover for final consistency
- No data loss or duplication

### Consistency Guarantees
- Source store is authoritative during migration
- Events are never lost (source always written first)
- Verification ensures data integrity before cutover

### Operational Safety
- Full rollback capability at any point before completion
- Pause/resume support for operational flexibility
- Comprehensive audit trail for compliance

## System Components

### Core Components

```
+-------------------+     +--------------------+     +------------------+
| MigrationCoordinator |-->| TenantStoreRouter  |-->| EventStore       |
+-------------------+     +--------------------+     | (Source/Target)  |
        |                         |                 +------------------+
        |                         |
        v                         v
+-------------------+     +--------------------+
| MigrationRepository|     | DualWriteInterceptor|
+-------------------+     +--------------------+
        |
        v
+-------------------+     +--------------------+
| TenantRoutingRepo |     | ConsistencyVerifier |
+-------------------+     +--------------------+
```

### Component Descriptions

#### MigrationCoordinator
The central orchestrator that manages the entire migration lifecycle:
- Starts and manages migrations
- Coordinates phase transitions
- Handles pause/resume/abort operations
- Emits audit events and metrics

#### TenantStoreRouter
Routes read/write operations based on tenant migration state:
- Implements EventStore interface for transparent integration
- Routes based on current migration phase
- Manages dual-write interceptors during migration
- Coordinates write pauses during cutover

#### DualWriteInterceptor
Ensures new events are written to both stores during sync:
- Writes to source first (authoritative)
- Best-effort write to target (failure logged, not fatal)
- Tracks failed writes for background recovery

#### BulkCopier
Copies historical events from source to target:
- Batch processing for efficiency
- Progress tracking and checkpointing
- Resume support for large datasets

#### ConsistencyVerifier
Validates data integrity between stores:
- Count-based verification (fast)
- Hash-based verification (thorough)
- Full comparison (most thorough)

#### CutoverManager
Orchestrates the final cutover:
- Pauses writes briefly
- Verifies consistency
- Updates routing
- Resumes writes

#### SubscriptionMigrator
Migrates subscription checkpoints:
- Translates positions using PositionMapper
- Ensures continuity without missed events
- Supports dry-run mode

## Migration States

```
                      Migration State Machine

    +--------+      start()       +-----------+
    | PENDING| -----------------> | BULK_COPY |
    +--------+                    +-----------+
                                       |
                                       | copy complete
                                       v
                                  +-----------+
                                  | DUAL_WRITE|<--+
                                  +-----------+   |
                                       |          | lag > threshold
                                       | lag ok   |
                                       v          |
                              +-----------------+ |
                              | CUTOVER_PAUSED  |-+
                              +-----------------+
                                       |
                                       | cutover complete
                                       v
                                  +----------+
                                  | MIGRATED |
                                  +----------+

    Any state can transition to FAILED or ROLLED_BACK via abort() or error
```

### State Descriptions

| State | Description | Reads | Writes |
|-------|-------------|-------|--------|
| `PENDING` | Migration created, not started | Source | Source |
| `BULK_COPY` | Copying historical events | Source | Source |
| `DUAL_WRITE` | Syncing new events to both stores | Source | Both |
| `CUTOVER_PAUSED` | Brief pause for consistency check | Source | Blocked |
| `MIGRATED` | Migration complete | Target | Target |
| `FAILED` | Migration failed, needs intervention | Source | Source |
| `ROLLED_BACK` | Migration aborted and cleaned up | Source | Source |

## Data Flow

### During Normal Operation
```
Application --> TenantStoreRouter --> SourceStore
```

### During Bulk Copy Phase
```
Application --> TenantStoreRouter --> SourceStore
                                          |
                                          v
                                    BulkCopier --> TargetStore
```

### During Dual-Write Phase
```
Application --> TenantStoreRouter --> DualWriteInterceptor --> SourceStore
                                              |
                                              +----------------> TargetStore
```

### After Migration Complete
```
Application --> TenantStoreRouter --> TargetStore
```

## Database Schema

The migration system uses the following tables:

### `tenant_migrations`
Stores migration state and progress:
- `id`: Unique migration identifier
- `tenant_id`: Tenant being migrated
- `source_store_id`: Source store identifier
- `target_store_id`: Target store identifier
- `state`: Current migration state
- `progress_percentage`: Copy progress (0-100)
- `events_copied`: Number of events copied
- `error_message`: Error details if failed
- `started_at`: Migration start time
- `completed_at`: Migration completion time

### `tenant_routing`
Maps tenants to their current store:
- `tenant_id`: Tenant identifier
- `store_id`: Current active store
- `migration_state`: Current migration phase
- `target_store_id`: Target during migration

### `migration_position_mappings`
Maps positions between source and target stores:
- `migration_id`: Associated migration
- `source_position`: Position in source store
- `target_position`: Corresponding position in target
- `event_id`: Event identifier

### `migration_audit_log`
Audit trail for compliance:
- `migration_id`: Associated migration
- `event_type`: Type of audit event
- `details`: Event details (JSON)
- `timestamp`: When the event occurred

## Performance Characteristics

### Bulk Copy Phase
- Processes events in configurable batch sizes (default: 1000)
- Throughput: ~10,000-50,000 events/second depending on store
- Memory efficient: streams events rather than loading all

### Dual-Write Phase
- Adds ~1-5ms latency per write (target store write)
- Target failures do not block writes (best-effort)
- Failed writes recovered via background catch-up

### Cutover Phase
- Pause duration: typically < 100ms
- Configurable timeout: default 5 seconds
- Automatic rollback if timeout exceeded

## Observability

### Metrics
The migration system emits the following key metrics:

| Metric | Type | Description |
|--------|------|-------------|
| `migration_state` | Gauge | Current state per migration |
| `migration_progress_percent` | Gauge | Copy progress percentage |
| `migration_events_copied` | Counter | Total events copied |
| `migration_dual_write_latency_ms` | Histogram | Dual-write operation latency |
| `migration_cutover_pause_duration_ms` | Histogram | Cutover pause duration |
| `migration_failures` | Counter | Failed migrations |

### Logging
All operations emit structured logs with:
- `migration_id`: Unique identifier
- `tenant_id`: Affected tenant
- `phase`: Current migration phase
- `operation`: Specific operation

### Tracing
OpenTelemetry spans are created for:
- `eventsource.migration.bulk_copy`
- `eventsource.migration.dual_write`
- `eventsource.migration.cutover`
- `eventsource.migration.verification`

## Error Handling

### Error Classification

| Error Type | Recoverable | Action |
|------------|-------------|--------|
| `MigrationNotFoundError` | No | Check migration ID |
| `InvalidStateTransitionError` | No | Verify current state |
| `CutoverTimeoutError` | Yes | Retry or abort |
| `ConsistencyError` | Yes | Investigate and retry |
| `PositionMappingError` | Yes | Check position mappings |

### Recovery Strategies

1. **Transient Errors**: Automatic retry with backoff
2. **Consistency Errors**: Pause, investigate, fix, resume
3. **Critical Errors**: Abort and rollback to source

## Security Considerations

### Multi-Tenancy
- Strict tenant isolation in all operations
- Events are filtered by tenant_id
- No cross-tenant data exposure

### Audit Trail
- All operations logged to audit log
- Immutable audit records
- Suitable for compliance requirements

## Limitations

1. **PostgreSQL Only**: Migration system requires PostgreSQL for advisory locks
2. **Single Store per Tenant**: Each tenant can only be on one store
3. **No Concurrent Migrations**: One migration per tenant at a time
4. **Position Translation**: Requires position mapping tables

## Next Steps

- [API Reference](./api-reference.md): Detailed API documentation
- [Migration Guide](./migration-guide.md): Step-by-step instructions
- [Runbook](./runbook.md): Operational procedures
- [Troubleshooting](./troubleshooting.md): Common issues and solutions
- [Monitoring](./monitoring.md): Metrics and alerting
