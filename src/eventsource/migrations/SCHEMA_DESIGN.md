# EventSource Database Schema Design

This document describes the database schema design for the eventsource library.

## Overview

The eventsource library requires four database tables:

1. **events** - Core event store for domain events
2. **event_outbox** - Transactional outbox for reliable publishing
3. **projection_checkpoints** - Projection position tracking
4. **dead_letter_queue** - Failed event processing storage

## Table Schemas

### 1. Events Table

The events table is the heart of the event sourcing system. It stores all domain events immutably.

**Columns:**

| Column | Type | Description |
|--------|------|-------------|
| event_id | UUID | Primary key, globally unique event identifier |
| aggregate_id | UUID | ID of the aggregate this event belongs to |
| aggregate_type | VARCHAR(255) | Type name of the aggregate (e.g., "Order", "User") |
| event_type | VARCHAR(255) | Fully qualified event type name |
| tenant_id | UUID (nullable) | Tenant ID for multi-tenant deployments |
| actor_id | VARCHAR(255) (nullable) | ID of user/system that caused the event |
| version | INTEGER | Aggregate version for optimistic concurrency |
| timestamp | TIMESTAMPTZ | When the event occurred in the domain |
| payload | JSONB | Event-specific data |
| created_at | TIMESTAMPTZ | When the event was persisted |

**Key Constraints:**
- Primary Key: `event_id`
- Unique: `(aggregate_id, aggregate_type, version)` - Ensures optimistic locking

**Indexes:**

| Index | Columns | Purpose |
|-------|---------|---------|
| idx_events_aggregate_id | aggregate_id | Load aggregate event streams |
| idx_events_aggregate_type | aggregate_type | Query events by aggregate type |
| idx_events_event_type | event_type | Event-type-specific queries |
| idx_events_timestamp | timestamp | Time-based queries, projections |
| idx_events_tenant_id | tenant_id (partial) | Multi-tenant queries |
| idx_events_type_tenant_timestamp | aggregate_type, tenant_id, timestamp | Projection queries |
| idx_events_aggregate_version | aggregate_id, aggregate_type, version | Ordered stream loading |

**Partitioning:**

For high-volume deployments (>10M events/month), use the partitioned version:

```sql
CREATE TABLE events (
    ...
    PRIMARY KEY (event_id, timestamp),  -- Must include partition key
    UNIQUE (aggregate_id, aggregate_type, version, timestamp)
) PARTITION BY RANGE (timestamp);
```

Benefits of partitioning:
- Improved query performance on time-based queries
- Efficient data lifecycle management (archival, deletion)
- Better I/O distribution across partition files

### 2. Event Outbox Table

Implements the transactional outbox pattern for reliable event publishing.

**Columns:**

| Column | Type | Description |
|--------|------|-------------|
| id | UUID | Primary key, outbox entry identifier |
| event_id | UUID | Reference to the original event |
| event_type | VARCHAR(255) | Type of event for routing |
| aggregate_id | UUID | Aggregate that produced this event |
| aggregate_type | VARCHAR(255) | Type of aggregate |
| tenant_id | UUID (nullable) | Tenant ID for multi-tenant deployments |
| event_data | JSONB | Serialized event data for publishing |
| created_at | TIMESTAMPTZ | When the entry was created |
| published_at | TIMESTAMPTZ (nullable) | When the event was published |
| retry_count | INTEGER | Number of publishing attempts |
| last_error | TEXT (nullable) | Last error message if publishing failed |
| status | VARCHAR(20) | Current status (pending/published/failed) |

**Workflow:**
1. Events are written to both `events` and `event_outbox` in the same transaction
2. Background worker polls for pending events
3. Events are published to the message broker
4. Successfully published events are marked as 'published'

**Indexes:**

| Index | Purpose |
|-------|---------|
| idx_outbox_status_created | Get oldest pending events |
| idx_outbox_pending | Partial index for pending events only |
| idx_outbox_event_id | Look up by event ID |
| idx_outbox_tenant_id | Tenant-specific queries |
| idx_outbox_failed | Monitor failed events |
| idx_outbox_published_at | Cleanup of published events |

### 3. Projection Checkpoints Table

Tracks the position of each projection in the event stream.

**Columns:**

| Column | Type | Description |
|--------|------|-------------|
| projection_name | VARCHAR(255) | Primary key, unique projection identifier |
| last_event_id | UUID (nullable) | Last processed event ID |
| last_event_type | VARCHAR(255) (nullable) | Type of last processed event |
| last_processed_at | TIMESTAMPTZ (nullable) | When the last event was processed |
| events_processed | BIGINT | Total count of events processed |
| created_at | TIMESTAMPTZ | When the checkpoint was created |
| updated_at | TIMESTAMPTZ | When the checkpoint was last updated |

**Benefits:**
- Resumable processing after restarts
- Exactly-once processing semantics (when combined with idempotent handlers)
- Lag monitoring and health checks
- Safe rebuilds from specific positions

### 4. Dead Letter Queue Table

Stores events that failed processing after all retry attempts.

**Columns:**

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL | Primary key, auto-incrementing ID |
| event_id | UUID | Reference to the failed event |
| projection_name | VARCHAR(255) | Projection that failed to process |
| event_type | VARCHAR(255) | Type of the failed event |
| event_data | JSONB | Full event data for replay |
| error_message | TEXT | Error message from the failure |
| error_stacktrace | TEXT (nullable) | Full stack trace for debugging |
| retry_count | INTEGER | Number of retry attempts made |
| first_failed_at | TIMESTAMPTZ | When the event first failed |
| last_failed_at | TIMESTAMPTZ | When the event most recently failed |
| status | VARCHAR(20) | Current status (failed/retrying/resolved) |
| resolved_at | TIMESTAMPTZ (nullable) | When the entry was resolved |
| resolved_by | VARCHAR(255) (nullable) | User who resolved the entry |

**Unique Constraint:**
- `(event_id, projection_name)` - An event can fail for multiple projections

**Indexes:**

| Index | Purpose |
|-------|---------|
| idx_dlq_status | Find events by status |
| idx_dlq_projection_name | Projection-specific queries |
| idx_dlq_event_id | Find by event ID |
| idx_dlq_first_failed_at | Find oldest failures |
| idx_dlq_projection_status | Combined projection + status |
| idx_dlq_active_failures | Partial index for active failures |
| idx_dlq_resolved_at | Cleanup of resolved entries |

## Multi-Tenancy Support

All tables support multi-tenancy through the optional `tenant_id` column:

- **Single-tenant deployments:** Leave `tenant_id` as NULL
- **Multi-tenant deployments:** Populate `tenant_id` for tenant isolation

Partial indexes on `tenant_id` ensure efficient queries without bloating indexes for single-tenant deployments.

## Index Strategy

The index strategy is optimized for common event sourcing access patterns:

1. **Aggregate Loading:** Primary pattern - load all events for an aggregate
   - `idx_events_aggregate_id` + `idx_events_aggregate_version`

2. **Projection Queries:** Get events of type X for tenant Y after time Z
   - `idx_events_type_tenant_timestamp`

3. **Outbox Processing:** Get oldest pending events efficiently
   - `idx_outbox_status_created` (partial)

4. **DLQ Monitoring:** Find active failures by projection
   - `idx_dlq_projection_status` + `idx_dlq_active_failures` (partial)

## PostgreSQL Version Requirements

- **Minimum:** PostgreSQL 12 (for native partitioning)
- **Recommended:** PostgreSQL 14+ (for better performance)

Required extensions:
- `uuid-ossp` - For UUID generation (or use `gen_random_uuid()`)

Optional extensions for partitioned deployments:
- `pg_partman` - Automatic partition management
- `pg_cron` - Scheduled partition maintenance

## Schema Migration

### Using Raw SQL

```python
from eventsource.migrations import get_all_schemas
from sqlalchemy import text

async with engine.begin() as conn:
    await conn.execute(text(get_all_schemas()))
```

### Using Alembic

Copy the appropriate template from `migrations/templates/alembic/` and customize:

```python
from eventsource.migrations import get_alembic_template

template = get_alembic_template("all_tables")
# Customize with your revision ID and create migration file
```

## Performance Considerations

1. **Event Table Size:** For tables with >100M events, consider partitioning
2. **Outbox Cleanup:** Run cleanup regularly to prevent table bloat
3. **DLQ Monitoring:** Set up alerts for DLQ entries
4. **Checkpoint Lag:** Monitor projection lag for health checks
5. **Index Maintenance:** Regular VACUUM and REINDEX for optimal performance

## Security Considerations

1. **Access Control:** Restrict direct table access; use application layer
2. **Tenant Isolation:** Always filter by tenant_id in multi-tenant deployments
3. **Audit Trail:** The events table provides a complete audit trail
4. **Sensitive Data:** Consider encryption for sensitive payload data
