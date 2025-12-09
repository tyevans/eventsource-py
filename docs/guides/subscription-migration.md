# Subscription Manager Migration Guide

This guide helps you migrate from manual event processing patterns to the new SubscriptionManager. The migration is designed to be incremental and non-breaking.

## Table of Contents

1. [Overview](#overview)
2. [Migration from Manual Event Processing](#migration-from-manual-event-processing)
3. [Migration from Simple EventBus Subscriptions](#migration-from-simple-eventbus-subscriptions)
4. [Database Schema Changes](#database-schema-changes)
5. [Breaking Changes](#breaking-changes)
6. [Migration Checklist](#migration-checklist)

---

## Overview

### What's New

The SubscriptionManager provides a unified approach to event processing that combines:

- **Automatic catch-up**: Reads historical events from the event store
- **Seamless transition**: Switches to live events without missing any
- **Checkpoint management**: Tracks position using global_position for reliable resumption
- **Error handling**: Built-in retry logic, circuit breakers, and dead letter queues
- **Health monitoring**: Kubernetes-ready liveness and readiness probes
- **Graceful shutdown**: Signal handling with configurable drain timeouts

### Benefits of Migrating

| Before | After |
|--------|-------|
| Manual catch-up logic scattered across codebase | Centralized, tested catch-up handling |
| Custom checkpoint tracking per projection | Consistent checkpoint management |
| No built-in error recovery | Automatic retries with exponential backoff |
| Manual health check implementation | Built-in health, readiness, and liveness APIs |
| Custom shutdown handling | Graceful shutdown with signal handling |
| Position tracking by event_id | Position tracking by global_position (more reliable) |

---

## Migration from Manual Event Processing

### Before: Manual Catch-Up Pattern

The traditional approach requires implementing catch-up logic manually:

```python
# Old approach: Manual catch-up with event_id tracking
class OrderProjection:
    def __init__(self, checkpoint_repo, event_store, event_bus):
        self.checkpoint_repo = checkpoint_repo
        self.event_store = event_store
        self.event_bus = event_bus

    async def start(self):
        # Step 1: Get last checkpoint
        last_event_id = await self.checkpoint_repo.get_checkpoint("OrderProjection")

        # Step 2: Manual catch-up from event store
        events = await self.event_store.read_all(
            after_event_id=last_event_id,
            batch_size=100,
        )

        for event in events:
            await self.handle(event)
            await self.checkpoint_repo.update_checkpoint(
                "OrderProjection",
                event.event_id,
                event.event_type,
            )

        # Step 3: Subscribe to live events
        self.event_bus.subscribe(self.handle, event_types=[OrderCreated, OrderShipped])

    async def handle(self, event):
        if isinstance(event, OrderCreated):
            await self._handle_created(event)
        elif isinstance(event, OrderShipped):
            await self._handle_shipped(event)
```

**Problems with this approach:**

- Risk of missing events during catch-up to live transition
- No built-in error handling or retries
- Checkpoint updates not atomic with event processing
- Must implement health checks manually
- Shutdown logic is error-prone

### After: Using SubscriptionManager

```python
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

# New approach: Projection just handles events
class OrderProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        if isinstance(event, OrderCreated):
            await self._handle_created(event)
        elif isinstance(event, OrderShipped):
            await self._handle_shipped(event)


async def main():
    # SubscriptionManager handles all the complexity
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    projection = OrderProjection()
    await manager.subscribe(projection)

    # Start with automatic catch-up and transition to live
    await manager.start()

    # Or run as a daemon with graceful shutdown
    await manager.run_until_shutdown()
```

### Step-by-Step Migration

1. **Create Subscriber Class**

   Convert your projection to implement the Subscriber protocol:

   ```python
   from eventsource.events import DomainEvent

   class MyProjection:
       def subscribed_to(self) -> list[type[DomainEvent]]:
           """Declare which events this projection handles."""
           return [EventA, EventB, EventC]

       async def handle(self, event: DomainEvent) -> None:
           """Process a single event."""
           # Your existing event handling logic
           pass
   ```

2. **Remove Manual Catch-Up Code**

   Delete the catch-up loops from your startup code. The SubscriptionManager handles this automatically.

3. **Remove Direct Event Bus Subscriptions**

   Remove calls like `event_bus.subscribe(handler, event_types=[...])`. The manager handles subscriptions.

4. **Configure the Manager**

   ```python
   config = SubscriptionConfig(
       start_from="checkpoint",  # Resume from last position (default)
       batch_size=100,           # Events per batch during catch-up
       checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
   )

   await manager.subscribe(projection, config=config)
   ```

5. **Update Startup Code**

   Replace your manual startup with:

   ```python
   manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
   await manager.subscribe(projection)
   await manager.start()
   ```

---

## Migration from Simple EventBus Subscriptions

### Before: Direct EventBus.subscribe() Usage

For live-only scenarios, you may have used direct event bus subscriptions:

```python
# Old approach: Direct event bus subscription (no catch-up)
async def handle_order_created(event: OrderCreated) -> None:
    await send_notification(event)

event_bus.subscribe(handle_order_created, event_types=[OrderCreated])
```

### After: Using SubscriptionManager

```python
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

# New approach: Wrap handler in a subscriber class
class OrderNotificationHandler:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        await send_notification(event)


# Configure for live-only (no catch-up)
config = SubscriptionConfig(start_from="end")

manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)
await manager.subscribe(OrderNotificationHandler(), config=config)
await manager.start()
```

### When to Use Each Approach

| Use Case | Recommended Approach |
|----------|---------------------|
| Building read models that need full history | SubscriptionManager with `start_from="checkpoint"` |
| Rebuilding projections from scratch | SubscriptionManager with `start_from="beginning"` |
| Live-only handlers (notifications, integrations) | SubscriptionManager with `start_from="end"` |
| Simple one-off subscriptions in tests | Direct EventBus (still supported) |
| Production event processing | Always use SubscriptionManager |

---

## Database Schema Changes

### New Checkpoint Field: global_position

The SubscriptionManager uses `global_position` for checkpoint tracking instead of relying solely on `event_id`. This provides more reliable ordering and faster catch-up queries.

### Migration Scripts

Migration scripts are provided in `src/eventsource/migrations/updates/`:

**PostgreSQL:** `001_add_global_position.sql`

```sql
-- Add new column (nullable for backward compatibility)
ALTER TABLE projection_checkpoints
ADD COLUMN IF NOT EXISTS global_position BIGINT;

-- Create index for position-based queries
CREATE INDEX IF NOT EXISTS idx_checkpoints_global_position
ON projection_checkpoints(global_position)
WHERE global_position IS NOT NULL;

-- Add column comment
COMMENT ON COLUMN projection_checkpoints.global_position IS 'Global position in the event stream';
```

**SQLite:** `001_add_global_position_sqlite.sql`

```sql
-- Add new column (nullable for backward compatibility)
ALTER TABLE projection_checkpoints
ADD COLUMN global_position INTEGER;
```

### Running Migrations

**Option 1: Run SQL directly**

```bash
# PostgreSQL
psql -d your_database -f src/eventsource/migrations/updates/001_add_global_position.sql

# SQLite
sqlite3 your_database.db < src/eventsource/migrations/updates/001_add_global_position_sqlite.sql
```

**Option 2: Use your migration tool**

Copy the SQL content into your migration framework (Alembic, Flyway, etc.).

### Backward Compatibility

The migration is backward compatible:

- The `global_position` column is nullable
- Existing checkpoints continue to work via `event_id`
- New checkpoints will include both `event_id` and `global_position`
- The system gracefully falls back to `event_id` if `global_position` is not available

---

## Breaking Changes

### API Changes

**SubscriptionManager Constructor:**

The constructor signature has new optional parameters:

```python
# Before (if upgrading from early versions)
manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)

# After (same signature, new optional params)
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    shutdown_timeout=30.0,        # New: graceful shutdown timeout
    drain_timeout=10.0,           # New: event drain timeout
    dlq_repo=dlq_repo,            # New: dead letter queue support
    error_handling_config=None,   # New: error handling configuration
    health_check_config=None,     # New: health check thresholds
)
```

### Deprecations

| Deprecated | Replacement |
|------------|-------------|
| Manual checkpoint tracking with `event_id` only | Automatic checkpoint with `global_position` |
| Custom catch-up loops | `SubscriptionManager.start()` |
| Direct `event_bus.subscribe()` for production | `SubscriptionManager.subscribe()` |

### Import Changes

All subscription-related imports are now from `eventsource.subscriptions`:

```python
from eventsource.subscriptions import (
    SubscriptionManager,
    SubscriptionConfig,
    CheckpointStrategy,
    SubscriptionState,
    # Error handling
    ErrorHandlingConfig,
    ErrorCategory,
    ErrorSeverity,
    # Health checks
    HealthCheckConfig,
    ManagerHealth,
    ReadinessStatus,
    LivenessStatus,
)
```

---

## Migration Checklist

### Pre-Migration Checklist

- [ ] **Review current event processing code**
  - Identify all manual catch-up implementations
  - Document current checkpoint tracking approach
  - List all event bus subscriptions

- [ ] **Run database migration**
  - Apply `001_add_global_position.sql` (PostgreSQL) or `001_add_global_position_sqlite.sql` (SQLite)
  - Verify migration with: `SELECT column_name FROM information_schema.columns WHERE table_name = 'projection_checkpoints';`

- [ ] **Update dependencies**
  - Ensure eventsource package is updated to version with SubscriptionManager

- [ ] **Plan rollback strategy**
  - The migration is non-destructive; manual approach still works
  - Can revert by removing SubscriptionManager and restoring manual code

### Migration Steps

- [ ] **Convert projections to Subscriber protocol**
  - Add `subscribed_to()` method returning event types
  - Ensure `handle()` method is async

- [ ] **Replace manual catch-up with SubscriptionManager**
  - Remove catch-up loops from startup code
  - Remove direct event bus subscriptions

- [ ] **Configure subscriptions**
  - Choose appropriate `start_from` setting
  - Set batch sizes based on your workload
  - Configure checkpoint strategy

- [ ] **Set up error handling**
  - Configure `ErrorHandlingConfig` if needed
  - Set up DLQ repository for failed events

- [ ] **Implement health endpoints**
  - Integrate `manager.health_check()` with your HTTP health endpoint
  - Integrate `manager.readiness_check()` for Kubernetes readiness probe
  - Integrate `manager.liveness_check()` for Kubernetes liveness probe

### Post-Migration Verification

- [ ] **Verify catch-up works correctly**
  - Stop application, add test events, restart
  - Confirm events are processed on startup

- [ ] **Verify live events work**
  - Publish events while application is running
  - Confirm immediate processing

- [ ] **Verify checkpoint persistence**
  - Check `projection_checkpoints` table has `global_position` values
  - Restart application and verify it resumes from correct position

- [ ] **Test graceful shutdown**
  - Send SIGTERM to application
  - Verify events are drained and checkpoints saved

- [ ] **Test error handling**
  - Introduce a failing event handler
  - Verify retry logic and DLQ behavior

- [ ] **Monitor health endpoints**
  - Check `/health`, `/ready`, `/live` endpoints return correct status

---

## Rollback Plan

If issues arise during migration, you can roll back:

1. **Revert code changes**: Restore manual catch-up and event bus subscriptions
2. **Database is safe**: The `global_position` column can remain (nullable, unused)
3. **Checkpoints are preserved**: Existing `event_id` checkpoints still work

The old manual approach continues to work alongside the new SubscriptionManager. You can migrate projections incrementally.

---

## See Also

- [Subscription Manager Guide](subscriptions.md) - Complete usage guide
- [Error Handling Guide](error-handling.md) - Error handling patterns
- [Production Guide](production.md) - Production deployment best practices
- [Observability Guide](observability.md) - Monitoring and tracing
