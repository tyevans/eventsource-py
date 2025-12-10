# Subscriptions Guide

Subscriptions are the backbone of event-driven projections. They ensure your read models stay in sync with the event stream, handling everything from historical catch-up to real-time delivery.

## Table of Contents

1. [What Are Subscriptions?](#what-are-subscriptions)
2. [Quick Start](#quick-start)
3. [Subscription States and Lifecycle](#subscription-states-and-lifecycle)
4. [Catch-Up vs Live Subscriptions](#catch-up-vs-live-subscriptions)
5. [Configuration Options](#configuration-options)
6. [Checkpointing](#checkpointing)
7. [Error Handling and Retry](#error-handling-and-retry)
8. [Pause and Resume](#pause-and-resume)
9. [Health Monitoring](#health-monitoring)
10. [Production Patterns](#production-patterns)

---

## What Are Subscriptions?

In event sourcing, a **subscription** is a mechanism that delivers events to a consumer (like a projection) in order. The `SubscriptionManager` handles:

- **Catch-up**: Reading historical events from the event store
- **Live streaming**: Receiving new events in real-time from the event bus
- **Position tracking**: Remembering where you left off via checkpoints
- **Resilience**: Retries, circuit breakers, and error handling

```
                    +------------------+
                    | Event Store      |
                    | [E1][E2][E3]...  |
                    +--------+---------+
                             |
                    catch-up | (historical)
                             v
+------------+      +------------------+      +------------+
| Event Bus  | ---> | Subscription     | ---> | Projection |
| (live)     |      | Manager          |      | (handler)  |
+------------+      +------------------+      +------------+
                             |
                    checkpoint saved
                             v
                    +------------------+
                    | Checkpoint Repo  |
                    +------------------+
```

---

## Quick Start

```python
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig

# 1. Define your projection
class OrderProjection:
    def __init__(self):
        self.orders = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Which events this projection handles."""
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event."""
        if isinstance(event, OrderCreated):
            self.orders[str(event.aggregate_id)] = {"status": "created"}
        elif isinstance(event, OrderShipped):
            self.orders[str(event.aggregate_id)]["status"] = "shipped"

# 2. Set up the manager
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
)

# 3. Register and start
projection = OrderProjection()
await manager.subscribe(projection)
await manager.start()

# 4. Graceful shutdown
await manager.stop()
```

---

## Subscription States and Lifecycle

Each subscription follows a state machine that tracks its progress:

```
                              +------------+
                              |  STARTING  |
                              +-----+------+
                                    |
              +---------------------+---------------------+
              |                     |                     |
              v                     v                     v
       +-------------+        +-----------+         +---------+
       | CATCHING_UP |<------>|   LIVE    |         | STOPPED |
       +------+------+        +-----+-----+         +---------+
              |                     |
              v                     v
         +---------+           +---------+
         | PAUSED  |<--------->|  ERROR  |
         +---------+           +---------+
```

### State Descriptions

| State | Description |
|-------|-------------|
| `STARTING` | Initializing, reading checkpoint |
| `CATCHING_UP` | Processing historical events from the event store |
| `LIVE` | Receiving real-time events from the event bus |
| `PAUSED` | Temporarily stopped (manual or backpressure) |
| `STOPPED` | Cleanly shut down (terminal) |
| `ERROR` | Failed with unrecoverable error |

### Checking State

```python
subscription = manager.get_subscription("OrderProjection")

# Current state
print(subscription.state)  # SubscriptionState.LIVE

# Convenience properties
subscription.is_running   # True if CATCHING_UP or LIVE
subscription.is_paused    # True if PAUSED
subscription.is_terminal  # True if STOPPED or ERROR
```

---

## Catch-Up vs Live Subscriptions

### The Catch-Up to Live Transition

When a subscription starts, it must process historical events before receiving live ones. The `SubscriptionManager` uses a **watermark algorithm** to ensure no events are lost:

```
Timeline:
t0: Get watermark (current max position = 1000)
t1: Subscribe to live events (buffering enabled)
t2: Events 1001, 1002 arrive -> buffered
t3: Catch-up reads events 1-1000 from store
t4: Events 1003, 1004 arrive -> buffered
t5: Catch-up continues to 1004 (overlaps with buffer)
t6: Caught up to watermark, switch to buffer processing
t7: Process buffer, skip duplicates (1001-1004 already seen)
t8: Buffer empty, switch to live mode
```

This guarantees gap-free event delivery during the transition.

### Start Position Options

```python
from eventsource.subscriptions import SubscriptionConfig

# Resume from last checkpoint (default)
config = SubscriptionConfig(start_from="checkpoint")

# Start from the beginning (rebuild projection)
config = SubscriptionConfig(start_from="beginning")

# Start from the end (live-only, skip history)
config = SubscriptionConfig(start_from="end")

# Start from specific global position
config = SubscriptionConfig(start_from=5000)
```

### Factory Functions for Common Patterns

```python
from eventsource.subscriptions import create_catch_up_config, create_live_only_config

# Optimized for rebuilding projections
config = create_catch_up_config(batch_size=1000)

# For notification handlers (no history needed)
config = create_live_only_config()
```

---

## Configuration Options

The `SubscriptionConfig` controls all aspects of subscription behavior:

```python
from eventsource.subscriptions import SubscriptionConfig, CheckpointStrategy

config = SubscriptionConfig(
    # Where to start
    start_from="checkpoint",

    # Batch processing
    batch_size=100,           # Events per batch during catch-up
    max_in_flight=1000,       # Max concurrent events
    backpressure_threshold=0.8,  # Signal backpressure at 80%

    # Checkpointing
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    checkpoint_interval_seconds=5.0,  # For PERIODIC strategy

    # Timeouts
    processing_timeout=30.0,
    shutdown_timeout=30.0,

    # Event filtering (optional)
    event_types=(OrderCreated, OrderShipped),
    aggregate_types=("Order",),

    # Error handling
    continue_on_error=True,

    # Retry settings
    max_retries=5,
    initial_retry_delay=1.0,
    max_retry_delay=60.0,
    retry_exponential_base=2.0,
    retry_jitter=0.1,

    # Circuit breaker
    circuit_breaker_enabled=True,
    circuit_breaker_failure_threshold=5,
    circuit_breaker_recovery_timeout=30.0,
)

await manager.subscribe(projection, config=config)
```

---

## Checkpointing

Checkpoints track the last successfully processed position, enabling resume after restart.

### Checkpoint Strategies

| Strategy | When Checkpoint Saved | Trade-off |
|----------|----------------------|-----------|
| `EVERY_EVENT` | After each event | Safest, slowest |
| `EVERY_BATCH` | After each batch (default) | Balanced |
| `PERIODIC` | On time interval | Fastest, may reprocess on crash |

```python
# Safest: checkpoint every event
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
)

# Balanced: checkpoint per batch
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    batch_size=100,
)

# Fastest: periodic checkpointing
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.PERIODIC,
    checkpoint_interval_seconds=5.0,
)
```

### Position Tracking

The subscription tracks position using `global_position` (the event's position in the total stream):

```python
subscription = manager.get_subscription("OrderProjection")

# Current position
print(subscription.last_processed_position)  # 12345
print(subscription.last_event_id)            # UUID of last event
print(subscription.last_event_type)          # "OrderCreated"

# Lag (how far behind)
print(subscription.lag)  # 50 events behind
```

---

## Error Handling and Retry

### Continue on Error

By default, subscriptions continue after handler errors:

```python
# Continue processing after errors (default)
config = SubscriptionConfig(continue_on_error=True)

# Stop on first error (for critical projections)
config = SubscriptionConfig(continue_on_error=False)
```

### Retry Configuration

Transient failures (network issues, temporary database unavailability) are automatically retried:

```python
config = SubscriptionConfig(
    max_retries=5,
    initial_retry_delay=1.0,    # Start with 1 second
    max_retry_delay=60.0,       # Cap at 60 seconds
    retry_exponential_base=2.0, # Double each retry
    retry_jitter=0.1,           # Add 10% randomization
)
```

Retry delay progression: `1s -> 2s -> 4s -> 8s -> 16s` (capped at 60s)

### Circuit Breaker

Prevents cascading failures by temporarily stopping requests to failing services:

```python
config = SubscriptionConfig(
    circuit_breaker_enabled=True,
    circuit_breaker_failure_threshold=5,   # Open after 5 failures
    circuit_breaker_recovery_timeout=30.0, # Try again after 30s
)
```

Circuit breaker states:
- **CLOSED**: Normal operation
- **OPEN**: Blocking requests (too many failures)
- **HALF_OPEN**: Testing if service recovered

### Error Callbacks

Register callbacks for error notifications:

```python
from eventsource.subscriptions import ErrorInfo, ErrorCategory, ErrorSeverity

# All errors
async def on_any_error(error: ErrorInfo) -> None:
    logger.error(f"Error in {error.subscription_name}: {error.error_message}")

manager.on_error(on_any_error)

# Specific categories
async def on_transient_error(error: ErrorInfo) -> None:
    await notify_ops("Transient error detected")

manager.on_error_category(ErrorCategory.TRANSIENT, on_transient_error)

# Specific severities
async def on_critical_error(error: ErrorInfo) -> None:
    await page_on_call_engineer(error)

manager.on_error_severity(ErrorSeverity.CRITICAL, on_critical_error)
```

### Error Statistics

```python
# Get error stats for all subscriptions
stats = manager.get_error_stats()
print(f"Total errors: {stats['total_errors']}")
print(f"Total DLQ: {stats['total_dlq_count']}")

# Per-subscription stats
for name, sub_stats in stats["subscriptions"].items():
    print(f"{name}: {sub_stats['total_errors']} errors")
```

---

## Pause and Resume

Subscriptions can be paused for maintenance or to handle backpressure.

### Manual Pause/Resume

```python
from eventsource.subscriptions import PauseReason

# Pause a subscription
await manager.pause_subscription("OrderProjection", reason=PauseReason.MANUAL)

# Pause all subscriptions
results = await manager.pause_all(reason=PauseReason.MAINTENANCE)

# Check paused subscriptions
for name in manager.paused_subscription_names:
    sub = manager.get_subscription(name)
    print(f"{name}: paused for {sub.pause_reason.value}")
    print(f"  paused at: {sub.paused_at}")
    print(f"  duration: {sub.pause_duration_seconds}s")

# Resume
await manager.resume_subscription("OrderProjection")
await manager.resume_all()
```

### Pause Reasons

| Reason | Description |
|--------|-------------|
| `MANUAL` | User-initiated via API |
| `BACKPRESSURE` | Automatic due to flow control |
| `MAINTENANCE` | Maintenance operations |

### Event Buffering During Pause

Events arriving during pause are buffered and processed on resume:

```python
# Check buffer size during pause
sub = manager.get_subscription("OrderProjection")
if sub.is_paused:
    # Live runner buffers events during pause
    coordinator = manager._coordinators.get("OrderProjection")
    if coordinator and coordinator.live_runner:
        print(f"Buffered: {coordinator.live_runner.pause_buffer_size} events")
```

---

## Health Monitoring

### Comprehensive Health Check

```python
health = await manager.health_check()

print(f"Status: {health.status}")           # healthy/degraded/unhealthy/critical
print(f"Running: {health.running}")
print(f"Subscriptions: {health.subscription_count}")
print(f"  Healthy: {health.healthy_count}")
print(f"  Degraded: {health.degraded_count}")
print(f"  Unhealthy: {health.unhealthy_count}")
print(f"Total lag: {health.total_lag_events} events")
print(f"Total processed: {health.total_events_processed}")

# Per-subscription details
for sub in health.subscriptions:
    print(f"  {sub.name}: {sub.status} (lag: {sub.lag_events})")
```

### Kubernetes Probes

```python
# Readiness probe - can the service handle traffic?
readiness = await manager.readiness_check()
if readiness.ready:
    return 200
else:
    return 503, {"reason": readiness.reason}

# Liveness probe - is the service alive?
liveness = await manager.liveness_check()
if liveness.alive:
    return 200
else:
    return 503, {"reason": liveness.reason}
```

### FastAPI Integration

```python
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/health")
async def health():
    h = await manager.health_check()
    return JSONResponse(h.to_dict(), status_code=200 if h.status == "healthy" else 503)

@app.get("/ready")
async def ready():
    r = await manager.readiness_check()
    return JSONResponse(r.to_dict(), status_code=200 if r.ready else 503)

@app.get("/live")
async def live():
    l = await manager.liveness_check()
    return JSONResponse(l.to_dict(), status_code=200 if l.alive else 503)
```

### Health Check Configuration

```python
from eventsource.subscriptions import HealthCheckConfig

health_config = HealthCheckConfig(
    # Error thresholds
    max_error_rate_per_minute=10.0,
    max_errors_warning=10,
    max_errors_critical=100,

    # Lag thresholds
    max_lag_events_warning=1000,
    max_lag_events_critical=10000,

    # DLQ thresholds
    max_dlq_events_warning=10,
    max_dlq_events_critical=100,
)

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    health_check_config=health_config,
)
```

---

## Production Patterns

### Daemon Mode with Signal Handling

For long-running services, use `run_until_shutdown()`:

```python
async def main():
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
        shutdown_timeout=30.0,
        drain_timeout=10.0,
    )

    await manager.subscribe(projection)

    # Runs until SIGTERM/SIGINT, then gracefully shuts down
    result = await manager.run_until_shutdown()

    print(f"Shutdown phase: {result.phase}")
    print(f"Checkpoints saved: {result.checkpoints_saved}")
    if result.forced:
        print("Warning: Shutdown was forced due to timeout")
```

### Context Manager Pattern

```python
async with SubscriptionManager(event_store, event_bus, checkpoint_repo) as manager:
    await manager.subscribe(projection)
    # Manager starts automatically, stops on exit
```

### Multiple Subscriptions with Different Configs

```python
# Real-time dashboard (live-only, low latency)
await manager.subscribe(
    DashboardProjection(),
    config=SubscriptionConfig(
        start_from="end",
        batch_size=10,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    ),
)

# Analytics (full history, high throughput)
await manager.subscribe(
    AnalyticsProjection(),
    config=SubscriptionConfig(
        start_from="beginning",
        batch_size=1000,
        checkpoint_strategy=CheckpointStrategy.PERIODIC,
        checkpoint_interval_seconds=10.0,
    ),
)

# Critical audit (every event matters)
await manager.subscribe(
    AuditProjection(),
    config=SubscriptionConfig(
        start_from="checkpoint",
        continue_on_error=False,
        checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    ),
)
```

### Idempotent Handlers

Always design handlers to be safely re-runnable:

```python
class IdempotentProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        # Use event_id for idempotency
        if await self._already_processed(event.event_id):
            return

        await self._process(event)
        await self._mark_processed(event.event_id)
```

### Partitioned Scaling

For high-volume scenarios, partition by aggregate:

```python
class PartitionedProjection:
    def __init__(self, partition: int, total: int):
        self.partition = partition
        self.total = total

    async def handle(self, event: DomainEvent) -> None:
        # Only process events for this partition
        if hash(str(event.aggregate_id)) % self.total != self.partition:
            return
        await self._process(event)

# Deploy 4 instances:
# Instance 0: PartitionedProjection(0, 4)
# Instance 1: PartitionedProjection(1, 4)
# ...
```

---

## Troubleshooting

### Subscription Stuck in CATCHING_UP

1. Check lag: `health.subscriptions[0].lag_events`
2. Increase `batch_size` for faster catch-up
3. Profile handler performance
4. Check database connection pool sizing

### High Event Lag

```python
# Quick diagnosis
health = await manager.health_check()
print(f"Total lag: {health.total_lag_events}")

# Solutions
config = SubscriptionConfig(
    batch_size=500,       # Larger batches
    max_in_flight=2000,   # More concurrency
)
```

### Circuit Breaker Open

```python
# Check circuit state
sub = manager.get_subscription("OrderProjection")
print(f"Recent errors: {len(sub.recent_errors)}")

# Review recent errors
for err in sub.recent_errors[-5:]:
    print(f"{err.timestamp}: {err.error_type} - {err.error_message}")
```

### Debug Logging

```python
import logging
logging.getLogger("eventsource.subscriptions").setLevel(logging.DEBUG)
```

---

## See Also

- [Migration Guide](subscription-migration.md) - Migrate from manual event processing
- [Error Handling Guide](error-handling.md) - Detailed error handling patterns
- [Production Guide](production.md) - Production deployment best practices
- [Kafka Event Bus](kafka-event-bus.md) - Kafka integration for live events
