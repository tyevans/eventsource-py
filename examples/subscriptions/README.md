# Subscription Manager Examples

This directory contains working examples demonstrating how to use the `SubscriptionManager` for building event-driven read models (projections) in Python.

## Overview

The `SubscriptionManager` provides:
- **Catch-up subscriptions**: Process historical events from the event store
- **Live subscriptions**: Receive new events in real-time via the event bus
- **Seamless transition**: Automatic transition from catch-up to live
- **Checkpoint tracking**: Resume from where you left off
- **Error handling**: Retry, circuit breaker, and dead letter queue support
- **Health monitoring**: Track subscription health and lag

## Prerequisites

Python 3.11 or newer. Install the eventsource library:

```bash
pip install eventsource-py
```

Or install from a source checkout (editable):

```bash
cd /path/to/eventsource-py
pip install -e .
```

All three examples run entirely on the in-memory event store, event bus, and
checkpoint repository, so no optional extras (`postgresql`, `sqlite`, `redis`,
`rabbitmq`, `kafka`, `telemetry`) and no external services are required. The
only third-party import beyond `eventsource` itself is `pydantic`, which is a
core dependency.

Run the examples from the repository root so that `examples.subscriptions.*`
resolves as a package:

```bash
python -m examples.subscriptions.basic_projection
```

## Examples

### 1. Basic Projection (`basic_projection.py`)

**What it demonstrates:**
- Creating a simple read model projection
- Setting up `SubscriptionManager` with in-memory implementations
- Subscribing a projection to receive events
- Starting catch-up and transitioning to live subscriptions
- Querying the projection's read model

**Use case:** Getting started with event-driven projections

**Run:**
```bash
python -m examples.subscriptions.basic_projection
```

**Key concepts:**
```python
# 1. Create your projection class
class OrderSummaryProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        # Update your read model
        pass

# 2. Set up the manager
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
)

# 3. Subscribe and start
await manager.subscribe(projection, config=config)
await manager.start()
```

---

### 2. Resilient Projection (`resilient_projection.py`)

**What it demonstrates:**
- Retry with exponential backoff for transient failures
- Circuit breaker pattern to prevent cascading failures
- Dead Letter Queue (DLQ) for permanently failed events
- Graceful shutdown with signal handling
- Error callbacks for monitoring/alerting
- Health checks and error statistics

**Use case:** Production deployments requiring fault tolerance

**Run:**
```bash
python -m examples.subscriptions.resilient_projection
```

**Key concepts:**
```python
# Configure error handling (DLQ behavior)
error_config = ErrorHandlingConfig(
    max_recent_errors=100,  # Keep last 100 errors in memory
    dlq_enabled=True,  # Send to DLQ after all retries fail
)

# Configure subscription with retry settings
config = SubscriptionConfig(
    start_from="beginning",
    max_retries=3,
    initial_retry_delay=0.1,
)

# Create manager with resilience features
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    dlq_repo=dlq_repo,  # For failed events
    error_handling_config=error_config,
)

# Register error callbacks
manager.on_error(log_all_errors)
manager.on_error_severity(ErrorSeverity.CRITICAL, send_alert)

# Use circuit breaker in your projection
circuit_breaker = CircuitBreaker(
    config=CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=30.0,
    )
)

await circuit_breaker.execute(
    lambda: external_api.call(),
    "api_call"
)
```

---

### 3. Multi-Subscriber (`multi_subscriber.py`)

**What it demonstrates:**
- Running multiple projections with a single `SubscriptionManager`
- Different configurations per projection
- Health monitoring across all projections
- Pause/resume functionality
- Kubernetes-style readiness and liveness probes

**Use case:** CQRS architectures with multiple read models

**Run:**
```bash
python -m examples.subscriptions.multi_subscriber
```

**Key concepts:**
```python
# Create multiple projections
catalog = ProductCatalogProjection()      # For browsing
inventory = InventoryDashboardProjection() # For operations
analytics = SalesAnalyticsProjection()     # For reporting

# Subscribe each with different configs
await manager.subscribe(catalog, SubscriptionConfig(
    start_from="beginning",
    batch_size=200,
), name="ProductCatalog")

await manager.subscribe(inventory, SubscriptionConfig(
    start_from="checkpoint",  # Resume from last position
    batch_size=50,
), name="InventoryDashboard")

# Start all concurrently
await manager.start(concurrent=True)

# Pause and resume an individual subscription by name
await manager.pause_subscription("InventoryDashboard")
await manager.resume_subscription("InventoryDashboard")

# Monitor health
health = await manager.health_check()
print(f"Healthy: {health.healthy_count}/{health.subscription_count}")

# Kubernetes probes
readiness = await manager.readiness_check()
liveness = await manager.liveness_check()
```

## Configuration Options

The tables below are an **abridged quick-reference** covering the options most
relevant to the three examples. They are not a complete API reference. The
canonical source of truth for every field, its default, and its validation
rules is the dataclass definitions themselves:

- `SubscriptionConfig`, `CheckpointStrategy`, factory helpers -- [`src/eventsource/subscriptions/config.py`](../../src/eventsource/subscriptions/config.py)
- `ErrorHandlingConfig`, `ErrorHandlingStrategy`, `ErrorSeverity` -- [`src/eventsource/subscriptions/error_handling.py`](../../src/eventsource/subscriptions/error_handling.py)
- `HealthCheckConfig` -- [`src/eventsource/subscriptions/health.py`](../../src/eventsource/subscriptions/health.py)
- `RetryConfig`, `CircuitBreakerConfig` -- [`src/eventsource/subscriptions/retry.py`](../../src/eventsource/subscriptions/retry.py)

If a default here ever disagrees with the source, the source wins.

### SubscriptionConfig

Frozen dataclass. Values are validated in `__post_init__`, so invalid
combinations (for example `max_retry_delay < initial_retry_delay`) raise
`ValueError` at construction time.

| Option | Default | Description |
|--------|---------|-------------|
| `start_from` | `"checkpoint"` | Where to start: `"beginning"`, `"end"`, `"checkpoint"`, or an opaque `Position` token |
| `batch_size` | `100` | Events per batch during catch-up (must be >= 1) |
| `checkpoint_strategy` | `CheckpointStrategy.EVERY_BATCH` | When to persist checkpoints: `EVERY_EVENT`, `EVERY_BATCH`, `PERIODIC` |
| `checkpoint_interval_seconds` | `5.0` | Checkpoint interval used by the `PERIODIC` strategy |
| `processing_timeout` | `30.0` | Max seconds for one handler call (a `handle_batch()` of N events is one call, not N) |
| `event_types` | `None` | Tuple of `DomainEvent` subclasses to filter on (`None` = all types) |
| `aggregate_types` | `None` | Tuple of aggregate type names to filter on (`None` = all types) |
| `tenant_id` | `None` | `UUID` to scope the subscription to a single tenant (`None` = all tenants) |
| `continue_on_error` | `True` | Continue processing after an event is DLQ'd |
| `max_retries` | `5` | Retry attempts for failed events (must be >= 0) |
| `initial_retry_delay` | `1.0` | Seconds before the first retry |
| `max_retry_delay` | `60.0` | Upper bound on backoff delay (must be >= `initial_retry_delay`) |
| `retry_exponential_base` | `2.0` | Backoff multiplier per attempt (must be > 1.0) |
| `retry_jitter` | `0.1` | Jitter fraction (0.0-1.0) applied to retry delays |
| `circuit_breaker_enabled` | `True` | Whether the circuit breaker is active |
| `circuit_breaker_failure_threshold` | `5` | Consecutive failures before the circuit opens (must be >= 1) |
| `circuit_breaker_recovery_timeout` | `30.0` | Seconds the circuit stays open before probing again |

Two accessors derive the sub-configs used internally:
`config.get_retry_config()` returns a `RetryConfig`, and
`config.get_circuit_breaker_config()` returns a `CircuitBreakerConfig`.

### SubscriptionConfig Factory Helpers

`config.py` also exports two factories for the common shapes:

```python
from eventsource.application.subscriptions.config import (
    create_catch_up_config,
    create_live_only_config,
)

# Optimized for replaying history: larger batches, batch checkpointing.
# start_from="checkpoint", batch_size=1000, checkpoint_strategy=EVERY_BATCH
catch_up = create_catch_up_config(batch_size=1000, checkpoint_every_batch=True)

# Passing checkpoint_every_batch=False switches to CheckpointStrategy.PERIODIC.
catch_up_periodic = create_catch_up_config(checkpoint_every_batch=False)

# Only new events, no history replay.
# start_from="end", batch_size=100, checkpoint_strategy=EVERY_EVENT
live_only = create_live_only_config()
```

Both return an ordinary `SubscriptionConfig`, so you can pass the result
straight to `manager.subscribe(projection, config=...)`.

### ErrorHandlingConfig

| Option | Default | Description |
|--------|---------|-------------|
| `strategy` | `ErrorHandlingStrategy.RETRY_THEN_CONTINUE` | Default strategy: also `STOP`, `CONTINUE`, `RETRY_THEN_DLQ`, `DLQ_ONLY` |
| `max_recent_errors` | `100` | Maximum recent errors to keep in memory |
| `max_errors_before_stop` | `None` | If set, stop the subscription after this many errors |
| `error_rate_threshold` | `None` | If set, alert when the per-minute error rate exceeds this |
| `dlq_enabled` | `True` | Send failed events to the DLQ after all retries fail |
| `notify_on_severity` | `ErrorSeverity.HIGH` | Minimum severity that triggers registered callbacks |

### HealthCheckConfig

| Option | Default | Description |
|--------|---------|-------------|
| `max_error_rate_per_minute` | `10.0` | Error rate above this is unhealthy |
| `max_errors_warning` | `10` | Warn if total errors exceed this |
| `max_errors_critical` | `100` | Critical if total errors exceed this |
| `max_lag_events_warning` | `1000` | Warn if lag exceeds this many events |
| `max_lag_events_critical` | `10000` | Critical if lag exceeds this many events |
| `circuit_open_is_unhealthy` | `True` | Treat an open circuit breaker as unhealthy |
| `max_dlq_events_warning` | `10` | Warn if the DLQ holds this many events |
| `max_dlq_events_critical` | `100` | Critical if the DLQ holds this many events |

## Projection Protocol

Your projection class must implement:

```python
class MyProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return event types this projection handles."""
        return [EventA, EventB]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event."""
        if isinstance(event, EventA):
            # Update read model
            pass
```

### Optional: Batch Processing

For high-throughput scenarios:

```python
class BulkProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [EventA]

    async def handle(self, event: DomainEvent) -> None:
        # Single event fallback
        pass

    async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
        # Efficient batch processing
        await self._bulk_insert(events)
```

## Production Checklist

When deploying to production:

1. **Use persistent storage**: Replace `InMemory*` with `PostgreSQL*` implementations
2. **Configure retries**: Tune retry delays for your latency requirements
3. **Enable DLQ**: Always enable dead letter queue for failed events
4. **Set up monitoring**: Register error callbacks to send alerts
5. **Use health checks**: Expose `/health/ready` and `/health/live` endpoints
6. **Handle signals**: Call `manager.register_signals()` for graceful shutdown
7. **Configure timeouts**: Set appropriate `shutdown_timeout` and `drain_timeout`
   on the `SubscriptionManager` constructor (they are manager-wide, not
   per-subscription)

## Common Patterns

### Graceful Shutdown (Production)

```python
# Register signal handlers
manager.register_signals()

# Run until SIGTERM/SIGINT
result = await manager.run_until_shutdown()

if result.forced:
    logger.warning("Shutdown was forced, some events may not be processed")
```

### HTTP Health Endpoint (FastAPI)

```python
@app.get("/health/ready")
async def readiness():
    status = await manager.readiness_check()
    return {"ready": status.ready, "reason": status.reason}

@app.get("/health/live")
async def liveness():
    status = await manager.liveness_check()
    return {"alive": status.alive, "reason": status.reason}
```

### Manual DLQ Resolution

```python
# Get failed events
failed = await dlq_repo.get_failed_events()

for entry in failed:
    # Investigate and fix the issue
    # ...

    # Mark as resolved
    await dlq_repo.mark_resolved(entry.id, "admin-user")

    # Or retry
    await dlq_repo.mark_retrying(entry.id)
```

## Troubleshooting

### Projection not receiving events

1. Check `subscribed_to()` returns the correct event types
2. Verify event types are registered with `@register_event`
3. Ensure events are being published to the event bus
4. Check checkpoint - you may need `start_from="beginning"`

### High event lag

1. Increase `batch_size` for faster catch-up -- it widens the store read, and during catch-up it is also the unit handed to `handle_batch()` if your subscriber implements one (see `handle_batch()` in the [subscriptions guide](../../docs/guides/subscriptions.md#handle-events-in-batches-with-handle_batch))
2. If lag is on the live path, `batch_size` only widens the store read: the live runner still delivers one event at a time through `handle()`
3. Check for slow database queries in your projection
4. Monitor for external service bottlenecks

### Events going to DLQ

1. Check DLQ entries for error messages
2. Review error classification (transient vs permanent)
3. Increase `max_retries` for flaky services
4. Add circuit breaker for external dependencies

## Further Reading

- [User Guide](../../docs/guides/subscription-manager-user-guide.md) - Comprehensive documentation
- [API Reference](../../docs/api/subscription-manager-api.md) - Detailed API documentation
- [Existing Examples](../) - Other eventsource examples
