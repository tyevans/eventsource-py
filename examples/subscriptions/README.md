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

Install the eventsource library:

```bash
pip install eventsource-py
```

Or install from source:

```bash
cd /path/to/eventsource-py
pip install -e .
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

# Monitor health
health = await manager.health_check()
print(f"Healthy: {health.healthy_count}/{health.subscription_count}")

# Kubernetes probes
readiness = await manager.readiness_check()
liveness = await manager.liveness_check()
```

## Configuration Options

### SubscriptionConfig

| Option | Default | Description |
|--------|---------|-------------|
| `start_from` | `"checkpoint"` | Where to start: `"beginning"`, `"end"`, `"checkpoint"`, or position number |
| `batch_size` | `100` | Events per batch during catch-up |
| `max_in_flight` | `1000` | Maximum concurrent events |
| `checkpoint_strategy` | `EVERY_BATCH` | When to save checkpoint: `EVERY_EVENT`, `EVERY_BATCH`, `PERIODIC` |
| `max_retries` | `5` | Retry attempts for failed events |
| `continue_on_error` | `True` | Continue processing after DLQ |

### ErrorHandlingConfig

| Option | Default | Description |
|--------|---------|-------------|
| `strategy` | `RETRY_THEN_CONTINUE` | Error handling strategy |
| `max_recent_errors` | `100` | Maximum recent errors to keep in memory |
| `max_errors_before_stop` | `None` | Stop subscription after N errors (optional) |
| `dlq_enabled` | `True` | Send to DLQ after all retries fail |

### HealthCheckConfig

| Option | Default | Description |
|--------|---------|-------------|
| `max_lag_events_warning` | `1000` | Events behind before warning |
| `max_errors_warning` | `10` | Total errors before warning |
| `max_errors_critical` | `100` | Total errors before critical |
| `max_error_rate_per_minute` | `10.0` | Error rate threshold |

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

1. Increase `batch_size` for faster catch-up
2. Check for slow database queries in your projection
3. Consider batch processing with `handle_batch()`
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
