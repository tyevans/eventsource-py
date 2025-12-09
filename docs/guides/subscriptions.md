# Subscription Manager Guide

This guide covers the subscription manager system for building event-driven projections with catch-up subscriptions and live event streaming.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Basic Usage Patterns](#basic-usage-patterns)
3. [Resilience Patterns](#resilience-patterns)
4. [Advanced Patterns](#advanced-patterns)
5. [Production Best Practices](#production-best-practices)
6. [Troubleshooting](#troubleshooting)

---

## Getting Started

### Overview

The subscription manager provides a unified way to consume events from an event store and event bus. It handles:

- **Catch-up**: Reading historical events from the event store
- **Live**: Streaming new events from the event bus in real-time
- **Transition**: Seamlessly switching from catch-up to live mode without missing events

This is essential for building projections (read models) that need to process all events, starting from any point in the event stream.

### Quick Start

```python
import asyncio
from eventsource.subscriptions import SubscriptionManager, SubscriptionConfig
from eventsource.events import DomainEvent, register_event

# Define events
@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    customer_id: str
    total: float

@register_event
class OrderShipped(DomainEvent):
    event_type: str = "OrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str

# Create a simple projection
class OrderSummaryProjection:
    def __init__(self):
        self.orders = {}

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Declare which events this projection handles."""
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event."""
        if isinstance(event, OrderCreated):
            self.orders[str(event.aggregate_id)] = {
                "customer_id": event.customer_id,
                "total": event.total,
                "status": "created",
            }
        elif isinstance(event, OrderShipped):
            if str(event.aggregate_id) in self.orders:
                self.orders[str(event.aggregate_id)]["status"] = "shipped"
                self.orders[str(event.aggregate_id)]["tracking"] = event.tracking_number

async def main():
    # Set up infrastructure (event store, bus, checkpoint repository)
    # See the installation guide for setup details

    # Create the subscription manager
    manager = SubscriptionManager(
        event_store=event_store,
        event_bus=event_bus,
        checkpoint_repo=checkpoint_repo,
    )

    # Create and register projection
    projection = OrderSummaryProjection()
    await manager.subscribe(projection)

    # Start processing events
    await manager.start()

    # ... projection now receives events ...

    # Graceful shutdown
    await manager.stop()

asyncio.run(main())
```

### Basic Concepts

#### Catch-up Mode

When a subscription starts, it first enters **catch-up mode** to process historical events:

```
Event Store: [E1] [E2] [E3] [E4] [E5] [E6] ...
                  ^
            Checkpoint (last processed position)
```

The subscription reads events in batches from the checkpoint position, processing each one through your handler.

#### Live Mode

Once caught up to the current position, the subscription transitions to **live mode**:

```
Event Bus: ---> [E7] ---> [E8] ---> [E9] --->
                 |         |         |
            (delivered in real-time)
```

New events are delivered as they occur via the event bus.

#### Transition

The transition between catch-up and live modes is handled automatically:

1. Start live subscription (buffering events)
2. Complete catch-up processing
3. Process buffered events to close the gap
4. Switch to live event delivery

This ensures no events are missed during the transition.

### When to Use Subscriptions

Use the subscription manager when you need to:

| Use Case | Example |
|----------|---------|
| Build read models | Dashboard showing order statistics |
| Maintain denormalized views | Search index populated from events |
| Trigger side effects | Send email when order ships |
| Synchronize external systems | Update inventory system |
| Rebuild projections | Recreate read model after schema change |

---

## Basic Usage Patterns

### Creating Projections with the Subscriber Protocol

The `Subscriber` protocol defines the interface for event handlers:

```python
from eventsource.subscriptions import Subscriber

class MyProjection:
    """A projection implementing the Subscriber protocol."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """Return the event types this projection handles."""
        return [OrderCreated, OrderShipped, OrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Process a single event asynchronously."""
        # Dispatch to specific handlers
        handler = getattr(self, f"_handle_{event.event_type}", None)
        if handler:
            await handler(event)

    async def _handle_OrderCreated(self, event: OrderCreated) -> None:
        # Update read model
        pass
```

#### Using Base Classes

For more functionality, extend the provided base classes:

```python
from eventsource.subscriptions import BaseSubscriber, BatchAwareSubscriber

class OrderProjection(BaseSubscriber):
    """Using BaseSubscriber for basic functionality."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        # BaseSubscriber provides can_handle() method
        if not self.can_handle(event):
            return
        await self._process(event)


class BulkOrderProjection(BatchAwareSubscriber):
    """Using BatchAwareSubscriber for efficient batch processing."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        # Single event processing (fallback)
        await self._insert_order(event)

    async def handle_batch(self, events: Sequence[DomainEvent]) -> None:
        # Optimized batch processing
        # E.g., bulk database insert
        await self._bulk_insert_orders(events)
```

### Configuring Subscriptions

#### Start Position Options

Control where the subscription starts reading events:

```python
from eventsource.subscriptions import SubscriptionConfig

# Resume from last checkpoint (default)
config = SubscriptionConfig(start_from="checkpoint")

# Start from the beginning (rebuild projection)
config = SubscriptionConfig(start_from="beginning")

# Start from the end (live-only, new events only)
config = SubscriptionConfig(start_from="end")

# Start from specific position
config = SubscriptionConfig(start_from=1000)
```

#### Batch Size Configuration

Optimize catch-up performance with batch sizing:

```python
# Standard configuration
config = SubscriptionConfig(
    batch_size=100,  # Events per batch during catch-up
)

# High-throughput catch-up
config = SubscriptionConfig(
    batch_size=1000,  # Larger batches for faster catch-up
    max_in_flight=5000,  # More concurrent events allowed
)

# Memory-constrained environment
config = SubscriptionConfig(
    batch_size=50,  # Smaller batches
    max_in_flight=200,  # Lower concurrency limit
)
```

#### Checkpoint Strategies

Choose when to persist checkpoints:

```python
from eventsource.subscriptions import SubscriptionConfig, CheckpointStrategy

# Checkpoint after each batch (default, balanced)
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
)

# Checkpoint after every event (safest, slowest)
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
)

# Checkpoint on time interval (fastest, riskiest)
config = SubscriptionConfig(
    checkpoint_strategy=CheckpointStrategy.PERIODIC,
    checkpoint_interval_seconds=5.0,
)
```

#### Using Factory Functions

For common configurations, use the factory functions:

```python
from eventsource.subscriptions import create_catch_up_config, create_live_only_config

# Optimized for catch-up scenarios
config = create_catch_up_config(
    batch_size=1000,
    checkpoint_every_batch=True,
)

# For live-only subscriptions (integration handlers)
config = create_live_only_config()
```

### Running the Subscription Manager

#### Basic Start/Stop

```python
manager = SubscriptionManager(event_store, event_bus, checkpoint_repo)

# Register subscriptions
await manager.subscribe(projection1)
await manager.subscribe(projection2, config=SubscriptionConfig(start_from="beginning"))

# Start all subscriptions
await manager.start()

# ... application runs ...

# Stop gracefully
await manager.stop(timeout=30.0)
```

#### Context Manager Pattern

```python
async with SubscriptionManager(event_store, event_bus, checkpoint_repo) as manager:
    await manager.subscribe(projection)
    # Manager starts automatically
    # ... do work ...
# Manager stops automatically on exit
```

#### Daemon Mode (Production)

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

    if result.forced:
        logger.warning("Shutdown was forced due to timeout")

    logger.info(f"Shutdown complete: {result.checkpoints_saved} checkpoints saved")
```

#### Starting Specific Subscriptions

```python
# Start only specific subscriptions
results = await manager.start(
    subscription_names=["OrderProjection", "InventoryProjection"]
)

# Check for failures
for name, error in results.items():
    if error:
        logger.error(f"Failed to start {name}: {error}")
```

---

## Resilience Patterns

### Handling Errors

#### Continue on Error (Default)

By default, subscriptions continue processing after errors:

```python
config = SubscriptionConfig(
    continue_on_error=True,  # Default
)
```

Events that fail processing are logged and can be sent to a dead letter queue (DLQ).

#### Stop on Error

For critical projections that must process every event:

```python
config = SubscriptionConfig(
    continue_on_error=False,
)
```

The subscription will stop on the first error, allowing manual intervention.

#### Error Callbacks

Register callbacks to be notified of errors:

```python
from eventsource.subscriptions import ErrorInfo, ErrorSeverity, ErrorCategory

# Global error callback
async def log_all_errors(error_info: ErrorInfo) -> None:
    logger.error(
        f"Subscription error: {error_info.error_message}",
        extra={
            "subscription": error_info.subscription_name,
            "event_id": str(error_info.event_id),
            "event_type": error_info.event_type,
            "category": error_info.classification.category.value,
        },
    )

manager.on_error(log_all_errors)

# Category-specific callback (e.g., transient errors)
async def handle_transient_errors(error_info: ErrorInfo) -> None:
    # Maybe trigger a retry or alert
    pass

manager.on_error_category(ErrorCategory.TRANSIENT, handle_transient_errors)

# Severity-specific callback (e.g., critical alerts)
async def alert_on_critical(error_info: ErrorInfo) -> None:
    await send_pager_alert(
        message=f"Critical subscription error: {error_info.error_message}",
        subscription=error_info.subscription_name,
    )

manager.on_error_severity(ErrorSeverity.CRITICAL, alert_on_critical)
```

#### Error Handling Configuration

```python
from eventsource.subscriptions import ErrorHandlingConfig, ErrorHandlingStrategy

error_config = ErrorHandlingConfig(
    # Default strategy for handling errors
    strategy=ErrorHandlingStrategy.RETRY_THEN_CONTINUE,

    # Stop subscription after this many total errors
    max_errors_before_stop=100,

    # Alert when error rate exceeds threshold
    error_rate_threshold=10.0,  # errors per minute

    # Send failed events to DLQ
    dlq_enabled=True,

    # Minimum severity to trigger callbacks
    notify_on_severity=ErrorSeverity.HIGH,
)

manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    error_handling_config=error_config,
)
```

### Backpressure and Flow Control

The subscription manager includes automatic backpressure to prevent overwhelming your handlers:

```python
config = SubscriptionConfig(
    max_in_flight=1000,  # Maximum events being processed concurrently
    backpressure_threshold=0.8,  # Signal backpressure at 80% capacity
)
```

When `max_in_flight` is reached, the subscription pauses reading new events until capacity is available.

#### Monitoring Backpressure

```python
# Get flow control statistics
health = manager.check_all_health()

for sub_name, sub_health in health["subscriptions"].items():
    indicators = sub_health.get("indicators", [])
    for indicator in indicators:
        if indicator["name"] == "backpressure":
            if indicator["status"] == "degraded":
                logger.warning(f"{sub_name} is experiencing backpressure")
```

### Graceful Shutdown in Production

The subscription manager supports graceful shutdown with signal handling:

```python
manager = SubscriptionManager(
    event_store=event_store,
    event_bus=event_bus,
    checkpoint_repo=checkpoint_repo,
    shutdown_timeout=30.0,  # Total shutdown timeout
    drain_timeout=10.0,  # Time to wait for in-flight events
)

# Register signal handlers (SIGTERM, SIGINT)
manager.register_signals()

await manager.start()

# Wait for shutdown signal
await manager.shutdown_coordinator.wait_for_shutdown()

# Or programmatically request shutdown
manager.request_shutdown()
```

#### Shutdown Phases

The shutdown process follows these phases:

1. **INITIATED**: Shutdown signal received
2. **STOPPING**: Stop accepting new events
3. **DRAINING**: Wait for in-flight events to complete
4. **CHECKPOINTING**: Save final checkpoints
5. **COMPLETED**: Shutdown complete

```python
# Check shutdown phase
phase = manager.shutdown_phase

# Check if shutting down
if manager.is_shutting_down:
    # Don't start new work
    pass
```

### Retry and Circuit Breaker Configuration

Configure automatic retries for transient failures:

```python
config = SubscriptionConfig(
    # Retry settings
    max_retries=5,
    initial_retry_delay=1.0,  # seconds
    max_retry_delay=60.0,  # seconds
    retry_exponential_base=2.0,
    retry_jitter=0.1,  # 10% randomization

    # Circuit breaker settings
    circuit_breaker_enabled=True,
    circuit_breaker_failure_threshold=5,  # Open after 5 failures
    circuit_breaker_recovery_timeout=30.0,  # Wait 30s before trying again
)
```

#### Circuit Breaker States

| State | Description |
|-------|-------------|
| CLOSED | Normal operation, requests flow through |
| OPEN | Too many failures, requests blocked |
| HALF_OPEN | Testing if service recovered |

---

## Advanced Patterns

### Event Filtering

#### Exact Type Matching

```python
# Filter by exact event types (via subscribed_to)
class OrderProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]  # Only these types
```

#### Wildcard Pattern Matching

```python
from eventsource.subscriptions import EventFilter

# Match event types by pattern
filter = EventFilter.from_patterns("Order*", "Payment*")
# Matches: OrderCreated, OrderShipped, PaymentReceived, etc.

# Combine patterns
filter = EventFilter(
    event_type_patterns=("*Created", "*Completed"),
)
```

#### Aggregate Type Filtering

```python
# Filter by aggregate type
config = SubscriptionConfig(
    aggregate_types=("Order", "Payment"),  # Only events from these aggregates
)

# Or via EventFilter
filter = EventFilter(
    aggregate_types=("Order",),
)
```

#### Using FilteringSubscriber

```python
from eventsource.subscriptions import FilteringSubscriber

class TenantOrderProjection(FilteringSubscriber):
    """Projection that filters events by tenant."""

    def __init__(self, tenant_id: str):
        self.tenant_id = tenant_id

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    def should_handle(self, event: DomainEvent) -> bool:
        """Custom filter: only handle events for our tenant."""
        return getattr(event, "tenant_id", None) == self.tenant_id

    async def _process_event(self, event: DomainEvent) -> None:
        """Process events that passed the filter."""
        # Update tenant-specific read model
        pass
```

### Multiple Subscriptions with Different Configurations

```python
# Real-time dashboard projection (live-only, fast)
dashboard_config = SubscriptionConfig(
    start_from="end",  # Only new events
    batch_size=50,
    checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
)

# Analytics projection (full history, large batches)
analytics_config = SubscriptionConfig(
    start_from="beginning",
    batch_size=1000,
    checkpoint_strategy=CheckpointStrategy.PERIODIC,
    checkpoint_interval_seconds=10.0,
)

# Critical projection (every event, with strict ordering)
critical_config = SubscriptionConfig(
    start_from="checkpoint",
    batch_size=1,
    continue_on_error=False,
    checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
)

# Register with different configurations
await manager.subscribe(dashboard_projection, config=dashboard_config)
await manager.subscribe(analytics_projection, config=analytics_config)
await manager.subscribe(critical_projection, config=critical_config)
```

### Pause and Resume for Maintenance

```python
# Pause a specific subscription
await manager.pause_subscription("OrderProjection")

# Pause all subscriptions
results = await manager.pause_all()
paused_count = sum(1 for success in results.values() if success)
logger.info(f"Paused {paused_count} subscriptions for maintenance")

# ... perform maintenance ...

# Resume
await manager.resume_subscription("OrderProjection")

# Or resume all
results = await manager.resume_all()
```

#### Pause Reasons

```python
from eventsource.subscriptions import PauseReason

# Manual pause (default)
await manager.pause_subscription("OrderProjection", reason=PauseReason.MANUAL)

# Check paused subscriptions
paused = manager.paused_subscription_names
for name in paused:
    sub = manager.get_subscription(name)
    logger.info(f"{name} paused: {sub.pause_reason}")
```

### Health Monitoring and Kubernetes Probes

#### Health Check API

```python
# Comprehensive health check
health = await manager.health_check()

print(f"Status: {health.status}")  # healthy/degraded/unhealthy
print(f"Running: {health.running}")
print(f"Subscriptions: {health.subscription_count}")
print(f"Healthy: {health.healthy_count}")
print(f"Degraded: {health.degraded_count}")
print(f"Unhealthy: {health.unhealthy_count}")
print(f"Total lag: {health.total_lag_events} events")

# Per-subscription details
for sub in health.subscriptions:
    print(f"  {sub.name}: {sub.status} (lag: {sub.lag_events})")
```

#### Kubernetes Readiness Probe

```python
# For HTTP readiness endpoint
readiness = await manager.readiness_check()

if readiness.ready:
    return {"status": "ready"}, 200
else:
    return {"status": "not_ready", "reason": readiness.reason}, 503
```

Ready when:
- Manager is running
- Not shutting down
- No subscriptions in error state
- No subscriptions still starting

#### Kubernetes Liveness Probe

```python
# For HTTP liveness endpoint
liveness = await manager.liveness_check()

if liveness.alive:
    return {"status": "alive"}, 200
else:
    return {"status": "dead", "reason": liveness.reason}, 503
```

Alive when:
- Internal lock is responsive (no deadlocks)
- Can enumerate subscriptions

#### FastAPI Integration Example

```python
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

@app.get("/health")
async def health():
    health = await manager.health_check()
    status_code = 200 if health.status == "healthy" else 503
    return JSONResponse(content=health.to_dict(), status_code=status_code)

@app.get("/ready")
async def ready():
    readiness = await manager.readiness_check()
    status_code = 200 if readiness.ready else 503
    return JSONResponse(content=readiness.to_dict(), status_code=status_code)

@app.get("/live")
async def live():
    liveness = await manager.liveness_check()
    status_code = 200 if liveness.alive else 503
    return JSONResponse(content=liveness.to_dict(), status_code=status_code)
```

#### Health Check Configuration

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

    # Backpressure thresholds
    backpressure_warning_duration_seconds=60.0,
    backpressure_critical_duration_seconds=300.0,

    # Circuit breaker
    circuit_open_is_unhealthy=True,

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

## Production Best Practices

### Recommended Configurations

#### Standard Production

```python
config = SubscriptionConfig(
    start_from="checkpoint",
    batch_size=100,
    max_in_flight=1000,
    backpressure_threshold=0.8,
    checkpoint_strategy=CheckpointStrategy.EVERY_BATCH,
    processing_timeout=30.0,
    shutdown_timeout=30.0,
    continue_on_error=True,
    max_retries=5,
    circuit_breaker_enabled=True,
)
```

#### High-Throughput Catch-up

```python
# For rebuilding projections or processing large backlogs
config = SubscriptionConfig(
    start_from="beginning",
    batch_size=1000,
    max_in_flight=5000,
    checkpoint_strategy=CheckpointStrategy.PERIODIC,
    checkpoint_interval_seconds=5.0,
    processing_timeout=60.0,
)
```

#### Low-Latency Live Processing

```python
# For real-time updates
config = SubscriptionConfig(
    start_from="end",
    batch_size=10,
    max_in_flight=100,
    checkpoint_strategy=CheckpointStrategy.EVERY_EVENT,
    processing_timeout=5.0,
)
```

### Monitoring with OpenTelemetry

The subscription manager emits OpenTelemetry metrics when available:

```python
# Metrics are automatically emitted when OpenTelemetry is installed
# pip install opentelemetry-api opentelemetry-sdk

from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

# Configure metrics export
reader = PeriodicExportingMetricReader(OTLPMetricExporter())
metrics.set_meter_provider(MeterProvider(metric_readers=[reader]))

# Subscription manager automatically emits:
# - subscription.events.processed (Counter)
# - subscription.events.failed (Counter)
# - subscription.processing.duration (Histogram)
# - subscription.lag (Gauge)
# - subscription.state (Gauge)
```

#### Available Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `subscription.events.processed` | Counter | Total events processed |
| `subscription.events.failed` | Counter | Total events failed |
| `subscription.processing.duration` | Histogram | Processing time (ms) |
| `subscription.lag` | Gauge | Current event lag |
| `subscription.state` | Gauge | Current state (numeric) |

All metrics include the `subscription` attribute for filtering.

#### Manual Metrics Access

```python
from eventsource.subscriptions import get_metrics

# Get metrics for a subscription
metrics = get_metrics("OrderProjection")

# Get snapshot of current values
snapshot = metrics.get_snapshot()
print(f"Processed: {snapshot.events_processed}")
print(f"Failed: {snapshot.events_failed}")
print(f"Lag: {snapshot.current_lag}")
```

### Handling Long-Running Subscriptions

#### Implement Idempotent Handlers

```python
class IdempotentProjection:
    """Projection that can safely reprocess events."""

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated]

    async def handle(self, event: DomainEvent) -> None:
        # Use event_id for idempotency
        if await self._already_processed(event.event_id):
            return

        await self._process(event)
        await self._mark_processed(event.event_id)

    async def _already_processed(self, event_id: UUID) -> bool:
        # Check if event was already processed
        pass

    async def _mark_processed(self, event_id: UUID) -> None:
        # Record that event was processed
        pass
```

#### Handle Handler Timeouts

```python
import asyncio

class TimeoutAwareProjection:
    """Projection with timeout handling."""

    async def handle(self, event: DomainEvent) -> None:
        try:
            await asyncio.wait_for(
                self._process(event),
                timeout=10.0,  # 10 second timeout
            )
        except asyncio.TimeoutError:
            logger.error(
                f"Handler timeout for event {event.event_id}",
                extra={"event_type": event.event_type},
            )
            # Decide: retry, skip, or raise
            raise
```

### Scaling Considerations

#### Horizontal Scaling with Partitioning

For high-volume scenarios, partition events across multiple instances:

```python
class PartitionedProjection:
    """Projection that processes a subset of aggregates."""

    def __init__(self, partition_id: int, total_partitions: int):
        self.partition_id = partition_id
        self.total_partitions = total_partitions

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderCreated, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        # Only process events for this partition
        event_partition = hash(str(event.aggregate_id)) % self.total_partitions
        if event_partition != self.partition_id:
            return  # Skip events for other partitions

        await self._process(event)

# Deploy multiple instances:
# Instance 0: PartitionedProjection(0, 4)
# Instance 1: PartitionedProjection(1, 4)
# Instance 2: PartitionedProjection(2, 4)
# Instance 3: PartitionedProjection(3, 4)
```

#### Connection Pool Sizing

Ensure adequate database connections for concurrent event processing:

```python
from sqlalchemy.ext.asyncio import create_async_engine

# Size pool based on max_in_flight
engine = create_async_engine(
    database_url,
    pool_size=20,  # Should support concurrent handlers
    max_overflow=40,
)
```

---

## Troubleshooting

### Common Issues and Solutions

#### Subscription Stuck in CATCHING_UP

**Symptoms**: Subscription never transitions to live mode

**Causes and Solutions**:

1. **Large backlog**: Check lag metrics
   ```python
   health = await manager.health_check()
   for sub in health.subscriptions:
       print(f"{sub.name}: lag={sub.lag_events}")
   ```

2. **Slow handler**: Profile your handler
   ```python
   with metrics.time_processing() as timer:
       await handler.handle(event)
   print(f"Processing took {timer.duration_ms}ms")
   ```

3. **Database bottleneck**: Check connection pool utilization

#### High Event Lag

**Symptoms**: `subscription.lag` metric consistently high

**Solutions**:

1. Increase batch size:
   ```python
   config = SubscriptionConfig(batch_size=500)
   ```

2. Increase concurrency:
   ```python
   config = SubscriptionConfig(max_in_flight=2000)
   ```

3. Optimize handler (batch database operations)

4. Scale horizontally with partitioning

#### Subscription in ERROR State

**Symptoms**: Health check shows subscription unhealthy

**Diagnosis**:

```python
sub = manager.get_subscription("OrderProjection")
print(f"State: {sub.state}")
print(f"Last error: {sub.last_error}")

# Get detailed error stats
handler = manager.get_error_handler("OrderProjection")
if handler:
    for error in handler.recent_errors[-5:]:
        print(f"  {error.timestamp}: {error.error_type} - {error.error_message}")
```

**Solutions**:

1. Fix the underlying error in your handler
2. Process events from the DLQ
3. Consider `continue_on_error=True` for non-critical projections

#### Circuit Breaker Open

**Symptoms**: Events not being processed, circuit breaker OPEN

**Diagnosis**:

```python
health = manager.check_all_health()
for sub_name, sub_health in health["subscriptions"].items():
    for indicator in sub_health.get("indicators", []):
        if indicator["name"] == "circuit_breaker":
            print(f"{sub_name}: {indicator['status']} - {indicator['message']}")
```

**Solutions**:

1. Wait for recovery timeout
2. Fix the failing dependency (database, external service)
3. Adjust circuit breaker thresholds if too sensitive:
   ```python
   config = SubscriptionConfig(
       circuit_breaker_failure_threshold=10,  # More tolerant
       circuit_breaker_recovery_timeout=60.0,  # Longer recovery
   )
   ```

### Debugging Tips

#### Enable Debug Logging

```python
import logging

# Enable debug logging for subscriptions
logging.getLogger("eventsource.subscriptions").setLevel(logging.DEBUG)
```

#### Inspect Subscription State

```python
# Get detailed status
status = manager.get_all_statuses()
for name, sub_status in status.items():
    print(f"{name}:")
    print(f"  State: {sub_status.state}")
    print(f"  Events processed: {sub_status.events_processed}")
    print(f"  Events failed: {sub_status.events_failed}")
    print(f"  Last position: {sub_status.last_processed_position}")
    print(f"  Lag: {sub_status.lag}")
```

#### Check Checkpoint Position

```python
# Verify checkpoint is being saved
checkpoint = await checkpoint_repo.get("OrderProjection")
if checkpoint:
    print(f"Position: {checkpoint.position}")
    print(f"Updated at: {checkpoint.updated_at}")
```

#### Monitor Event Flow

```python
# Simple logging projection for debugging
class DebugProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return []  # Subscribe to all events

    async def handle(self, event: DomainEvent) -> None:
        logger.debug(
            f"Received event",
            extra={
                "event_type": event.event_type,
                "event_id": str(event.event_id),
                "aggregate_id": str(event.aggregate_id),
                "position": getattr(event, "global_position", "N/A"),
            },
        )
```

---

## See Also

- [Migration Guide](subscription-migration.md) - Migrate from manual event processing
- [Event Stores API](../api/stores.md) - Event store interfaces
- [Production Deployment](production.md) - Production deployment guide
- [Observability Guide](observability.md) - OpenTelemetry integration
- [Error Handling Guide](error-handling.md) - Error handling patterns
- [Kafka Event Bus Guide](kafka-event-bus.md) - Kafka integration
