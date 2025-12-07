# Error Handling Guide

This guide covers all exception types in eventsource and provides patterns for handling and recovering from errors in your event-sourced applications.

## Overview

Eventsource provides a structured exception hierarchy that helps you handle different types of failures appropriately. The library distinguishes between:

- **Recoverable errors**: Concurrency conflicts that can be resolved with retry logic
- **Not-found errors**: Missing resources that may require user notification or fallback logic
- **Permanent failures**: Bugs or data corruption that require investigation
- **Transient failures**: Temporary issues that resolve with retries

### Error Handling Philosophy

1. **Fail fast with clear information**: Exceptions include context (IDs, versions) for debugging
2. **Enable recovery**: Errors like `OptimisticLockError` provide enough information to retry intelligently
3. **Preserve data**: The dead letter queue (DLQ) ensures no events are silently lost
4. **Isolation**: Failures in one component (e.g., a projection) don't crash the entire system

---

## Exception Hierarchy

All eventsource exceptions inherit from `EventSourceError`, enabling both specific and broad exception handling:

```
EventSourceError (base)
    |
    +-- OptimisticLockError      # Version conflicts during concurrent writes
    +-- AggregateNotFoundError   # Aggregate does not exist
    +-- EventNotFoundError       # Specific event not found
    +-- ProjectionError          # Projection failed to process event
    +-- EventStoreError          # Event store operation failed
    +-- EventBusError            # Event bus operation failed
    +-- CheckpointError          # Checkpoint operation failed
    +-- SerializationError       # Event serialization/deserialization failed
```

### Importing Exceptions

```python
from eventsource.exceptions import (
    EventSourceError,
    OptimisticLockError,
    AggregateNotFoundError,
    EventNotFoundError,
    ProjectionError,
    EventStoreError,
    EventBusError,
    CheckpointError,
    SerializationError,
)
```

---

## OptimisticLockError

Raised when there is a version conflict during event append. This is the most common exception you will handle in production.

### When It Happens

- Two processes load the same aggregate simultaneously
- Both attempt to append events at the same expected version
- The second append fails because the version has changed

```
Process A: Load Order (v5) --> Process --> Save (expect v5) --> Success (now v6)
Process B: Load Order (v5) --> Process --> Save (expect v5) --> CONFLICT!
```

### Exception Details

```python
class OptimisticLockError(EventSourceError):
    aggregate_id: UUID        # Which aggregate had the conflict
    expected_version: int     # Version the code expected
    actual_version: int       # Actual version in the store
```

### Handling Pattern: Retry with Reload

The standard pattern is to catch the error, reload the aggregate with fresh state, and retry:

```python
from uuid import UUID
from eventsource.exceptions import OptimisticLockError
from eventsource.aggregates import AggregateRepository

async def ship_order_with_retry(
    repo: AggregateRepository,
    order_id: UUID,
    tracking_number: str,
    max_retries: int = 3,
) -> None:
    """Ship an order with automatic retry on conflict."""
    for attempt in range(max_retries):
        try:
            # Load the current state
            order = await repo.load(order_id)

            # Execute business logic
            order.ship(tracking_number)

            # Persist changes
            await repo.save(order)
            return  # Success

        except OptimisticLockError as e:
            if attempt == max_retries - 1:
                # Exhausted retries - log and re-raise
                logger.error(
                    "Failed to ship order after %d retries",
                    max_retries,
                    extra={
                        "order_id": str(e.aggregate_id),
                        "expected_version": e.expected_version,
                        "actual_version": e.actual_version,
                    },
                )
                raise

            # Log the conflict and retry
            logger.warning(
                "Version conflict on order %s (expected v%d, actual v%d), retrying...",
                e.aggregate_id,
                e.expected_version,
                e.actual_version,
            )
            # Loop continues with fresh load
```

### Generic Retry Helper

For reusable retry logic across commands:

```python
from typing import Callable, TypeVar
from eventsource.aggregates import Aggregate, AggregateRepository
from eventsource.exceptions import OptimisticLockError

TAggregate = TypeVar("TAggregate", bound=Aggregate)

async def execute_with_retry(
    repo: AggregateRepository[TAggregate],
    aggregate_id: UUID,
    command: Callable[[TAggregate], None],
    max_retries: int = 3,
) -> TAggregate:
    """
    Execute a command with automatic retry on version conflict.

    Args:
        repo: The aggregate repository
        aggregate_id: ID of the aggregate to operate on
        command: A callable that modifies the aggregate
        max_retries: Maximum retry attempts

    Returns:
        The aggregate after successful save

    Raises:
        OptimisticLockError: If max retries exceeded
    """
    for attempt in range(max_retries):
        try:
            aggregate = await repo.load(aggregate_id)
            command(aggregate)  # Execute the business logic
            await repo.save(aggregate)
            return aggregate
        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise

    raise RuntimeError("Unreachable")  # For type checker


# Usage
await execute_with_retry(
    repo=order_repo,
    aggregate_id=order_id,
    command=lambda order: order.ship("TRACK-123"),
    max_retries=3,
)
```

### Best Practices for Concurrency

1. **Always implement retry logic** for aggregate save operations
2. **Keep aggregates small** to reduce conflict probability
3. **Log conflicts** for monitoring contention patterns
4. **Consider command deduplication** if retries might cause duplicate side effects
5. **Use exponential backoff** for high-contention scenarios

See [ADR-0003: Optimistic Locking](../adrs/0003-optimistic-locking.md) for the design rationale.

---

## AggregateNotFoundError

Raised when attempting to load an aggregate that does not exist.

### When It Happens

- `repo.load(aggregate_id)` with a non-existent ID
- User provides an invalid or stale reference

### Exception Details

```python
class AggregateNotFoundError(EventSourceError):
    aggregate_id: UUID            # The requested ID
    aggregate_type: str | None    # Type of aggregate (if known)
```

### Handling Pattern: User-Friendly Response

```python
from eventsource.exceptions import AggregateNotFoundError

async def get_order(order_id: UUID) -> Order:
    """Get an order, raising HTTP 404 if not found."""
    try:
        return await order_repo.load(order_id)
    except AggregateNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Order {order_id} not found",
        )
```

### Alternative: Load or Create

For upsert patterns where you want to create if not exists:

```python
from eventsource.exceptions import AggregateNotFoundError

async def ensure_cart_exists(cart_id: UUID, customer_id: UUID) -> ShoppingCart:
    """Get existing cart or create a new one."""
    cart = await cart_repo.load_or_create(cart_id)

    if cart.version == 0:
        # New cart - initialize it
        cart.create(customer_id=customer_id)
        await cart_repo.save(cart)

    return cart
```

### Best Practices

1. **Don't catch broadly**: Only catch when you have a specific recovery strategy
2. **Validate early**: Check existence before complex operations if appropriate
3. **Use load_or_create**: For initialization patterns instead of try/except

---

## EventNotFoundError

Raised when a specific event cannot be found by its ID.

### When It Happens

- Querying for an event by ID that does not exist
- Referencing an event ID that was never persisted

### Exception Details

```python
class EventNotFoundError(EventSourceError):
    event_id: UUID  # The requested event ID
```

### Handling Pattern

```python
from eventsource.exceptions import EventNotFoundError

async def get_event_details(event_id: UUID) -> EventDetails:
    """Get details of a specific event."""
    try:
        event = await event_store.get_event(event_id)
        return EventDetails.from_event(event)
    except EventNotFoundError:
        raise HTTPException(
            status_code=404,
            detail=f"Event {event_id} not found",
        )
```

---

## ProjectionError

Raised when a projection fails to process an event after exhausting retry attempts.

### When It Happens

- Bug in projection code (e.g., unhandled event type)
- Database constraint violation in read model
- Missing required external data
- After exhausting all retry attempts

### Exception Details

```python
class ProjectionError(EventSourceError):
    projection_name: str  # Name of the failed projection
    event_id: UUID        # ID of the event that caused failure
```

### Automatic Handling via DLQ

Projections extending `CheckpointTrackingProjection` automatically handle errors with retry and DLQ:

```python
from eventsource.projections import DeclarativeProjection, handles

class OrderProjection(DeclarativeProjection):
    """Projection that automatically handles errors."""

    # Configure retry behavior
    MAX_RETRIES = 3              # Retry 3 times before DLQ
    RETRY_BACKOFF_BASE = 2       # Exponential backoff: 1s, 2s, 4s

    @handles(OrderCreated)
    async def _on_order_created(self, event: OrderCreated) -> None:
        # If this fails, automatic retry with backoff
        # After MAX_RETRIES failures, event goes to DLQ
        await self._db.execute(
            "INSERT INTO orders ...",
            ...
        )
```

### Custom Error Handling

For additional error handling beyond automatic DLQ:

```python
from eventsource.projections import DeclarativeProjection
from eventsource.events import DomainEvent

class RobustProjection(DeclarativeProjection):
    """Projection with custom error handling."""

    async def handle(self, event: DomainEvent) -> None:
        """Override handle to add custom behavior."""
        try:
            await super().handle(event)
        except Exception as e:
            # Custom handling (in addition to DLQ)
            await self._send_alert(
                f"Projection failed on {event.event_id}: {e}"
            )
            # Re-raise to let parent handle DLQ
            raise
```

### Monitoring the DLQ

Query the DLQ to monitor and resolve failures:

```python
from eventsource.repositories import PostgreSQLDLQRepository

async def check_dlq_health(dlq_repo: PostgreSQLDLQRepository) -> None:
    """Check DLQ for unresolved failures."""
    stats = await dlq_repo.get_failure_stats()

    if stats["failed_count"] > 0:
        logger.warning(
            "DLQ has %d unresolved failures",
            stats["failed_count"],
            extra=stats,
        )

        # Get recent failures for investigation
        failures = await dlq_repo.get_failed_events(
            status="failed",
            limit=10,
        )
        for failure in failures:
            logger.error(
                "Failed event: %s in %s: %s",
                failure["event_id"],
                failure["projection_name"],
                failure["error_message"],
            )
```

### Resolving DLQ Entries

After fixing the underlying issue:

```python
async def resolve_dlq_entry(
    dlq_repo: PostgreSQLDLQRepository,
    dlq_id: int,
    resolved_by: UUID,
) -> None:
    """Mark a DLQ entry as resolved after manual intervention."""
    await dlq_repo.mark_resolved(dlq_id, resolved_by)
    logger.info("Resolved DLQ entry %d by %s", dlq_id, resolved_by)
```

See [ADR-0004: Projection Error Handling](../adrs/0004-projection-error-handling.md) for the design rationale.

---

## SerializationError

Raised when event serialization or deserialization fails.

### When It Happens

- Unknown event type during deserialization
- Event data does not match expected schema
- Missing required fields in event payload

### Exception Details

```python
class SerializationError(EventSourceError):
    event_type: str  # The problematic event type
```

### Handling Pattern

```python
from eventsource.exceptions import SerializationError

async def replay_aggregate_events(aggregate_id: UUID) -> None:
    """Safely replay events, handling serialization errors."""
    try:
        events = await event_store.get_events(aggregate_id)
        for event in events:
            process_event(event)
    except SerializationError as e:
        logger.error(
            "Failed to deserialize event type %s for aggregate %s",
            e.event_type,
            aggregate_id,
            exc_info=True,
        )
        # Consider: skip, use fallback, or fail entirely
        raise
```

### Prevention

1. **Register all event types** before deserialization:

```python
from eventsource import register_event

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    # ...

# Ensure all events are imported at startup
from myapp.events import orders, payments, shipping
```

2. **Use event versioning** for schema evolution:

```python
@register_event
class OrderCreatedV2(DomainEvent):
    event_type: str = "OrderCreated"
    version: int = 2  # Schema version
    # New fields with defaults for backward compatibility
    currency: str = "USD"
```

3. **Validate events on write** to catch issues early:

```python
# Pydantic validation happens automatically
order_created = OrderCreated(
    aggregate_id=order_id,
    customer_id=customer_id,
    # Missing required fields raise ValidationError
)
```

---

## EventStoreError

Raised for general errors in event store operations.

### When It Happens

- Database connection failure
- Query execution error
- Transaction failure
- Storage layer issues

### Handling Pattern

```python
from eventsource.exceptions import EventStoreError

async def robust_append(events: list[DomainEvent]) -> None:
    """Append events with error handling."""
    try:
        await event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=events,
            expected_version=current_version,
        )
    except EventStoreError as e:
        logger.error("Event store error: %s", e, exc_info=True)
        # May be transient - consider retry with backoff
        raise ServiceUnavailableError("Unable to save order, please retry")
```

### Recovery Strategy

For transient store errors, implement exponential backoff:

```python
import asyncio
from eventsource.exceptions import EventStoreError

async def append_with_backoff(
    events: list[DomainEvent],
    max_retries: int = 3,
) -> None:
    """Append events with exponential backoff for transient errors."""
    for attempt in range(max_retries):
        try:
            await event_store.append_events(...)
            return
        except EventStoreError as e:
            if attempt == max_retries - 1:
                raise
            backoff = 2 ** attempt  # 1s, 2s, 4s
            logger.warning(
                "Event store error (attempt %d/%d), retrying in %ds: %s",
                attempt + 1, max_retries, backoff, e,
            )
            await asyncio.sleep(backoff)
```

---

## EventBusError

Raised for errors in event bus operations (publishing/subscribing).

### When It Happens

- Message queue unavailable
- Subscriber registration failure
- Event delivery failure

### Handling Pattern

```python
from eventsource.exceptions import EventBusError

async def publish_event_safely(event: DomainEvent) -> None:
    """Publish event with fallback handling."""
    try:
        await event_bus.publish(event)
    except EventBusError as e:
        logger.error("Event bus error: %s", e, exc_info=True)
        # Fallback: queue for later retry
        await failed_event_queue.add(event)
```

---

## CheckpointError

Raised for errors in checkpoint operations.

### When It Happens

- Checkpoint storage unavailable
- Checkpoint read/write failure
- Checkpoint data corruption

### Handling Pattern

```python
from eventsource.exceptions import CheckpointError

async def get_projection_position(projection_name: str) -> UUID | None:
    """Get projection checkpoint with error handling."""
    try:
        return await checkpoint_repo.get_checkpoint(projection_name)
    except CheckpointError as e:
        logger.error(
            "Checkpoint error for %s: %s",
            projection_name, e,
            exc_info=True,
        )
        # Consider: return None to replay from beginning, or raise
        raise
```

---

## Pydantic Validation Errors

Events use Pydantic for validation. Invalid event data raises `pydantic.ValidationError`.

### When It Happens

- Missing required fields when creating events
- Invalid field types
- Failed field validators

### Handling Pattern

```python
from pydantic import ValidationError

def create_order_event(data: dict) -> OrderCreated:
    """Create event with validation error handling."""
    try:
        return OrderCreated(
            aggregate_id=data["order_id"],
            customer_id=data["customer_id"],
            total_amount=data["total"],
        )
    except ValidationError as e:
        logger.warning("Invalid order data: %s", e.errors())
        raise HTTPException(
            status_code=422,
            detail=e.errors(),
        )
```

### Prevention

1. **Define clear schemas** with appropriate types and constraints
2. **Use Optional fields** with defaults for backward compatibility
3. **Add custom validators** for complex business rules:

```python
from pydantic import field_validator

@register_event
class OrderCreated(DomainEvent):
    event_type: str = "OrderCreated"
    aggregate_type: str = "Order"
    total_amount: float

    @field_validator("total_amount")
    @classmethod
    def amount_must_be_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("Total amount must be positive")
        return v
```

---

## Best Practices

### 1. Catch Specific Exceptions

Handle specific exceptions for appropriate recovery:

```python
# Good - specific handling
try:
    await repo.save(aggregate)
except OptimisticLockError:
    # Reload and retry
    pass
except AggregateNotFoundError:
    # Create new aggregate
    pass

# Avoid - too broad
try:
    await repo.save(aggregate)
except EventSourceError:  # Catches everything, loses context
    pass
```

### 2. Log with Context

Include relevant context for debugging:

```python
except OptimisticLockError as e:
    logger.error(
        "Version conflict saving order",
        extra={
            "aggregate_id": str(e.aggregate_id),
            "expected_version": e.expected_version,
            "actual_version": e.actual_version,
            "user_id": str(current_user.id),
            "command": "ship_order",
        },
    )
```

### 3. Don't Swallow Errors

Always log or handle errors meaningfully:

```python
# Bad - silently ignores errors
except ProjectionError:
    pass

# Good - log and decide on action
except ProjectionError as e:
    logger.warning(
        "Projection %s failed on %s: %s",
        e.projection_name,
        e.event_id,
        str(e),
    )
    # Event is in DLQ; continue processing
```

### 4. Use Error Boundaries

Isolate errors to prevent cascade failures:

```python
async def process_event_batch(events: list[DomainEvent]) -> None:
    """Process events with error isolation."""
    for event in events:
        try:
            await process_event(event)
        except Exception as e:
            logger.error("Error processing %s: %s", event.event_id, e)
            # Continue with next event; don't fail entire batch
            continue
```

### 5. Implement Circuit Breakers for External Services

For projections that depend on external services:

```python
from datetime import datetime, timedelta

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, reset_timeout: int = 60):
        self.failures = 0
        self.threshold = failure_threshold
        self.reset_timeout = reset_timeout
        self.last_failure: datetime | None = None
        self.is_open = False

    def record_failure(self) -> None:
        self.failures += 1
        self.last_failure = datetime.now()
        if self.failures >= self.threshold:
            self.is_open = True

    def record_success(self) -> None:
        self.failures = 0
        self.is_open = False

    def should_allow_request(self) -> bool:
        if not self.is_open:
            return True
        # Check if reset timeout has passed
        if self.last_failure and datetime.now() - self.last_failure > timedelta(seconds=self.reset_timeout):
            self.is_open = False  # Half-open state
            return True
        return False
```

---

## Error Recovery Strategies

### Retry with Exponential Backoff

For transient failures:

```python
import asyncio
import random

async def retry_with_backoff(
    operation,
    max_retries: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    jitter: bool = True,
):
    """Retry an operation with exponential backoff."""
    for attempt in range(max_retries):
        try:
            return await operation()
        except Exception as e:
            if attempt == max_retries - 1:
                raise

            delay = min(base_delay * (2 ** attempt), max_delay)
            if jitter:
                delay *= (0.5 + random.random())  # 50-150% of delay

            logger.warning(
                "Attempt %d failed, retrying in %.1fs: %s",
                attempt + 1, delay, e,
            )
            await asyncio.sleep(delay)
```

### Graceful Degradation

When a component fails, provide reduced functionality:

```python
async def get_order_with_stats(order_id: UUID) -> OrderResponse:
    """Get order with graceful degradation if stats unavailable."""
    order = await order_repo.load(order_id)

    try:
        stats = await stats_service.get_order_stats(order_id)
    except ServiceUnavailableError:
        # Degrade gracefully - return order without stats
        stats = None
        logger.warning("Stats unavailable for order %s", order_id)

    return OrderResponse(order=order, stats=stats)
```

### Dead Letter Queue Processing

For manual intervention on failed events:

```python
async def process_dlq(
    dlq_repo: PostgreSQLDLQRepository,
    projection: DeclarativeProjection,
    operator_id: UUID,
) -> int:
    """Process DLQ entries after fixing underlying issues."""
    resolved_count = 0

    failed_events = await dlq_repo.get_failed_events(status="failed")

    for entry in failed_events:
        try:
            # Mark as retrying
            await dlq_repo.mark_retrying(entry["id"])

            # Reconstruct and retry
            event = reconstruct_event(entry["event_data"])
            await projection.handle(event)

            # Mark as resolved
            await dlq_repo.mark_resolved(entry["id"], operator_id)
            resolved_count += 1

        except Exception as e:
            logger.error(
                "DLQ replay failed for %s: %s",
                entry["event_id"], e,
            )
            # Entry remains in failed state

    return resolved_count
```

---

## Logging and Observability

### Structured Logging

Use structured logging for machine-parseable error information:

```python
import structlog

logger = structlog.get_logger()

except OptimisticLockError as e:
    logger.error(
        "version_conflict",
        aggregate_id=str(e.aggregate_id),
        expected_version=e.expected_version,
        actual_version=e.actual_version,
        error_type="OptimisticLockError",
    )
```

### Metrics to Track

Consider tracking these metrics for error observability:

| Metric | Description |
|--------|-------------|
| `eventsource_lock_errors_total` | Count of OptimisticLockError by aggregate type |
| `eventsource_not_found_errors_total` | Count of AggregateNotFoundError |
| `eventsource_projection_errors_total` | Count of projection failures by projection name |
| `eventsource_dlq_size` | Current size of dead letter queue |
| `eventsource_retry_attempts` | Histogram of retry attempts before success |

### Alerting Recommendations

| Error Type | Alert Threshold | Urgency |
|------------|-----------------|---------|
| OptimisticLockError | High rate (>10/min) | Low - investigate contention |
| ProjectionError | Any in DLQ | Medium - investigate within 4h |
| SerializationError | Any occurrence | High - likely deployment issue |
| EventStoreError | Connection failures | High - database health |

---

## See Also

- [ADR-0003: Optimistic Locking](../adrs/0003-optimistic-locking.md) - Concurrency control design
- [ADR-0004: Projection Error Handling](../adrs/0004-projection-error-handling.md) - DLQ and retry strategy
- [Projections Guide](../examples/projections.md) - Building read models with error handling
- [Events API](../api/events.md) - Event handling and exceptions
