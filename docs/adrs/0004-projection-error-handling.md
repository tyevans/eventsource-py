# ADR-0004: Projection Error Handling

**Status:** Accepted

**Date:** 2025-12-06

**Deciders:** Tyler Evans

---

## Context

Projections are a fundamental component of event sourcing systems, responsible for building read models from domain events. Unlike the event store (which is append-only and immutable), projections transform events into queryable data structures optimized for specific use cases.

Projection processing can fail for various reasons:

### Failure Categories

1. **Transient failures** (recoverable):
   - Database connection timeouts
   - Network interruptions
   - Temporary service unavailability
   - Resource exhaustion (connection pool depleted)

2. **Permanent failures** (not recoverable by retry):
   - Bug in projection code
   - Invalid event data (schema mismatch)
   - Missing required external data
   - Constraint violations in read model

3. **Infrastructure failures**:
   - Database server crash
   - Disk space exhaustion
   - Out-of-memory conditions

### Critical Requirements

The error handling strategy must satisfy these requirements:

1. **No event loss**: Failed events must never be silently dropped. Every event must either be successfully processed or preserved for manual intervention.

2. **Event ordering preservation**: Events for a given aggregate must be processed in order. Skipping events would create gaps in the read model and violate consistency.

3. **System resilience**: Transient failures should not cause permanent data loss or require manual intervention.

4. **Observability**: Operations teams need visibility into failures for monitoring, alerting, and debugging.

5. **Recovery paths**: Clear mechanisms must exist for recovering from failures, including projection rebuilds.

6. **Isolation**: Failures in one projection should not block other projections from processing events.

### Forces at Play

- **Availability vs. Consistency**: Skipping failed events maintains availability but creates data inconsistencies
- **Performance vs. Safety**: Aggressive retries handle transient issues but can amplify load during outages
- **Simplicity vs. Flexibility**: A fixed retry policy is simple but may not suit all failure types
- **Automatic vs. Manual recovery**: Automatic recovery reduces operational burden but may not handle all scenarios

## Decision

We implement a **retry-with-dead-letter-queue (DLQ) strategy** for projection error handling. This approach combines automatic recovery for transient failures with guaranteed preservation of events that cannot be processed.

### Core Strategy

The error handling flow is:

```
Event arrives
    |
    v
Process Event (attempt 1)
    |
    +--[success]--> Update checkpoint --> Done
    |
    +--[failure]--> Log error
                      |
                      v
                    Backoff (2^0 = 1 second)
                      |
                      v
                    Process Event (attempt 2)
                        |
                        +--[success]--> Update checkpoint --> Done
                        |
                        +--[failure]--> Backoff (2^1 = 2 seconds)
                                          |
                                          v
                                        Process Event (attempt 3)
                                            |
                                            +--[success]--> Update checkpoint --> Done
                                            |
                                            +--[failure]--> Send to DLQ --> Re-raise exception
```

### Retry Configuration

The `CheckpointTrackingProjection` base class supports configurable retry behavior via the `retry_policy` parameter (recommended) or class attributes (deprecated):

```python
# Using RetryPolicy (recommended)
from eventsource.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.subscriptions.retry import RetryConfig

policy = ExponentialBackoffRetryPolicy(
    config=RetryConfig(
        max_retries=3,        # Number of retry attempts
        initial_delay=2.0,    # First retry delay in seconds
        exponential_base=2.0, # Backoff multiplier
    )
)
projection = MyProjection(retry_policy=policy)

# Using class attributes (deprecated, backward compatible)
class MyProjection(CheckpointTrackingProjection):
    MAX_RETRIES: int = 3           # Total attempts
    RETRY_BACKOFF_BASE: int = 2    # Initial delay seconds
```

**Default timing (ExponentialBackoffRetryPolicy):**
- Attempt 1: Immediate
- Attempt 2: After 2 seconds
- Attempt 3: After 4 seconds
- Attempt 4: After 8 seconds (if max_retries=3)
- Total retry window: ~14 seconds before DLQ

### Retry Implementation

The retry logic is implemented in `CheckpointTrackingProjection._handle_with_retry()`:

```python
async def _handle_with_retry(self, event: DomainEvent, span) -> None:
    max_attempts = self._retry_policy.max_retries + 1  # Include initial attempt

    for attempt in range(max_attempts):
        try:
            await self._process_event(event)
            await self._checkpoint_manager.update(event)
            return  # Success
        except Exception as e:
            logger.error(
                "Projection %s failed to process event %s (attempt %d/%d): %s",
                self._projection_name, event.event_id,
                attempt + 1, max_attempts, e,
                exc_info=True,
            )

            if not self._retry_policy.should_retry(attempt, e):
                await self._dlq_manager.send_to_dlq(event, e, attempt + 1)
                raise  # Re-raise to signal failure
            else:
                backoff = self._retry_policy.get_backoff(attempt)
                await asyncio.sleep(backoff)
```

Key aspects:
- Uses configurable `RetryPolicy` for backoff calculation
- Uses `asyncio.sleep()` for non-blocking backoff (see [ADR-0001](0001-async-first-design.md))
- Delegates checkpoint updates to `ProjectionCheckpointManager`
- Delegates DLQ operations to `ProjectionDLQManager`
- Re-raises exception after DLQ write to signal failure to upstream coordinators

### Dead Letter Queue

Events that fail all retry attempts are sent to the dead letter queue via `ProjectionDLQManager`:

```python
# ProjectionDLQManager.send_to_dlq()
await self._dlq_repo.add_failed_event(
    event_id=event.event_id,
    projection_name=self._projection_name,
    event_type=event.event_type,
    event_data=event.model_dump(mode="json"),
    error=error,
    retry_count=retry_count,
)
```

#### DLQ Entry Contents

Each DLQ entry preserves:

| Field | Description |
|-------|-------------|
| `event_id` | Original event identifier |
| `projection_name` | Which projection failed |
| `event_type` | Type of the failed event |
| `event_data` | Complete serialized event payload |
| `error_message` | Exception message |
| `error_stacktrace` | Full Python traceback |
| `retry_count` | Number of attempts made |
| `first_failed_at` | Initial failure timestamp |
| `last_failed_at` | Most recent failure timestamp |
| `status` | Current status: `failed`, `retrying`, `resolved` |

#### DLQ Repository

The `DLQRepository` protocol (see `src/eventsource/repositories/dlq.py`) provides:

```python
class DLQRepository(Protocol):
    async def add_failed_event(...) -> None:
        """Add or update a failed event (UPSERT pattern)."""

    async def get_failed_events(
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Query failed events with filtering."""

    async def mark_resolved(dlq_id: int | str, resolved_by: str | UUID) -> None:
        """Mark an entry as resolved after manual intervention."""

    async def mark_retrying(dlq_id: int | str) -> None:
        """Mark an entry as being retried."""

    async def get_failure_stats() -> dict[str, Any]:
        """Get aggregate DLQ statistics."""
```

Implementations:
- `PostgreSQLDLQRepository`: Production implementation with `dead_letter_queue` table
- `InMemoryDLQRepository`: Testing implementation

### Checkpoint Tracking

Checkpoints enable resumable processing and exactly-once semantics:

```python
# src/eventsource/repositories/checkpoint.py
class CheckpointRepository(Protocol):
    async def get_checkpoint(projection_name: str) -> UUID | None:
        """Get last processed event ID."""

    async def update_checkpoint(
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """Update checkpoint after successful processing (UPSERT)."""

    async def reset_checkpoint(projection_name: str) -> None:
        """Reset checkpoint for projection rebuild."""

    async def get_lag_metrics(
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """Get projection lag metrics."""
```

**Checkpoint data includes:**
- `last_event_id`: Last successfully processed event
- `last_event_type`: Type of last processed event
- `last_processed_at`: Timestamp of last processing
- `events_processed`: Running count of processed events

### Projection Rebuild

The `reset()` method enables clean projection rebuilds:

```python
async def reset(self) -> None:
    """Reset the projection by clearing checkpoint and read model data."""
    logger.warning("Resetting projection %s", self._projection_name)

    # Reset checkpoint via manager
    await self._checkpoint_manager.reset()

    # Clear read model tables (subclass implementation)
    await self._truncate_read_models()
```

Subclasses implement `_truncate_read_models()` to clear their specific tables.

### DLQ Failure Handling

If writing to the DLQ itself fails, the error is logged but does not crash the system:

```python
# In ProjectionDLQManager.send_to_dlq()
except Exception as dlq_error:
    logger.critical(
        "Failed to write event %s to DLQ: %s",
        event.event_id,
        dlq_error,
        exc_info=True,
    )
    # Original exception is still re-raised
```

This ensures the system continues operating even if the DLQ is temporarily unavailable, though events may be lost in this edge case.

## Consequences

### Positive

- **Transient failures handled automatically**: Network hiccups and temporary database issues resolve without human intervention in most cases.

- **No event loss for permanent failures**: Events that cannot be processed are preserved in the DLQ with full context for debugging and manual replay.

- **Checkpoints enable exactly-once semantics**: After restart, projections resume from their last checkpoint without reprocessing events.

- **Full observability**: Every failure is logged with structured context, and DLQ statistics enable monitoring and alerting.

- **Projection rebuilds are straightforward**: Reset the checkpoint and truncate tables, then replay all events.

- **Isolation between projections**: Each projection tracks its own checkpoint, so failures in one do not block others.

- **Configurable retry behavior**: Subclasses can tune `MAX_RETRIES` and `RETRY_BACKOFF_BASE` for their specific reliability requirements.

### Negative

- **DLQ requires monitoring**: Operations teams must monitor DLQ size and age, and establish processes for investigating and resolving failures.

- **Projection may lag during retries**: The 3-second retry window adds latency when transient failures occur.

- **No automatic DLQ reprocessing**: Failed events in the DLQ require manual intervention or custom tooling to retry.

- **Memory usage during retries**: Events are held in memory during the retry loop, which could be problematic for very large events.

- **DLQ failure edge case**: If the DLQ write fails, the event may be lost (logged but not persisted).

### Neutral

- **Retry count and backoff are configurable**: Each projection subclass can override defaults for specific needs.

- **In-memory implementations available for testing**: `InMemoryCheckpointRepository` and `InMemoryDLQRepository` enable unit testing without database dependencies.

- **PostgreSQL-specific implementations**: Production implementations assume PostgreSQL; other databases would need new implementations.

- **Operational processes required**: Teams need runbooks for DLQ investigation, resolution, and projection rebuilds.

## References

### Code References

- `src/eventsource/projections/base.py` - `CheckpointTrackingProjection`, `DeclarativeProjection`, `DatabaseProjection`
- `src/eventsource/projections/retry.py` - `RetryPolicy`, `ExponentialBackoffRetryPolicy`, `NoRetryPolicy`, `FilteredRetryPolicy`
- `src/eventsource/projections/checkpoint_manager.py` - `ProjectionCheckpointManager`
- `src/eventsource/projections/dlq_manager.py` - `ProjectionDLQManager`
- `src/eventsource/repositories/dlq.py` - `DLQRepository` protocol and implementations
- `src/eventsource/repositories/checkpoint.py` - `CheckpointRepository` protocol and implementations

### Related ADRs

- [ADR-0001: Async-First Design](0001-async-first-design.md) - Explains why `asyncio.sleep()` is used for backoff

### External Documentation

- [Enterprise Integration Patterns - Dead Letter Channel](https://www.enterpriseintegrationpatterns.com/DeadLetterChannel.html)
- [Microsoft - Retry Pattern](https://docs.microsoft.com/en-us/azure/architecture/patterns/retry)
- [AWS - Exponential Backoff and Jitter](https://aws.amazon.com/blogs/architecture/exponential-backoff-and-jitter/)

## Notes

### Alternatives Considered

1. **No retry (fail-fast)**
   - **Description**: Immediately send to DLQ on first failure.
   - **Why rejected**: Transient failures are common in distributed systems (network blips, connection pool exhaustion). Fail-fast would generate excessive DLQ entries for recoverable issues, increasing operational burden.

2. **Infinite retry (never give up)**
   - **Description**: Keep retrying until success.
   - **Why rejected**: Permanent failures (bugs, schema mismatches) would block the projection forever, preventing any new events from being processed. This violates the isolation principle.

3. **Skip on failure (best-effort)**
   - **Description**: Log the error and continue to the next event.
   - **Why rejected**: This would create gaps in the read model, violating event ordering guarantees. Users querying the read model would see inconsistent data.

4. **Circuit breaker pattern**
   - **Description**: After repeated failures, "open" the circuit and stop processing entirely.
   - **Why considered**: Could prevent cascade failures during widespread outages.
   - **Current status**: Not implemented, but may be added later if operational experience shows it's needed. The current retry-with-DLQ pattern handles most cases.

5. **Exponential backoff with jitter**
   - **Description**: Add random jitter to backoff delays to prevent thundering herd.
   - **Why not implemented**: With bounded retries (3 attempts), jitter provides minimal benefit. Could be added if projections frequently retry simultaneously.

### Future Considerations

- **DLQ reprocessing tooling**: A CLI or API for replaying events from the DLQ would reduce operational burden.
- **Automatic DLQ retry**: Time-based automatic retry of DLQ entries (with different backoff) for issues that might self-resolve.
- **Circuit breaker**: For projections that interact with external services, a circuit breaker could prevent cascade failures.
- **Metrics emission**: Integrate with metrics systems (Prometheus, StatsD) for retry/DLQ metrics beyond logging.
- **Per-event-type error handling**: Allow projections to specify different handling strategies for different event types.
