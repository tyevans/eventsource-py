# ADR-0003: Optimistic Locking for Concurrency Control

**Status:** Accepted

**Date:** 2025-12-06

**Deciders:** Tyler Evans

---

## Context

Event sourcing systems must handle concurrent modifications to aggregates safely. When multiple processes or threads attempt to modify the same aggregate simultaneously, the system must ensure data integrity and prevent lost updates.

### The Problem

Consider a scenario where two processes load the same aggregate at version 5:

```
Process A: Load Order (version 5) -> Process Command -> Append Events (expect v5)
Process B: Load Order (version 5) -> Process Command -> Append Events (expect v5)
```

Without concurrency control:
- Both processes believe they are working with the current state
- Both attempt to append events expecting version 5
- One of the appends would silently overwrite or conflict with the other
- Event history could become inconsistent
- Business invariants could be violated

### Requirements

The concurrency control mechanism must:

1. **Detect concurrent modifications** - Identify when two processes modify the same aggregate
2. **Preserve event ordering** - Maintain strict ordering within each aggregate's event stream
3. **Avoid deadlocks** - Not require long-held locks that could cause blocking
4. **Support high throughput** - Not become a bottleneck under normal operation
5. **Work across distributed systems** - Function correctly with multiple application instances

### Forces

- **Consistency vs. Availability:** Strict locking provides consistency but reduces throughput
- **Simplicity vs. Flexibility:** Simple version checks are easy to understand but require retry logic
- **Performance vs. Safety:** Optimistic approaches perform better but require conflict handling

## Decision

We implement **optimistic locking with version-based conflict detection** for all event store append operations.

### Version Tracking

Each aggregate maintains a version number representing the count of events applied. When appending events, callers must specify the expected current version:

```python
result = await store.append_events(
    aggregate_id=order_id,
    aggregate_type="Order",
    events=[OrderShipped(...)],
    expected_version=5,  # Must match current version in store
)
```

The event store atomically:
1. Reads the current version for the aggregate
2. Compares it to the expected version
3. If they match, appends the events and increments the version
4. If they differ, raises `OptimisticLockError`

### ExpectedVersion Constants

The `ExpectedVersion` class provides semantic constants for common scenarios:

```python
class ExpectedVersion:
    ANY: int = -1          # Skip version check (disable optimistic locking)
    NO_STREAM: int = 0     # Expect stream to not exist (new aggregate)
    STREAM_EXISTS: int = -2  # Expect stream to exist (updating aggregate)
```

Usage:
- `ExpectedVersion.ANY`: Use with caution; disables conflict detection
- `ExpectedVersion.NO_STREAM`: Creating a new aggregate; fails if aggregate already exists
- `ExpectedVersion.STREAM_EXISTS`: Updating an aggregate; fails if aggregate does not exist
- Specific version (e.g., `5`): Standard optimistic lock; fails if version mismatch

### OptimisticLockError

When a version conflict occurs, the store raises `OptimisticLockError`:

```python
class OptimisticLockError(EventSourceError):
    def __init__(self, aggregate_id: UUID, expected_version: int, actual_version: int):
        self.aggregate_id = aggregate_id
        self.expected_version = expected_version
        self.actual_version = actual_version
```

The exception includes both expected and actual versions, enabling informed retry decisions.

### Repository Integration

The `AggregateRepository.save()` method automatically calculates the expected version:

```python
async def save(self, aggregate: TAggregate) -> None:
    uncommitted_events = aggregate.uncommitted_events
    if not uncommitted_events:
        return

    # Calculate expected version:
    # Current version minus new events = version before changes
    expected_version = aggregate.version - len(uncommitted_events)

    result = await self._event_store.append_events(
        aggregate_id=aggregate.aggregate_id,
        aggregate_type=self._aggregate_type,
        events=uncommitted_events,
        expected_version=expected_version,
    )
```

### Recommended Retry Pattern

Applications should implement retry logic for handling conflicts:

```python
async def execute_with_retry(
    repo: AggregateRepository,
    aggregate_id: UUID,
    command: Callable[[TAggregate], None],
    max_retries: int = 3,
) -> TAggregate:
    """Execute a command with automatic retry on conflict."""
    for attempt in range(max_retries):
        try:
            # Load fresh aggregate state
            aggregate = await repo.load(aggregate_id)

            # Execute command (may raise domain exceptions)
            command(aggregate)

            # Persist changes
            await repo.save(aggregate)
            return aggregate

        except OptimisticLockError:
            if attempt == max_retries - 1:
                raise  # Max retries exceeded
            # Loop continues with fresh load

    raise RuntimeError("Unreachable")
```

### Implementation Details

**PostgreSQL Implementation:**

The PostgreSQL event store uses a combination of:
1. Pre-check: Query current version before insert
2. Atomic insert with version
3. Unique constraint on `(aggregate_id, aggregate_type, version)` as a safety net

```python
# From src/eventsource/stores/postgresql.py
if current_version != expected_version:
    raise OptimisticLockError(
        aggregate_id, expected_version, current_version
    )
```

If a race condition occurs despite the pre-check (two transactions reading same version), the unique constraint catches it:

```python
except IntegrityError as e:
    if "uq_events_aggregate_version" in str(e).lower():
        raise OptimisticLockError(aggregate_id, expected_version, actual_version)
```

**In-Memory Implementation:**

The in-memory store uses thread locking to ensure atomicity:

```python
# From src/eventsource/stores/in_memory.py
with self._lock:
    current_version = len(current_events)
    if current_version != expected_version:
        raise OptimisticLockError(aggregate_id, expected_version, current_version)
    # ... append events
```

## Consequences

### Positive

- **No deadlocks:** No locks are held during command processing; conflicts are detected at write time
- **High throughput:** Non-conflicting operations proceed without blocking
- **Clear conflict semantics:** `OptimisticLockError` provides actionable information
- **Works in distributed environments:** Each node can operate independently; conflict detection happens at database level
- **Atomic operations:** Events are appended atomically; no partial writes
- **Idempotent appends:** Event ID checking prevents duplicate events on retry
- **Testable:** Both PostgreSQL and in-memory stores implement identical semantics

### Negative

- **Retry logic required:** Applications must handle `OptimisticLockError` and implement retry patterns
- **High contention scenarios:** Under heavy concurrent writes to the same aggregate, many retries may occur
- **Version tracking overhead:** Aggregates must track their version throughout their lifecycle
- **Stale read possible:** An aggregate could be modified between load and save; the conflict is only detected at save time
- **Command idempotency concerns:** Retried commands must be idempotent or the application must handle re-execution carefully

### Neutral

- **Database constraint enforcement:** PostgreSQL uses a unique constraint as a secondary safety mechanism
- **Version is per-aggregate:** Each aggregate stream has its own version counter, not a global counter
- **Event store abstraction:** The concurrency control logic is encapsulated in the store interface, not leaked to aggregates

## References

### Code

- `src/eventsource/exceptions.py` - `OptimisticLockError` definition
- `src/eventsource/stores/interface.py` - `ExpectedVersion` class, `append_events()` interface
- `src/eventsource/stores/postgresql.py` - PostgreSQL implementation with version checking
- `src/eventsource/stores/in_memory.py` - In-memory implementation
- `src/eventsource/aggregates/repository.py` - Repository save with expected_version

### External Resources

- [Optimistic Concurrency Control - Wikipedia](https://en.wikipedia.org/wiki/Optimistic_concurrency_control)
- [Martin Fowler: Optimistic Offline Lock](https://martinfowler.com/eaaCatalog/optimisticOfflineLock.html)

### Related ADRs

- [ADR-0001: Async-First Design](0001-async-first-design.md) - Async operations enable concurrent aggregate processing

## Notes

### Alternatives Considered

1. **Pessimistic Locking (Database-level locks)**

   With pessimistic locking, a lock would be acquired when loading an aggregate and held until save:

   ```python
   async with store.lock(aggregate_id):
       aggregate = await repo.load(aggregate_id)
       aggregate.process_command(...)
       await repo.save(aggregate)
   ```

   **Rejected because:**
   - Deadlock potential when aggregates interact
   - Reduced throughput due to lock contention
   - Complexity in distributed environments (distributed locks)
   - Locks held during potentially long business logic execution

2. **Last-Write-Wins (No locking)**

   Simply overwrite without checking versions:

   ```python
   # No expected_version check
   await store.append_events(aggregate_id, events)
   ```

   **Rejected because:**
   - Silent data loss when concurrent modifications occur
   - Business invariants cannot be enforced
   - Event history may become inconsistent
   - No way to detect or handle conflicts

3. **Compare-and-Swap with Full State**

   Store full aggregate state and use CAS operations:

   ```python
   await store.cas(aggregate_id, old_state_hash, new_state)
   ```

   **Rejected because:**
   - Loses event history (not event sourcing)
   - Cannot replay events to rebuild state
   - No audit trail of changes
   - Larger storage and network overhead

### Future Considerations

- **Conflict resolution strategies:** The current approach requires manual retry. Future versions could support automatic conflict resolution for specific event types.
- **Version caching:** For high-read scenarios, caching aggregate versions could reduce database round-trips during conflict detection.
- **Retry policies:** A standardized retry policy with exponential backoff could be added to the repository.
