---
id: '0014'
title: Stage and Publish Events via Transactional Outbox
status: Accepted
persona: Jordan (The Streaming & Distributed Systems Platform Engineer)
target_bc: outbox
governing_prd: PRD-0003
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0105
- ADR-0107
scenarios:
- Atomically staging events via PostgreSQLEventStore outbox_enabled integration
- Staging and polling pending events via OutboxRepository port
- Mark published records atomically preventing duplicate processing
- Increment retry count and mark failed on delivery exhaustion
- Retention pruning of published outbox records via cleanup_published
- Outbox metrics and health reporting via get_stats
---

# US-0014: Stage and Publish Events via Transactional Outbox

## User Story

As Jordan, the Streaming & Distributed Systems Platform Engineer,
I want to persist uncommitted domain events to a local transactional outbox table through `OutboxRepository` or `PostgreSQLEventStore(outbox_enabled=True)`,
So that event publishing to message brokers never loses events if broker connections fail during write commits (dual-write prevention).

## Acceptance Criteria

### Scenario 1: Atomically staging events via PostgreSQLEventStore outbox_enabled integration
```gherkin
Given a PostgreSQL event store configured with "outbox_enabled=True"
When an aggregate transaction appends domain events to the stream
Then corresponding outbox records are written atomically in the same database transaction
And no messages are dispatched over the external network until database commit succeeds.
```

### Scenario 2: Staging and polling pending events via OutboxRepository port
```gherkin
Given unpublished events recorded in an OutboxRepository adapter
When a consumer requests pending events with a bounded batch limit
Then messages are returned in ascending chronological position order
And pending messages remain visible for dispatch until explicitly marked published.
```

### Scenario 3: Mark published records atomically preventing duplicate processing
```gherkin
Given outbox records successfully acknowledged by the message broker
When the caller invokes "mark_published(message_ids)"
Then the records are flagged as published with completion timestamps
And subsequent calls to "get_pending_events()" exclude the published records.
```

### Scenario 4: Increment retry count and mark failed on delivery exhaustion
```gherkin
Given an outbox message encountering broker dispatch failures
When the caller invokes "increment_retry(message_id)"
Then the retry count is incremented and status remains pending until retry limit is exceeded
When "mark_failed(message_id, error)" is invoked after exhaustion
Then the record status transitions to FAILED with error diagnostic metadata preserved.
```

### Scenario 5: Retention pruning of published outbox records via cleanup_published
```gherkin
Given published outbox records older than the configured retention threshold of 7 days
When "cleanup_published(days=7)" is executed
Then expired published rows are purged from storage
And pending or failed records are strictly preserved.
```

### Scenario 6: Outbox metrics and health reporting via get_stats
```gherkin
Given an OutboxRepository containing pending, published, and failed messages
When "get_stats()" is queried
Then an OutboxStats object returns accurate counts of pending, published, and failed messages
And reports the timestamp and age of the oldest pending message for lag alerting.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified
- **Implementation Modules**:
  - `src/eventsource/ports/outbox.py`: `OutboxRepository`, `OutboxMessage`, `OutboxStatus` interfaces.
  - `src/eventsource/adapters/postgresql/outbox.py`: PostgreSQL SQL transactional outbox.
  - `src/eventsource/adapters/sqlite/outbox.py`: SQLite transactional outbox.
  - `src/eventsource/adapters/memory/outbox.py`: In-memory thread-safe atomic outbox.
- **Verification Proof**:
  - `tests/integration/repositories/test_outbox.py`: Outbox repository transactional persistence and polling.
  - `tests/unit/adapters/test_memory_outbox.py`: In-memory outbox state machine, duplicate prevention, and status transitions.
  - `tests/unit/adapters/test_sqlite_outbox.py`: SQLite transactional commit and rollback boundary checks.
  - Test suites passed 100% across memory and relational adapters.
