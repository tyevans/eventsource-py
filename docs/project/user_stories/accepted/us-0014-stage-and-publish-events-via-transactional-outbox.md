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
- Atomically staging events in outbox repository
- Bounded polling and sequential publication to message bus
- Mark published records atomically preventing duplicate processing
---

# US-0014: Stage and Publish Events via Transactional Outbox

## User Story

As Jordan, the Streaming & Distributed Systems Platform Engineer,
I want to write uncommitted domain events to a local transactional outbox table within the same database transaction as aggregate state persistence,
So that event publishing to message brokers never loses events if broker network connections fail during write commits (dual-write prevention).

## Acceptance Criteria

### Scenario 1: Atomically staging events in outbox repository
```gherkin
Given a database transaction containing aggregate state changes
When domain events are appended to the outbox repository within the active transaction
Then outbox entries are committed atomically with the primary write
And no messages are dispatched over the external network until transaction commit succeeds.
```

### Scenario 2: Bounded polling and sequential publication to message bus
```gherkin
Given unpublished events recorded in the outbox repository
When the background outbox publisher processes a bounded batch of records
Then messages are published to the target event bus in chronological position order
And published status is updated without blocking application writes.
```

### Scenario 3: Mark published records atomically preventing duplicate processing
```gherkin
Given outbox records successfully acknowledged by the message broker
When the publisher marks the batch as published
Then the records are flagged with timestamps and removed or excluded from subsequent polls
And failing broker deliveries leave records in pending state for exponential backoff retry.
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
