# Tutorial 1: Introduction to Event Sourcing

**Difficulty:** Beginner

## Prerequisites

- Basic Python programming knowledge
- Familiarity with database concepts (tables, queries, updates)
- No event sourcing experience required

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain what event sourcing is and how it differs from traditional CRUD
2. Identify real-world systems that use event sourcing principles
3. List the key benefits and trade-offs of event sourcing
4. Define core terminology: event, aggregate, projection, event store
5. Decide when event sourcing is appropriate for a project

---

## What is Event Sourcing?

Event sourcing stores *what happened* rather than *what is*. Instead of overwriting current state, you store a sequence of immutable events describing every change.

### Traditional CRUD

Most applications use CRUD (Create, Read, Update, Delete). When you update a record, the previous value is lost:

| id  | name       | email              | status   |
|-----|------------|--------------------|----------|
| 1   | Jane Smith | jane@example.com   | active   |

After an UPDATE, the old email is gone forever. You lose all history unless you build separate audit infrastructure.

### Event Sourcing Approach

Store every change as an immutable event:

| event_id | user_id | event_type     | data                                  | timestamp           |
|----------|---------|----------------|---------------------------------------|---------------------|
| 1        | 1       | UserRegistered | {"name": "Jane Smith", "email": "jane@example.com"} | 2024-01-15 09:00:00 |
| 2        | 1       | EmailChanged   | {"old": "jane@example.com", "new": "jane.s@example.com"} | 2024-03-20 14:30:00 |

Replay events in order to derive current state. You get the same result, plus complete history.

---

## Real-World Examples

**Bank ledgers**: Store every transaction (deposits, withdrawals), not just current balance. Balance = sum of all transactions.

**Git**: Stores every commit, not just current code. Current state = replay all commits.

**Accounting**: 500+ years of immutable journal entries. Corrections are new entries, never edits.

**Medical records**: Every visit, test, prescription recorded separately. Critical for legal/medical audit trails.

---

## Benefits

- **Complete audit trail**: Every change recorded with timestamp and context. Built-in compliance.
- **Time travel**: Reconstruct state at any point in history. "What was inventory on Dec 31st?"
- **Debugging**: Replay exact event sequence that caused an issue.
- **Analytics**: Rich event data for behavioral analysis and ML pipelines.
- **Event-driven architecture**: Natural fit for microservices, CQRS, reactive systems.
- **Conflict resolution**: Examine user intent, not just "last write wins".

---

## Trade-offs

- **Increased complexity**: Learning curve for aggregates, projections, eventual consistency.
- **Storage growth**: Events accumulate forever. Requires more storage than current-state-only systems.
- **Eventual consistency**: Read models may lag slightly behind writes.
- **Schema evolution**: Immutable events require versioning strategies for changes.
- **Not always needed**: Simple CRUD apps may not benefit from added complexity.

---

## When to Use Event Sourcing

**Good candidates:**
- Financial systems, healthcare, e-commerce
- Compliance-driven industries
- Collaborative applications
- Complex business domains with many state transitions
- Analytics-heavy applications
- Systems requiring undo/redo

**Less suitable:**
- Simple CRUD apps
- Static content
- Systems where history doesn't matter
- Small teams without event sourcing experience

---

## Core Concepts

**Event**: Immutable record of something that happened. Named in past tense (OrderPlaced, PaymentReceived). Source of truth for your system.

**Aggregate**: Consistency boundary that enforces business rules and emits events. Reconstructs state by replaying events.

**Event Store**: Database of record. Stores complete history in append-only fashion. Supports optimistic concurrency control.

**Projection**: Read model derived from events, optimized for queries. Can be rebuilt anytime by replaying events.

**Event Bus**: Distributes events to subscribers (projections, integrations, notifications).

**Command**: Intent to do something ("Create order"). Validated by aggregates, results in events if valid.

**Repository**: Interface for loading and saving aggregates. Handles event storage and state reconstruction.

---

## How Event Sourcing Differs from CRUD

| Aspect | Traditional CRUD | Event Sourcing |
|--------|-----------------|----------------|
| **Storage** | Current state only | Complete history of changes |
| **Updates** | Modify in place | Append new events |
| **Deletes** | Remove data | Append deletion event |
| **History** | Lost (unless separately logged) | Built-in and complete |
| **Audit** | Requires extra infrastructure | Natural by-product |
| **Read models** | Query the same data | Build optimized projections |
| **Complexity** | Lower for simple cases | Higher, with benefits for complex domains |
| **Recovery** | Restore from backup | Replay events to any point |

---

## The eventsource-py Library

| Component | Purpose |
|-----------|---------|
| `DomainEvent` | Base class for events |
| `AggregateRoot` | Base class for aggregates |
| `EventStore` | Persist/retrieve events (PostgreSQL, SQLite, In-Memory) |
| `AggregateRepository` | Load and save aggregates |
| `Projection` | Build read models from event streams |
| `EventBus` | Distribute events (In-Memory, Redis, Kafka) |

---

## Next Steps

Continue to [Tutorial 2: Your First Domain Event](02-first-event.md).
