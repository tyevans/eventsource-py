# Tutorial 1: Introduction to Event Sourcing

**Estimated Time:** 30-45 minutes (reading)
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

Event sourcing is a way of storing data that focuses on *what happened* rather than *what is*. Instead of storing the current state of your data and overwriting it when things change, you store a sequence of events that describe every change that ever occurred.

Think of it like this: rather than keeping a single photograph of your living room that you update whenever you rearrange the furniture, you keep a detailed journal of every change you have ever made. "Moved the couch to the west wall. Added a new bookshelf. Replaced the old lamp." From that journal, you can always figure out what your living room looks like right now, but you can also answer questions like "What did it look like six months ago?" or "When did I get rid of that old armchair?"

### The Traditional Approach: CRUD

Most applications use what we call the CRUD pattern: Create, Read, Update, Delete. When you update a record, the previous value is gone forever. Consider a user profile:

| id  | name       | email              | status   |
|-----|------------|--------------------|----------|
| 1   | Jane Smith | jane@example.com   | active   |

When Jane changes her email address, you run an UPDATE statement and the old email vanishes:

| id  | name       | email              | status   |
|-----|------------|--------------------|----------|
| 1   | Jane Smith | jane.s@example.com | active   |

This approach is simple and works well for many applications. But what if you need to know what her old email was? What if there is a compliance requirement to track every change? What if you need to understand how the data evolved over time? With traditional CRUD, that information is lost unless you build additional audit logging infrastructure.

### The Event Sourcing Approach

With event sourcing, instead of storing the current state, you store every change as an immutable event:

| event_id | user_id | event_type     | data                                  | timestamp           |
|----------|---------|----------------|---------------------------------------|---------------------|
| 1        | 1       | UserRegistered | {"name": "Jane Smith", "email": "jane@example.com"} | 2024-01-15 09:00:00 |
| 2        | 1       | EmailChanged   | {"old": "jane@example.com", "new": "jane.s@example.com"} | 2024-03-20 14:30:00 |

To know Jane's current state, you replay these events in order. The first event creates her profile, the second updates her email. You always arrive at the same current state, but now you have a complete history of how you got there.

Events are:

- **Immutable** - Once recorded, an event never changes. You cannot edit history.
- **Append-only** - New events are added to the end. You never delete events.
- **Named in past tense** - Events describe things that have already happened: UserRegistered, OrderPlaced, PaymentReceived.

---

## Real-World Analogies

Event sourcing is not a new invention. Many real-world systems have used this pattern for decades or even centuries.

### The Bank Ledger

Your bank does not store a single "balance" field that gets overwritten with every transaction. Instead, it maintains a ledger of every deposit, withdrawal, transfer, and fee. Your current balance is calculated by summing up all those transactions.

This is event sourcing in its purest form. The ledger entries are events:

- **Deposited $500** on January 1st
- **Withdrew $50** on January 3rd
- **Transferred $200 to savings** on January 5th
- **Received interest of $0.50** on January 31st

If there is ever a dispute, the bank can trace back through every transaction to verify the balance. They can tell you exactly when and how every penny moved. They can even generate historical statements showing your balance at any point in time.

### Git Version Control

If you have used Git, you already understand event sourcing. A Git repository does not store "the current code." It stores every commit ever made - each one an event describing what changed.

- `Initial commit` - Added project structure
- `Added user authentication` - 15 files changed
- `Fixed login bug` - 2 files changed
- `Refactored database layer` - 8 files changed

The current state of your codebase is derived by replaying all commits from the beginning. Git gives you powerful capabilities: see the code at any point in history, understand who changed what and when, branch and merge, and even undo changes by adding new commits that reverse previous ones.

### Double-Entry Bookkeeping

Accountants have used event sourcing principles for over 500 years. In double-entry bookkeeping, every financial transaction is recorded as an immutable journal entry. These entries are never erased or modified - if a correction is needed, a new entry is added to reverse or adjust the previous one.

This approach gives accountants what they call an "audit trail" - a complete, verifiable history of every financial change. Auditors can trace any balance back to its source transactions.

### Medical Records

Patient medical records follow the same principle. Doctors do not maintain a single "current health status" document that gets overwritten. Instead, they record every visit, test result, prescription, and procedure as separate, timestamped entries. Even corrections are recorded as new entries that reference the original, rather than modifying historical records.

This append-only approach is critical for medical and legal reasons. You need to know what a doctor believed to be true at the time they made a decision, not what you know now with hindsight.

---

## Why Event Sourcing?

Understanding the benefits helps you decide if event sourcing is right for your project.

### Complete Audit Trail

Every change is recorded with a timestamp, the data that changed, and optionally who made the change and why. This is invaluable for:

- **Compliance** - Meeting regulatory requirements (GDPR, HIPAA, SOX)
- **Debugging** - Understanding exactly how a system reached its current state
- **Customer support** - Investigating issues by reviewing what happened
- **Security** - Detecting and investigating suspicious activity

With CRUD, adding audit logging is an afterthought that requires extra code and infrastructure. With event sourcing, it is built into the core architecture.

### Time Travel (Temporal Queries)

Because you have complete history, you can reconstruct the state of your system at any point in time. This enables:

- **Historical reporting** - "What did our inventory look like on December 31st?"
- **Point-in-time recovery** - Rebuild state before a bug was introduced
- **Debugging** - Replay events to understand how an error occurred
- **Retroactive analysis** - Apply new analytics to historical data

### Debugging and Reproducibility

When something goes wrong, you can replay the exact sequence of events that led to the problem. There is no mystery about how the system reached an unexpected state. This makes bugs far easier to diagnose and reproduce.

In traditional systems, when you find data in an unexpected state, you often have no way to know how it got there. With event sourcing, the answer is always in the event log.

### Rich Analytics and Business Intelligence

Event streams are a treasure trove of data. You can:

- Analyze behavioral patterns over time
- Build sophisticated analytics without impacting the main application
- Create new reports from historical data without migrations
- Feed events into machine learning pipelines

### Natural Fit for Event-Driven Architecture

If your system needs to react to changes - sending notifications, updating search indexes, syncing with external systems - events are the natural abstraction. Event sourcing produces a stream of events that can be consumed by any number of downstream systems.

This aligns well with:

- **Microservices** - Services communicate through events
- **CQRS** (Command Query Responsibility Segregation) - Separate read and write models
- **Reactive systems** - Systems that respond to change

### Conflict Resolution in Collaborative Systems

When multiple users can modify the same data concurrently, events provide a foundation for sophisticated conflict resolution. Rather than "last write wins," you can examine what each user intended and merge appropriately.

---

## Trade-offs to Consider

Event sourcing is not a silver bullet. Understanding the challenges helps you make an informed decision.

### Increased Complexity

Event sourcing introduces concepts that may be unfamiliar to developers used to CRUD: aggregates, projections, eventual consistency, event versioning. The learning curve is real, and your team needs to understand the paradigm to work effectively.

Simple CRUD operations become more elaborate. What was a single UPDATE statement becomes: load events, replay to current state, validate the change, emit new event, persist event, update projections.

### Storage Growth

Events accumulate over time. An aggregate with thousands of events takes longer to load (though snapshotting mitigates this). Your event store will grow indefinitely, requiring more storage than a system that only keeps current state.

For most applications, storage is cheap and this is not a significant concern. But for high-frequency systems with massive event volumes, it requires planning.

### Eventual Consistency

In many event-sourced systems, read models (projections) are updated asynchronously. This means there can be a brief delay between when an event occurs and when it appears in queries. Users might submit a change and not immediately see it reflected.

For many use cases, this is acceptable. For others, it requires careful UI design or synchronous projection updates.

### Schema Evolution

Over time, your events will need to change - new fields, deprecated fields, renamed fields. Unlike a database where you can run a migration to update all rows, events are immutable. You need strategies for handling old event formats alongside new ones.

The eventsource-py library provides tools for this (event versioning, upcasters), but it requires forethought and discipline.

### Not Always the Right Tool

Event sourcing adds value when you need what it provides. If your application is simple CRUD with no audit requirements, no temporal queries, and no event-driven integrations, the added complexity may not be justified.

---

## When to Use Event Sourcing

### Good Candidates

Event sourcing shines in these scenarios:

- **Financial systems** - Audit trails are mandatory, every transaction must be traceable
- **Healthcare applications** - Patient history must be complete and immutable
- **E-commerce platforms** - Order history, inventory tracking, customer journey analysis
- **Collaborative applications** - Multiple users editing shared data, conflict resolution needed
- **Complex business domains** - Rich business rules, many state transitions
- **Compliance-driven industries** - Regulatory requirements for data history
- **Analytics-heavy applications** - Business intelligence, behavioral analysis
- **Systems requiring undo/redo** - Document editing, workflow management

### Less Suitable Scenarios

Consider alternatives for:

- **Simple CRUD applications** - A basic blog or content management system rarely needs event sourcing
- **Static content management** - Pages that rarely change do not benefit from change tracking
- **High-frequency, low-value writes** - Logging millions of trivial events per second may not be worth the overhead
- **Systems where history truly does not matter** - Temporary data, caches, session storage
- **Small teams unfamiliar with the pattern** - The learning curve may slow development

### Decision Framework

Ask yourself these questions:

1. **Do we need a complete audit trail?** If yes, event sourcing provides this naturally.
2. **Will we need to answer questions about historical state?** If yes, event sourcing makes this trivial.
3. **Are we building an event-driven architecture?** Event sourcing produces the events you need.
4. **Is our domain complex with many state transitions?** Event sourcing models this explicitly.
5. **Is our team willing to learn a new paradigm?** This is essential for success.

If you answered "no" to all of these, traditional CRUD may serve you better.

---

## Core Concepts

Let us define the key terms you will encounter throughout this tutorial series.

### Events

An **event** is an immutable record of something that happened in your system. Events are:

- Named in past tense (OrderPlaced, PaymentReceived, UserDeactivated)
- Immutable once created
- The source of truth for your system

Events capture both the fact that something happened and the data associated with that change. A `PaymentReceived` event might include the payment amount, currency, payment method, and transaction ID.

In eventsource-py, you define events as Python classes that inherit from `DomainEvent`.

### Aggregates

An **aggregate** is a cluster of related objects treated as a single unit for data changes. It is a consistency boundary - all changes within an aggregate are atomic.

Think of an Order aggregate that includes the order itself plus its line items. When you add an item to an order, the entire order (including its items) is loaded, modified, and saved as a unit.

Aggregates:

- Enforce business rules (invariants)
- Emit events when state changes
- Reconstruct their state by replaying events

In eventsource-py, aggregates inherit from `AggregateRoot`.

### Event Store

The **event store** is where events are persisted. It is the database of record for an event-sourced system. Unlike a traditional database that stores current state, the event store contains the complete history of every change.

Event stores provide:

- Append-only event persistence
- Reading events by aggregate ID
- Optimistic concurrency control (detecting conflicting writes)
- Optionally, reading all events (for projections)

eventsource-py provides multiple event store backends: PostgreSQL for production, SQLite for development, and in-memory for testing.

### Projections

A **projection** (also called a "read model" or "view") is a derived view of data built by processing events. Projections transform the event stream into a format optimized for specific queries.

For example, from a stream of Order events, you might build:

- An "orders by customer" projection for customer service
- A "daily sales totals" projection for reporting
- A "low inventory alerts" projection for operations

Projections can be rebuilt at any time by replaying all events. If you discover a bug in a projection, you can fix it and rebuild from the complete event history.

### Event Bus

The **event bus** distributes events to interested subscribers. When an event is saved, the event bus notifies all listeners, which might include projections, external system integrations, or notification services.

eventsource-py supports multiple event bus backends including in-memory for simple applications, Redis for distributed systems, and Kafka for high-throughput scenarios.

### Commands

A **command** represents an intent to do something. "Create an order," "Ship the package," "Cancel the subscription." Commands are validated by aggregates and, if valid, result in one or more events being emitted.

Commands can fail (invalid state, business rule violation), but events never fail - they represent things that have already happened.

### Repository

The **repository** provides a clean interface for loading and saving aggregates. It abstracts the details of event storage, state reconstruction, and optimistic locking.

When you load an aggregate through the repository, it fetches all events for that aggregate and replays them to reconstruct the current state. When you save, it persists any new events and handles concurrency conflicts.

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

This library provides everything you need to build event-sourced applications in Python:

| Component | Purpose | Tutorial |
|-----------|---------|----------|
| `DomainEvent` | Base class for defining events | Tutorial 2 |
| `AggregateRoot` | Base class for aggregates with state reconstruction | Tutorial 3 |
| `EventStore` | Persist and retrieve events (PostgreSQL, SQLite, In-Memory) | Tutorial 4 |
| `AggregateRepository` | Load and save aggregates through the event store | Tutorial 5 |
| `Projection` | Build read models from event streams | Tutorial 6 |
| `EventBus` | Distribute events to subscribers (In-Memory, Redis, Kafka) | Tutorial 7 |
| `SnapshotStore` | Optimize loading of aggregates with many events | Advanced Topics |

The library is designed to be:

- **Pythonic** - Follows Python conventions and idioms
- **Type-safe** - Full typing support for modern Python
- **Async-first** - Built on async/await for performance
- **Modular** - Use only what you need
- **Production-ready** - Battle-tested patterns and error handling

---

## What You Will Build

Throughout this tutorial series, you will build real applications using event sourcing:

**Phase 1: Foundations**

- A simple task management system that tracks task creation, assignment, and completion
- You will learn the core concepts hands-on with minimal complexity

**Phase 2: Growing Complexity**

- A more sophisticated system with projections for different views of the same data
- Event buses for real-time updates and integrations
- Testing strategies for event-sourced systems

**Phase 3: Production Patterns**

- Error handling and resilience
- Schema evolution and event versioning
- Performance optimization with snapshots
- Multi-tenancy and access control

By the end, you will have the knowledge and experience to build production event-sourced applications with confidence.

---

## Key Takeaways

- **Event sourcing stores what happened, not just what is.** Every change is captured as an immutable event, creating a complete audit trail.

- **Events are immutable facts.** Once recorded, events never change. This gives you reliable history and reproducibility.

- **Current state is derived, not stored.** You reconstruct state by replaying events, which enables time travel and debugging.

- **Real systems use event sourcing.** Banks, Git, medical records, and accounting all follow this pattern.

- **Event sourcing has trade-offs.** Increased complexity, eventual consistency, and storage growth are real considerations.

- **Not every application needs event sourcing.** Use it when you need audit trails, temporal queries, or event-driven architecture.

- **Core concepts are interconnected.** Events, aggregates, projections, and the event store work together as a system.

---

## Glossary

| Term | Definition |
|------|------------|
| **Event** | An immutable record of something that happened, named in past tense |
| **Event Store** | A database optimized for storing and retrieving event streams |
| **Aggregate** | A consistency boundary that enforces business rules and emits events |
| **Projection** | A read model built by processing events, optimized for specific queries |
| **Event Bus** | Infrastructure for distributing events to subscribers |
| **Command** | An intent to perform an action, which may result in events if valid |
| **Repository** | An abstraction for loading and saving aggregates |
| **Eventual Consistency** | The state where read models may temporarily lag behind writes |
| **Snapshot** | A cached copy of aggregate state to speed up loading |
| **Replay** | Reconstructing state by processing events from the beginning |

---

## Next Steps

You now understand the fundamental concepts of event sourcing. In the next tutorial, you will put this knowledge into practice by writing your first domain event.

Continue to [Tutorial 2: Your First Domain Event](02-first-event.md) to write your first event.

---

## Related Documentation

- [Getting Started Guide](../getting-started.md) - Quick start with code examples
- [API Reference: Events](../api/events.md) - Detailed event API documentation
- [API Reference: Aggregates](../api/aggregates.md) - Detailed aggregate API documentation
