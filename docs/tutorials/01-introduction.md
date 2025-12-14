# Tutorial 1: Introduction to Event Sourcing

**Difficulty:** Beginner

## Prerequisites

- Basic Python programming knowledge
- Familiarity with database concepts (tables, queries, updates)
- Python 3.10 or higher
- No event sourcing experience required

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain what event sourcing is and how it differs from traditional CRUD
2. Identify real-world systems that use event sourcing principles
3. List the key benefits and trade-offs of event sourcing
4. Define core terminology: event, aggregate, projection, event store
5. Decide when event sourcing is appropriate for a project
6. Understand the eventsource-py library components

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

**E-commerce orders**: Track every state transition (placed, paid, shipped, delivered). Essential for customer service and dispute resolution.

---

## Benefits

- **Complete audit trail**: Every change recorded with timestamp and context. Built-in compliance.
- **Time travel**: Reconstruct state at any point in history. "What was inventory on Dec 31st?"
- **Debugging**: Replay exact event sequence that caused an issue.
- **Analytics**: Rich event data for behavioral analysis and ML pipelines.
- **Event-driven architecture**: Natural fit for microservices, CQRS, reactive systems.
- **Conflict resolution**: Examine user intent, not just "last write wins".
- **Natural correlation**: Track causation chains across aggregates and services.

---

## Trade-offs

- **Increased complexity**: Learning curve for aggregates, projections, eventual consistency.
- **Storage growth**: Events accumulate forever. Requires more storage than current-state-only systems (mitigated by snapshots).
- **Eventual consistency**: Read models may lag slightly behind writes.
- **Schema evolution**: Immutable events require versioning strategies for changes.
- **Not always needed**: Simple CRUD apps may not benefit from added complexity.
- **Async operations**: Most operations are async, requiring async/await understanding.

---

## When to Use Event Sourcing

**Good candidates:**
- Financial systems, healthcare, e-commerce
- Compliance-driven industries (audit requirements)
- Collaborative applications (understanding concurrent changes)
- Complex business domains with many state transitions
- Analytics-heavy applications
- Systems requiring undo/redo
- Distributed systems with microservices

**Less suitable:**
- Simple CRUD apps
- Static content management
- Systems where history doesn't matter
- Small teams without event sourcing experience
- Real-time systems with strict latency requirements (<1ms)

---

## Core Concepts

**Event (DomainEvent)**: Immutable record of something that happened. Named in past tense (OrderPlaced, PaymentReceived). Contains both business data and metadata (aggregate_id, correlation_id, causation_id). Source of truth for your system.

**Aggregate (AggregateRoot)**: Consistency boundary that enforces business rules and emits events. Reconstructs state by replaying events. Each aggregate has a unique ID and maintains an internal state model.

**Event Store**: Database of record. Stores complete history in append-only fashion. Supports optimistic concurrency control to prevent conflicting updates.

**Projection**: Read model derived from events, optimized for queries. Can be rebuilt anytime by replaying events. Examples: order lists, customer statistics, daily revenue reports.

**Event Bus**: Distributes events to subscribers (projections, integrations, notifications) in real-time. Supports in-memory, Redis, RabbitMQ, and Kafka backends.

**Repository (AggregateRepository)**: Interface for loading and saving aggregates. Handles event storage, state reconstruction, and optionally event publishing.

**Subscription**: Mechanism for projections to receive events. SubscriptionManager coordinates catch-up (historical events) and live (new events) phases.

**Checkpoint**: Position tracking for projections. Enables resumable processing after restarts.

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
| **Concurrency** | Locks or last-write-wins | Optimistic locking with version checks |
| **Data model** | Normalized tables | Event streams + denormalized projections |

---

## The eventsource-py Library

eventsource-py is a production-ready event sourcing library for Python that provides all the building blocks you need.

### Core Components

| Component | Purpose | Import From |
|-----------|---------|-------------|
| `DomainEvent` | Base class for events (uses Pydantic) | `eventsource` |
| `AggregateRoot` | Base class for aggregates | `eventsource` |
| `DeclarativeAggregate` | Aggregate with @handles decorators | `eventsource` |
| `EventStore` | Persist/retrieve events | `eventsource` |
| `InMemoryEventStore` | For testing and development | `eventsource` |
| `PostgreSQLEventStore` | Production event store | `eventsource` |
| `AggregateRepository` | Load and save aggregates | `eventsource` |
| `EventBus` | Distribute events to subscribers | `eventsource` |
| `SubscriptionManager` | Coordinate projections | `eventsource.subscriptions` |
| `@handles` | Decorator for event handlers | `eventsource` |

### Event Store Implementations

- **InMemoryEventStore**: For testing and development. No persistence.
- **PostgreSQLEventStore**: Production-ready with optimistic locking, global ordering, and partition support.
- **SQLiteEventStore**: Lightweight option (requires aiosqlite).

### Event Bus Implementations

- **InMemoryEventBus**: Simple in-process pub/sub for testing.
- **RedisEventBus**: Production event bus using Redis Streams.
- **RabbitMQEventBus**: Enterprise messaging with RabbitMQ.
- **KafkaEventBus**: High-throughput distributed event streaming.

### Repository Infrastructure

- **CheckpointRepository**: Track projection positions for resumable processing.
- **DLQRepository**: Dead letter queue for failed events.
- **OutboxRepository**: Transactional outbox pattern for reliable event publishing.

Implementations available for PostgreSQL, SQLite, and in-memory.

### Key Features

1. **Pydantic-based events**: Type-safe events with validation
2. **Async/await**: Native async support throughout
3. **Optimistic locking**: Prevent concurrent modification conflicts
4. **Snapshots**: Performance optimization for large event streams
5. **Correlation/Causation tracking**: Built-in event chain tracking
6. **Multi-tenancy support**: Optional tenant_id on all events
7. **Schema versioning**: Event and snapshot version tracking
8. **Observability**: OpenTelemetry tracing integration

---

## A Quick Example

Here's what event sourcing looks like with eventsource-py:

```python
from uuid import UUID, uuid4
from pydantic import BaseModel
from eventsource import (
    DomainEvent,
    AggregateRoot,
    AggregateRepository,
    InMemoryEventStore,
    register_event,
)

# Define an event
@register_event
class AccountOpened(DomainEvent):
    event_type: str = "AccountOpened"
    aggregate_type: str = "BankAccount"
    owner_name: str
    initial_balance: float

# Define aggregate state
class BankAccountState(BaseModel):
    account_id: UUID
    owner_name: str = ""
    balance: float = 0.0

# Create the aggregate
class BankAccountAggregate(AggregateRoot[BankAccountState]):
    aggregate_type = "BankAccount"

    def _get_initial_state(self) -> BankAccountState:
        return BankAccountState(account_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, AccountOpened):
            self._state = BankAccountState(
                account_id=self.aggregate_id,
                owner_name=event.owner_name,
                balance=event.initial_balance,
            )

    def open(self, owner_name: str, initial_balance: float = 0.0) -> None:
        event = AccountOpened(
            aggregate_id=self.aggregate_id,
            owner_name=owner_name,
            initial_balance=initial_balance,
            aggregate_version=self.get_next_version(),
        )
        self.apply_event(event)

# Use it
async def main():
    event_store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=event_store,
        aggregate_factory=BankAccountAggregate,
        aggregate_type="BankAccount",
    )

    # Create and save
    account = repo.create_new(uuid4())
    account.open("Alice", 100.0)
    await repo.save(account)

    # Load and use
    loaded = await repo.load(account.aggregate_id)
    print(f"Balance: ${loaded.state.balance}")
```

This example demonstrates:
- **Type-safe events** using Pydantic
- **Aggregate pattern** with state management
- **Repository pattern** for persistence
- **Async/await** throughout

---

## Architecture Patterns

### Command-Query Responsibility Segregation (CQRS)

Event sourcing naturally supports CQRS:
- **Commands**: Write operations that emit events (via aggregates)
- **Queries**: Read operations from projections (optimized read models)

This separation allows:
- Different scalability for reads vs writes
- Optimized read models for specific queries
- Multiple views of the same data

### Event-Driven Architecture

Events are the integration mechanism:
- Aggregates emit events
- Event bus distributes to subscribers
- Projections, notifications, integrations react
- Loose coupling between components

### Projection Pattern

Build specialized read models from events:
```python
class OrderListProjection:
    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [OrderPlaced, OrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        # Update read model
        if isinstance(event, OrderPlaced):
            self.orders[event.aggregate_id] = {...}
```

---

## Next Steps

Now that you understand the fundamentals, you're ready to build your first event-sourced application.

Continue to [Tutorial 2: Your First Domain Event](02-first-event.md) to start building.

For working examples, see:
- `examples/basic_usage.py` - Basic event sourcing patterns
- `examples/aggregate_example.py` - Advanced aggregate with @handles
- `examples/projection_example.py` - Building read models with SubscriptionManager
