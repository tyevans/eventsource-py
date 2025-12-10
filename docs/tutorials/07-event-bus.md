# Tutorial 7: Distributing Events with Event Bus

**Estimated Time:** 30-45 minutes
**Difficulty:** Intermediate
**Progress:** Tutorial 7 of 21 | Phase 2: Core Patterns

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 5: Repositories Pattern](05-repositories.md)
- Completed [Tutorial 6: Building Read Models with Projections](06-projections.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial builds on concepts from Tutorials 5 and 6. You will use the `TaskAggregate` and `TaskListProjection` patterns to demonstrate event bus operations.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Explain the role of an event bus in event sourcing architecture
2. Create and configure an `InMemoryEventBus`
3. Subscribe handlers to specific event types using `subscribe()`
4. Use wildcard subscriptions with `subscribe_to_all_events()` for cross-cutting concerns
5. Connect the event bus to repositories for automatic event publishing
6. Choose between synchronous and background publishing modes
7. Handle errors in event handlers with proper error isolation

---

## What is an Event Bus?

In Tutorial 5, you learned how to save aggregates using repositories. In Tutorial 6, you built projections that transform events into read models. But how do events get from repositories to projections?

This is where the **event bus** comes in.

### The Problem: Manual Event Distribution

Without an event bus, you would have to manually distribute events to every interested handler:

```python
# Without event bus - manually calling each handler
async def save_order(repo, order, projection, notifier, auditor):
    await repo.save(order)
    events = order.uncommitted_events

    # Manually distribute to every handler
    for event in events:
        await projection.handle(event)
        await notifier.handle(event)
        await auditor.handle(event)
```

This approach has several problems:

- **Tight coupling**: The save function must know about every handler
- **Hard to extend**: Adding a new handler requires changing all save functions
- **Error propagation**: One handler failure stops all subsequent handlers
- **Difficult testing**: Must mock all handlers in tests

### The Solution: Publish/Subscribe Pattern

An **event bus** implements the publish/subscribe (pub/sub) pattern:

1. **Publishers** (repositories) send events to the bus without knowing who will receive them
2. **Subscribers** (projections, handlers) register interest in specific event types
3. The **bus** routes events to all interested subscribers

```
                    Event Bus
                  +------------+
Repository -----> |            | -----> Projection A
   (publish)      |   Routes   | -----> Projection B
                  |   Events   | -----> Notification Handler
                  +------------+ -----> Audit Logger
```

This decouples event producers from consumers, making your system more modular and extensible.

### Benefits of Event Bus

| Without Event Bus | With Event Bus |
|------------------|----------------|
| Publisher knows all handlers | Publisher just publishes |
| Adding handler changes publisher | Adding handler is self-contained |
| One failure stops all handlers | Failures are isolated |
| Hard to test in isolation | Easy to test each component |
| Synchronous coupling | Asynchronous possible |

---

## The EventBus Interface

eventsource-py defines an `EventBus` abstract base class that all implementations follow. The key methods are:

| Method | Description |
|--------|-------------|
| `publish(events, background)` | Publish events to all registered subscribers |
| `subscribe(event_type, handler)` | Subscribe handler to a specific event type |
| `unsubscribe(event_type, handler)` | Remove a subscription |
| `subscribe_all(subscriber)` | Subscribe a subscriber to all its declared event types |
| `subscribe_to_all_events(handler)` | Wildcard subscription - receive ALL events |
| `unsubscribe_from_all_events(handler)` | Remove wildcard subscription |

---

## Using InMemoryEventBus

For development, testing, and single-process applications, eventsource-py provides `InMemoryEventBus`.

### Creating the Bus

Creating an in-memory event bus is straightforward:

```python
from eventsource import InMemoryEventBus

# Create a new event bus
bus = InMemoryEventBus()

# Optional: disable tracing for tests
bus = InMemoryEventBus(enable_tracing=False)
```

### When to Use InMemoryEventBus

`InMemoryEventBus` is suitable for:

- **Unit testing**: Fast, isolated tests without external dependencies
- **Development**: Quick iteration without infrastructure setup
- **Single-process applications**: When all handlers run in the same process

It is **not suitable** for:

- Distributed systems with multiple processes
- Scenarios requiring event persistence in the bus
- High-availability production deployments

For production distributed systems, see Tutorials 17-19 covering Redis, RabbitMQ, and Kafka event buses.

---

## Subscribing Handlers

There are several ways to subscribe handlers to the event bus.

### Method 1: Subscribe to Specific Event Type

The most common pattern is subscribing a handler to a specific event type:

```python
from eventsource import InMemoryEventBus, DomainEvent, register_event
from uuid import uuid4

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str

# Create bus
bus = InMemoryEventBus()

# Handler function
async def on_task_created(event: DomainEvent) -> None:
    print(f"Task created: {event.title}")

# Subscribe to TaskCreated events
bus.subscribe(TaskCreated, on_task_created)
```

You can also use objects with a `handle` method:

```python
class TaskNotificationHandler:
    async def handle(self, event: DomainEvent) -> None:
        print(f"Sending notification for task: {event.title}")

handler = TaskNotificationHandler()
bus.subscribe(TaskCreated, handler)
```

### Method 2: Subscribe All Event Types (for Projections)

For projections that handle multiple event types, use `subscribe_all()`:

```python
from eventsource import DeclarativeProjection, handles

class TaskListProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks: dict = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[str(event.aggregate_id)] = {
            "title": event.title,
            "status": "pending",
        }

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        task_id = str(event.aggregate_id)
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = "completed"

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()

# Create projection
projection = TaskListProjection()

# Subscribe projection to all its declared event types
# This calls subscribe() for each type returned by subscribed_to()
bus.subscribe_all(projection)
```

The `subscribe_all()` method automatically subscribes the projection to `TaskCreated` and `TaskCompleted` because those are the types declared in its `@handles` decorators.

### Method 3: Wildcard Subscription (All Events)

For cross-cutting concerns like audit logging or metrics, use `subscribe_to_all_events()`:

```python
class AuditLogger:
    def __init__(self):
        self.log: list = []

    async def handle(self, event: DomainEvent) -> None:
        self.log.append({
            "event_type": event.event_type,
            "aggregate_id": str(event.aggregate_id),
            "timestamp": event.timestamp.isoformat(),
        })
        print(f"[AUDIT] {event.event_type} on {event.aggregate_id}")

auditor = AuditLogger()

# Subscribe to ALL events, regardless of type
bus.subscribe_to_all_events(auditor)
```

**Important**: Use wildcard subscriptions sparingly. They are called for every event, which can impact performance in high-throughput scenarios.

### Unsubscribing

You can remove subscriptions when they are no longer needed:

```python
# Unsubscribe from specific event type
removed = bus.unsubscribe(TaskCreated, handler)
print(f"Handler removed: {removed}")  # True if found and removed

# Unsubscribe from wildcard subscription
removed = bus.unsubscribe_from_all_events(auditor)
```

---

## Publishing Events

Once handlers are subscribed, you can publish events to the bus.

### Manual Publishing

You can publish events directly to the bus:

```python
import asyncio
from uuid import uuid4

async def publish_example():
    bus = InMemoryEventBus()

    # Subscribe a handler
    events_received = []
    async def handler(event):
        events_received.append(event)

    bus.subscribe(TaskCreated, handler)

    # Create an event
    event = TaskCreated(
        aggregate_id=uuid4(),
        title="Learn Event Bus",
        aggregate_version=1,
    )

    # Publish the event
    await bus.publish([event])

    print(f"Events received: {len(events_received)}")  # 1

asyncio.run(publish_example())
```

### Synchronous vs Background Publishing

The `publish()` method accepts a `background` parameter that controls execution:

```python
# Synchronous (default): Wait for all handlers to complete
await bus.publish([event])  # Blocks until handlers finish

# Background: Fire-and-forget, returns immediately
await bus.publish([event], background=True)  # Returns immediately
```

**Synchronous Publishing (background=False)**:

- Waits for all handlers to complete
- Caller knows when processing is done
- Errors are logged but do not propagate to caller
- Good for: Ensuring read-after-write consistency

**Background Publishing (background=True)**:

- Returns immediately, handlers run in background tasks
- Better API response times
- Introduces eventual consistency
- Good for: High-throughput scenarios where latency matters

### Waiting for Background Tasks

If you use background publishing, you can wait for all pending tasks:

```python
# Publish in background
await bus.publish([event1], background=True)
await bus.publish([event2], background=True)

# Later, wait for all background tasks to complete
await bus.shutdown(timeout=30.0)
```

The `shutdown()` method is especially important during application teardown to ensure all events are processed.

---

## Integration with Repository

The most common way to use an event bus is to connect it to your repository. When you save an aggregate, events are automatically published.

### Setting Up Repository with Event Bus

```python
from eventsource import (
    AggregateRepository,
    InMemoryEventStore,
    InMemoryEventBus,
)

# Create infrastructure
store = InMemoryEventStore()
bus = InMemoryEventBus()

# Create repository WITH event publisher
repo = AggregateRepository(
    event_store=store,
    aggregate_factory=TaskAggregate,
    aggregate_type="Task",
    event_publisher=bus,  # Key: connect the bus!
)
```

### Complete Flow Example

Here is how all the pieces work together:

```python
import asyncio
from uuid import uuid4

from eventsource import (
    AggregateRepository,
    InMemoryEventStore,
    InMemoryEventBus,
)

async def complete_flow():
    # 1. Create infrastructure
    store = InMemoryEventStore()
    bus = InMemoryEventBus()

    # 2. Create projection and subscribe to bus
    projection = TaskListProjection()
    bus.subscribe_all(projection)

    # 3. Create repository with event bus
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=bus,
    )

    # 4. Create and save an aggregate
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Learn Event Bus")
    await repo.save(task)  # Events automatically published!

    # 5. Projection received the event
    print(f"Projection has {len(projection.tasks)} task(s)")
    print(f"Task: {projection.tasks[str(task_id)]}")
```

When `repo.save(task)` is called:

1. Events are appended to the event store
2. Events are marked as committed on the aggregate
3. Events are published to the event bus
4. Event bus dispatches to all subscribed handlers
5. Projection receives and processes the event

---

## Error Handling and Isolation

A critical feature of the event bus is **error isolation**. When one handler fails, it does not prevent other handlers from receiving the event.

### Error Isolation in Action

```python
async def error_isolation_demo():
    bus = InMemoryEventBus()

    results = []

    async def failing_handler(event):
        raise ValueError("Handler failed!")

    async def working_handler(event):
        results.append("success")

    # Subscribe both handlers
    bus.subscribe(TaskCreated, failing_handler)
    bus.subscribe(TaskCreated, working_handler)

    # Publish event
    event = TaskCreated(
        aggregate_id=uuid4(),
        title="Test",
        aggregate_version=1,
    )

    await bus.publish([event])

    # Working handler still executed!
    print(f"Results: {results}")  # ["success"]
```

Even though `failing_handler` raised an exception, `working_handler` still executed. The error was logged but did not stop event distribution.

### How Errors Are Handled

When a handler raises an exception:

1. The exception is caught by the event bus
2. The error is logged with full context (handler name, event type, event ID)
3. Other handlers continue to receive the event
4. Statistics are updated (`handler_errors` counter)

You can check error statistics:

```python
stats = bus.get_stats()
print(f"Handler errors: {stats['handler_errors']}")
```

### Best Practices for Handler Error Handling

Handlers should be defensive about errors:

```python
class ResilientHandler:
    async def handle(self, event: DomainEvent) -> None:
        try:
            await self.process(event)
        except TransientError as e:
            # Log and re-raise - this error will be counted
            logger.warning(f"Transient error: {e}")
            raise
        except PermanentError as e:
            # Log and swallow - don't block or pollute stats
            logger.error(f"Permanent error (ignoring): {e}")
            # Optionally: send to dead letter queue
```

---

## Event Bus Statistics

`InMemoryEventBus` tracks statistics about its operation:

```python
bus = InMemoryEventBus()

# ... publish some events ...

stats = bus.get_stats()
print(f"Events published: {stats['events_published']}")
print(f"Handlers invoked: {stats['handlers_invoked']}")
print(f"Handler errors: {stats['handler_errors']}")
print(f"Background tasks created: {stats['background_tasks_created']}")
print(f"Background tasks completed: {stats['background_tasks_completed']}")

# Check subscriber counts
print(f"Total subscribers: {bus.get_subscriber_count()}")
print(f"Wildcard subscribers: {bus.get_wildcard_subscriber_count()}")
print(f"TaskCreated subscribers: {bus.get_subscriber_count(TaskCreated)}")
```

---

## Complete Example

Here is a complete example demonstrating all event bus concepts:

```python
"""
Tutorial 7: Distributing Events with Event Bus

This example demonstrates event distribution using InMemoryEventBus.
Run with: python tutorial_07_event_bus.py
"""
import asyncio
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DeclarativeProjection,
    DomainEvent,
    InMemoryEventBus,
    InMemoryEventStore,
    handles,
    register_event,
)


# =============================================================================
# Events
# =============================================================================

@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


# =============================================================================
# Aggregate
# =============================================================================

class TaskState(BaseModel):
    task_id: UUID
    title: str = ""
    status: str = "pending"


class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=self.aggregate_id,
                title=event.title,
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str) -> None:
        if self.version > 0:
            raise ValueError("Already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Projection
# =============================================================================

class TaskListProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks: dict[str, dict] = {}
        self.events_received = 0

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[str(event.aggregate_id)] = {
            "title": event.title,
            "status": "pending",
        }
        self.events_received += 1

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        task_id = str(event.aggregate_id)
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = "completed"
        self.events_received += 1

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()
        self.events_received = 0


# =============================================================================
# Examples
# =============================================================================

async def basic_subscription():
    """Demonstrate basic subscription and publishing."""
    print("=" * 60)
    print("Basic Subscription")
    print("=" * 60)
    print()

    bus = InMemoryEventBus()
    received_events: list[DomainEvent] = []

    # Simple handler function
    async def my_handler(event: DomainEvent) -> None:
        received_events.append(event)
        print(f"  Handler received: {event.event_type}")

    # Subscribe to specific event type
    bus.subscribe(TaskCreated, my_handler)

    # Publish an event
    event = TaskCreated(
        aggregate_id=uuid4(),
        title="Test Task",
        aggregate_version=1,
    )

    print("Publishing TaskCreated event...")
    await bus.publish([event])
    print(f"Received {len(received_events)} event(s)")


async def wildcard_subscription():
    """Demonstrate wildcard subscription."""
    print()
    print("=" * 60)
    print("Wildcard Subscription")
    print("=" * 60)
    print()

    bus = InMemoryEventBus()
    all_events: list[DomainEvent] = []

    async def audit_handler(event: DomainEvent) -> None:
        all_events.append(event)
        print(f"  [AUDIT] {event.event_type}")

    # Subscribe to ALL events
    bus.subscribe_to_all_events(audit_handler)

    # Publish different event types
    task_id = uuid4()
    events = [
        TaskCreated(aggregate_id=task_id, title="Task", aggregate_version=1),
        TaskCompleted(aggregate_id=task_id, aggregate_version=2),
    ]

    print("Publishing TaskCreated and TaskCompleted events...")
    await bus.publish(events)
    print(f"Audit handler received {len(all_events)} events")


async def repository_integration():
    """Demonstrate automatic publishing via repository."""
    print()
    print("=" * 60)
    print("Repository Integration")
    print("=" * 60)
    print()

    # Create infrastructure
    store = InMemoryEventStore()
    bus = InMemoryEventBus()
    projection = TaskListProjection()

    # Subscribe projection to events
    bus.subscribe_all(projection)

    # Create repository WITH event bus
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=bus,
    )

    # Create and save a task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Learn Event Bus")
    await repo.save(task)

    print("After saving task:")
    print(f"  Projection events received: {projection.events_received}")
    print(f"  Projection tasks: {len(projection.tasks)}")

    # Complete the task
    loaded = await repo.load(task_id)
    loaded.complete()
    await repo.save(loaded)

    print()
    print("After completing task:")
    print(f"  Projection events received: {projection.events_received}")
    print(f"  Task status: {projection.tasks[str(task_id)]['status']}")


async def background_publishing():
    """Demonstrate background publishing."""
    print()
    print("=" * 60)
    print("Background Publishing")
    print("=" * 60)
    print()

    bus = InMemoryEventBus()

    async def slow_handler(event: DomainEvent) -> None:
        await asyncio.sleep(0.1)  # Simulate slow processing
        print(f"  Slow handler processed: {event.event_type}")

    bus.subscribe(TaskCreated, slow_handler)

    event = TaskCreated(
        aggregate_id=uuid4(),
        title="Background Task",
        aggregate_version=1,
    )

    # Sync publish - waits for handlers
    print("Sync publish (blocking)...")
    await bus.publish([event], background=False)
    print("  Done (waited for handler)")

    # Background publish - returns immediately
    print()
    print("Background publish (fire-and-forget)...")
    await bus.publish([event], background=True)
    print("  Returned immediately!")

    # Wait for background tasks to complete
    await bus.shutdown(timeout=5.0)
    print("  Background tasks completed")


async def error_isolation():
    """Demonstrate error isolation between handlers."""
    print()
    print("=" * 60)
    print("Error Isolation")
    print("=" * 60)
    print()

    bus = InMemoryEventBus()

    async def failing_handler(event: DomainEvent) -> None:
        raise ValueError("Handler failed!")

    async def working_handler(event: DomainEvent) -> None:
        print("  Working handler executed!")

    # Subscribe both handlers to same event type
    bus.subscribe(TaskCreated, failing_handler)
    bus.subscribe(TaskCreated, working_handler)

    event = TaskCreated(
        aggregate_id=uuid4(),
        title="Test",
        aggregate_version=1,
    )

    # Even though one handler fails, the other still runs
    print("Publishing with failing handler...")
    await bus.publish([event])
    print("  Both handlers were attempted (errors logged but isolated)")

    # Check stats
    stats = bus.get_stats()
    print(f"  Handler errors: {stats['handler_errors']}")


async def statistics_demo():
    """Demonstrate event bus statistics."""
    print()
    print("=" * 60)
    print("Event Bus Statistics")
    print("=" * 60)
    print()

    bus = InMemoryEventBus()

    # Subscribe multiple handlers
    async def handler1(e): pass
    async def handler2(e): pass
    async def wildcard(e): pass

    bus.subscribe(TaskCreated, handler1)
    bus.subscribe(TaskCreated, handler2)
    bus.subscribe(TaskCompleted, handler1)
    bus.subscribe_to_all_events(wildcard)

    print("Subscriber counts:")
    print(f"  Total specific subscribers: {bus.get_subscriber_count()}")
    print(f"  TaskCreated subscribers: {bus.get_subscriber_count(TaskCreated)}")
    print(f"  TaskCompleted subscribers: {bus.get_subscriber_count(TaskCompleted)}")
    print(f"  Wildcard subscribers: {bus.get_wildcard_subscriber_count()}")

    # Publish some events
    task_id = uuid4()
    await bus.publish([
        TaskCreated(aggregate_id=task_id, title="Task", aggregate_version=1),
        TaskCompleted(aggregate_id=task_id, aggregate_version=2),
    ])

    print()
    print("After publishing 2 events:")
    stats = bus.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")


async def main():
    """Run all examples."""
    await basic_subscription()
    await wildcard_subscription()
    await repository_integration()
    await background_publishing()
    await error_isolation()
    await statistics_demo()

    print()
    print("=" * 60)
    print("Tutorial 7 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
```

Save this as `tutorial_07_event_bus.py` and run it:

```bash
python tutorial_07_event_bus.py
```

---

## Exercises

Now it is time to practice what you have learned!

### Exercise 1: Complete Event Flow

**Objective:** Wire together repository, event bus, and projection to see the complete event flow.

**Time:** 15-20 minutes

**Requirements:**

1. Create an `InMemoryEventBus`
2. Create a `TaskListProjection`
3. Subscribe the projection to task events
4. Create a repository with the bus as publisher
5. Create and save a task
6. Verify the projection received the events
7. Complete the task and verify the projection updated

**Starter Code:**

```python
"""
Tutorial 7 - Exercise 1: Complete Event Flow

Your task: Wire together all components and verify the flow.
"""
import asyncio
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DeclarativeProjection,
    DomainEvent,
    InMemoryEventBus,
    InMemoryEventStore,
    handles,
    register_event,
)


# Events
@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


# State
class TaskState(BaseModel):
    task_id: str
    title: str = ""
    status: str = "pending"


# Aggregate
class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=str(self.aggregate_id),
                title=event.title,
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str) -> None:
        if self.version > 0:
            raise ValueError("Already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


# Projection
class TaskListProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks: dict = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[str(event.aggregate_id)] = {
            "title": event.title,
            "status": "pending",
        }

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        task_id = str(event.aggregate_id)
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = "completed"

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()


async def main():
    print("=== Complete Event Flow ===\n")

    # Step 1: Create InMemoryEventBus
    # TODO: Create the bus

    # Step 2: Create TaskListProjection
    # TODO: Create the projection

    # Step 3: Subscribe projection to task events
    # TODO: Use subscribe_all() to subscribe the projection

    # Step 4: Create repository with bus as publisher
    # TODO: Create store and repository with event_publisher=bus

    # Step 5: Create and save a task
    # TODO: Use create_new(), task.create(), and repo.save()

    # Step 6: Verify projection received events
    # TODO: Print projection.tasks to see the task

    # Step 7: Complete the task and verify update
    # TODO: Load, complete, save, and print final state


if __name__ == "__main__":
    asyncio.run(main())
```

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 7 - Exercise 1 Solution: Complete Event Flow
"""
import asyncio
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DeclarativeProjection,
    DomainEvent,
    InMemoryEventBus,
    InMemoryEventStore,
    handles,
    register_event,
)


@register_event
class TaskCreated(DomainEvent):
    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


class TaskState(BaseModel):
    task_id: str
    title: str = ""
    status: str = "pending"


class TaskAggregate(AggregateRoot[TaskState]):
    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=str(self.aggregate_id),
                title=event.title,
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str) -> None:
        if self.version > 0:
            raise ValueError("Already exists")
        self.apply_event(TaskCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            aggregate_version=self.get_next_version(),
        ))

    def complete(self) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(TaskCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        ))


class TaskListProjection(DeclarativeProjection):
    def __init__(self):
        super().__init__()
        self.tasks: dict = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        self.tasks[str(event.aggregate_id)] = {
            "title": event.title,
            "status": "pending",
        }

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        task_id = str(event.aggregate_id)
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = "completed"

    async def _truncate_read_models(self) -> None:
        self.tasks.clear()


async def main():
    print("=== Complete Event Flow ===\n")

    # Step 1: Create InMemoryEventBus
    bus = InMemoryEventBus()
    print("1. Created InMemoryEventBus")

    # Step 2: Create TaskListProjection
    projection = TaskListProjection()
    print("2. Created TaskListProjection")

    # Step 3: Subscribe projection to task events
    bus.subscribe_all(projection)
    print("3. Subscribed projection to TaskCreated and TaskCompleted")

    # Step 4: Create repository with bus as publisher
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=bus,
    )
    print("4. Created repository with event bus")

    # Step 5: Create and save a task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Complete Exercise 7-1")
    await repo.save(task)
    print("5. Created and saved task")

    # Step 6: Verify projection received events
    print("\n6. Verification after create:")
    print(f"   Projection has {len(projection.tasks)} task(s)")
    task_data = projection.tasks.get(str(task_id))
    if task_data:
        print(f"   Task title: {task_data['title']}")
        print(f"   Task status: {task_data['status']}")
    else:
        print("   ERROR: Projection did not receive the event")

    # Step 7: Complete the task and verify update
    loaded = await repo.load(task_id)
    loaded.complete()
    await repo.save(loaded)

    print("\n7. Verification after complete:")
    task_data = projection.tasks.get(str(task_id))
    if task_data:
        print(f"   Task status: {task_data['status']}")
        if task_data['status'] == "completed":
            print("\n   SUCCESS: Complete event flow working!")
    else:
        print("   ERROR: Task not found in projection")


if __name__ == "__main__":
    asyncio.run(main())
```

</details>

The solution file is also available at: `docs/tutorials/exercises/solutions/07-1.py`

---

## Summary

In this tutorial, you learned:

- **Event bus** distributes events using the publish/subscribe pattern
- **InMemoryEventBus** is suitable for single-process applications and testing
- **subscribe()** registers handlers for specific event types
- **subscribe_all()** registers projections for all their declared event types
- **subscribe_to_all_events()** provides wildcard subscription for cross-cutting concerns
- **Repository integration** with `event_publisher` enables automatic publishing on save
- **Background publishing** improves response times but introduces eventual consistency
- **Error isolation** ensures one handler failure does not stop other handlers

---

## Key Takeaways

!!! note "Remember"
    - The event bus decouples event producers from consumers
    - Use `subscribe_all()` for projections that declare their event types
    - Use `subscribe_to_all_events()` sparingly - only for cross-cutting concerns
    - Connect the bus to repositories via the `event_publisher` parameter
    - Handler errors are isolated - one failure does not stop other handlers

!!! tip "Best Practice"
    For production deployments, prefer synchronous publishing (`background=False`) unless you have specific latency requirements and understand the eventual consistency tradeoffs. Always call `shutdown()` during application teardown.

!!! warning "Common Mistake"
    Do not forget to subscribe handlers before publishing events. Events published with no subscribers are silently dropped (this is correct behavior, but can lead to confusion during debugging).

---

## Next Steps

You now understand how to distribute events using the event bus. In the next tutorial, you will learn how to test event-sourced applications effectively.

Continue to [Tutorial 8: Testing Event-Sourced Applications](08-testing.md) to learn testing patterns and strategies.

---

## Related Documentation

- [Event Bus Guide](../guides/event-bus.md) - Comprehensive event bus reference
- [API Reference: Event Bus](../api/bus.md) - Complete API documentation
- [Tutorial 6: Projections](06-projections.md) - Building read models
- [Tutorial 17: Redis Event Bus](17-redis.md) - Distributed event bus with Redis
- [Tutorial 18: Kafka Event Bus](18-kafka.md) - High-throughput event streaming
- [Tutorial 19: RabbitMQ Event Bus](19-rabbitmq.md) - Enterprise messaging
