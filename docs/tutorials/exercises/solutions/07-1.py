"""
Tutorial 7 - Exercise 1 Solution: Complete Event Flow

This solution demonstrates wiring together repository, event bus, and projection
to see the complete event flow in action.

Run with: python 07-1.py
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
# State
# =============================================================================


class TaskState(BaseModel):
    task_id: str
    title: str = ""
    status: str = "pending"


# =============================================================================
# Aggregate
# =============================================================================


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
        elif isinstance(event, TaskCompleted) and self._state:
            self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str) -> None:
        if self.version > 0:
            raise ValueError("Already exists")
        self.apply_event(
            TaskCreated(
                aggregate_id=self.aggregate_id,
                title=title,
                aggregate_version=self.get_next_version(),
            )
        )

    def complete(self) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(
            TaskCompleted(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Projection
# =============================================================================


class TaskListProjection(DeclarativeProjection):
    """Projection that maintains a list of tasks from events."""

    def __init__(self):
        super().__init__()
        self.tasks: dict = {}

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        """Handle TaskCreated events by adding task to the list."""
        self.tasks[str(event.aggregate_id)] = {
            "title": event.title,
            "status": "pending",
        }

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        """Handle TaskCompleted events by updating task status."""
        task_id = str(event.aggregate_id)
        if task_id in self.tasks:
            self.tasks[task_id]["status"] = "completed"

    async def _truncate_read_models(self) -> None:
        """Reset the projection by clearing all tasks."""
        self.tasks.clear()


# =============================================================================
# Main Exercise Solution
# =============================================================================


async def main():
    print("=== Complete Event Flow ===\n")

    # Step 1: Create InMemoryEventBus
    bus = InMemoryEventBus()
    print("1. Created InMemoryEventBus")

    # Step 2: Create TaskListProjection
    projection = TaskListProjection()
    print("2. Created TaskListProjection")

    # Step 3: Subscribe projection to task events
    # subscribe_all() automatically subscribes to all event types
    # declared in the projection's @handles decorators
    bus.subscribe_all(projection)
    print("3. Subscribed projection to TaskCreated and TaskCompleted")

    # Step 4: Create repository with bus as publisher
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
        event_publisher=bus,  # Key: connect the event bus!
    )
    print("4. Created repository with event bus")

    # Step 5: Create and save a task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create("Complete Exercise 7-1")
    await repo.save(task)  # This automatically publishes TaskCreated to the bus!
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
        return

    # Step 7: Complete the task and verify update
    loaded = await repo.load(task_id)
    loaded.complete()
    await repo.save(loaded)  # This automatically publishes TaskCompleted to the bus!

    print("\n7. Verification after complete:")
    task_data = projection.tasks.get(str(task_id))
    if task_data:
        print(f"   Task status: {task_data['status']}")
        if task_data["status"] == "completed":
            print("\n   SUCCESS: Complete event flow working!")
        else:
            print("\n   ERROR: Task status should be 'completed'")
    else:
        print("   ERROR: Task not found in projection")

    # Bonus: Show event bus statistics
    print("\n--- Event Bus Statistics ---")
    stats = bus.get_stats()
    print(f"   Events published: {stats['events_published']}")
    print(f"   Handlers invoked: {stats['handlers_invoked']}")
    print(f"   Handler errors: {stats['handler_errors']}")


if __name__ == "__main__":
    asyncio.run(main())
