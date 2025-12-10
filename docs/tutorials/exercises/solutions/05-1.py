"""
Tutorial 5 - Exercise 1 Solution: Task Management Workflow

Complete workflow using all Phase 1 components.
Run with: python 05-1.py
"""

import asyncio
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    AggregateRepository,
    AggregateRoot,
    DomainEvent,
    InMemoryEventStore,
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
    description: str
    assigned_to: str | None = None


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    new_assignee: str


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
    description: str = ""
    assigned_to: str | None = None
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
                description=event.description,
                assigned_to=event.assigned_to,
                status="pending",
            )
        elif isinstance(event, TaskReassigned):
            if self._state:
                self._state = self._state.model_copy(
                    update={
                        "assigned_to": event.new_assignee,
                    }
                )
        elif isinstance(event, TaskCompleted) and self._state:
            self._state = self._state.model_copy(update={"status": "completed"})

    def create(self, title: str, description: str, assigned_to: str | None = None) -> None:
        if self.version > 0:
            raise ValueError("Already exists")
        self.apply_event(
            TaskCreated(
                aggregate_id=self.aggregate_id,
                title=title,
                description=description,
                assigned_to=assigned_to,
                aggregate_version=self.get_next_version(),
            )
        )

    def reassign(self, new_assignee: str) -> None:
        if not self.state or self.state.status == "completed":
            raise ValueError("Invalid state")
        self.apply_event(
            TaskReassigned(
                aggregate_id=self.aggregate_id,
                new_assignee=new_assignee,
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
# Main Workflow
# =============================================================================


async def main():
    print("=== Task Management Workflow ===\n")

    # Step 1: Create repository
    store = InMemoryEventStore()
    repo = AggregateRepository(
        event_store=store,
        aggregate_factory=TaskAggregate,
        aggregate_type="Task",
    )
    print("1. Created repository")

    # Step 2: Create a new task
    task_id = uuid4()
    task = repo.create_new(task_id)
    task.create(
        title="Complete Tutorial Series",
        description="Finish all 21 tutorials",
        assigned_to="Alice",
    )
    print(f"2. Created task: {task.state.title}")

    # Step 3: Save the task
    await repo.save(task)
    print(f"3. Saved task (version {task.version})")

    # Step 4: Load the task by ID
    loaded = await repo.load(task_id)
    print(f"4. Loaded task: {loaded.state.title} (v{loaded.version})")

    # Step 5: Reassign and complete
    loaded.reassign("Bob")
    print(f"5a. Reassigned to: {loaded.state.assigned_to}")

    loaded.complete()
    print("5b. Completed task")

    # Step 6: Save changes
    await repo.save(loaded)
    print(f"6. Saved changes (version {loaded.version})")

    # Step 7: Verify final state
    final = await repo.load(task_id)
    print("\n=== Final State ===")
    print(f"Title: {final.state.title}")
    print(f"Assigned to: {final.state.assigned_to}")
    print(f"Status: {final.state.status}")
    print(f"Version: {final.version}")

    # Step 8: Print events
    stream = await store.get_events(task_id, "Task")
    print(f"\n=== Events Stored ({len(stream.events)}) ===")
    for i, event in enumerate(stream.events):
        print(f"  {i + 1}. {event.event_type} (v{event.aggregate_version})")


if __name__ == "__main__":
    asyncio.run(main())
