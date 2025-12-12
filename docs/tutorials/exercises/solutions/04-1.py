"""
Tutorial 4 - Exercise 1 Solution: Conflict Resolution

This solution demonstrates how to detect and recover from
optimistic locking conflicts when working with event stores.

Run with: python 04-1.py
"""

import asyncio
from uuid import uuid4

from eventsource import (
    DomainEvent,
    InMemoryEventStore,
    OptimisticLockError,
    register_event,
)


@register_event
class TaskCreated(DomainEvent):
    """Event emitted when a task is created."""

    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str


@register_event
class TaskUpdated(DomainEvent):
    """Event emitted when a task's title is updated."""

    event_type: str = "TaskUpdated"
    aggregate_type: str = "Task"
    new_title: str


async def main():
    """Demonstrate conflict resolution with event stores."""
    store = InMemoryEventStore()
    task_id = uuid4()

    print("=" * 60)
    print("Exercise 1: Conflict Resolution")
    print("=" * 60)
    print()

    # Step 1: Create the task
    print("Step 1: Create the task...")
    await store.append_events(
        aggregate_id=task_id,
        aggregate_type="Task",
        events=[
            TaskCreated(
                aggregate_id=task_id,
                title="Original Title",
                aggregate_version=1,
            )
        ],
        expected_version=0,
    )
    print("  Task created at version 1")
    print()

    # Step 2: Load events and note version
    print("Step 2: Load events and check version...")
    stream = await store.get_events(task_id, "Task")
    print(f"  Current version: {stream.version}")
    print(f"  Events in stream: {len(stream.events)}")
    print()

    # Step 3: Try to append with WRONG expected_version
    wrong_version = 0  # Should be 1!
    print(f"Step 3: Try to append with wrong version ({wrong_version})...")

    try:
        await store.append_events(
            aggregate_id=task_id,
            aggregate_type="Task",
            events=[
                TaskUpdated(
                    aggregate_id=task_id,
                    new_title="Updated Title",
                    aggregate_version=2,
                )
            ],
            expected_version=wrong_version,
        )
        print("  Unexpected success - this should have failed!")
    except OptimisticLockError as e:
        # Step 4: Handle the error
        print("  Caught OptimisticLockError!")
        print(f"    Expected version: {e.expected_version}")
        print(f"    Actual version: {e.actual_version}")
        print(f"    Aggregate ID: {e.aggregate_id}")
        print()

        # Step 5: Retry with correct version
        print(f"Step 5: Retry with correct version ({e.actual_version})...")

        # In a real application, you would typically:
        # 1. Reload the aggregate to get fresh state
        # 2. Re-validate business rules
        # 3. Then retry the operation

        result = await store.append_events(
            aggregate_id=task_id,
            aggregate_type="Task",
            events=[
                TaskUpdated(
                    aggregate_id=task_id,
                    new_title="Updated Title",
                    aggregate_version=e.actual_version + 1,
                )
            ],
            expected_version=e.actual_version,
        )

        print("  Retry successful!")
        print(f"    New version: {result.new_version}")
        print(f"    Global position: {result.global_position}")
        print()

    # Verify final state
    print("Verification: Final state of the task...")
    final_stream = await store.get_events(task_id, "Task")
    print(f"  Final version: {final_stream.version}")
    print("  Event history:")
    for i, event in enumerate(final_stream.events, 1):
        print(f"    {i}. {event.event_type}")
        if hasattr(event, "title"):
            print(f"       title: {event.title}")
        if hasattr(event, "new_title"):
            print(f"       new_title: {event.new_title}")

    print()
    print("=" * 60)
    print("Exercise 1 Complete!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
