"""
Tutorial 6 - Exercise 1 Solution: Task Statistics Projection

This solution demonstrates how to build a projection that tracks
various task statistics including counts, assignee distribution,
and completion rates.
"""

import asyncio
from collections import defaultdict
from uuid import UUID, uuid4

from eventsource import (
    DeclarativeProjection,
    DomainEvent,
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
    assigned_to: UUID | None = None


@register_event
class TaskCompleted(DomainEvent):
    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


@register_event
class TaskReassigned(DomainEvent):
    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    previous_assignee: UUID | None
    new_assignee: UUID


# =============================================================================
# Solution: TaskStatisticsProjection
# =============================================================================


class TaskStatisticsProjection(DeclarativeProjection):
    """
    Projection that tracks task statistics.

    This projection maintains:
    - Total count of tasks created
    - Count of completed vs pending tasks
    - Count of tasks per assignee

    The projection also tracks which task is assigned to whom
    so that reassignments can correctly update the per-assignee counts.
    """

    def __init__(self):
        super().__init__()
        # Counters
        self.total_created = 0
        self.completed_count = 0
        self.pending_count = 0

        # Tasks per assignee (including None for unassigned)
        self.tasks_by_assignee: dict[UUID | None, int] = defaultdict(int)

        # Internal tracking: which task is currently assigned to whom
        # This is needed to correctly update counts on reassignment
        self._task_assignees: dict[UUID, UUID | None] = {}

    # =========================================================================
    # Event Handlers
    # =========================================================================

    @handles(TaskCreated)
    async def _on_created(self, event: TaskCreated) -> None:
        """
        Handle task creation.

        Increments total and pending counts, and updates assignee distribution.
        """
        self.total_created += 1
        self.pending_count += 1
        self.tasks_by_assignee[event.assigned_to] += 1
        self._task_assignees[event.aggregate_id] = event.assigned_to

    @handles(TaskCompleted)
    async def _on_completed(self, event: TaskCompleted) -> None:
        """
        Handle task completion.

        Moves the task from pending to completed count.
        """
        self.completed_count += 1
        self.pending_count -= 1

    @handles(TaskReassigned)
    async def _on_reassigned(self, event: TaskReassigned) -> None:
        """
        Handle task reassignment.

        Updates the per-assignee counts by decrementing the old assignee
        and incrementing the new assignee.
        """
        # Get the current assignee from our tracking
        old_assignee = self._task_assignees.get(event.aggregate_id)

        # Decrement old assignee count
        if old_assignee in self.tasks_by_assignee:
            self.tasks_by_assignee[old_assignee] -= 1
            # Clean up zero counts
            if self.tasks_by_assignee[old_assignee] <= 0:
                del self.tasks_by_assignee[old_assignee]

        # Increment new assignee count
        self.tasks_by_assignee[event.new_assignee] += 1

        # Update our tracking
        self._task_assignees[event.aggregate_id] = event.new_assignee

    # =========================================================================
    # Reset
    # =========================================================================

    async def _truncate_read_models(self) -> None:
        """Clear all statistics for rebuild."""
        self.total_created = 0
        self.completed_count = 0
        self.pending_count = 0
        self.tasks_by_assignee.clear()
        self._task_assignees.clear()

    # =========================================================================
    # Query Methods
    # =========================================================================

    def get_total_created(self) -> int:
        """Get total number of tasks ever created."""
        return self.total_created

    def get_completion_stats(self) -> dict[str, int]:
        """
        Get completion statistics.

        Returns:
            Dictionary with 'completed', 'pending', and 'total' counts.
        """
        return {
            "completed": self.completed_count,
            "pending": self.pending_count,
            "total": self.total_created,
        }

    def get_tasks_per_assignee(self) -> dict[UUID | None, int]:
        """
        Get task counts per assignee.

        Returns:
            Dictionary mapping assignee UUID to task count.
            None key represents unassigned tasks.
        """
        return dict(self.tasks_by_assignee)

    def get_completion_rate(self) -> float:
        """
        Get the completion rate as a decimal (0.0 to 1.0).

        Returns:
            Completion rate, or 0.0 if no tasks exist.
        """
        if self.total_created == 0:
            return 0.0
        return self.completed_count / self.total_created

    def get_summary(self) -> str:
        """
        Get a human-readable summary of statistics.

        Returns:
            Formatted string with all statistics.
        """
        assignee_summary = ", ".join(
            f"{uid}: {count}" if uid else f"unassigned: {count}"
            for uid, count in self.tasks_by_assignee.items()
        )
        return (
            f"Tasks: {self.total_created} total, "
            f"{self.completed_count} completed, "
            f"{self.pending_count} pending "
            f"({self.get_completion_rate():.0%} completion rate)\n"
            f"By assignee: {assignee_summary or 'none'}"
        )


# =============================================================================
# Demo
# =============================================================================


async def main():
    """Demonstrate the TaskStatisticsProjection."""
    print("=== Task Statistics Projection Demo ===\n")

    projection = TaskStatisticsProjection()

    # Show subscribed events
    print("Subscribed to events:")
    for event_type in projection.subscribed_to():
        print(f"  - {event_type.__name__}")
    print()

    # Create some users
    user1 = uuid4()
    user2 = uuid4()
    user3 = uuid4()
    task_ids = []

    # Create 10 tasks with various assignments
    print("Creating 10 tasks...")
    for i in range(10):
        task_id = uuid4()
        task_ids.append(task_id)

        # Distribute tasks: 4 to user1, 3 to user2, 2 to user3, 1 unassigned
        if i < 4:
            assignee = user1
        elif i < 7:
            assignee = user2
        elif i < 9:
            assignee = user3
        else:
            assignee = None

        await projection.handle(
            TaskCreated(
                aggregate_id=task_id,
                title=f"Task {i + 1}",
                assigned_to=assignee,
                aggregate_version=1,
            )
        )

    print("\nAfter creating 10 tasks:")
    print(projection.get_summary())
    print()

    # Complete some tasks
    print("Completing 4 tasks...")
    for task_id in task_ids[:4]:
        await projection.handle(
            TaskCompleted(
                aggregate_id=task_id,
                aggregate_version=2,
            )
        )

    print("\nAfter completing 4 tasks:")
    print(projection.get_summary())
    print()

    # Reassign some tasks
    print("Reassigning 2 tasks from user1 to user3...")
    # Note: tasks 0-3 were assigned to user1, but we completed them
    # So let's reassign tasks 4-5 (which were assigned to user2) to user3
    for task_id in task_ids[4:6]:
        await projection.handle(
            TaskReassigned(
                aggregate_id=task_id,
                previous_assignee=user2,  # These were originally user2's tasks
                new_assignee=user3,
                aggregate_version=2,
            )
        )

    print("\nAfter reassignments:")
    print(projection.get_summary())
    print()

    # Show detailed stats
    print("=== Detailed Statistics ===")
    stats = projection.get_completion_stats()
    print(f"Total created: {stats['total']}")
    print(f"Completed: {stats['completed']}")
    print(f"Pending: {stats['pending']}")
    print(f"Completion rate: {projection.get_completion_rate():.1%}")
    print()

    print("Tasks by assignee:")
    for assignee, count in projection.get_tasks_per_assignee().items():
        label = "Unassigned" if assignee is None else f"User {str(assignee)[:8]}"
        print(f"  {label}: {count}")

    # Demonstrate reset
    print("\n=== Reset Demo ===")
    print(f"Before reset: {projection.get_total_created()} tasks")
    await projection.reset()
    print(f"After reset: {projection.get_total_created()} tasks")


if __name__ == "__main__":
    asyncio.run(main())
