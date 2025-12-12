"""
Tutorial 8 - Exercise 1 Solution: Complete Test Suite

This solution demonstrates comprehensive testing patterns for event-sourced
applications. All tests follow the Given-When-Then pattern.

Run with: pytest 08-1.py -v
"""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource import (
    AggregateRoot,
    DomainEvent,
    register_event,
)

# =============================================================================
# Domain Code (from Tutorial)
# =============================================================================


@register_event
class TaskCreated(DomainEvent):
    """Event emitted when a task is created."""

    event_type: str = "TaskCreated"
    aggregate_type: str = "Task"
    title: str
    description: str


@register_event
class TaskCompleted(DomainEvent):
    """Event emitted when a task is completed."""

    event_type: str = "TaskCompleted"
    aggregate_type: str = "Task"


@register_event
class TaskReassigned(DomainEvent):
    """Event emitted when a task is reassigned."""

    event_type: str = "TaskReassigned"
    aggregate_type: str = "Task"
    previous_assignee: UUID | None
    new_assignee: UUID


class TaskState(BaseModel):
    """State model for tasks."""

    task_id: UUID
    title: str = ""
    description: str = ""
    assigned_to: UUID | None = None
    status: str = "pending"


class TaskAggregate(AggregateRoot[TaskState]):
    """Task aggregate with business rules."""

    aggregate_type = "Task"

    def _get_initial_state(self) -> TaskState:
        return TaskState(task_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, TaskCreated):
            self._state = TaskState(
                task_id=self.aggregate_id,
                title=event.title,
                description=event.description,
                status="pending",
            )
        elif isinstance(event, TaskCompleted):
            if self._state:
                self._state = self._state.model_copy(update={"status": "completed"})
        elif isinstance(event, TaskReassigned) and self._state:
            self._state = self._state.model_copy(update={"assigned_to": event.new_assignee})

    def create(self, title: str, description: str) -> None:
        """Command: Create a new task."""
        if self.version > 0:
            raise ValueError("Task already exists")
        self.apply_event(
            TaskCreated(
                aggregate_id=self.aggregate_id,
                title=title,
                description=description,
                aggregate_version=self.get_next_version(),
            )
        )

    def complete(self) -> None:
        """Command: Complete the task."""
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Task already completed")
        self.apply_event(
            TaskCompleted(
                aggregate_id=self.aggregate_id,
                aggregate_version=self.get_next_version(),
            )
        )

    def reassign(self, new_assignee: UUID) -> None:
        """Command: Reassign the task to a new user."""
        if not self.state:
            raise ValueError("Task does not exist")
        if self.state.status == "completed":
            raise ValueError("Cannot reassign completed task")
        self.apply_event(
            TaskReassigned(
                aggregate_id=self.aggregate_id,
                previous_assignee=self.state.assigned_to,
                new_assignee=new_assignee,
                aggregate_version=self.get_next_version(),
            )
        )


# =============================================================================
# Exercise 1 Solution: Complete Test Suite
# =============================================================================


class TestTaskAggregateComplete:
    """Complete test suite for TaskAggregate.

    This test suite covers all business rules with explicit, focused tests.
    Each test follows the Given-When-Then pattern.
    """

    # =========================================================================
    # 1. Test create command produces TaskCreated
    # =========================================================================

    def test_create_produces_event(self):
        """Given a new task, when create is called, then TaskCreated is emitted."""
        # Given
        task = TaskAggregate(uuid4())

        # When
        task.create("Test Task", "Test Description")

        # Then
        assert len(task.uncommitted_events) == 1
        event = task.uncommitted_events[0]
        assert isinstance(event, TaskCreated)
        assert event.title == "Test Task"
        assert event.description == "Test Description"

    def test_create_updates_state(self):
        """Given a new task, when create is called, then state is updated."""
        # Given
        task = TaskAggregate(uuid4())

        # When
        task.create("Test Task", "Test Description")

        # Then
        assert task.state is not None
        assert task.state.title == "Test Task"
        assert task.state.description == "Test Description"
        assert task.state.status == "pending"

    def test_create_sets_version(self):
        """Given a new task, when create is called, then version is 1."""
        # Given
        task = TaskAggregate(uuid4())

        # When
        task.create("Test Task", "Description")

        # Then
        assert task.version == 1

    def test_cannot_create_twice(self):
        """Given an existing task, when create is called again, then error is raised."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("First", "Description")

        # When/Then
        with pytest.raises(ValueError, match="already exists"):
            task.create("Second", "Description")

    # =========================================================================
    # 2. Test complete command produces TaskCompleted
    # =========================================================================

    def test_complete_produces_event(self):
        """Given a pending task, when complete is called, then TaskCompleted is emitted."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")
        task.mark_events_as_committed()  # Clear for clean assertion

        # When
        task.complete()

        # Then
        assert len(task.uncommitted_events) == 1
        assert isinstance(task.uncommitted_events[0], TaskCompleted)

    def test_complete_updates_status(self):
        """Given a pending task, when complete is called, then status is completed."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")

        # When
        task.complete()

        # Then
        assert task.state.status == "completed"

    # =========================================================================
    # 3. Test cannot complete twice (business rule)
    # =========================================================================

    def test_cannot_complete_twice(self):
        """Given a completed task, when complete is called again, then error is raised."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")
        task.complete()

        # When/Then
        with pytest.raises(ValueError, match="already completed"):
            task.complete()

    # =========================================================================
    # 4. Test cannot complete non-existent task
    # =========================================================================

    def test_cannot_complete_nonexistent(self):
        """Given a new task (not created), when complete is called, then error is raised."""
        # Given
        task = TaskAggregate(uuid4())

        # When/Then
        with pytest.raises(ValueError, match="does not exist"):
            task.complete()

    # =========================================================================
    # 5. Test reassign command
    # =========================================================================

    def test_reassign_produces_event(self):
        """Given a pending task, when reassign is called, then TaskReassigned is emitted."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")
        task.mark_events_as_committed()
        new_user = uuid4()

        # When
        task.reassign(new_user)

        # Then
        assert len(task.uncommitted_events) == 1
        event = task.uncommitted_events[0]
        assert isinstance(event, TaskReassigned)
        assert event.new_assignee == new_user

    def test_reassign_captures_previous_assignee(self):
        """Given a task with an assignee, when reassigned, both assignees are captured."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")
        user1 = uuid4()
        user2 = uuid4()
        task.reassign(user1)
        task.mark_events_as_committed()

        # When
        task.reassign(user2)

        # Then
        event = task.uncommitted_events[0]
        assert isinstance(event, TaskReassigned)
        assert event.previous_assignee == user1
        assert event.new_assignee == user2

    def test_initial_reassign_has_none_previous(self):
        """Given a task with no assignee, when reassigned, previous_assignee is None."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")
        task.mark_events_as_committed()
        user = uuid4()

        # When
        task.reassign(user)

        # Then
        event = task.uncommitted_events[0]
        assert event.previous_assignee is None
        assert event.new_assignee == user

    def test_reassign_updates_state(self):
        """Given a pending task, when reassign is called, then state is updated."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")
        user = uuid4()

        # When
        task.reassign(user)

        # Then
        assert task.state.assigned_to == user

    # =========================================================================
    # 6. Test cannot reassign completed task (business rule)
    # =========================================================================

    def test_cannot_reassign_completed_task(self):
        """Given a completed task, when reassign is called, then error is raised."""
        # Given
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")
        task.complete()

        # When/Then
        with pytest.raises(ValueError, match="Cannot reassign"):
            task.reassign(uuid4())

    def test_cannot_reassign_nonexistent_task(self):
        """Given a new task (not created), when reassign is called, then error is raised."""
        # Given
        task = TaskAggregate(uuid4())

        # When/Then
        with pytest.raises(ValueError, match="does not exist"):
            task.reassign(uuid4())

    # =========================================================================
    # 7. Test event replay reconstructs state
    # =========================================================================

    def test_replay_reconstructs_state(self):
        """Given historical events, when loaded, then state is correctly reconstructed."""
        # Given
        task_id = uuid4()
        user = uuid4()
        events = [
            TaskCreated(
                aggregate_id=task_id,
                title="Replay Test",
                description="Testing replay",
                aggregate_version=1,
            ),
            TaskReassigned(
                aggregate_id=task_id,
                previous_assignee=None,
                new_assignee=user,
                aggregate_version=2,
            ),
            TaskCompleted(
                aggregate_id=task_id,
                aggregate_version=3,
            ),
        ]

        # When
        task = TaskAggregate(task_id)
        task.load_from_history(events)

        # Then
        assert task.version == 3
        assert task.state.title == "Replay Test"
        assert task.state.description == "Testing replay"
        assert task.state.assigned_to == user
        assert task.state.status == "completed"
        assert len(task.uncommitted_events) == 0  # No new events

    def test_replay_empty_history(self):
        """Given empty history, when loaded, then aggregate is at initial state."""
        # Given
        task_id = uuid4()

        # When
        task = TaskAggregate(task_id)
        task.load_from_history([])

        # Then
        assert task.version == 0
        assert task.state is None
        assert len(task.uncommitted_events) == 0

    def test_replay_partial_history(self):
        """Given partial history (only created), then state reflects that."""
        # Given
        task_id = uuid4()
        events = [
            TaskCreated(
                aggregate_id=task_id,
                title="Partial Test",
                description="Partial replay",
                aggregate_version=1,
            ),
        ]

        # When
        task = TaskAggregate(task_id)
        task.load_from_history(events)

        # Then
        assert task.version == 1
        assert task.state.status == "pending"
        assert task.state.assigned_to is None

    def test_can_apply_commands_after_replay(self):
        """Given replayed history, commands should continue from correct version."""
        # Given
        task_id = uuid4()
        events = [
            TaskCreated(
                aggregate_id=task_id,
                title="Continued Task",
                description="Will be completed",
                aggregate_version=1,
            ),
        ]
        task = TaskAggregate(task_id)
        task.load_from_history(events)

        # When
        task.complete()

        # Then
        assert task.version == 2
        assert task.state.status == "completed"
        assert len(task.uncommitted_events) == 1
        assert task.uncommitted_events[0].aggregate_version == 2


# =============================================================================
# Additional Tests: Edge Cases
# =============================================================================


class TestTaskAggregateEdgeCases:
    """Additional tests for edge cases and boundary conditions."""

    def test_event_aggregate_id_matches_aggregate(self):
        """Events should have the same aggregate_id as the aggregate."""
        task_id = uuid4()
        task = TaskAggregate(task_id)
        task.create("Task", "Description")

        event = task.uncommitted_events[0]
        assert event.aggregate_id == task_id

    def test_event_versions_are_sequential(self):
        """Event versions should be sequential starting from 1."""
        task = TaskAggregate(uuid4())
        user = uuid4()

        task.create("Task", "Description")
        task.reassign(user)
        task.complete()

        events = task.uncommitted_events
        assert events[0].aggregate_version == 1
        assert events[1].aggregate_version == 2
        assert events[2].aggregate_version == 3

    def test_state_is_immutable_between_events(self):
        """State should be a new object after each event (immutability)."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")

        state1 = task.state
        task.reassign(uuid4())
        state2 = task.state

        # States should be different objects
        assert state1 is not state2

    def test_multiple_reassignments_track_history(self):
        """Multiple reassignments should correctly track the chain."""
        task = TaskAggregate(uuid4())
        task.create("Task", "Description")

        user1 = uuid4()
        user2 = uuid4()
        user3 = uuid4()

        task.reassign(user1)
        task.reassign(user2)
        task.reassign(user3)

        events = task.uncommitted_events
        reassign_events = [e for e in events if isinstance(e, TaskReassigned)]

        assert len(reassign_events) == 3
        assert reassign_events[0].previous_assignee is None
        assert reassign_events[0].new_assignee == user1
        assert reassign_events[1].previous_assignee == user1
        assert reassign_events[1].new_assignee == user2
        assert reassign_events[2].previous_assignee == user2
        assert reassign_events[2].new_assignee == user3


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
