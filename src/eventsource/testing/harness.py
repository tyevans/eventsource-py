"""
Test harness providing pre-configured in-memory infrastructure.

This module provides an InMemoryTestHarness class that bundles all the in-memory
implementations needed for testing event-sourced applications. It eliminates
boilerplate setup code and ensures consistent configuration across tests.

Example:
    >>> from eventsource.testing import InMemoryTestHarness
    >>>
    >>> async def test_order_processing():
    ...     harness = InMemoryTestHarness()
    ...     repo = harness.create_repository(OrderAggregate)
    ...
    ...     # Create and save an aggregate
    ...     order = OrderAggregate.create(customer_id=customer_id)
    ...     await repo.save(order)
    ...
    ...     # Verify events were published
    ...     assert len(harness.published_events) == 1
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.stores.in_memory import InMemoryEventStore

if TYPE_CHECKING:
    pass


class InMemoryTestHarness:
    """
    Pre-configured test infrastructure for event sourcing tests.

    Provides in-memory implementations of all infrastructure components,
    wired together and ready to use. This eliminates boilerplate setup
    in tests and ensures consistent configuration.

    Components provided:
    - event_store: InMemoryEventStore for persisting events
    - event_bus: InMemoryEventBus for publishing/subscribing to events
    - checkpoint_repo: InMemoryCheckpointRepository for projection checkpoints
    - dlq_repo: InMemoryDLQRepository for dead letter queue

    Example:
        >>> @pytest.fixture
        ... def harness():
        ...     h = InMemoryTestHarness()
        ...     yield h
        ...     h.reset()
        >>>
        >>> async def test_order_creation(harness):
        ...     # Use harness.event_store, harness.event_bus, etc.
        ...     await harness.event_store.append_events(...)
        ...     assert len(harness.published_events) == 1

    Thread Safety:
        The harness itself is not thread-safe, but the underlying
        components (InMemoryEventBus) have thread-safe operations.
        Use one harness per test to avoid cross-test contamination.
    """

    def __init__(self) -> None:
        """
        Initialize the test harness with fresh in-memory components.

        All components are created immediately and ready to use.
        Tracing is disabled by default for test performance and
        to avoid polluting traces.
        """
        self._event_store = InMemoryEventStore(enable_tracing=False)
        self._event_bus = InMemoryEventBus(enable_tracing=False)
        self._checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)
        self._dlq_repo = InMemoryDLQRepository(enable_tracing=False)

    @property
    def event_store(self) -> InMemoryEventStore:
        """
        Get the in-memory event store.

        Returns:
            InMemoryEventStore instance for persisting and retrieving events

        Example:
            >>> await harness.event_store.append_events(
            ...     aggregate_id=order_id,
            ...     aggregate_type="Order",
            ...     events=[order_created],
            ...     expected_version=0,
            ... )
        """
        return self._event_store

    @property
    def event_bus(self) -> InMemoryEventBus:
        """
        Get the in-memory event bus.

        Returns:
            InMemoryEventBus instance for publishing and subscribing to events

        Example:
            >>> harness.event_bus.subscribe(OrderCreated, my_handler)
            >>> await harness.event_bus.publish([order_created])
        """
        return self._event_bus

    @property
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        """
        Get the in-memory checkpoint repository.

        Returns:
            InMemoryCheckpointRepository instance for projection checkpoints

        Example:
            >>> await harness.checkpoint_repo.save_position("my-projection", 42, event_id, "EventType")
            >>> position = await harness.checkpoint_repo.get_position("my-projection")
        """
        return self._checkpoint_repo

    @property
    def dlq_repo(self) -> InMemoryDLQRepository:
        """
        Get the in-memory dead letter queue repository.

        Returns:
            InMemoryDLQRepository instance for failed event handling

        Example:
            >>> await harness.dlq_repo.add_failed_event(
            ...     event_id=event_id,
            ...     projection_name="handler-name",
            ...     event_type="MyEvent",
            ...     event_data={},
            ...     error=Exception("Test error"),
            ... )
        """
        return self._dlq_repo

    @property
    def published_events(self) -> list[DomainEvent]:
        """
        Get all events published through the event bus.

        This is a convenience property that delegates to the event bus's
        published_events property. Useful for assertions in tests.

        Returns:
            List of all published events in publication order

        Example:
            >>> await harness.event_bus.publish([event1, event2])
            >>> assert len(harness.published_events) == 2
            >>> assert harness.published_events[0] == event1
        """
        return self._event_bus.published_events

    def reset(self) -> None:
        """
        Reset all state and create fresh components.

        Call this between tests to ensure complete isolation.
        Creates new instances of all components.

        Example:
            >>> @pytest.fixture
            ... def harness():
            ...     h = InMemoryTestHarness()
            ...     yield h
            ...     h.reset()  # Clean up after test
        """
        self._event_store = InMemoryEventStore(enable_tracing=False)
        self._event_bus = InMemoryEventBus(enable_tracing=False)
        self._checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)
        self._dlq_repo = InMemoryDLQRepository(enable_tracing=False)

    def clear_published_events(self) -> None:
        """
        Clear only the published events, keeping other state.

        Useful when you want to verify events published after a certain point
        without resetting the entire harness.

        Example:
            >>> # Setup phase publishes some events
            >>> await setup_test_data(harness)
            >>> harness.clear_published_events()
            >>> # Now test only new events
            >>> await execute_command(harness)
            >>> assert len(harness.published_events) == 1
        """
        self._event_bus.clear_published_events()

    def get_events_of_type(self, event_type: type[DomainEvent]) -> list[DomainEvent]:
        """
        Filter published events by type.

        This is a convenience method for filtering published events
        to only those matching a specific event type.

        Args:
            event_type: The event class to filter by

        Returns:
            List of events matching the type

        Example:
            >>> await harness.event_bus.publish([order_created, order_shipped])
            >>> shipped_events = harness.get_events_of_type(OrderShipped)
            >>> assert len(shipped_events) == 1
        """
        return [event for event in self.published_events if isinstance(event, event_type)]

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return f"InMemoryTestHarness(published={len(self.published_events)})"


__all__ = ["InMemoryTestHarness"]
