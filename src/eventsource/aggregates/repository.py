"""
Repository pattern for event-sourced aggregates.

Repositories provide a clean interface for loading and saving aggregates,
abstracting away the details of event store operations.
"""

from typing import Any, Generic, TypeVar
from uuid import UUID

from eventsource.aggregates.base import AggregateRoot
from eventsource.exceptions import AggregateNotFoundError
from eventsource.stores.interface import EventPublisher, EventStore

# Type variable for aggregate root
# Using Any as the bound parameter to satisfy mypy while allowing any state type
TAggregate = TypeVar("TAggregate", bound="AggregateRoot[Any]")


class AggregateRepository(Generic[TAggregate]):
    """
    Repository for event-sourced aggregates.

    Provides a clean abstraction for loading aggregates from event history
    and persisting new events. Handles the coordination between aggregates
    and the event store.

    Features:
    - Load aggregates by reconstituting state from events
    - Save aggregates by persisting uncommitted events
    - Optional event publishing after successful save
    - Optimistic locking via event store

    The repository uses a factory pattern to create aggregate instances,
    allowing for proper dependency injection and testing.

    Example:
        >>> from eventsource import AggregateRepository, InMemoryEventStore
        >>>
        >>> store = InMemoryEventStore()
        >>> repo = AggregateRepository(
        ...     event_store=store,
        ...     aggregate_factory=OrderAggregate,
        ...     aggregate_type="Order",
        ... )
        >>>
        >>> # Create and save new aggregate
        >>> order = OrderAggregate(uuid4())
        >>> order.create(customer_id=uuid4())
        >>> await repo.save(order)
        >>>
        >>> # Load existing aggregate
        >>> loaded = await repo.load(order.aggregate_id)
        >>> assert loaded.version == order.version

    Attributes:
        _event_store: The event store for persistence
        _aggregate_factory: Factory (class) for creating aggregate instances
        _aggregate_type: String identifier for the aggregate type
        _event_publisher: Optional publisher for event distribution
    """

    def __init__(
        self,
        event_store: EventStore,
        aggregate_factory: type[TAggregate],
        aggregate_type: str,
        event_publisher: EventPublisher | None = None,
    ) -> None:
        """
        Initialize the repository.

        Args:
            event_store: Event store for persistence and retrieval
            aggregate_factory: Class to instantiate when loading aggregates
            aggregate_type: Type name of the aggregate (e.g., 'Order')
            event_publisher: Optional publisher for broadcasting events

        Example:
            >>> repo = AggregateRepository(
            ...     event_store=PostgreSQLEventStore(session_factory),
            ...     aggregate_factory=OrderAggregate,
            ...     aggregate_type="Order",
            ...     event_publisher=event_bus,
            ... )
        """
        self._event_store = event_store
        self._aggregate_factory = aggregate_factory
        self._aggregate_type = aggregate_type
        self._event_publisher = event_publisher

    @property
    def aggregate_type(self) -> str:
        """Get the aggregate type this repository manages."""
        return self._aggregate_type

    @property
    def event_store(self) -> EventStore:
        """Get the event store used by this repository."""
        return self._event_store

    @property
    def event_publisher(self) -> EventPublisher | None:
        """Get the event publisher, if configured."""
        return self._event_publisher

    async def load(self, aggregate_id: UUID) -> TAggregate:
        """
        Load an aggregate from its event history.

        Retrieves all events for the aggregate from the event store
        and reconstitutes the aggregate state by replaying them.

        Args:
            aggregate_id: ID of the aggregate to load

        Returns:
            The reconstituted aggregate with current state

        Raises:
            AggregateNotFoundError: If no events exist for the aggregate

        Example:
            >>> order = await repo.load(order_id)
            >>> print(f"Order status: {order.state.status}")
        """
        # Get events from event store
        event_stream = await self._event_store.get_events(
            aggregate_id,
            aggregate_type=self._aggregate_type,
        )

        if not event_stream.events:
            raise AggregateNotFoundError(aggregate_id, self._aggregate_type)

        # Create aggregate instance and load from history
        aggregate = self._aggregate_factory(aggregate_id)
        aggregate.load_from_history(event_stream.events)

        return aggregate

    async def load_or_create(self, aggregate_id: UUID) -> TAggregate:
        """
        Load an existing aggregate or create a new one.

        Useful when you want to work with an aggregate regardless of
        whether it already exists.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            Existing aggregate if found, or new empty aggregate

        Example:
            >>> order = await repo.load_or_create(order_id)
            >>> if order.version == 0:
            ...     order.create(customer_id=customer_id)
        """
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return self._aggregate_factory(aggregate_id)

    async def save(self, aggregate: TAggregate) -> None:
        """
        Save an aggregate by persisting its uncommitted events.

        Appends all uncommitted events to the event store atomically.
        Uses optimistic locking to detect concurrent modifications.

        After successful persistence:
        1. Marks events as committed on the aggregate
        2. Publishes events to event publisher (if configured)

        Args:
            aggregate: The aggregate to save

        Raises:
            OptimisticLockError: If there's a version conflict

        Note:
            If there are no uncommitted events, this is a no-op.

        Example:
            >>> order.ship(tracking_number="TRACK123")
            >>> await repo.save(order)
            >>> assert not order.has_uncommitted_events
        """
        uncommitted_events = aggregate.uncommitted_events

        if not uncommitted_events:
            # No changes to persist
            return

        # Calculate expected version
        # Current version minus number of new events = version before changes
        expected_version = aggregate.version - len(uncommitted_events)

        # Append events to event store
        result = await self._event_store.append_events(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type=self._aggregate_type,
            events=uncommitted_events,
            expected_version=expected_version,
        )

        if result.success:
            # Mark events as committed on the aggregate
            aggregate.mark_events_as_committed()

            # Publish events if publisher is configured
            if self._event_publisher:
                await self._event_publisher.publish(uncommitted_events)

    async def exists(self, aggregate_id: UUID) -> bool:
        """
        Check if an aggregate exists.

        Args:
            aggregate_id: ID of the aggregate to check

        Returns:
            True if aggregate has events, False otherwise

        Example:
            >>> if await repo.exists(order_id):
            ...     order = await repo.load(order_id)
        """
        event_stream = await self._event_store.get_events(
            aggregate_id,
            aggregate_type=self._aggregate_type,
        )
        return len(event_stream.events) > 0

    async def get_version(self, aggregate_id: UUID) -> int:
        """
        Get the current version of an aggregate.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            Current version (0 if aggregate doesn't exist)
        """
        event_stream = await self._event_store.get_events(
            aggregate_id,
            aggregate_type=self._aggregate_type,
        )
        return event_stream.version

    async def get_or_raise(self, aggregate_id: UUID) -> TAggregate:
        """
        Get an aggregate, raising if it doesn't exist.

        This is an alias for load() that makes the intent clearer
        in calling code.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            The loaded aggregate

        Raises:
            AggregateNotFoundError: If aggregate doesn't exist
        """
        return await self.load(aggregate_id)

    def create_new(self, aggregate_id: UUID) -> TAggregate:
        """
        Create a new, empty aggregate instance.

        This does not persist anything - it just creates an in-memory
        aggregate that can have commands applied and then saved.

        Args:
            aggregate_id: ID for the new aggregate

        Returns:
            New aggregate instance with version 0

        Example:
            >>> order = repo.create_new(uuid4())
            >>> order.create(customer_id=customer_id)
            >>> await repo.save(order)
        """
        return self._aggregate_factory(aggregate_id)


__all__ = [
    "AggregateRepository",
    "TAggregate",
]
