"""
Repository pattern for event-sourced aggregates.

Repositories provide a clean interface for loading and saving aggregates,
abstracting away the details of event store operations.
"""

import logging
from typing import TYPE_CHECKING, Any, Generic, Literal, TypeVar
from uuid import UUID

from eventsource.aggregates.base import AggregateRoot
from eventsource.aggregates.snapshot_manager import AggregateSnapshotManager
from eventsource.exceptions import AggregateNotFoundError
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_VERSION,
)
from eventsource.snapshots.strategies import (
    SnapshotStrategy,
    create_snapshot_strategy,
)
from eventsource.stores.interface import EventPublisher, EventStore

if TYPE_CHECKING:
    from eventsource.snapshots import Snapshot, SnapshotStore

logger = logging.getLogger(__name__)

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
    - **Optional snapshot support for fast aggregate loading**

    The repository uses a factory pattern to create aggregate instances,
    allowing for proper dependency injection and testing.

    Snapshot Configuration:
        To enable snapshotting, provide a snapshot_store and optionally
        configure when snapshots are created:

        >>> from eventsource.snapshots import InMemorySnapshotStore
        >>>
        >>> repo = AggregateRepository(
        ...     event_store=event_store,
        ...     aggregate_factory=OrderAggregate,
        ...     aggregate_type="Order",
        ...     # Snapshot configuration
        ...     snapshot_store=InMemorySnapshotStore(),
        ...     snapshot_threshold=100,  # Create snapshot every 100 events
        ...     snapshot_mode="sync",    # "sync" | "background" | "manual"
        ... )

    Snapshot Modes:
        - "sync": Create snapshot synchronously after save (default).
                 Simplest and most predictable, but adds latency to saves.
        - "background": Create snapshot asynchronously in background task.
                       Best for high-throughput scenarios.
        - "manual": Never create snapshots automatically.
                   Use create_snapshot() method explicitly.

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
        _snapshot_store: Optional snapshot store for state caching
        _snapshot_threshold: Events between automatic snapshots
        _snapshot_mode: When to create snapshots ("sync", "background", "manual")
    """

    def __init__(
        self,
        event_store: EventStore,
        aggregate_factory: type[TAggregate],
        aggregate_type: str,
        event_publisher: EventPublisher | None = None,
        # Snapshot configuration
        snapshot_store: "SnapshotStore | None" = None,
        snapshot_threshold: int | None = None,
        snapshot_mode: Literal["sync", "background", "manual"] = "sync",
        # Tracing configuration
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the repository.

        Args:
            event_store: Event store for persistence and retrieval
            aggregate_factory: Class to instantiate when loading aggregates
            aggregate_type: Type name of the aggregate (e.g., 'Order')
            event_publisher: Optional publisher for broadcasting events
            snapshot_store: Optional snapshot store for state caching.
                          When provided, enables snapshot-aware loading.
            snapshot_threshold: Number of events between automatic snapshots.
                              If None, snapshots are only created manually or
                              when explicitly calling create_snapshot().
                              Example: 100 means create snapshot every 100 events.
            snapshot_mode: When to create snapshots:
                          - "sync": Immediately after save (blocking)
                          - "background": Asynchronously after save
                          - "manual": Only via explicit create_snapshot() call
                          Default is "sync" for simplicity.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Defaults to True for consistency with other components.
                          Ignored if tracer is explicitly provided.

        Example:
            >>> # Basic repository (no snapshots)
            >>> repo = AggregateRepository(
            ...     event_store=PostgreSQLEventStore(session_factory),
            ...     aggregate_factory=OrderAggregate,
            ...     aggregate_type="Order",
            ... )
            >>>
            >>> # With snapshot support
            >>> repo = AggregateRepository(
            ...     event_store=PostgreSQLEventStore(session_factory),
            ...     aggregate_factory=OrderAggregate,
            ...     aggregate_type="Order",
            ...     snapshot_store=PostgreSQLSnapshotStore(session_factory),
            ...     snapshot_threshold=100,
            ...     snapshot_mode="background",
            ... )

        Note:
            If snapshot_store is provided but snapshot_threshold is None,
            snapshots must be created manually via create_snapshot().
            This is useful when you want control over exactly when
            snapshots are taken (e.g., after major state transitions).
        """
        # Initialize tracing via composition (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self._event_store = event_store
        self._aggregate_factory = aggregate_factory
        self._aggregate_type = aggregate_type
        self._event_publisher = event_publisher

        # Snapshot configuration (exposed via public properties)
        self._snapshot_store = snapshot_store
        self._snapshot_threshold = snapshot_threshold
        self._snapshot_mode = snapshot_mode

        # Create snapshot strategy from mode/threshold for OCP compliance
        self._snapshot_strategy: SnapshotStrategy | None = None
        if snapshot_store is not None:
            self._snapshot_strategy = create_snapshot_strategy(snapshot_mode, snapshot_threshold)

            # Create snapshot manager for load/validation operations
            self._snapshot_manager: AggregateSnapshotManager[TAggregate] | None = (
                AggregateSnapshotManager(
                    snapshot_store=snapshot_store,
                    aggregate_type=aggregate_type,
                    strategy=self._snapshot_strategy,
                    enable_tracing=enable_tracing,
                )
            )
        else:
            self._snapshot_manager = None

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

    @property
    def snapshot_store(self) -> "SnapshotStore | None":
        """Get the snapshot store, if configured."""
        return self._snapshot_store

    @property
    def snapshot_threshold(self) -> int | None:
        """Get the snapshot threshold (events between snapshots)."""
        return self._snapshot_threshold

    @property
    def snapshot_mode(self) -> Literal["sync", "background", "manual"]:
        """Get the snapshot creation mode."""
        return self._snapshot_mode

    @property
    def has_snapshot_support(self) -> bool:
        """Check if snapshot support is enabled."""
        return self._snapshot_store is not None

    async def load(self, aggregate_id: UUID) -> TAggregate:
        """
        Load an aggregate from its event history.

        If a snapshot store is configured and a valid snapshot exists,
        the aggregate is restored from the snapshot and only events
        since the snapshot are replayed. This significantly improves
        load time for aggregates with many events.

        Retrieves all events for the aggregate from the event store
        and reconstitutes the aggregate state by replaying them.

        Args:
            aggregate_id: ID of the aggregate to load

        Returns:
            The reconstituted aggregate with current state

        Raises:
            AggregateNotFoundError: If no events exist for the aggregate

        Loading Sequence:
            1. Check for valid snapshot (if snapshot_store configured)
            2. If snapshot valid: restore state, get events from snapshot.version
            3. If no snapshot: get all events from version 0
            4. Apply events to aggregate
            5. Return hydrated aggregate

        Example:
            >>> order = await repo.load(order_id)
            >>> print(f"Order status: {order.state.status}")
        """
        with self._tracer.span(
            "eventsource.repository.load",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
            },
        ) as span:
            from_version = 0
            snapshot = None

            # Try to load from snapshot if configured (using manager)
            if self._snapshot_manager is not None:
                snapshot = await self._snapshot_manager.load_valid_snapshot(
                    aggregate_id, self._aggregate_factory
                )
                if snapshot is not None:
                    from_version = snapshot.version
                    if span:
                        span.set_attribute("snapshot.used", True)
                        span.set_attribute("snapshot.version", from_version)
                    logger.debug(
                        "Using snapshot for %s/%s at version %d",
                        self._aggregate_type,
                        aggregate_id,
                        from_version,
                    )

            # Get events from event store (from snapshot version or 0)
            event_stream = await self._event_store.get_events(
                aggregate_id,
                aggregate_type=self._aggregate_type,
                from_version=from_version,
            )

            # Handle case: no snapshot and no events
            if snapshot is None and not event_stream.events:
                raise AggregateNotFoundError(aggregate_id, self._aggregate_type)

            # Create aggregate instance
            aggregate = self._aggregate_factory(aggregate_id)

            # Restore from snapshot if available
            if snapshot is not None:
                try:
                    aggregate._restore_from_snapshot(snapshot.state, snapshot.version)
                except Exception as e:
                    # Deserialization failed - fall back to full replay
                    logger.warning(
                        "Failed to restore from snapshot for %s/%s: %s. "
                        "Falling back to full event replay.",
                        self._aggregate_type,
                        aggregate_id,
                        e,
                        exc_info=True,
                    )
                    # Re-fetch all events
                    event_stream = await self._event_store.get_events(
                        aggregate_id,
                        aggregate_type=self._aggregate_type,
                        from_version=0,
                    )
                    if not event_stream.events:
                        raise AggregateNotFoundError(aggregate_id, self._aggregate_type) from None
                    # Reset aggregate
                    aggregate = self._aggregate_factory(aggregate_id)

            # Apply events since snapshot (or all events if no snapshot)
            if event_stream.events:
                aggregate.load_from_history(event_stream.events)

            if span:
                span.set_attribute("events.replayed", len(event_stream.events))
                span.set_attribute(ATTR_VERSION, aggregate.version)

            logger.debug(
                "Loaded %s/%s at version %d (snapshot: %s, events replayed: %d)",
                self._aggregate_type,
                aggregate_id,
                aggregate.version,
                "yes" if snapshot else "no",
                len(event_stream.events),
            )

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
        3. Creates snapshot if threshold is met (if configured)

        Args:
            aggregate: The aggregate to save

        Raises:
            OptimisticLockError: If there's a version conflict

        Note:
            - If there are no uncommitted events, this is a no-op.
            - Snapshot creation failure does not fail the save operation.

        Example:
            >>> order.ship(tracking_number="TRACK123")
            >>> await repo.save(order)
            >>> assert not order.has_uncommitted_events
        """
        uncommitted_events = aggregate.uncommitted_events

        if not uncommitted_events:
            # No changes to persist
            return

        with self._tracer.span(
            "eventsource.repository.save",
            {
                ATTR_AGGREGATE_ID: str(aggregate.aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
                ATTR_EVENT_COUNT: len(uncommitted_events),
                ATTR_VERSION: aggregate.version,
            },
        ) as span:
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

                if span:
                    span.set_attribute("save.success", True)
                    span.set_attribute("new_version", aggregate.version)

                # Publish events if publisher is configured
                if self._event_publisher:
                    await self._event_publisher.publish(uncommitted_events)

                # Create snapshot via strategy if conditions are met (OCP compliant)
                if self._snapshot_manager is not None:
                    await self._snapshot_manager.maybe_create_snapshot(
                        aggregate,
                        events_since_snapshot=len(uncommitted_events),
                    )

    async def create_snapshot(self, aggregate: TAggregate) -> "Snapshot":
        """
        Manually create a snapshot for the given aggregate.

        Creates a snapshot of the aggregate's current state and saves it
        to the snapshot store. This method can be called regardless of
        the snapshot_mode or snapshot_threshold settings.

        Use cases:
        - Creating snapshots at specific business milestones
        - Forcing snapshot creation before maintenance
        - Pre-warming snapshots for frequently accessed aggregates
        - Testing snapshot functionality

        Args:
            aggregate: The aggregate to create a snapshot for.
                      The aggregate should have its current state loaded
                      (via load() or after applying events).

        Returns:
            The created Snapshot object with all metadata.

        Raises:
            RuntimeError: If snapshot_store is not configured.

        Example:
            >>> # Create snapshot after a major state transition
            >>> order = await repo.load(order_id)
            >>> order.complete_fulfillment()
            >>> await repo.save(order)
            >>> snapshot = await repo.create_snapshot(order)
            >>> print(f"Created snapshot at version {snapshot.version}")

            >>> # Create snapshot for frequently accessed aggregate
            >>> user = await repo.load(user_id)
            >>> await repo.create_snapshot(user)

        Note:
            The snapshot is saved immediately (synchronously) regardless
            of the configured snapshot_mode.

            If a snapshot already exists for the aggregate, it will be
            replaced (upsert semantics).
        """
        if self._snapshot_manager is None:
            raise RuntimeError(
                "Cannot create snapshot: snapshot_store is not configured. "
                "Provide a snapshot_store when creating the repository."
            )

        with self._tracer.span(
            "eventsource.repository.create_snapshot",
            {
                ATTR_AGGREGATE_ID: str(aggregate.aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
                ATTR_VERSION: aggregate.version,
            },
        ):
            return await self._snapshot_manager.create_snapshot(aggregate)

    async def await_pending_snapshots(self) -> int:
        """
        Wait for all pending background snapshot tasks to complete.

        This method is primarily useful for testing to ensure all
        background snapshots are complete before assertions.

        Returns:
            Number of tasks that were awaited.

        Example:
            >>> # In tests
            >>> await repo.save(aggregate)  # Triggers background snapshot
            >>> count = await repo.await_pending_snapshots()
            >>> print(f"Waited for {count} background snapshots")
            >>> # Now safe to check snapshot store

        Note:
            In production, you typically don't need to call this method.
            Background snapshots complete independently.
        """
        if self._snapshot_manager is None:
            return 0

        return await self._snapshot_manager.await_pending()

    @property
    def pending_snapshot_count(self) -> int:
        """
        Get the number of pending background snapshot tasks.

        Useful for monitoring and debugging.

        Returns:
            Number of background snapshot tasks not yet complete.
        """
        if self._snapshot_manager is None:
            return 0

        return self._snapshot_manager.pending_count

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
        with self._tracer.span(
            "eventsource.repository.exists",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
            },
        ) as span:
            event_stream = await self._event_store.get_events(
                aggregate_id,
                aggregate_type=self._aggregate_type,
            )
            exists = len(event_stream.events) > 0

            if span:
                span.set_attribute("exists", exists)

            return exists

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
