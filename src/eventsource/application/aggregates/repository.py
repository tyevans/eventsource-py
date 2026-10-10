"""
Repository pattern for event-sourced aggregates.

Repositories provide a clean interface for loading and saving aggregates,
abstracting away the details of event store operations.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from eventsource.application.aggregates.repository_query import AggregateRepositoryQueryMixin
from eventsource.application.aggregates.repository_save import AggregateRepositorySaveMixin
from eventsource.application.aggregates.repository_snapshots import (
    AggregateRepositorySnapshotMixin,
)
from eventsource.application.aggregates.snapshotting import (
    BackgroundScheduler,
    EveryNEvents,
    ImmediateScheduler,
    Never,
    SnapshotPolicy,
    SnapshotScheduler,
)
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_VERSION,
)
from eventsource.ports.bus import EventPublisher
from eventsource.ports.store import AggregateStore

if TYPE_CHECKING:
    from eventsource.ports.snapshots import SnapshotStore

logger = logging.getLogger(__name__)


class AggregateRepository[TAggregate: AggregateRoot[Any]](
    AggregateRepositoryQueryMixin[TAggregate],
    AggregateRepositorySaveMixin[TAggregate],
    AggregateRepositorySnapshotMixin[TAggregate],
):
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

        >>> from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
        >>>
        >>> repo = AggregateRepository(
        ...     event_store=event_store,
        ...     aggregate_factory=OrderAggregate,
        ...     # aggregate_type inferred from OrderAggregate.aggregate_type
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
        ...     # aggregate_type inferred from OrderAggregate.aggregate_type
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
        event_store: AggregateStore,
        aggregate_factory: type[TAggregate],
        event_publisher: EventPublisher | None = None,
        # Snapshot configuration
        snapshot_store: SnapshotStore | None = None,
        snapshot_threshold: int | None = None,
        snapshot_mode: Literal["sync", "background", "manual"] = "sync",
        snapshot_policy: SnapshotPolicy | None = None,
        snapshot_scheduler: SnapshotScheduler | None = None,
        # Tracing configuration
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the repository.

        The aggregate_type is always inferred from the
        aggregate_factory.aggregate_type class attribute -- the factory's
        class attribute is the single source of truth. There is no way to
        override it here, so it can never diverge from the value stamped
        onto the aggregate's events.

        Args:
            event_store: Event store port for appending and reading this aggregate's stream
            aggregate_factory: Class to instantiate when loading aggregates.
                          Its aggregate_type class attribute is used as the
                          type name (e.g., 'Order').
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
            snapshot_policy: Optional SnapshotPolicy controlling *when* to
                          snapshot. Mutually exclusive with snapshot_mode/
                          snapshot_threshold. Use for custom policies beyond
                          the built-in mode/threshold knobs.
            snapshot_scheduler: Optional SnapshotScheduler controlling *how*
                          the snapshot write executes. Mutually exclusive
                          with snapshot_mode/snapshot_threshold.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Defaults to True for consistency with other components.
                          Ignored if tracer is explicitly provided.

        Raises:
            ValueError: If aggregate_type cannot be inferred from the factory.

        Example with inference:
            >>> class OrderAggregate(DeclarativeAggregate[OrderState]):
            ...     aggregate_type = "Order"
            ...
            >>> repo = AggregateRepository(
            ...     event_store=store,
            ...     aggregate_factory=OrderAggregate,
            ...     # aggregate_type inferred from OrderAggregate.aggregate_type
            ... )

        Example with snapshot support:
            >>> repo = AggregateRepository(
            ...     event_store=PostgreSQLEventStore(session_factory),
            ...     aggregate_factory=OrderAggregate,
            ...     # aggregate_type inferred
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
        self._event_publisher = event_publisher

        # aggregate_type is always inferred from the factory's class attribute
        self._aggregate_type = self._infer_aggregate_type(aggregate_factory)

        # Snapshot configuration (exposed via public properties)
        self._snapshot_store = snapshot_store
        self._snapshot_threshold = snapshot_threshold
        self._snapshot_mode = snapshot_mode

        if (snapshot_policy is not None or snapshot_scheduler is not None) and (
            snapshot_threshold is not None or snapshot_mode != "sync"
        ):
            raise ValueError(
                "Pass either snapshot_mode/snapshot_threshold or "
                "snapshot_policy/snapshot_scheduler, not both."
            )
        if snapshot_policy is not None:
            self._snapshot_policy: SnapshotPolicy = snapshot_policy
        elif snapshot_mode != "manual" and snapshot_threshold is not None:
            self._snapshot_policy = EveryNEvents(snapshot_threshold)
        else:
            self._snapshot_policy = Never()
        if snapshot_scheduler is not None:
            self._snapshot_scheduler: SnapshotScheduler = snapshot_scheduler
        elif snapshot_mode == "background":
            self._snapshot_scheduler = BackgroundScheduler()
        else:
            self._snapshot_scheduler = ImmediateScheduler()

    def _infer_aggregate_type(self, factory: type[TAggregate]) -> str:
        """
        Infer aggregate_type from factory's aggregate_type attribute.

        Attempts to read the aggregate_type class attribute from the factory.
        Rejects an empty string, and the attribute being altogether unset now
        raises AggregateTypeNotSetError at construction time before this
        method ever sees the factory (aggregate_type has no default).

        Args:
            factory: The aggregate factory (class)

        Returns:
            The inferred aggregate type string

        Raises:
            ValueError: If inference fails with helpful error message
        """
        # Check for aggregate_type attribute
        if hasattr(factory, "aggregate_type"):
            inferred = factory.aggregate_type
            if isinstance(inferred, str) and inferred not in ("", "Unknown"):
                return inferred

        # Inference failed - provide helpful error
        factory_name = factory.__name__ if hasattr(factory, "__name__") else str(factory)

        raise ValueError(
            f"Cannot infer aggregate_type from {factory_name}. "
            f"Add an 'aggregate_type' class attribute to your aggregate class:\n"
            f"     class {factory_name}(DeclarativeAggregate[...]):\n"
            f'         aggregate_type = "YourTypeName"'
        )

    def _stream(self, aggregate_id: UUID) -> StreamId:
        """Stream identity for one aggregate of this repository's type."""
        return StreamId(aggregate_id=aggregate_id, category=self._aggregate_type)

    @property
    def aggregate_type(self) -> str:
        """Get the aggregate type this repository manages."""
        return self._aggregate_type

    @property
    def event_store(self) -> AggregateStore:
        """Get the event store used by this repository."""
        return self._event_store

    @property
    def event_publisher(self) -> EventPublisher | None:
        """Get the event publisher, if configured."""
        return self._event_publisher


__all__ = [
    "ATTR_AGGREGATE_ID",
    "ATTR_AGGREGATE_TYPE",
    "ATTR_EVENT_COUNT",
    "ATTR_VERSION",
    "AggregateRepository",
]
