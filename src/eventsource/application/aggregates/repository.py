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
from eventsource.domain.exceptions import AggregateNotFoundError
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
    and persisting new events. Handles coordination between aggregates,
    the event store, optional publishers, and optional snapshot stores.

    Features:
    - Load aggregates by reconstituting state from events
    - Save aggregates by persisting uncommitted events
    - Optional event publishing after successful save
    - Optimistic locking via event store
    - Optional snapshot support ("sync", "background", or "manual")

    Example:
        >>> store = InMemoryEventStore()
        >>> repo = AggregateRepository(event_store=store, aggregate_factory=OrderAggregate)
        >>> order = OrderAggregate(uuid4())
        >>> order.create(customer_id=uuid4())
        >>> await repo.save(order)
        >>> loaded = await repo.load(order.aggregate_id)
        >>> assert loaded.version == order.version
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

        ``aggregate_type`` is inferred from ``aggregate_factory.aggregate_type``
        (single source of truth).

        Args:
            event_store: Event store port for appending and reading this aggregate's stream.
            aggregate_factory: Class to instantiate when loading aggregates.
            event_publisher: Optional publisher for broadcasting events.
            snapshot_store: Optional snapshot store for state caching.
            snapshot_threshold: Number of events between automatic snapshots.
            snapshot_mode: When to create snapshots ("sync", "background", "manual").
            snapshot_policy: Optional SnapshotPolicy controlling *when* to snapshot
                (mutually exclusive with snapshot_mode/snapshot_threshold).
            snapshot_scheduler: Optional SnapshotScheduler controlling *how* the
                snapshot write executes (mutually exclusive with snapshot_mode/threshold).
            tracer: Optional custom Tracer instance.
            enable_tracing: If True and OpenTelemetry is available, emit traces.

        Raises:
            ValueError: If aggregate_type cannot be inferred from the factory, or
                both policy/scheduler and mode/threshold are provided.
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self._event_store = event_store
        self._aggregate_factory = aggregate_factory
        self._event_publisher = event_publisher
        self._aggregate_type = self._infer_aggregate_type(aggregate_factory)

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

        Raises:
            ValueError: If inference fails with helpful error message.
        """
        if hasattr(factory, "aggregate_type"):
            inferred = factory.aggregate_type
            if isinstance(inferred, str) and inferred not in ("", "Unknown"):
                return inferred

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

    async def get_or_none(self, aggregate_id: UUID) -> TAggregate | None:
        """
        Get an aggregate if it exists, or return None if not found.

        Args:
            aggregate_id: ID of the aggregate

        Returns:
            The loaded aggregate if events exist, or None if not found.
        """
        try:
            return await self.load(aggregate_id)
        except AggregateNotFoundError:
            return None

    async def exists(self, aggregate_id: UUID) -> bool:
        """
        Check if an aggregate exists.

        Args:
            aggregate_id: ID of the aggregate to check

        Returns:
            True if aggregate has events, False otherwise.
        """
        return await super().exists(aggregate_id)

    async def execute(
        self,
        aggregate_id: UUID,
        command: Any,
        *,
        create_if_missing: bool = True,
    ) -> TAggregate:
        """
        Load (or create) an aggregate, execute a command on it, and save.

        ``command`` may be either a callable ``(TAggregate) -> Any`` invoked
        directly with the loaded aggregate (works with any ``AggregateRoot``
        or ``DeclarativeAggregate``), or a command value dispatched via
        ``aggregate.execute(command)`` / ``aggregate.apply_command(command)``
        (e.g. ``DeciderAggregate``).

        Raises:
            AggregateNotFoundError: If ``create_if_missing=False`` and missing.
            TypeError: If ``command`` is not callable and the aggregate does
                not define ``execute`` or ``apply_command``.
        """
        if create_if_missing:
            aggregate = await self.load_or_create(aggregate_id)
        else:
            aggregate = await self.load(aggregate_id)

        if callable(command) and not isinstance(command, type):
            command(aggregate)
        else:
            runner = getattr(aggregate, "execute", None)
            if not callable(runner):
                runner = getattr(aggregate, "apply_command", None)
            if not callable(runner):
                raise TypeError(
                    f"{type(aggregate).__name__} does not support execute(); "
                    "pass a callable (e.g. lambda agg: agg.do_something()) or use "
                    "an aggregate supporting execute(command) or apply_command(command), "
                    "such as DeciderAggregate."
                )
            runner(command)

        await self.save(aggregate)
        return aggregate


__all__ = [
    "ATTR_AGGREGATE_ID",
    "ATTR_AGGREGATE_TYPE",
    "ATTR_EVENT_COUNT",
    "ATTR_VERSION",
    "AggregateRepository",
]
