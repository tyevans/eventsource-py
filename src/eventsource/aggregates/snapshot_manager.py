"""
Snapshot manager for aggregate repositories.

The manager handles:
- Loading and validating snapshots
- Creating snapshots (sync, background, manual)
- Managing background snapshot tasks
- Coordinating with snapshot strategies

Example:
    >>> from eventsource.aggregates.snapshot_manager import AggregateSnapshotManager
    >>> from eventsource.snapshots import InMemorySnapshotStore
    >>> from eventsource.snapshots.strategies import ThresholdSnapshotStrategy
    >>>
    >>> manager = AggregateSnapshotManager(
    ...     snapshot_store=InMemorySnapshotStore(),
    ...     aggregate_type="Order",
    ...     strategy=ThresholdSnapshotStrategy(threshold=100),
    ... )
    >>> snapshot = await manager.load_valid_snapshot(aggregate_id, aggregate_factory)
"""

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Generic, TypeVar
from uuid import UUID

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)

if TYPE_CHECKING:
    from eventsource.aggregates.base import AggregateRoot
    from eventsource.snapshots.interface import Snapshot, SnapshotStore
    from eventsource.snapshots.strategies import SnapshotStrategy

logger = logging.getLogger(__name__)

TAggregate = TypeVar("TAggregate", bound="AggregateRoot[Any]")


class AggregateSnapshotManager(Generic[TAggregate]):
    """
    Manager for aggregate snapshot operations.

    Encapsulates all snapshot-related operations for aggregates,
    providing a clean interface for:
    - Loading and validating snapshots
    - Creating snapshots based on strategy
    - Managing background snapshot tasks

    This extraction addresses SRP by separating snapshot management
    from the repository's core load/save responsibilities.

    Example:
        >>> from eventsource.snapshots.strategies import BackgroundSnapshotStrategy
        >>>
        >>> manager = AggregateSnapshotManager(
        ...     snapshot_store=PostgreSQLSnapshotStore(session_factory),
        ...     aggregate_type="Order",
        ...     strategy=BackgroundSnapshotStrategy(threshold=100),
        ... )
        >>>
        >>> # Load snapshot for aggregate
        >>> snapshot = await manager.load_valid_snapshot(
        ...     aggregate_id, OrderAggregate
        ... )
        >>>
        >>> # Create snapshot after save if strategy says to
        >>> await manager.maybe_create_snapshot(aggregate)
        >>>
        >>> # Wait for background snapshots (in tests)
        >>> count = await manager.await_pending()
    """

    def __init__(
        self,
        snapshot_store: "SnapshotStore",
        aggregate_type: str,
        strategy: "SnapshotStrategy | None" = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the snapshot manager.

        Args:
            snapshot_store: Store for snapshot persistence
            aggregate_type: Type name of the aggregate (e.g., "Order")
            strategy: Strategy for deciding when/how to create snapshots.
                     If None, snapshots are only created manually.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is True for consistency.
                          Ignored if tracer is explicitly provided.
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._snapshot_store = snapshot_store
        self._aggregate_type = aggregate_type
        self._strategy = strategy

    @property
    def snapshot_store(self) -> "SnapshotStore":
        """Get the snapshot store."""
        return self._snapshot_store

    @property
    def aggregate_type(self) -> str:
        """Get the aggregate type name."""
        return self._aggregate_type

    @property
    def strategy(self) -> "SnapshotStrategy | None":
        """Get the snapshot strategy."""
        return self._strategy

    async def load_valid_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_factory: type[TAggregate],
    ) -> "Snapshot | None":
        """
        Load and validate a snapshot for an aggregate.

        Retrieves the snapshot from the store and validates that
        the schema version matches the aggregate's expected version.

        Args:
            aggregate_id: ID of the aggregate to load snapshot for
            aggregate_factory: Factory class for the aggregate
                              (used to get expected schema version)

        Returns:
            Valid snapshot if found and valid, None otherwise.
            Returns None (doesn't raise) for any validation failure,
            enabling graceful fallback to full event replay.
        """
        with self._tracer.span(
            "eventsource.snapshot_manager.load_valid_snapshot",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
            },
        ) as span:
            try:
                snapshot = await self._snapshot_store.get_snapshot(
                    aggregate_id,
                    self._aggregate_type,
                )
            except Exception as e:
                # Snapshot store error - log and fall back
                logger.warning(
                    "Error loading snapshot for %s/%s: %s. Falling back to event replay.",
                    self._aggregate_type,
                    aggregate_id,
                    e,
                )
                return None

            if snapshot is None:
                return None

            # Validate schema version
            aggregate_schema_version = getattr(aggregate_factory, "schema_version", 1)

            if snapshot.schema_version != aggregate_schema_version:
                logger.info(
                    "Snapshot schema version mismatch for %s/%s: "
                    "snapshot has v%d, aggregate expects v%d. "
                    "Falling back to full event replay.",
                    self._aggregate_type,
                    aggregate_id,
                    snapshot.schema_version,
                    aggregate_schema_version,
                )
                return None

            if span:
                span.set_attribute("snapshot.found", True)
                span.set_attribute("snapshot.version", snapshot.version)

            logger.debug(
                "Loaded valid snapshot for %s/%s at version %d",
                self._aggregate_type,
                aggregate_id,
                snapshot.version,
            )

            return snapshot

    async def maybe_create_snapshot(
        self,
        aggregate: TAggregate,
        events_since_snapshot: int = 0,
    ) -> "Snapshot | None":
        """
        Create snapshot if strategy determines it should be created.

        Called after saving an aggregate. Uses the configured strategy
        to determine if and how to create the snapshot.

        Args:
            aggregate: The aggregate that was just saved
            events_since_snapshot: Number of events since last snapshot

        Returns:
            Created snapshot if sync strategy, None otherwise
        """
        if self._strategy is None:
            return None

        if not self._strategy.should_snapshot(aggregate, events_since_snapshot):
            return None

        with self._tracer.span(
            "eventsource.snapshot_manager.maybe_create_snapshot",
            {
                ATTR_AGGREGATE_ID: str(aggregate.aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
                ATTR_VERSION: aggregate.version,
            },
        ):
            return await self._strategy.execute_snapshot(
                aggregate,
                self._snapshot_store,
                self._aggregate_type,
            )

    async def create_snapshot(self, aggregate: TAggregate) -> "Snapshot":
        """
        Explicitly create a snapshot for the aggregate.

        Creates a snapshot immediately, regardless of strategy settings.
        Use for manual snapshot creation at business milestones.

        Args:
            aggregate: The aggregate to snapshot

        Returns:
            The created Snapshot object
        """
        from eventsource.snapshots.interface import Snapshot

        with self._tracer.span(
            "eventsource.snapshot_manager.create_snapshot",
            {
                ATTR_AGGREGATE_ID: str(aggregate.aggregate_id),
                ATTR_AGGREGATE_TYPE: self._aggregate_type,
                ATTR_VERSION: aggregate.version,
            },
        ):
            # Get schema version from aggregate class
            schema_version = getattr(type(aggregate), "schema_version", 1)

            # Serialize state
            state_dict = aggregate._serialize_state()

            # Create snapshot
            snapshot = Snapshot(
                aggregate_id=aggregate.aggregate_id,
                aggregate_type=self._aggregate_type,
                version=aggregate.version,
                state=state_dict,
                schema_version=schema_version,
                created_at=datetime.now(UTC),
            )

            # Save to store
            await self._snapshot_store.save_snapshot(snapshot)

            logger.info(
                "Created snapshot for %s/%s at version %d (schema_version=%d)",
                self._aggregate_type,
                aggregate.aggregate_id,
                snapshot.version,
                schema_version,
            )

            return snapshot

    @property
    def pending_count(self) -> int:
        """
        Get the number of pending background snapshot tasks.

        Only relevant when using BackgroundSnapshotStrategy.

        Returns:
            Number of pending background tasks
        """
        from eventsource.snapshots.strategies import BackgroundSnapshotStrategy

        if isinstance(self._strategy, BackgroundSnapshotStrategy):
            return self._strategy.pending_count
        return 0

    async def await_pending(self) -> int:
        """
        Wait for all pending background snapshot tasks.

        Only relevant when using BackgroundSnapshotStrategy.
        Useful in tests to ensure snapshots complete before assertions.

        Returns:
            Number of tasks that were awaited
        """
        from eventsource.snapshots.strategies import BackgroundSnapshotStrategy

        if isinstance(self._strategy, BackgroundSnapshotStrategy):
            return await self._strategy.await_pending()
        return 0

    def __repr__(self) -> str:
        strategy_name = type(self._strategy).__name__ if self._strategy else "None"
        return f"AggregateSnapshotManager({self._aggregate_type}, strategy={strategy_name})"


__all__ = [
    "AggregateSnapshotManager",
]
