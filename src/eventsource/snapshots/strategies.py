"""
Snapshot creation strategies for aggregate repositories.

This module implements the Strategy pattern for snapshot creation,
addressing the Open/Closed Principle violation where adding new
snapshot modes required modifying the AggregateRepository.

Strategies:
- ThresholdSnapshotStrategy: Create snapshots every N events (sync)
- BackgroundSnapshotStrategy: Create snapshots asynchronously
- NoSnapshotStrategy: Never create snapshots automatically (manual mode)

Example:
    >>> from eventsource.snapshots.strategies import (
    ...     ThresholdSnapshotStrategy,
    ...     BackgroundSnapshotStrategy,
    ...     create_snapshot_strategy,
    ... )
    >>>
    >>> # Create strategy directly
    >>> strategy = ThresholdSnapshotStrategy(threshold=100)
    >>>
    >>> # Or use factory for backward compatibility
    >>> strategy = create_snapshot_strategy("background", threshold=100)
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Literal, Protocol, runtime_checkable

if TYPE_CHECKING:
    from eventsource.aggregates.base import AggregateRoot
    from eventsource.snapshots.interface import Snapshot, SnapshotStore

logger = logging.getLogger(__name__)


@runtime_checkable
class SnapshotStrategy(Protocol):
    """
    Protocol for snapshot creation strategies.

    Strategies encapsulate the decision of when and how to create
    snapshots, allowing different behaviors without modifying
    the repository code.

    This addresses the Open/Closed Principle: new snapshot behaviors
    can be added by implementing this protocol without modifying
    existing repository code.
    """

    def should_snapshot(self, aggregate: "AggregateRoot[Any]", events_since_snapshot: int) -> bool:
        """
        Determine if a snapshot should be created.

        Args:
            aggregate: The aggregate that was just saved
            events_since_snapshot: Number of events since last snapshot

        Returns:
            True if a snapshot should be created, False otherwise
        """
        ...

    async def execute_snapshot(
        self,
        aggregate: "AggregateRoot[Any]",
        snapshot_store: "SnapshotStore",
        aggregate_type: str,
    ) -> "Snapshot | None":
        """
        Execute snapshot creation according to the strategy.

        Args:
            aggregate: The aggregate to snapshot
            snapshot_store: Store to save the snapshot to
            aggregate_type: Type name of the aggregate

        Returns:
            The created Snapshot, or None if skipped/failed
        """
        ...


class BaseSnapshotStrategy(ABC):
    """
    Base class for snapshot strategies with common functionality.

    Provides the snapshot creation logic that all strategies share,
    while leaving the decision logic (should_snapshot) and execution
    mode (execute_snapshot) to subclasses.
    """

    def __init__(self, threshold: int | None = None) -> None:
        """
        Initialize the strategy.

        Args:
            threshold: Number of events between snapshots.
                      If None, snapshots are not created based on event count.
        """
        self._threshold = threshold

    @property
    def threshold(self) -> int | None:
        """Get the configured threshold."""
        return self._threshold

    def should_snapshot(self, aggregate: "AggregateRoot[Any]", events_since_snapshot: int) -> bool:
        """
        Determine if a snapshot should be created based on threshold.

        Default implementation checks if aggregate version is at
        a threshold boundary.

        Args:
            aggregate: The aggregate that was just saved
            events_since_snapshot: Number of events since last snapshot

        Returns:
            True if at threshold boundary, False otherwise
        """
        if self._threshold is None:
            return False

        # Check if at threshold boundary
        return aggregate.version > 0 and aggregate.version % self._threshold == 0

    async def _create_snapshot(
        self,
        aggregate: "AggregateRoot[Any]",
        snapshot_store: "SnapshotStore",
        aggregate_type: str,
    ) -> "Snapshot":
        """
        Create and save a snapshot for the aggregate.

        Args:
            aggregate: The aggregate to snapshot
            snapshot_store: Store to save the snapshot to
            aggregate_type: Type name of the aggregate

        Returns:
            The created Snapshot object
        """
        from eventsource.snapshots.interface import Snapshot

        # Get schema version from aggregate class
        schema_version = getattr(type(aggregate), "schema_version", 1)

        # Serialize state
        state_dict = aggregate._serialize_state()

        # Create snapshot
        snapshot = Snapshot(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type=aggregate_type,
            version=aggregate.version,
            state=state_dict,
            schema_version=schema_version,
            created_at=datetime.now(UTC),
        )

        # Save to store
        await snapshot_store.save_snapshot(snapshot)

        logger.info(
            "Created snapshot for %s/%s at version %d (schema_version=%d)",
            aggregate_type,
            aggregate.aggregate_id,
            snapshot.version,
            schema_version,
        )

        return snapshot

    @abstractmethod
    async def execute_snapshot(
        self,
        aggregate: "AggregateRoot[Any]",
        snapshot_store: "SnapshotStore",
        aggregate_type: str,
    ) -> "Snapshot | None":
        """Execute snapshot creation - implemented by subclasses."""
        pass


class ThresholdSnapshotStrategy(BaseSnapshotStrategy):
    """
    Strategy that creates snapshots synchronously at threshold boundaries.

    Snapshots are created immediately after save, blocking until complete.
    This is the simplest and most predictable mode, but adds latency to saves.

    Best for:
    - Development and testing
    - Low-throughput scenarios
    - When snapshot consistency is critical

    Example:
        >>> strategy = ThresholdSnapshotStrategy(threshold=100)
        >>> # Snapshots created at versions 100, 200, 300, etc.
    """

    async def execute_snapshot(
        self,
        aggregate: "AggregateRoot[Any]",
        snapshot_store: "SnapshotStore",
        aggregate_type: str,
    ) -> "Snapshot | None":
        """
        Create snapshot synchronously.

        Args:
            aggregate: The aggregate to snapshot
            snapshot_store: Store to save the snapshot to
            aggregate_type: Type name of the aggregate

        Returns:
            The created Snapshot, or None if creation failed
        """
        try:
            return await self._create_snapshot(aggregate, snapshot_store, aggregate_type)
        except Exception as e:
            logger.warning(
                "Failed to create snapshot for %s/%s: %s",
                aggregate_type,
                aggregate.aggregate_id,
                e,
                exc_info=True,
            )
            return None


class BackgroundSnapshotStrategy(BaseSnapshotStrategy):
    """
    Strategy that creates snapshots asynchronously in background tasks.

    Snapshots are created in fire-and-forget background tasks,
    allowing the save operation to complete without waiting.

    Best for:
    - High-throughput scenarios
    - When save latency is critical
    - Production environments with async infrastructure

    Example:
        >>> strategy = BackgroundSnapshotStrategy(threshold=100)
        >>> # Snapshots created in background at versions 100, 200, 300, etc.
        >>>
        >>> # Wait for pending snapshots (useful in tests)
        >>> count = await strategy.await_pending()
    """

    def __init__(self, threshold: int | None = None) -> None:
        """
        Initialize the background snapshot strategy.

        Args:
            threshold: Number of events between snapshots
        """
        super().__init__(threshold)
        self._pending_tasks: list[asyncio.Task[None]] = []

    async def execute_snapshot(
        self,
        aggregate: "AggregateRoot[Any]",
        snapshot_store: "SnapshotStore",
        aggregate_type: str,
    ) -> "Snapshot | None":
        """
        Schedule snapshot creation in background.

        Args:
            aggregate: The aggregate to snapshot
            snapshot_store: Store to save the snapshot to
            aggregate_type: Type name of the aggregate

        Returns:
            None (snapshot is created asynchronously)
        """
        task = asyncio.create_task(
            self._create_snapshot_background(aggregate, snapshot_store, aggregate_type)
        )
        self._pending_tasks.append(task)
        self._cleanup_completed_tasks()
        return None

    async def _create_snapshot_background(
        self,
        aggregate: "AggregateRoot[Any]",
        snapshot_store: "SnapshotStore",
        aggregate_type: str,
    ) -> None:
        """
        Create snapshot in background with error handling.

        Args:
            aggregate: The aggregate to snapshot
            snapshot_store: Store to save the snapshot to
            aggregate_type: Type name of the aggregate
        """
        try:
            await self._create_snapshot(aggregate, snapshot_store, aggregate_type)
            logger.debug(
                "Background snapshot created for %s/%s at version %d",
                aggregate_type,
                aggregate.aggregate_id,
                aggregate.version,
            )
        except Exception as e:
            logger.warning(
                "Background snapshot creation failed for %s/%s: %s",
                aggregate_type,
                aggregate.aggregate_id,
                e,
                exc_info=True,
            )

    def _cleanup_completed_tasks(self) -> None:
        """Remove completed tasks from the pending list."""
        self._pending_tasks = [task for task in self._pending_tasks if not task.done()]

    @property
    def pending_count(self) -> int:
        """Get the number of pending background tasks."""
        self._cleanup_completed_tasks()
        return len(self._pending_tasks)

    async def await_pending(self) -> int:
        """
        Wait for all pending background snapshot tasks.

        Useful for testing to ensure snapshots complete before assertions.

        Returns:
            Number of tasks that were awaited
        """
        if not self._pending_tasks:
            return 0

        count = len(self._pending_tasks)

        # Wait for all tasks
        results = await asyncio.gather(
            *self._pending_tasks,
            return_exceptions=True,
        )

        # Log any unexpected exceptions
        for result in results:
            if isinstance(result, Exception):
                logger.error(
                    "Unexpected error in background snapshot task: %s",
                    result,
                    exc_info=result,
                )

        self._pending_tasks.clear()
        return count


class NoSnapshotStrategy(BaseSnapshotStrategy):
    """
    Strategy that never creates snapshots automatically.

    Use for manual snapshot mode where snapshots are only created
    via explicit calls to create_snapshot().

    Best for:
    - When you want full control over snapshot timing
    - Creating snapshots at specific business milestones
    - Testing scenarios without automatic snapshots

    Example:
        >>> strategy = NoSnapshotStrategy()
        >>> strategy.should_snapshot(aggregate, 1000)  # Always False
    """

    def __init__(self) -> None:
        """Initialize with no threshold."""
        super().__init__(threshold=None)

    def should_snapshot(self, aggregate: "AggregateRoot[Any]", events_since_snapshot: int) -> bool:
        """Never create snapshots automatically."""
        return False

    async def execute_snapshot(
        self,
        aggregate: "AggregateRoot[Any]",
        snapshot_store: "SnapshotStore",
        aggregate_type: str,
    ) -> "Snapshot | None":
        """No-op - manual mode never auto-creates snapshots."""
        return None


def create_snapshot_strategy(
    mode: Literal["sync", "background", "manual"],
    threshold: int | None = None,
) -> BaseSnapshotStrategy:
    """
    Factory function to create a snapshot strategy from mode string.

    Provides backward compatibility with the mode string API used by
    AggregateRepository.

    Args:
        mode: Snapshot mode:
            - "sync": Create snapshots synchronously (ThresholdSnapshotStrategy)
            - "background": Create snapshots asynchronously (BackgroundSnapshotStrategy)
            - "manual": Never create automatically (NoSnapshotStrategy)
        threshold: Number of events between snapshots (ignored for "manual")

    Returns:
        Appropriate SnapshotStrategy instance

    Example:
        >>> # For backward compatibility with mode strings
        >>> strategy = create_snapshot_strategy("background", threshold=100)
        >>>
        >>> # Equivalent to:
        >>> strategy = BackgroundSnapshotStrategy(threshold=100)
    """
    strategies: dict[str, type[BaseSnapshotStrategy]] = {
        "sync": ThresholdSnapshotStrategy,
        "background": BackgroundSnapshotStrategy,
        "manual": NoSnapshotStrategy,
    }

    strategy_class = strategies.get(mode)
    if strategy_class is None:
        raise ValueError(f"Unknown snapshot mode: {mode}. Must be one of: sync, background, manual")

    if mode == "manual":
        return NoSnapshotStrategy()

    return strategy_class(threshold=threshold)


__all__ = [
    "SnapshotStrategy",
    "BaseSnapshotStrategy",
    "ThresholdSnapshotStrategy",
    "BackgroundSnapshotStrategy",
    "NoSnapshotStrategy",
    "create_snapshot_strategy",
]
