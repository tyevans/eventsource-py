"""
Checkpoint manager for projection event processing.

This module provides the ProjectionCheckpointManager class which
extracts checkpoint management from CheckpointTrackingProjection,
addressing the Single Responsibility Principle violation.

The manager handles:
- Updating checkpoints after successful event processing
- Retrieving current checkpoint position
- Getting lag metrics
- Resetting checkpoints for projection rebuilds

Example:
    >>> from eventsource.projections.checkpoint_manager import ProjectionCheckpointManager
    >>> from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
    >>>
    >>> manager = ProjectionCheckpointManager(
    ...     projection_name="OrderProjection",
    ...     checkpoint_repo=InMemoryCheckpointRepository(),
    ... )
    >>> await manager.update(event)
    >>> checkpoint = await manager.get_checkpoint()
"""

import logging
from typing import Any

from eventsource.events.base import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.repositories.checkpoint import (
    CheckpointRepository,
    InMemoryCheckpointRepository,
)

logger = logging.getLogger(__name__)


class ProjectionCheckpointManager:
    """
    Manager for projection checkpoint operations.

    Encapsulates all checkpoint-related operations for a projection,
    providing a clean interface for:
    - Updating checkpoints after event processing
    - Querying current checkpoint position
    - Getting lag metrics
    - Resetting checkpoints

    This extraction addresses SRP by separating checkpoint management
    from the projection's event handling logic.

    Example:
        >>> manager = ProjectionCheckpointManager(
        ...     projection_name="OrderProjection",
        ...     checkpoint_repo=PostgreSQLCheckpointRepository(conn),
        ... )
        >>>
        >>> # After processing an event
        >>> await manager.update(event)
        >>>
        >>> # Get current position
        >>> checkpoint = await manager.get_checkpoint()
        >>> print(f"Last processed: {checkpoint}")
        >>>
        >>> # Get lag metrics
        >>> metrics = await manager.get_lag_metrics(["OrderCreated", "OrderShipped"])
        >>> if metrics:
        ...     print(f"Lag: {metrics['lag_seconds']}s")
    """

    def __init__(
        self,
        projection_name: str,
        checkpoint_repo: CheckpointRepository | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the checkpoint manager.

        Args:
            projection_name: Name of the projection (used as checkpoint key)
            checkpoint_repo: Repository for checkpoint storage.
                           If None, uses InMemoryCheckpointRepository.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False for high-frequency projection operations.
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._projection_name = projection_name
        self._checkpoint_repo = checkpoint_repo or InMemoryCheckpointRepository()

    @property
    def projection_name(self) -> str:
        """Get the projection name."""
        return self._projection_name

    @property
    def checkpoint_repo(self) -> CheckpointRepository:
        """Get the underlying checkpoint repository."""
        return self._checkpoint_repo

    async def update(self, event: DomainEvent) -> None:
        """
        Update checkpoint after successfully processing an event.

        This should be called after the projection has successfully
        processed the event. The checkpoint is updated atomically.

        Args:
            event: The domain event that was processed
        """
        with self._tracer.span(
            "eventsource.checkpoint_manager.update",
            {
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_EVENT_TYPE: event.event_type,
            },
        ):
            await self._checkpoint_repo.update_checkpoint(
                projection_name=self._projection_name,
                event_id=event.event_id,
                event_type=event.event_type,
            )

            logger.debug(
                "Updated checkpoint for %s: event_id=%s, type=%s",
                self._projection_name,
                event.event_id,
                event.event_type,
                extra={
                    "projection": self._projection_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                },
            )

    async def get_checkpoint(self) -> str | None:
        """
        Get the last processed event ID.

        Returns:
            Last processed event ID as string, or None if no checkpoint exists
        """
        with self._tracer.span(
            "eventsource.checkpoint_manager.get_checkpoint",
            {ATTR_PROJECTION_NAME: self._projection_name},
        ):
            event_id = await self._checkpoint_repo.get_checkpoint(self._projection_name)
            return str(event_id) if event_id else None

    async def get_lag_metrics(
        self,
        event_types: list[str] | None = None,
    ) -> dict[str, Any] | None:
        """
        Get projection lag metrics.

        Calculates how far behind the projection is based on
        the checkpoint and latest events in the store.

        Args:
            event_types: List of event type names this projection handles.
                        Used to filter relevant events for lag calculation.

        Returns:
            Dictionary with lag information:
            - projection_name: Name of the projection
            - last_event_id: Last processed event ID
            - latest_event_id: Latest relevant event in store
            - lag_seconds: Time lag in seconds
            - events_processed: Total events processed
            - last_processed_at: When last event was processed
            Returns None if no checkpoint exists.
        """
        with self._tracer.span(
            "eventsource.checkpoint_manager.get_lag_metrics",
            {ATTR_PROJECTION_NAME: self._projection_name},
        ):
            metrics = await self._checkpoint_repo.get_lag_metrics(
                self._projection_name,
                event_types=event_types,
            )

            if metrics is None:
                return None

            return {
                "projection_name": metrics.projection_name,
                "last_event_id": metrics.last_event_id,
                "latest_event_id": metrics.latest_event_id,
                "lag_seconds": metrics.lag_seconds,
                "events_processed": metrics.events_processed,
                "last_processed_at": metrics.last_processed_at,
            }

    async def reset(self) -> None:
        """
        Reset the checkpoint.

        Deletes the checkpoint, causing the projection to restart
        from the beginning when it next processes events.

        Use this when rebuilding a projection from scratch.
        """
        with self._tracer.span(
            "eventsource.checkpoint_manager.reset",
            {ATTR_PROJECTION_NAME: self._projection_name},
        ):
            await self._checkpoint_repo.reset_checkpoint(self._projection_name)

            logger.info(
                "Reset checkpoint for projection %s",
                self._projection_name,
                extra={"projection": self._projection_name},
            )

    def __repr__(self) -> str:
        return f"ProjectionCheckpointManager({self._projection_name})"


__all__ = [
    "ProjectionCheckpointManager",
]
