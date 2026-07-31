"""In-memory checkpoint adapter."""

import asyncio
from datetime import UTC, datetime
from uuid import UUID

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.ports.checkpoints import CheckpointData, LagMetrics


class InMemoryCheckpointRepository:
    """
    In-memory implementation of checkpoint repository for testing.

    Stores checkpoints in memory. All data is lost when the process terminates.

    Example:
        >>> repo = InMemoryCheckpointRepository()
        >>> await repo.update_checkpoint("MyProjection", event_id, "EventType")
        >>> checkpoint = await repo.get_checkpoint("MyProjection")
    """

    def __init__(
        self,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize an empty in-memory checkpoint repository.

        Args:
            tracer: Optional tracer for tracing (if not provided, one will be created)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True)
        """
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._checkpoints: dict[str, CheckpointData] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """
        Get the last processed event ID for a projection.

        Args:
            projection_name: Name of the projection

        Returns:
            Last processed event ID, or None if no checkpoint exists
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            async with self._lock:
                checkpoint = self._checkpoints.get(projection_name)
                return checkpoint.last_event_id if checkpoint else None

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Update the checkpoint for a projection.

        Args:
            projection_name: Name of the projection
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        with self._tracer.span(
            "eventsource.checkpoint.update_checkpoint",
            {
                ATTR_PROJECTION_NAME: projection_name,
                ATTR_EVENT_TYPE: event_type,
            },
        ):
            now = datetime.now(UTC)
            async with self._lock:
                existing = self._checkpoints.get(projection_name)
                events_processed = (existing.events_processed + 1) if existing else 1

                self._checkpoints[projection_name] = CheckpointData(
                    projection_name=projection_name,
                    last_event_id=event_id,
                    last_event_type=event_type,
                    last_processed_at=now,
                    events_processed=events_processed,
                )

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """
        Get lag metrics for a projection.

        Note: In-memory implementation cannot calculate real lag against
        an event store. Returns placeholder metrics based on checkpoint data.

        Args:
            projection_name: Name of the projection
            event_types: List of event types (ignored in in-memory impl)

        Returns:
            LagMetrics if checkpoint exists, None otherwise
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_lag_metrics",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            async with self._lock:
                checkpoint = self._checkpoints.get(projection_name)
                if not checkpoint:
                    return None

                return LagMetrics(
                    projection_name=checkpoint.projection_name,
                    last_event_id=str(checkpoint.last_event_id)
                    if checkpoint.last_event_id
                    else None,
                    latest_event_id=None,  # Cannot determine without event store
                    lag_seconds=0.0,  # Cannot calculate without event store
                    events_processed=checkpoint.events_processed,
                    last_processed_at=(
                        checkpoint.last_processed_at.isoformat()
                        if checkpoint.last_processed_at
                        else None
                    ),
                )

    async def reset_checkpoint(self, projection_name: str) -> None:
        """
        Reset the checkpoint for a projection.

        Args:
            projection_name: Name of the projection
        """
        with self._tracer.span(
            "eventsource.checkpoint.reset_checkpoint",
            {ATTR_PROJECTION_NAME: projection_name},
        ):
            async with self._lock:
                self._checkpoints.pop(projection_name, None)

    async def get_position(self, subscription_id: str) -> int | None:
        """
        Get last processed global position for a subscription.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)

        Returns:
            Last processed global position, or None if no checkpoint exists
            or if checkpoint doesn't have position data.
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_position",
            {ATTR_PROJECTION_NAME: subscription_id},
        ):
            async with self._lock:
                checkpoint = self._checkpoints.get(subscription_id)
                return checkpoint.global_position if checkpoint else None

    async def save_position(
        self,
        subscription_id: str,
        position: int,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Save checkpoint with global position.

        Updates the position, event_id, and event_type for the checkpoint.
        Uses UPSERT pattern for idempotency.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)
            position: Global position of the event
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        with self._tracer.span(
            "eventsource.checkpoint.save_position",
            {
                ATTR_PROJECTION_NAME: subscription_id,
                ATTR_EVENT_TYPE: event_type,
                "global_position": position,
            },
        ):
            now = datetime.now(UTC)
            async with self._lock:
                existing = self._checkpoints.get(subscription_id)
                events_processed = (existing.events_processed + 1) if existing else 1

                self._checkpoints[subscription_id] = CheckpointData(
                    projection_name=subscription_id,
                    last_event_id=event_id,
                    last_event_type=event_type,
                    last_processed_at=now,
                    events_processed=events_processed,
                    global_position=position,
                )

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all projection checkpoints.

        Returns:
            List of CheckpointData for all projections
        """
        with self._tracer.span(
            "eventsource.checkpoint.get_all_checkpoints",
            {},
        ):
            async with self._lock:
                return sorted(
                    self._checkpoints.values(),
                    key=lambda c: c.projection_name,
                )

    async def clear(self) -> None:
        """Clear all checkpoints. Useful for test setup/teardown."""
        with self._tracer.span(
            "eventsource.checkpoint.clear",
            {},
        ):
            async with self._lock:
                self._checkpoints.clear()


__all__ = ["InMemoryCheckpointRepository"]
