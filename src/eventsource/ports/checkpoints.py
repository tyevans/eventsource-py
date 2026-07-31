"""Projection checkpoint and subscription position ports.

Pure boundary interfaces: stdlib, typing, uuid, datetime, dataclasses only.
No sqlalchemy, no observability, no implementation code.

The contract splits along its two real consumer groups (ISP, ADR 0019):
subscription runners persist an opaque global position; projections persist
a checkpoint plus lag metadata. Both land in one table in the SQL adapter,
which is why the composed `CheckpointRepository` exists.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID


@dataclass(frozen=True)
class CheckpointData:
    """
    Data structure for checkpoint information.

    Attributes:
        projection_name: Name of the projection
        last_event_id: Last processed event ID
        last_event_type: Type of the last processed event
        last_processed_at: When the last event was processed
        events_processed: Total count of events processed
        global_position: Last processed global position in the event stream
    """

    projection_name: str
    last_event_id: UUID | None = None
    last_event_type: str | None = None
    last_processed_at: datetime | None = None
    events_processed: int = 0
    global_position: int | None = None


@dataclass(frozen=True)
class LagMetrics:
    """
    Data structure for projection lag metrics.

    Attributes:
        projection_name: Name of the projection
        last_event_id: Last event ID processed by the projection
        latest_event_id: Latest relevant event ID in the event store
        lag_seconds: Time lag in seconds (0 if up to date)
        events_processed: Total events processed by this projection
        last_processed_at: When the projection last processed an event
    """

    projection_name: str
    last_event_id: str | None = None
    latest_event_id: str | None = None
    lag_seconds: float = 0.0
    events_processed: int = 0
    last_processed_at: str | None = None


@runtime_checkable
class ProjectionCheckpoints(Protocol):
    """Checkpoint persistence for projections: position, lag, reset."""

    async def get_checkpoint(self, projection_name: str) -> UUID | None:
        """
        Get the last processed event ID for a projection.

        Args:
            projection_name: Name of the projection

        Returns:
            Last processed event ID, or None if no checkpoint exists
        """
        ...

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """
        Update the checkpoint for a projection.

        Uses UPSERT pattern for idempotency - safe to call multiple times.

        Args:
            projection_name: Name of the projection
            event_id: Event ID that was processed
            event_type: Type of event processed
        """
        ...

    async def reset_checkpoint(self, projection_name: str) -> None:
        """
        Reset the checkpoint for a projection.

        Used when rebuilding a projection from scratch.

        Args:
            projection_name: Name of the projection
        """
        ...

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """
        Get lag metrics for a projection.

        Compares the checkpoint against the latest relevant events to
        determine how far behind the projection is.

        Args:
            projection_name: Name of the projection
            event_types: List of event types this projection handles.
                        Used to filter relevant events for lag calculation.

        Returns:
            LagMetrics if checkpoint exists, None otherwise
        """
        ...

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """
        Get all projection checkpoints.

        Returns:
            List of CheckpointData for all projections
        """
        ...


@runtime_checkable
class SubscriptionPositions(Protocol):
    """Global-position persistence for subscription runners."""

    async def get_position(self, subscription_id: str) -> int | None:
        """
        Get last processed global position for a subscription.

        Args:
            subscription_id: Identifier for the subscription (typically projection name)

        Returns:
            Last processed global position, or None if no checkpoint exists
            or if checkpoint doesn't have position data.
        """
        ...

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
        ...


@runtime_checkable
class CheckpointRepository(ProjectionCheckpoints, SubscriptionPositions, Protocol):
    """Composed convenience protocol: both capabilities in one table."""


__all__ = [
    "CheckpointData",
    "CheckpointRepository",
    "LagMetrics",
    "ProjectionCheckpoints",
    "SubscriptionPositions",
]
