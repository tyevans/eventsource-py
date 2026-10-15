"""Projection checkpoint and subscription position ports.

Pure boundary interfaces: stdlib, typing, uuid, datetime, dataclasses only.
No sqlalchemy, no observability, no implementation code.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from eventsource.ports.positions import Position


@dataclass(frozen=True)
class CheckpointData:
    """Data structure for checkpoint information."""

    projection_name: str
    last_event_id: UUID | None = None
    last_event_type: str | None = None
    last_processed_at: datetime | None = None
    events_processed: int = 0
    position: Position | None = None


@dataclass(frozen=True)
class LagMetrics:
    """Data structure for projection lag metrics."""

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
        """Get the last processed event ID for a projection, or None if none exists."""
        ...

    async def update_checkpoint(
        self,
        projection_name: str,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """Update the checkpoint for a projection (idempotent UPSERT)."""
        ...

    async def reset_checkpoint(self, projection_name: str) -> None:
        """Reset the checkpoint for a projection when rebuilding from scratch."""
        ...

    async def get_lag_metrics(
        self,
        projection_name: str,
        event_types: list[str] | None = None,
    ) -> LagMetrics | None:
        """Get lag metrics for a projection, or None if no checkpoint exists."""
        ...

    async def get_all_checkpoints(self) -> list[CheckpointData]:
        """Get all projection checkpoints."""
        ...


@runtime_checkable
class SubscriptionPositions(Protocol):
    """Opaque global-feed position persistence for subscription runners."""

    async def get_position(self, subscription_id: str) -> Position | None:
        """Get last processed global-feed position for a subscription, or None."""
        ...

    async def save_position(
        self,
        subscription_id: str,
        position: Position,
        event_id: UUID,
        event_type: str,
    ) -> None:
        """Save checkpoint with an opaque global-feed position (idempotent UPSERT)."""
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
