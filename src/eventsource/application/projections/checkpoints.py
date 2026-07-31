"""Checkpoint operations for projections.

These four functions replace ProjectionCheckpointManager, which was a
stateless wrapper around one repository plus a tracer and held no invariant
of its own (ADR 0024). The tracer is passed in rather than constructed:
the projection already owns one.

Span names still read `eventsource.checkpoint_manager.*`. That class no
longer exists; renaming the spans would break users' dashboards for no
functional gain, so the names are kept deliberately.
"""

import logging
from typing import Any

from eventsource.events.base import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints

logger = logging.getLogger(__name__)


async def record_checkpoint(
    repo: ProjectionCheckpoints,
    projection_name: str,
    event: DomainEvent,
    tracer: Tracer,
) -> None:
    """Record the checkpoint after successfully processing an event."""
    with tracer.span(
        "eventsource.checkpoint_manager.update",
        {
            ATTR_PROJECTION_NAME: projection_name,
            ATTR_EVENT_TYPE: event.event_type,
        },
    ):
        await repo.update_checkpoint(
            projection_name=projection_name,
            event_id=event.event_id,
            event_type=event.event_type,
        )

        logger.debug(
            "Updated checkpoint for %s: event_id=%s, type=%s",
            projection_name,
            event.event_id,
            event.event_type,
            extra={
                "projection": projection_name,
                "event_id": str(event.event_id),
                "event_type": event.event_type,
            },
        )


async def read_checkpoint(
    repo: ProjectionCheckpoints,
    projection_name: str,
    tracer: Tracer,
) -> str | None:
    """Return the last processed event ID as a string, or None."""
    with tracer.span(
        "eventsource.checkpoint_manager.get_checkpoint",
        {ATTR_PROJECTION_NAME: projection_name},
    ):
        event_id = await repo.get_checkpoint(projection_name)
        return str(event_id) if event_id else None


async def lag_metrics_dict(
    repo: ProjectionCheckpoints,
    projection_name: str,
    event_types: list[str] | None,
    tracer: Tracer,
) -> dict[str, Any] | None:
    """Return projection lag metrics as a plain dict, or None.

    The dict shape (six keys) is the projection's public surface;
    returning `LagMetrics` instead would be a public API change.
    """
    with tracer.span(
        "eventsource.checkpoint_manager.get_lag_metrics",
        {ATTR_PROJECTION_NAME: projection_name},
    ):
        metrics = await repo.get_lag_metrics(projection_name, event_types=event_types)

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


async def reset_checkpoint(
    repo: ProjectionCheckpoints,
    projection_name: str,
    tracer: Tracer,
) -> None:
    """Delete the checkpoint so the projection restarts from the beginning."""
    with tracer.span(
        "eventsource.checkpoint_manager.reset",
        {ATTR_PROJECTION_NAME: projection_name},
    ):
        await repo.reset_checkpoint(projection_name)

        logger.info(
            "Reset checkpoint for projection %s",
            projection_name,
            extra={"projection": projection_name},
        )


__all__ = [
    "lag_metrics_dict",
    "read_checkpoint",
    "record_checkpoint",
    "reset_checkpoint",
]
