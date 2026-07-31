"""Dead letter queue operations for projections.

These two functions replace ProjectionDLQManager (ADR 0024). Span names
still read `eventsource.dlq_manager.*` deliberately -- see the sibling
checkpoints module.
"""

import logging

from eventsource.events.base import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.ports.dlq import DLQEntry, DLQRepository

logger = logging.getLogger(__name__)


async def send_to_dlq(
    repo: DLQRepository,
    projection_name: str,
    event: DomainEvent,
    error: Exception,
    retry_count: int,
    tracer: Tracer,
) -> bool:
    """Send a permanently failed event to the DLQ.

    Returns True on success, False if the DLQ write itself failed. A DLQ
    failure is logged at critical and swallowed: the caller is about to
    re-raise the original processing error and must not have it masked.
    """
    with tracer.span(
        "eventsource.dlq_manager.send_to_dlq",
        {
            ATTR_PROJECTION_NAME: projection_name,
            ATTR_EVENT_TYPE: event.event_type,
            ATTR_EVENT_ID: str(event.event_id),
            ATTR_RETRY_COUNT: retry_count,
        },
    ):
        try:
            await repo.add_failed_event(
                event_id=event.event_id,
                projection_name=projection_name,
                event_type=event.event_type,
                event_data=event.model_dump(mode="json"),
                error=error,
                retry_count=retry_count,
            )

            logger.warning(
                "Event %s sent to DLQ for projection %s after %d attempts",
                event.event_id,
                projection_name,
                retry_count,
                extra={
                    "projection": projection_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "retry_count": retry_count,
                    "error": str(error),
                    "error_type": type(error).__name__,
                },
            )
            return True

        except Exception as dlq_error:
            logger.critical(
                "Failed to write event %s to DLQ for projection %s: %s",
                event.event_id,
                projection_name,
                dlq_error,
                exc_info=True,
                extra={
                    "projection": projection_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "original_error": str(error),
                    "dlq_error": str(dlq_error),
                },
            )
            return False


async def read_failed_events(
    repo: DLQRepository,
    projection_name: str,
    tracer: Tracer,
    limit: int = 100,
) -> list[DLQEntry]:
    """Read this projection's failed events. Errors collapse to an empty list."""
    with tracer.span(
        "eventsource.dlq_manager.get_failed_events",
        {ATTR_PROJECTION_NAME: projection_name},
    ):
        try:
            return await repo.get_failed_events(
                projection_name=projection_name,
                limit=limit,
            )
        except Exception as e:
            logger.error(
                "Failed to get DLQ events for projection %s: %s",
                projection_name,
                e,
                exc_info=True,
            )
            return []


__all__ = ["read_failed_events", "send_to_dlq"]
