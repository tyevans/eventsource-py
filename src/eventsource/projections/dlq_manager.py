"""
Dead Letter Queue manager for projection event processing.

This module provides the ProjectionDLQManager class which extracts
DLQ management from CheckpointTrackingProjection, addressing the
Single Responsibility Principle violation.

The manager handles:
- Sending failed events to the dead letter queue
- Error logging and handling for DLQ operations

Example:
    >>> from eventsource.projections.dlq_manager import ProjectionDLQManager
    >>> from eventsource.repositories.dlq import InMemoryDLQRepository
    >>>
    >>> manager = ProjectionDLQManager(
    ...     projection_name="OrderProjection",
    ...     dlq_repo=InMemoryDLQRepository(),
    ... )
    >>> await manager.send_to_dlq(event, error, retry_count=3)
"""

import logging

from eventsource.events.base import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.repositories.dlq import DLQEntry, DLQRepository, InMemoryDLQRepository

logger = logging.getLogger(__name__)


class ProjectionDLQManager:
    """
    Manager for projection Dead Letter Queue operations.

    Encapsulates DLQ operations for a projection, providing a clean
    interface for handling permanently failed events.

    Dead Letter Queues (DLQs) store events that failed processing
    after all retry attempts, enabling:
    - Investigation of processing failures
    - Manual reprocessing after fixes
    - Audit trails for failed events

    This extraction addresses SRP by separating DLQ management
    from the projection's retry logic.

    Example:
        >>> manager = ProjectionDLQManager(
        ...     projection_name="OrderProjection",
        ...     dlq_repo=PostgreSQLDLQRepository(conn),
        ... )
        >>>
        >>> # After all retries exhausted
        >>> try:
        ...     await process_event(event)
        ... except Exception as e:
        ...     await manager.send_to_dlq(event, e, retry_count=3)
        ...     raise
    """

    def __init__(
        self,
        projection_name: str,
        dlq_repo: DLQRepository | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the DLQ manager.

        Args:
            projection_name: Name of the projection (used in DLQ entries)
            dlq_repo: Repository for DLQ storage.
                     If None, uses InMemoryDLQRepository.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False for high-frequency operations.
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._projection_name = projection_name
        self._dlq_repo = dlq_repo or InMemoryDLQRepository()

    @property
    def projection_name(self) -> str:
        """Get the projection name."""
        return self._projection_name

    @property
    def dlq_repo(self) -> DLQRepository:
        """Get the underlying DLQ repository."""
        return self._dlq_repo

    async def send_to_dlq(
        self,
        event: DomainEvent,
        error: Exception,
        retry_count: int,
    ) -> bool:
        """
        Send a failed event to the dead letter queue.

        Called when all retry attempts have been exhausted and the
        event cannot be processed. The event is serialized and stored
        along with error information.

        Args:
            event: The domain event that failed processing
            error: The exception that caused the final failure
            retry_count: Number of retry attempts made

        Returns:
            True if event was successfully added to DLQ, False if DLQ write failed
        """
        with self._tracer.span(
            "eventsource.dlq_manager.send_to_dlq",
            {
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_EVENT_TYPE: event.event_type,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_RETRY_COUNT: retry_count,
            },
        ):
            try:
                await self._dlq_repo.add_failed_event(
                    event_id=event.event_id,
                    projection_name=self._projection_name,
                    event_type=event.event_type,
                    event_data=event.model_dump(mode="json"),
                    error=error,
                    retry_count=retry_count,
                )

                logger.warning(
                    "Event %s sent to DLQ for projection %s after %d attempts",
                    event.event_id,
                    self._projection_name,
                    retry_count,
                    extra={
                        "projection": self._projection_name,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "retry_count": retry_count,
                        "error": str(error),
                        "error_type": type(error).__name__,
                    },
                )
                return True

            except Exception as dlq_error:
                # DLQ write failed - this is critical as we're losing
                # visibility into the failed event
                logger.critical(
                    "Failed to write event %s to DLQ for projection %s: %s",
                    event.event_id,
                    self._projection_name,
                    dlq_error,
                    exc_info=True,
                    extra={
                        "projection": self._projection_name,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "original_error": str(error),
                        "dlq_error": str(dlq_error),
                    },
                )
                return False

    async def get_failed_events(
        self,
        limit: int = 100,
    ) -> list[DLQEntry]:
        """
        Get failed events from the DLQ for this projection.

        Retrieves events that failed processing for investigation
        or manual reprocessing.

        Args:
            limit: Maximum number of events to retrieve

        Returns:
            List of failed event records with error information
        """
        with self._tracer.span(
            "eventsource.dlq_manager.get_failed_events",
            {ATTR_PROJECTION_NAME: self._projection_name},
        ):
            try:
                events = await self._dlq_repo.get_failed_events(
                    projection_name=self._projection_name,
                    limit=limit,
                )
                return events
            except Exception as e:
                logger.error(
                    "Failed to get DLQ events for projection %s: %s",
                    self._projection_name,
                    e,
                    exc_info=True,
                )
                return []

    def __repr__(self) -> str:
        return f"ProjectionDLQManager({self._projection_name})"


__all__ = [
    "ProjectionDLQManager",
]
