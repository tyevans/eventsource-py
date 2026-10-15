"""
Checkpoint tracking projection base implementation with retry and DLQ support.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.trace import Span

from eventsource.application.projections.checkpoints import (
    lag_metrics_dict,
    read_checkpoint,
    record_checkpoint,
    reset_checkpoint,
)
from eventsource.application.projections.dlq import send_to_dlq
from eventsource.application.projections.retry import (
    ExponentialBackoffRetryPolicy,
    ProjectionRetryPolicy,
)
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository
from eventsource.ports.handlers import EventSubscriber

logger = logging.getLogger(__name__)


class CheckpointTrackingProjection(EventSubscriber, ABC):
    """
    Base class for projections with automatic checkpoint tracking, retry, and DLQ.

    Subclasses must implement:
    - subscribed_to(): List of event types to handle
    - _process_event(): Actual projection logic
    - _truncate_read_models(): Optional table truncation for reset
    """

    def __init__(
        self,
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        retry_policy: ProjectionRetryPolicy | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the checkpoint-tracking projection.

        Args:
            checkpoint_repo: Repository for checkpoint storage (None disables tracking).
            dlq_repo: Repository for dead letter queue (None disables DLQ capture).
            retry_policy: Policy for retry behavior (defaults to ExponentialBackoffRetryPolicy).
            tracer: Optional custom Tracer instance.
            enable_tracing: Emit traces if True and OpenTelemetry is available.
        """
        self._projection_name = self.__class__.__name__
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self._checkpoint_repo: ProjectionCheckpoints | None = checkpoint_repo
        self._dlq_repo: DLQRepository | None = dlq_repo

        if retry_policy is not None:
            self._retry_policy = retry_policy
        else:
            from eventsource.application.subscriptions.retry import RetryConfig

            self._retry_policy = ExponentialBackoffRetryPolicy(
                config=RetryConfig(
                    max_retries=2,
                    initial_delay=2.0,
                    exponential_base=2.0,
                    jitter=0.1,
                )
            )

    async def handle(self, event: DomainEvent) -> None:
        """Handle event with retry logic, DLQ fallback, and checkpoint tracking."""
        with self._tracer.span(
            "eventsource.projection.handle",
            {
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_EVENT_ID: str(event.event_id),
            },
        ) as span:
            await self._handle_with_retry(event, span)

    async def _handle_with_retry(self, event: DomainEvent, span: Span | None) -> None:
        """Execute event handling with configured retry policy."""
        max_attempts = self._retry_policy.max_retries + 1

        for attempt in range(max_attempts):
            try:
                # Only _process_event belongs in this try block so unrelated failures
                # are not treated as poison events.
                await self._process_event(event)

            except Exception as e:
                logger.error(
                    "Projection %s failed to process event %s (attempt %d/%d): %s",
                    self._projection_name,
                    event.event_id,
                    attempt + 1,
                    max_attempts,
                    e,
                    exc_info=True,
                    extra={
                        "projection": self._projection_name,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "attempt": attempt + 1,
                        "max_attempts": max_attempts,
                        "error": str(e),
                    },
                )

                # Check if we should retry
                if not self._retry_policy.should_retry(attempt, e):
                    # Add retry count to span before final failure
                    if span is not None:
                        span.set_attribute(ATTR_RETRY_COUNT, attempt + 1)

                    # Send to DLQ
                    sent_to_dlq = False
                    if self._dlq_repo is not None:
                        sent_to_dlq = await send_to_dlq(
                            self._dlq_repo,
                            self._projection_name,
                            event,
                            e,
                            attempt + 1,
                            self._tracer,
                        )
                    log_extra = {
                        "projection": self._projection_name,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "retry_count": attempt + 1,
                    }
                    if sent_to_dlq:
                        logger.critical(
                            "Event %s sent to DLQ after %d attempts",
                            event.event_id,
                            attempt + 1,
                            extra=log_extra,
                        )
                    else:
                        logger.critical(
                            "Event %s failed permanently after %d attempts; "
                            "NO DLQ entry was recorded",
                            event.event_id,
                            attempt + 1,
                            extra=log_extra,
                        )
                    # Re-raise the exception after exhausting all retries
                    raise
                else:
                    # Get backoff from policy
                    backoff = self._retry_policy.get_backoff(attempt)
                    logger.info(
                        "Retrying in %.1f seconds...",
                        backoff,
                        extra={
                            "projection": self._projection_name,
                            "event_id": str(event.event_id),
                            "backoff_seconds": backoff,
                        },
                    )
                    await asyncio.sleep(backoff)
                    continue

            # The event applied cleanly. Checkpointing happens here, outside
            # the retry loop, because a checkpoint-store failure is a liveness
            # problem, not a poison event: retrying it would re-run
            # `_process_event` (re-applying the read-model mutation), and
            # exhausting the retries would DLQ an event the projection has
            # already applied successfully -- so an operator replaying the DLQ
            # would apply it yet again. `docs/architecture.md` has always
            # described the intended ordering; the code had it in the wrong
            # block.
            await self._record_checkpoint_after_success(event, span)
            logger.debug(
                "Projection %s processed event %s (type: %s)",
                self._projection_name,
                event.event_id,
                event.event_type,
                extra={
                    "projection": self._projection_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                },
            )
            return

    async def _record_checkpoint_after_success(self, event: DomainEvent, span: Span | None) -> None:
        """Record the checkpoint for an event the projection already applied.

        Failure is re-raised so the caller (a subscription runner) sees a
        stalled projection rather than silent progress loss, but it is never
        routed to the DLQ and never re-runs the handler.
        """
        checkpoint_repo = self._checkpoint_repo
        if checkpoint_repo is None:
            if span is not None:
                span.set_attribute("checkpoint.updated", False)
            return

        try:
            await record_checkpoint(checkpoint_repo, self._projection_name, event, self._tracer)
        except Exception as e:
            if span is not None:
                span.set_attribute("checkpoint.updated", False)
                span.set_attribute("checkpoint.failed", True)
            logger.critical(
                "Projection %s applied event %s but could not record its "
                "checkpoint: %s. The event will be re-delivered and re-applied "
                "on restart; this is a checkpoint-store outage, not a bad event.",
                self._projection_name,
                event.event_id,
                e,
                exc_info=True,
                extra={
                    "projection": self._projection_name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                    "error": str(e),
                },
            )
            raise

        if span is not None:
            span.set_attribute("checkpoint.updated", True)

    @abstractmethod
    async def _process_event(self, event: DomainEvent) -> None:
        """
        Process event in projection-specific way.

        This method must be implemented by subclasses to define how
        the projection updates its read models based on events.

        Args:
            event: The domain event to process
        """
        pass

    async def get_checkpoint(self) -> str | None:
        """
        Get last processed event ID.

        Returns:
            Last processed event ID as string, or None if no checkpoint exists
            (including when checkpoint tracking is disabled)
        """
        if self._checkpoint_repo is None:
            return None
        return await read_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)

    async def get_lag_metrics(self) -> dict[str, Any] | None:
        """
        Get projection lag metrics.

        Returns:
            Dictionary with lag information, or None if no checkpoint exists
            (including when checkpoint tracking is disabled)
        """
        if self._checkpoint_repo is None:
            return None
        return await lag_metrics_dict(
            self._checkpoint_repo,
            self._projection_name,
            [et.__name__ for et in self.subscribed_to()],
            self._tracer,
        )

    async def reset(self) -> None:
        """
        Reset the projection by clearing checkpoint and read model data.

        Calls _truncate_read_models() which subclasses may override.
        """
        logger.warning(
            "Resetting projection %s",
            self._projection_name,
            extra={"projection": self._projection_name},
        )

        # Reset checkpoint
        if self._checkpoint_repo is not None:
            await reset_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)

        # Subclass truncates its read model tables
        await self._truncate_read_models()

    async def _truncate_read_models(self) -> None:
        """
        Truncate read model tables for this projection.

        Override in subclasses to specify which tables to clear.
        Default implementation does nothing.
        """
        pass

    @property
    def projection_name(self) -> str:
        """Get the projection name."""
        return self._projection_name


__all__ = ["CheckpointTrackingProjection"]
