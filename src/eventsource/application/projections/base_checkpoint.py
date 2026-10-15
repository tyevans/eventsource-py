"""Checkpoint tracking projection base implementation with retry and DLQ support."""

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

    Subclasses must implement ``subscribed_to()`` and ``_process_event()``,
    and may override ``_truncate_read_models()`` for reset.
    """

    def __init__(
        self,
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        retry_policy: ProjectionRetryPolicy | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """Initialize the checkpoint-tracking projection."""
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

                if not self._retry_policy.should_retry(attempt, e):
                    if span is not None:
                        span.set_attribute(ATTR_RETRY_COUNT, attempt + 1)

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
                    raise
                else:
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

            # Checkpointing happens outside the retry loop because a checkpoint-store
            # failure is a liveness problem, not a poison event: retrying it would
            # re-run _process_event and exhausting retries would DLQ an already-applied event.
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
        """Record the checkpoint for an event the projection already applied."""
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
        """Process event in projection-specific way (implemented by subclasses)."""
        pass

    async def get_checkpoint(self) -> str | None:
        """Get last processed event ID, or None if no checkpoint exists or tracking disabled."""
        if self._checkpoint_repo is None:
            return None
        return await read_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)

    async def get_lag_metrics(self) -> dict[str, Any] | None:
        """Get projection lag metrics, or None if no checkpoint exists or tracking disabled."""
        if self._checkpoint_repo is None:
            return None
        return await lag_metrics_dict(
            self._checkpoint_repo,
            self._projection_name,
            [et.__name__ for et in self.subscribed_to()],
            self._tracer,
        )

    async def reset(self) -> None:
        """Reset the projection by clearing checkpoint and read model data."""
        logger.warning(
            "Resetting projection %s",
            self._projection_name,
            extra={"projection": self._projection_name},
        )
        if self._checkpoint_repo is not None:
            await reset_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)
        await self._truncate_read_models()

    async def _truncate_read_models(self) -> None:
        """Truncate read model tables for this projection (default no-op)."""
        pass

    @property
    def projection_name(self) -> str:
        """Get the projection name."""
        return self._projection_name


__all__ = ["CheckpointTrackingProjection"]
