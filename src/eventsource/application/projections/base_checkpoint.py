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
    """Base class for projections with automatic checkpoint tracking, retry, and DLQ."""

    def __init__(
        self,
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        retry_policy: ProjectionRetryPolicy | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None:
        self._projection_name = self.__class__.__name__
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._checkpoint_repo: ProjectionCheckpoints | None = checkpoint_repo
        self._dlq_repo: DLQRepository | None = dlq_repo
        if retry_policy is not None:
            self._retry_policy = retry_policy
        else:
            from eventsource.application.subscriptions.retry import RetryConfig

            cfg = RetryConfig(max_retries=2, initial_delay=2.0, exponential_base=2.0, jitter=0.1)
            self._retry_policy = ExponentialBackoffRetryPolicy(config=cfg)

    async def handle(self, event: DomainEvent) -> None:
        """Handle event with retry logic, DLQ fallback, and checkpoint tracking."""
        attrs = {
            ATTR_PROJECTION_NAME: self._projection_name,
            ATTR_EVENT_TYPE: type(event).__name__,
            ATTR_EVENT_ID: str(event.event_id),
        }
        with self._tracer.span("eventsource.projection.handle", attrs) as span:
            await self._handle_with_retry(event, span)

    async def _handle_with_retry(self, event: DomainEvent, span: Span | None) -> None:
        max_attempts = self._retry_policy.max_retries + 1
        name, eid, etype = self._projection_name, str(event.event_id), event.event_type
        base_extra = {"projection": name, "event_id": eid, "event_type": etype}
        for attempt in range(max_attempts):
            try:
                await self._process_event(event)
            except Exception as e:
                err_extra = {
                    **base_extra,
                    "attempt": attempt + 1,
                    "max_attempts": max_attempts,
                    "error": str(e),
                }
                logger.error(
                    "Projection %s failed to process event %s (attempt %d/%d): %s",
                    name,
                    event.event_id,
                    attempt + 1,
                    max_attempts,
                    e,
                    exc_info=True,
                    extra=err_extra,
                )
                if not self._retry_policy.should_retry(attempt, e):
                    if span is not None:
                        span.set_attribute(ATTR_RETRY_COUNT, attempt + 1)
                    dlq_repo = self._dlq_repo
                    sent = (
                        await send_to_dlq(dlq_repo, name, event, e, attempt + 1, self._tracer)
                        if dlq_repo is not None
                        else False
                    )
                    msg = (
                        "Event %s sent to DLQ after %d attempts"
                        if sent
                        else "Event %s failed permanently after %d attempts; NO DLQ entry was recorded"
                    )
                    crit_extra = {**base_extra, "retry_count": attempt + 1}
                    logger.critical(msg, event.event_id, attempt + 1, extra=crit_extra)
                    raise
                backoff = self._retry_policy.get_backoff(attempt)
                retry_extra = {"projection": name, "event_id": eid, "backoff_seconds": backoff}
                logger.info("Retrying in %.1f seconds...", backoff, extra=retry_extra)
                await asyncio.sleep(backoff)
                continue

            await self._record_checkpoint_after_success(event, span)
            logger.debug(
                "Projection %s processed event %s (type: %s)",
                name,
                event.event_id,
                etype,
                extra=base_extra,
            )
            return

    async def _record_checkpoint_after_success(self, event: DomainEvent, span: Span | None) -> None:
        if self._checkpoint_repo is None:
            if span is not None:
                span.set_attribute("checkpoint.updated", False)
            return
        try:
            await record_checkpoint(
                self._checkpoint_repo, self._projection_name, event, self._tracer
            )
        except Exception as e:
            if span is not None:
                span.set_attribute("checkpoint.updated", False)
                span.set_attribute("checkpoint.failed", True)
            err_extra = {
                "projection": self._projection_name,
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "error": str(e),
            }
            logger.critical(
                "Projection %s applied event %s but could not record its "
                "checkpoint: %s. The event will be re-delivered and re-applied "
                "on restart; this is a checkpoint-store outage, not a bad event.",
                self._projection_name,
                event.event_id,
                e,
                exc_info=True,
                extra=err_extra,
            )
            raise
        if span is not None:
            span.set_attribute("checkpoint.updated", True)

    @abstractmethod
    async def _process_event(self, event: DomainEvent) -> None:
        """Process event in projection-specific way (implemented by subclasses)."""

    async def get_checkpoint(self) -> str | None:
        if self._checkpoint_repo is None:
            return None
        return await read_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)

    async def get_lag_metrics(self) -> dict[str, Any] | None:
        if self._checkpoint_repo is None:
            return None
        types = [et.__name__ for et in self.subscribed_to()]
        return await lag_metrics_dict(
            self._checkpoint_repo, self._projection_name, types, self._tracer
        )

    async def reset(self) -> None:
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

    @property
    def projection_name(self) -> str:
        return self._projection_name


__all__ = ["CheckpointTrackingProjection"]
