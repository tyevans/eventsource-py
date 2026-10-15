"""Checkpoint tracking projection base implementation with retry and DLQ support."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.trace import Span

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

from .checkpoints import lag_metrics_dict, read_checkpoint, record_checkpoint, reset_checkpoint
from .dlq import send_to_dlq
from .retry import ExponentialBackoffRetryPolicy, ProjectionRetryPolicy

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
        p, et, eid = self._projection_name, type(event).__name__, str(event.event_id)
        attrs = {ATTR_PROJECTION_NAME: p, ATTR_EVENT_TYPE: et, ATTR_EVENT_ID: eid}
        with self._tracer.span("eventsource.projection.handle", attrs) as span:
            await self._handle_with_retry(event, span)

    async def _handle_with_retry(self, event: DomainEvent, span: Span | None) -> None:
        max_att = self._retry_policy.max_retries + 1
        name, eid, etype = self._projection_name, str(event.event_id), event.event_type
        base_ex = {"projection": name, "event_id": eid, "event_type": etype}
        for attempt in range(max_att):
            try:
                await self._process_event(event)
            except Exception as e:
                att = attempt + 1
                ex = {**base_ex, "attempt": att, "max_attempts": max_att, "error": str(e)}
                msg = "Projection %s failed to process event %s (attempt %d/%d): %s"
                logger.error(msg, name, event.event_id, att, max_att, e, exc_info=True, extra=ex)
                if not self._retry_policy.should_retry(attempt, e):
                    if span is not None:
                        span.set_attribute(ATTR_RETRY_COUNT, att)
                    dlq, tr = self._dlq_repo, self._tracer
                    sent = await send_to_dlq(dlq, name, event, e, att, tr) if dlq else False
                    m1 = "Event %s sent to DLQ after %d attempts"
                    m2 = "Event %s failed permanently after %d attempts; NO DLQ entry was recorded"
                    ex_dlq = {**base_ex, "retry_count": att}
                    logger.critical(m1 if sent else m2, event.event_id, att, extra=ex_dlq)
                    raise
                backoff = self._retry_policy.get_backoff(attempt)
                retry_ex = {"projection": name, "event_id": eid, "backoff_seconds": backoff}
                logger.info("Retrying in %.1f seconds...", backoff, extra=retry_ex)
                await asyncio.sleep(backoff)
                continue

            await self._record_checkpoint_after_success(event, span)
            dbg = "Projection %s processed event %s (type: %s)"
            logger.debug(dbg, name, event.event_id, etype, extra=base_ex)
            return

    async def _record_checkpoint_after_success(self, event: DomainEvent, span: Span | None) -> None:
        if (repo := self._checkpoint_repo) is None:
            if span is not None:
                span.set_attribute("checkpoint.updated", False)
            return
        try:
            await record_checkpoint(repo, self._projection_name, event, self._tracer)
        except Exception as e:
            if span is not None:
                span.set_attribute("checkpoint.updated", False)
                span.set_attribute("checkpoint.failed", True)
            p, eid, et = self._projection_name, event.event_id, event.event_type
            ex = {"projection": p, "event_id": str(eid), "event_type": et, "error": str(e)}
            msg = (
                "Projection %s applied event %s but could not record its checkpoint: %s. "
                "The event will be re-delivered and re-applied on restart; this is a "
                "checkpoint-store outage, not a bad event."
            )
            logger.critical(msg, p, eid, e, exc_info=True, extra=ex)
            raise
        if span is not None:
            span.set_attribute("checkpoint.updated", True)

    @abstractmethod
    async def _process_event(self, event: DomainEvent) -> None: ...

    async def get_checkpoint(self) -> str | None:
        if (repo := self._checkpoint_repo) is None:
            return None
        return await read_checkpoint(repo, self._projection_name, self._tracer)

    async def get_lag_metrics(self) -> dict[str, Any] | None:
        if (repo := self._checkpoint_repo) is None:
            return None
        types = [et.__name__ for et in self.subscribed_to()]
        return await lag_metrics_dict(repo, self._projection_name, types, self._tracer)

    async def reset(self) -> None:
        p = self._projection_name
        logger.warning("Resetting projection %s", p, extra={"projection": p})
        if (repo := self._checkpoint_repo) is not None:
            await reset_checkpoint(repo, p, self._tracer)
        await self._truncate_read_models()

    async def _truncate_read_models(self) -> None: ...

    projection_name: Any = property(lambda self: self._projection_name)


__all__ = ["CheckpointTrackingProjection"]
