"""Retry, checkpoint, and per-attempt logging for `DatabaseProjection` (ADR 0024)."""

import asyncio
import logging
from typing import TYPE_CHECKING

from eventsource.application.projections.checkpoints import record_checkpoint
from eventsource.application.projections.dlq import send_to_dlq
from eventsource.application.projections.retry import ProjectionRetryPolicy
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import ATTR_RETRY_COUNT
from eventsource.observability.error_summary import error_summary
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository

if TYPE_CHECKING:
    from opentelemetry.trace import Span

# Keeps the logger name the emitting class had before this helper moved, so the
# telemetry schema (log records filtered by logger name) does not change.
logger = logging.getLogger("eventsource.adapters.sql.projection")


class DatabaseProjectionRetryMixin:
    """Retry loop and checkpoint recording mixed into `DatabaseProjection`.

    The host class supplies `_execute_in_transaction` and the attributes below.
    """

    _projection_name: str
    _retry_policy: ProjectionRetryPolicy
    _checkpoint_repo: ProjectionCheckpoints | None
    _dlq_repo: DLQRepository | None
    _tracer: Tracer

    async def _execute_in_transaction(self, event: DomainEvent) -> None:
        """Run one attempt in a transaction. Overridden by `DatabaseProjection`."""
        raise NotImplementedError

    def _event_extra(self, event: DomainEvent) -> dict[str, str]:
        return {"projection": self._projection_name, "event_id": str(event.event_id), "event_type": event.event_type}  # fmt: skip

    async def _handle_with_retry(self, event: DomainEvent, span: "Span | None") -> None:
        """Execute retry logic with a fresh database transaction per attempt."""
        max_attempts = self._retry_policy.max_retries + 1
        base_extra = self._event_extra(event)
        name, eid = self._projection_name, event.event_id

        for attempt in range(max_attempts):
            try:
                await self._execute_in_transaction(event)
            except Exception as e:
                # No exc_info and no str(e): the traceback and message can echo bound SQL parameters.
                err_extra = {**base_extra, "attempt": attempt + 1, "max_attempts": max_attempts, "error": error_summary(e)}  # fmt: skip
                logger.error(
                    "Projection %s failed to process event %s (attempt %d/%d): %s",
                    name,
                    eid,
                    attempt + 1,
                    max_attempts,
                    error_summary(e),
                    extra=err_extra,
                )
                if not self._retry_policy.should_retry(attempt, e):
                    if span is not None:
                        span.set_attribute(ATTR_RETRY_COUNT, attempt + 1)
                    sent_to_dlq = self._dlq_repo is not None and await send_to_dlq(
                        self._dlq_repo, name, event, e, attempt + 1, self._tracer
                    )
                    msg = (
                        "Event %s sent to DLQ after %d attempts"
                        if sent_to_dlq
                        else "Event %s failed permanently after %d attempts; NO DLQ entry was recorded"
                    )
                    logger.critical(msg, eid, attempt + 1, extra={**base_extra, "retry_count": attempt + 1})  # fmt: skip
                    raise

                backoff = self._retry_policy.get_backoff(attempt)
                logger.info("Retrying in %.1f seconds...", backoff, extra={**base_extra, "backoff_seconds": backoff})  # fmt: skip
                await asyncio.sleep(backoff)
                continue

            # Outside the retry loop: the transaction already committed, so a
            # checkpoint failure must not re-run the handler.
            await self._record_checkpoint_after_success(event, span)
            logger.debug("Projection %s processed event %s (type: %s)", name, eid, event.event_type, extra=base_extra)  # fmt: skip
            return

    async def _record_checkpoint_after_success(
        self, event: DomainEvent, span: "Span | None"
    ) -> None:
        """Record the checkpoint for an applied event. Errors propagate to the caller."""
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
            summary = error_summary(e)
            ex = {"projection": p, "event_id": str(eid), "event_type": et, "error": summary}
            msg = (
                "Projection %s applied event %s but could not record its checkpoint: %s. "
                "The event will be re-delivered and re-applied on restart; this is a "
                "checkpoint-store outage, not a bad event."
            )
            logger.critical(msg, p, eid, summary, extra=ex)
            raise
        if span is not None:
            span.set_attribute("checkpoint.updated", True)
