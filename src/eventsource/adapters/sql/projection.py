"""DatabaseProjection: SQL adapter for declarative projections (ADR 0024)."""

import asyncio
import logging
from typing import TYPE_CHECKING

from eventsource.application.projections.base import DeclarativeProjection, TenantFilter
from eventsource.application.projections.checkpoints import record_checkpoint
from eventsource.application.projections.dlq import send_to_dlq
from eventsource.application.projections.retry import ProjectionRetryPolicy
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository

if TYPE_CHECKING:
    from opentelemetry.trace import Span

logger = logging.getLogger(__name__)


class DatabaseProjection(DeclarativeProjection):
    """Projection with database connection support for 1- and 2-parameter handlers."""

    def __init__(
        self,
        session_factory: "async_sessionmaker[AsyncSession]",
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        enable_tracing: bool = False,
        *,
        retry_policy: ProjectionRetryPolicy | None = None,
        tracer: Tracer | None = None,
        tenant_filter: TenantFilter = None,
    ) -> None:
        """Initialize the database projection."""
        super().__init__(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            enable_tracing=enable_tracing,
            retry_policy=retry_policy,
            tracer=tracer,
            tenant_filter=tenant_filter,
        )
        self._session_factory = session_factory
        self._current_connection: AsyncConnection | None = None
        init_extra = {"projection": self._projection_name, "session_factory_type": type(session_factory).__name__}  # fmt: skip
        logger.info("DatabaseProjection %s initialized with session factory", self._projection_name, extra=init_extra)  # fmt: skip

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
                if self._checkpoint_repo is not None:
                    await record_checkpoint(self._checkpoint_repo, name, event, self._tracer)
                if span is not None:
                    span.set_attribute("checkpoint.updated", self._checkpoint_repo is not None)
                logger.debug("Projection %s processed event %s (type: %s)", name, eid, event.event_type, extra=base_extra)  # fmt: skip
                return
            except Exception as e:
                err_extra = {**base_extra, "attempt": attempt + 1, "max_attempts": max_attempts, "error": str(e)}  # fmt: skip
                logger.error(
                    "Projection %s failed to process event %s (attempt %d/%d): %s",
                    name,
                    eid,
                    attempt + 1,
                    max_attempts,
                    e,
                    exc_info=True,
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

    async def _execute_in_transaction(self, event: DomainEvent) -> None:
        """Execute event processing within a database transaction."""
        extra = self._event_extra(event)
        name, eid = self._projection_name, event.event_id
        logger.debug("DatabaseProjection %s beginning transaction for event %s", name, eid, extra=extra)  # fmt: skip

        async with self._session_factory() as session, session.begin():
            self._current_connection = await session.connection()
            try:
                await self._process_event(event)
                logger.debug("DatabaseProjection %s committing transaction for event %s", name, eid, extra=extra)  # fmt: skip
            except Exception as e:
                logger.error("DatabaseProjection %s rolling back transaction for event %s: %s", name, eid, e, extra={**extra, "error": str(e)})  # fmt: skip
                raise
            finally:
                self._current_connection = None

    async def _process_event(self, event: DomainEvent) -> None:
        """Route event to appropriate handler with database connection."""
        if (handler_info := self._handler_registry.get_handler(type(event))) is None:
            return await super()._process_event(event)

        handler_name = handler_info.handler_name
        attrs = {
            ATTR_PROJECTION_NAME: self._projection_name,
            ATTR_EVENT_TYPE: type(event).__name__,
            ATTR_HANDLER_NAME: handler_name,
        }
        with self._tracer.span("eventsource.projection.handler", attrs):
            if handler_info.param_count == 1:
                await self._handler_registry.dispatch(event, context=None)
            else:
                if self._current_connection is None:
                    raise RuntimeError(
                        f"Handler {handler_name} requires database connection "
                        f"but DatabaseProjection.handle() was not used. "
                        f"Ensure you call handle() rather than _process_event() directly."
                    )
                await self._handler_registry.dispatch(event, context=self._current_connection)


__all__ = ["DatabaseProjection"]

try:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker
except ImportError:
    if TYPE_CHECKING:
        from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker
