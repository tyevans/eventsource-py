"""DatabaseProjection: SQL adapter for declarative projections (ADR 0024)."""

import logging
from typing import TYPE_CHECKING

from eventsource.adapters.sql.projection_retry import DatabaseProjectionRetryMixin
from eventsource.application.projections.base import DeclarativeProjection, TenantFilter
from eventsource.application.projections.retry import ProjectionRetryPolicy
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
)
from eventsource.observability.error_summary import error_summary
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository

logger = logging.getLogger(__name__)


class DatabaseProjection(DatabaseProjectionRetryMixin, DeclarativeProjection):
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
        projection_name: str | None = None,
    ) -> None:
        """Initialize the database projection."""
        super().__init__(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            enable_tracing=enable_tracing,
            retry_policy=retry_policy,
            tracer=tracer,
            tenant_filter=tenant_filter,
            projection_name=projection_name,
        )
        self._session_factory = session_factory
        self._current_connection: AsyncConnection | None = None
        init_extra = {"projection": self._projection_name, "session_factory_type": type(session_factory).__name__}  # fmt: skip
        logger.info("DatabaseProjection %s initialized with session factory", self._projection_name, extra=init_extra)  # fmt: skip

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
                summary = error_summary(e)
                logger.error("DatabaseProjection %s rolling back transaction for event %s: %s", name, eid, summary, extra={**extra, "error": summary})  # fmt: skip
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
