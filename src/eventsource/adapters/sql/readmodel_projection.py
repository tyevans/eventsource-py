"""Read model projection base class extending DatabaseProjection."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.application.projections.base import TenantFilter
from eventsource.application.projections.retry import ProjectionRetryPolicy
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
    ATTR_READMODEL_TYPE,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository
from eventsource.ports.readmodels.model import ReadModel

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

    from eventsource.ports.readmodels.repository import ReadModelRepository

logger = logging.getLogger(__name__)


class ReadModelProjection[TModel: ReadModel](DatabaseProjection):
    """Projection base class with automatic dialect-aware read model repository support."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        model_class: type[TModel],
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        enable_tracing: bool = False,
        *,
        retry_policy: ProjectionRetryPolicy | None = None,
        tracer: Tracer | None = None,
        tenant_filter: TenantFilter = None,
    ) -> None:
        """Initialize the read model projection."""
        super().__init__(
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            enable_tracing=enable_tracing,
            retry_policy=retry_policy,
            tracer=tracer,
            tenant_filter=tenant_filter,
        )
        self._model_class = model_class
        self._current_repository: ReadModelRepository[TModel] | None = None
        extra = {
            ATTR_PROJECTION_NAME: self._projection_name,
            ATTR_READMODEL_TYPE: model_class.__name__,
            "table_name": model_class.table_name(),
        }
        msg = "ReadModelProjection %s initialized for %s"
        logger.info(msg, self._projection_name, model_class.__name__, extra=extra)

    async def handle(self, event: DomainEvent) -> None:
        """Handle event within a database transaction with a scoped repository."""
        extra = {
            ATTR_PROJECTION_NAME: self._projection_name,
            "event_id": str(event.event_id),
            ATTR_EVENT_TYPE: event.event_type,
        }
        msg = "ReadModelProjection %s handling event %s"
        logger.debug(msg, self._projection_name, event.event_id, extra=extra)
        async with self._session_factory() as session, session.begin():
            conn = await session.connection()
            self._current_connection = conn
            self._current_repository = await self._create_repository(conn)
            try:
                await super(DatabaseProjection, self).handle(event)
            finally:
                self._current_connection = None
                self._current_repository = None

    async def _create_repository(self, conn: AsyncConnection) -> ReadModelRepository[TModel]:
        """Create a dialect-appropriate repository instance for the current connection."""
        from eventsource.adapters.postgresql.readmodels import PostgreSQLReadModelRepository

        dialect_name = conn.dialect.name
        if dialect_name == "sqlite":
            from aiosqlite import Connection as AiosqliteConnection

            from eventsource.adapters.sqlite.readmodels import SQLiteReadModelRepository

            raw_conn = await conn.get_raw_connection()
            driver_conn = raw_conn.driver_connection
            if driver_conn is None or not isinstance(driver_conn, AiosqliteConnection):
                raise RuntimeError(f"Expected aiosqlite Connection but got {type(driver_conn)}")
            return SQLiteReadModelRepository(
                driver_conn, self._model_class, enable_tracing=self._enable_tracing
            )

        if dialect_name != "postgresql":
            logger.warning(
                "Unknown dialect %s, using PostgreSQL repository",
                dialect_name,
                extra={ATTR_PROJECTION_NAME: self._projection_name, "dialect": dialect_name},
            )
        return PostgreSQLReadModelRepository(
            conn, self._model_class, enable_tracing=self._enable_tracing
        )

    async def _process_event(self, event: DomainEvent) -> None:
        """Route event to handler with repository context."""
        handler_info = self._handler_registry.get_handler(type(event))
        if handler_info is None:
            await self._handler_registry.dispatch(event, context=None)
            return

        handler_name = handler_info.handler_name
        span_attrs = {
            ATTR_PROJECTION_NAME: self._projection_name,
            ATTR_EVENT_TYPE: type(event).__name__,
            ATTR_HANDLER_NAME: handler_name,
        }
        with self._tracer.span("eventsource.projection.handler", span_attrs):
            if handler_info.param_count == 1:
                await self._handler_registry.dispatch(event, context=None)
            else:
                if (repo := self._current_repository) is None:
                    raise RuntimeError(
                        f"Handler {handler_name} requires repository but none available. "
                        f"Ensure you call handle() rather than _process_event() directly."
                    )
                await self._handler_registry.dispatch(event, context=repo)

    async def _truncate_read_models(self) -> None:
        """Truncate read model table via repository during reset()."""
        async with self._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await self._create_repository(conn)
            count = await repo.truncate()
            table = self._model_class.table_name()
            extra = {
                ATTR_PROJECTION_NAME: self._projection_name,
                "table": table,
                "records_deleted": count,
            }
            msg = "ReadModelProjection %s truncated %d records from %s"
            logger.info(msg, self._projection_name, count, table, extra=extra)

    @property
    def model_class(self) -> type[TModel]:
        """Get the model class this projection manages."""
        return self._model_class

    @property
    def repository(self) -> ReadModelRepository[TModel] | None:
        """Get the current repository instance (only available during handle())."""
        return self._current_repository

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        tracing = "enabled" if self._enable_tracing else "disabled"
        return (
            f"{self.__class__.__name__}(model={self._model_class.__name__}, "
            f"table={self._model_class.table_name()}, tracing={tracing})"
        )
