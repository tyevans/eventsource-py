"""
Read model projection base class.

Extends DatabaseProjection to provide automatic read model repository
creation and routing to handler methods, simplifying common projection patterns.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Generic, TypeVar

from eventsource.events.base import DomainEvent
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
    ATTR_READMODEL_TYPE,
)
from eventsource.projections.base import DatabaseProjection
from eventsource.readmodels.base import ReadModel
from eventsource.repositories.checkpoint import CheckpointRepository
from eventsource.repositories.dlq import DLQRepository

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker

    from eventsource.readmodels.repository import ReadModelRepository

logger = logging.getLogger(__name__)

# Type variable for read model types
TModel = TypeVar("TModel", bound=ReadModel)


class ReadModelProjection(DatabaseProjection, Generic[TModel]):
    """
    Projection base class with automatic read model repository support.

    Extends DatabaseProjection to provide:
    - Automatic repository creation based on session factory
    - Handler methods receive repository instead of raw connection
    - Automatic truncate via repository for reset operations
    - Type-safe generic for model class
    - Integration with @handles decorator for declarative event handling
    - Automatic handler discovery via HandlerRegistry

    The repository type is automatically determined from the session factory:
    - PostgreSQL session -> PostgreSQLReadModelRepository
    - SQLite session -> SQLiteReadModelRepository

    Handler Signatures:
        ReadModelProjection handlers can have two signatures:

        1. Single parameter (event only):
           @handles(EventType)
           async def _on_event(self, event: EventType) -> None:
               # No database access needed
               pass

        2. Two parameters (repository + event):
           @handles(EventType)
           async def _on_event(self, repo: ReadModelRepository, event: EventType) -> None:
               # Full repository access
               await repo.save(model)

        The handler signature is auto-detected via inspection:
        - 1 param: context=None passed to handler registry
        - 2 params: context=repository passed to handler registry

    Handler Requirements:
        - All handlers MUST be async (use 'async def')
        - Handlers must accept 1 or 2 parameters (event only, or repo + event)
        - Use @handles(EventType) decorator to mark handler methods

    Example:
        >>> from eventsource.readmodels import ReadModelProjection, ReadModel
        >>> from eventsource.projections import handles
        >>> # Or: from eventsource import ReadModelProjection, handles
        >>>
        >>> class OrderSummary(ReadModel):
        ...     order_number: str
        ...     status: str
        ...
        >>> class OrderProjection(ReadModelProjection[OrderSummary]):
        ...     @handles(OrderCreated)
        ...     async def _on_created(self, repo, event: OrderCreated) -> None:
        ...         await repo.save(OrderSummary(
        ...             id=event.aggregate_id,
        ...             order_number=event.order_number,
        ...             status="pending",
        ...         ))
        ...
        ...     @handles(OrderShipped)
        ...     async def _on_shipped(self, repo, event: OrderShipped) -> None:
        ...         summary = await repo.get(event.aggregate_id)
        ...         if summary:
        ...             summary.status = "shipped"
        ...             await repo.save(summary)
        ...
        ...     @handles(OrderCancelled)
        ...     async def _on_cancelled(self, repo, event: OrderCancelled) -> None:
        ...         await repo.soft_delete(event.aggregate_id)
        >>>
        >>> # Usage
        >>> projection = OrderProjection(
        ...     session_factory=async_session_factory,
        ...     model_class=OrderSummary,
        ... )
        >>> await projection.handle(event)

    Unregistered Event Handling:
        By default, events without handlers are silently ignored. Configure
        this behavior via class attribute:

        >>> class StrictProjection(ReadModelProjection[MyModel]):
        ...     unregistered_event_handling = "error"  # Raise UnhandledEventError
        ...     # Or "warn" to log a warning
        ...     # Or "ignore" (default) to silently skip

    Note:
        - Each handler invocation gets a fresh repository instance
        - Repository operations share the same transaction as the projection
        - Soft delete, queries, and all repository features are available
        - Handler discovery happens at construction time via HandlerRegistry
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        model_class: type[TModel],
        checkpoint_repo: CheckpointRepository | None = None,
        dlq_repo: DLQRepository | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the read model projection.

        Args:
            session_factory: SQLAlchemy async session factory for database connections
            model_class: The ReadModel subclass this projection manages
            checkpoint_repo: Repository for checkpoint storage
            dlq_repo: Repository for dead letter queue
            enable_tracing: Whether to enable OpenTelemetry tracing
        """
        super().__init__(
            session_factory=session_factory,
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            enable_tracing=enable_tracing,
        )
        self._model_class = model_class
        self._current_repository: ReadModelRepository[TModel] | None = None

        logger.info(
            "ReadModelProjection %s initialized for %s",
            self._projection_name,
            model_class.__name__,
            extra={
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_READMODEL_TYPE: model_class.__name__,
                "table_name": model_class.table_name(),
            },
        )

    async def handle(self, event: DomainEvent) -> None:
        """
        Handle event within a database transaction with repository.

        Wraps the parent handle() and creates a repository instance
        scoped to the transaction. The repository is available to
        handler methods via the context parameter.

        Args:
            event: The domain event to process
        """
        logger.debug(
            "ReadModelProjection %s handling event %s",
            self._projection_name,
            event.event_id,
            extra={
                ATTR_PROJECTION_NAME: self._projection_name,
                "event_id": str(event.event_id),
                ATTR_EVENT_TYPE: event.event_type,
            },
        )

        async with self._session_factory() as session, session.begin():
            conn = await session.connection()
            self._current_connection = conn

            # Create repository for this transaction
            self._current_repository = await self._create_repository(conn)

            try:
                # Call parent's handle logic (retry, checkpoint, etc.)
                # But we override _process_event to use repository
                await super(DatabaseProjection, self).handle(event)

            finally:
                self._current_connection = None
                self._current_repository = None

    async def _create_repository(self, conn: AsyncConnection) -> ReadModelRepository[TModel]:
        """
        Create a repository instance for the current connection.

        Detects the database type from the connection and creates
        the appropriate repository implementation.

        Args:
            conn: Database connection

        Returns:
            ReadModelRepository instance
        """
        # Import here to avoid circular imports
        from eventsource.readmodels.postgresql import PostgreSQLReadModelRepository

        # Detect dialect from connection
        dialect_name = conn.dialect.name

        if dialect_name == "postgresql":
            return PostgreSQLReadModelRepository(
                conn,
                self._model_class,
                enable_tracing=self._enable_tracing,
            )
        elif dialect_name == "sqlite":
            # For SQLite, we need the raw connection
            from aiosqlite import Connection as AiosqliteConnection

            from eventsource.readmodels.sqlite import SQLiteReadModelRepository

            # Get raw aiosqlite connection from SQLAlchemy
            raw_conn = await conn.get_raw_connection()
            driver_conn = raw_conn.driver_connection
            if driver_conn is None or not isinstance(driver_conn, AiosqliteConnection):
                raise RuntimeError(f"Expected aiosqlite Connection but got {type(driver_conn)}")
            return SQLiteReadModelRepository(
                driver_conn,
                self._model_class,
                enable_tracing=self._enable_tracing,
            )
        else:
            # Fallback to PostgreSQL-style (works with most SQL databases)
            logger.warning(
                "Unknown dialect %s, using PostgreSQL repository",
                dialect_name,
                extra={
                    ATTR_PROJECTION_NAME: self._projection_name,
                    "dialect": dialect_name,
                },
            )
            return PostgreSQLReadModelRepository(
                conn,
                self._model_class,
                enable_tracing=self._enable_tracing,
            )

    async def _process_event(self, event: DomainEvent) -> None:
        """
        Route event to handler with repository context.

        Overrides parent to pass repository instead of raw connection
        to handler methods.

        Args:
            event: The domain event to process
        """
        handler_info = self._handler_registry.get_handler(type(event))

        if handler_info is None:
            # Delegate unregistered event handling to registry
            # This will either ignore, warn, or raise based on unregistered_event_handling
            await self._handler_registry.dispatch(event, context=None)
            return

        handler_name = handler_info.handler_name

        with self._tracer.span(
            "eventsource.projection.handler",
            {
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_HANDLER_NAME: handler_name,
            },
        ):
            # Check handler parameter count
            if handler_info.param_count == 1:
                # Single parameter: just event
                await self._handler_registry.dispatch(event, context=None)
            else:
                # Two parameters: pass repository as context
                repo = self._current_repository
                if repo is None:
                    raise RuntimeError(
                        f"Handler {handler_name} requires repository but none available. "
                        f"Ensure you call handle() rather than _process_event() directly."
                    )
                await self._handler_registry.dispatch(event, context=repo)

    async def _truncate_read_models(self) -> None:
        """
        Truncate read model table via repository.

        Called during reset() to clear all projection data.
        """
        async with self._session_factory() as session, session.begin():
            conn = await session.connection()
            repo = await self._create_repository(conn)
            count = await repo.truncate()

            logger.info(
                "ReadModelProjection %s truncated %d records from %s",
                self._projection_name,
                count,
                self._model_class.table_name(),
                extra={
                    ATTR_PROJECTION_NAME: self._projection_name,
                    "table": self._model_class.table_name(),
                    "records_deleted": count,
                },
            )

    @property
    def model_class(self) -> type[TModel]:
        """Get the model class this projection manages."""
        return self._model_class

    @property
    def repository(self) -> ReadModelRepository[TModel] | None:
        """
        Get the current repository instance.

        Only available during event handling. Returns None outside
        of handle() context.

        Returns:
            The current repository instance or None
        """
        return self._current_repository

    def __repr__(self) -> str:
        """Return string representation for debugging."""
        return (
            f"{self.__class__.__name__}("
            f"model={self._model_class.__name__}, "
            f"table={self._model_class.table_name()}, "
            f"tracing={'enabled' if self._enable_tracing else 'disabled'})"
        )
