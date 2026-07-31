"""DatabaseProjection: the SQL adapter for declarative projections.

Takes an `async_sessionmaker[AsyncSession]` and opens transactions -- a
framework dependency in a class signature, which makes this an adapter,
not a use case (ADR 0024). It subclasses DeclarativeProjection from the
application ring: an adapter depending inward is exactly the dependency
rule.
"""

import asyncio
import logging
from typing import TYPE_CHECKING

from eventsource.application.projections.base import DeclarativeProjection, TenantFilter
from eventsource.application.projections.checkpoints import record_checkpoint
from eventsource.application.projections.dlq import send_to_dlq
from eventsource.events.base import DomainEvent
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
    """
    Projection with database connection support for handlers.

    Extends DeclarativeProjection to provide a real database connection to
    handlers with 2-parameter signatures (conn, event). This enables handlers
    to execute SQL operations within the projection's transaction context.

    Inherits tenant_filter support from DeclarativeProjection.

    The database session wraps all handler operations, ensuring that:
    - Handler SQL operations are transactional
    - Checkpoint updates share the same transaction (when using compatible repos)
    - Errors cause automatic rollback

    Handler Signatures:
        - (event): Single parameter handler, no database access
        - (conn, event): Two parameter handler, receives AsyncConnection

    Example:
        >>> from sqlalchemy.ext.asyncio import async_sessionmaker
        >>>
        >>> class OrderProjection(DatabaseProjection):
        ...     @handles(OrderCreated)
        ...     async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        ...         await conn.execute(text(
        ...             "INSERT INTO orders (id, number) VALUES (:id, :num)"
        ...         ), {"id": str(event.aggregate_id), "num": event.order_number})
        ...
        ...     @handles(OrderShipped)
        ...     async def _handle_order_shipped(self, event: OrderShipped) -> None:
        ...         # Single param handler also works
        ...         print(f"Order shipped: {event.tracking_number}")
        >>>
        >>> # Usage
        >>> projection = OrderProjection(session_factory=async_session_factory)
        >>> await projection.handle(event)

    Example with tenant filter:
        >>> from eventsource.multitenancy import get_current_tenant
        >>> projection = OrderProjection(
        ...     session_factory=async_session_factory,
        ...     tenant_filter=get_current_tenant,
        ... )

    Attributes:
        _session_factory: SQLAlchemy async session factory for database connections
        _current_connection: Current database connection within handle() context
    """

    def __init__(
        self,
        session_factory: "async_sessionmaker[AsyncSession]",
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        enable_tracing: bool = False,
        *,
        tenant_filter: TenantFilter = None,
    ) -> None:
        """
        Initialize the database projection.

        Args:
            session_factory: SQLAlchemy async session factory for creating
                           database sessions. Required for database operations.
            checkpoint_repo: Repository for checkpoint storage (ProjectionCheckpoints).
                           If None, checkpoints are not persisted.
            dlq_repo: Repository for dead letter queue (DLQRepository).
                     If None, permanently failed events are not persisted.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False (tracing off for high-frequency projections).
            tenant_filter: Optional tenant filter. Can be:
                - UUID: Static filter, only process events with this tenant_id
                - Callable[[], UUID | None]: Dynamic filter, called per event
                - None: No filtering, process all events (default)
        """
        super().__init__(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            enable_tracing=enable_tracing,
            tenant_filter=tenant_filter,
        )
        self._session_factory = session_factory
        self._current_connection: AsyncConnection | None = None

        logger.info(
            "DatabaseProjection %s initialized with session factory",
            self._projection_name,
            extra={
                "projection": self._projection_name,
                "session_factory_type": type(session_factory).__name__,
            },
        )

    async def _handle_with_retry(self, event: DomainEvent, span: "Span | None") -> None:
        """
        Internal method that implements retry logic with fresh transactions.

        Overrides parent to ensure each retry attempt gets a fresh database
        transaction. This is necessary because PostgreSQL marks transactions
        as "aborted" after any error, and further SQL commands will fail with
        "current transaction is aborted, commands ignored until end of
        transaction block".

        Args:
            event: The domain event to process
            span: Optional OpenTelemetry span for adding attributes
        """
        max_attempts = self._retry_policy.max_retries + 1  # Include initial attempt

        for attempt in range(max_attempts):
            try:
                # Each attempt gets a fresh session/transaction
                await self._execute_in_transaction(event)

                # Update checkpoint after successful processing
                if self._checkpoint_repo is not None:
                    await record_checkpoint(
                        self._checkpoint_repo, self._projection_name, event, self._tracer
                    )

                # Add success attribute to span if tracing enabled
                if span is not None:
                    span.set_attribute("checkpoint.updated", True)

                # Success - log and return
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
                    if self._dlq_repo is not None:
                        await send_to_dlq(
                            self._dlq_repo,
                            self._projection_name,
                            event,
                            e,
                            attempt + 1,
                            self._tracer,
                        )
                    logger.critical(
                        "Event %s sent to DLQ after %d attempts",
                        event.event_id,
                        attempt + 1,
                        extra={
                            "projection": self._projection_name,
                            "event_id": str(event.event_id),
                            "event_type": event.event_type,
                            "retry_count": attempt + 1,
                        },
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

    async def _execute_in_transaction(self, event: DomainEvent) -> None:
        """
        Execute event processing within a database transaction.

        Creates a fresh session/transaction for the handler. On success,
        the transaction is committed. On error, the transaction is rolled back.

        Args:
            event: The domain event to process
        """
        logger.debug(
            "DatabaseProjection %s beginning transaction for event %s",
            self._projection_name,
            event.event_id,
            extra={
                "projection": self._projection_name,
                "event_id": str(event.event_id),
                "event_type": event.event_type,
            },
        )

        async with self._session_factory() as session, session.begin():
            # Get connection from session and store for use by _process_event
            conn = await session.connection()
            self._current_connection = conn

            try:
                await self._process_event(event)

                logger.debug(
                    "DatabaseProjection %s committing transaction for event %s",
                    self._projection_name,
                    event.event_id,
                    extra={
                        "projection": self._projection_name,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                    },
                )
            except Exception as e:
                logger.error(
                    "DatabaseProjection %s rolling back transaction for event %s: %s",
                    self._projection_name,
                    event.event_id,
                    e,
                    extra={
                        "projection": self._projection_name,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "error": str(e),
                    },
                )
                raise
            finally:
                self._current_connection = None

    async def _process_event(self, event: DomainEvent) -> None:
        """
        Route event to appropriate handler with database connection.

        Extends DeclarativeProjection._process_event to provide a real
        database connection to handlers with 2-parameter signatures.

        Args:
            event: The domain event to process

        Raises:
            RuntimeError: If called without an active database connection
                         (i.e., not called via handle())
        """
        handler_info = self._handler_registry.get_handler(type(event))

        if handler_info is None:
            # Use parent class warning behavior for unhandled events
            return await super()._process_event(event)

        handler_name = handler_info.handler_name

        # Dispatch to handler with tracing
        with self._tracer.span(
            "eventsource.projection.handler",
            {
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_HANDLER_NAME: handler_name,
            },
        ):
            # Check if handler needs connection (2-param) or not (1-param)
            if handler_info.param_count == 1:
                # Single parameter handler: just event (no database needed)
                await self._handler_registry.dispatch(event, context=None)
            else:
                # Two parameter handler: provide real connection
                conn = self._current_connection
                if conn is None:
                    raise RuntimeError(
                        f"Handler {handler_name} requires database connection "
                        f"but DatabaseProjection.handle() was not used. "
                        f"Ensure you call handle() rather than _process_event() directly."
                    )
                await self._handler_registry.dispatch(event, context=conn)


__all__ = ["DatabaseProjection"]


# Type hints for SQLAlchemy (imported at runtime if available)
try:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker
except ImportError:
    # SQLAlchemy not installed - provide type stubs for type checking
    if TYPE_CHECKING:
        from sqlalchemy.ext.asyncio import (
            AsyncConnection,
            AsyncSession,
            async_sessionmaker,
        )
