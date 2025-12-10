"""
Base classes for projections and event handlers.

Projections build read models from domain events. This module provides:
- Projection: Abstract base class for all projections
- EventHandler: Base class for event handlers
- CheckpointTrackingProjection: Adds checkpoint, retry, and DLQ support
- DeclarativeProjection: Adds @handles decorator support
- DatabaseProjection: Adds database connection support for handlers

Projections are a core concept in event sourcing, responsible for
maintaining read models optimized for specific query patterns.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from opentelemetry.trace import Span

from eventsource.events.base import DomainEvent
from eventsource.handlers.registry import HandlerRegistry
from eventsource.observability import TracingMixin
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.projections.checkpoint_manager import ProjectionCheckpointManager
from eventsource.projections.dlq_manager import ProjectionDLQManager
from eventsource.projections.retry import (
    ExponentialBackoffRetryPolicy,
    RetryPolicy,
)
from eventsource.protocols import EventSubscriber
from eventsource.repositories.checkpoint import (
    CheckpointRepository,
    InMemoryCheckpointRepository,
)
from eventsource.repositories.dlq import DLQRepository, InMemoryDLQRepository

logger = logging.getLogger(__name__)

# Type alias for unregistered event handling mode
UnregisteredEventHandling = str  # "ignore" | "warn" | "error"


class Projection(ABC):
    """
    Base class for projections.

    Projections consume domain events and build read models
    optimized for specific query patterns. They provide the
    query side in CQRS architecture.

    Subclasses must implement:
    - handle(): Process a single event
    - reset(): Clear all read model data

    Example:
        >>> class OrderSummaryProjection(Projection):
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             await self._create_summary(event)
        ...
        ...     async def reset(self) -> None:
        ...         await self._clear_all_summaries()
    """

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event.

        Args:
            event: The domain event to process
        """
        pass

    @abstractmethod
    async def reset(self) -> None:
        """
        Reset the projection (clear all read model data).

        Useful for rebuilding projections from scratch.
        """
        pass


class SyncProjection(ABC):
    """
    Synchronous base class for projections.

    Useful for projections that don't require async I/O,
    or for testing scenarios.
    """

    @abstractmethod
    def handle(self, event: DomainEvent) -> None:
        """
        Handle a domain event synchronously.

        Args:
            event: The domain event to process
        """
        pass

    @abstractmethod
    def reset(self) -> None:
        """
        Reset the projection (clear all read model data).
        """
        pass


class EventHandlerBase(ABC):
    """
    Base class for event handlers.

    Event handlers react to specific event types and perform actions
    (update read models, send notifications, trigger workflows, etc.)

    Unlike projections, handlers are focused on individual event types
    and provide explicit can_handle() checking.

    Example:
        >>> class OrderNotificationHandler(EventHandlerBase):
        ...     def can_handle(self, event: DomainEvent) -> bool:
        ...         return isinstance(event, (OrderShipped, OrderDelivered))
        ...
        ...     async def handle(self, event: DomainEvent) -> None:
        ...         await send_notification(event)
    """

    @abstractmethod
    def can_handle(self, event: DomainEvent) -> bool:
        """
        Check if this handler can process the given event.

        Args:
            event: The event to check

        Returns:
            True if this handler can process the event
        """
        pass

    @abstractmethod
    async def handle(self, event: DomainEvent) -> None:
        """
        Handle the event.

        Args:
            event: The event to process
        """
        pass


class CheckpointTrackingProjection(TracingMixin, EventSubscriber, ABC):
    """
    Base class for projections with automatic checkpoint tracking.

    Provides:
    - Automatic checkpoint management after each event
    - Idempotent event processing
    - Retry logic with exponential backoff (configurable via RetryPolicy)
    - Dead letter queue for permanent failures
    - Lag monitoring support
    - Optional OpenTelemetry tracing support (disabled by default)

    Subclasses must implement:
    - subscribed_to(): List of event types to handle
    - _process_event(): Actual projection logic
    - _truncate_read_models(): Table truncation for reset

    Configuration:
    - retry_policy: RetryPolicy instance for configurable retry behavior
    - MAX_RETRIES: Number of retry attempts (default: 3) - DEPRECATED, use retry_policy
    - RETRY_BACKOFF_BASE: Base for exponential backoff in seconds (default: 2) - DEPRECATED

    Tracing:
    - Tracing is disabled by default for projections (high-frequency processing)
    - Enable with enable_tracing=True in constructor
    - Emits spans: eventsource.projection.handle

    Example:
        >>> class OrderProjection(CheckpointTrackingProjection):
        ...     def subscribed_to(self) -> list[type[DomainEvent]]:
        ...         return [OrderCreated, OrderShipped]
        ...
        ...     async def _process_event(self, conn, event: DomainEvent) -> None:
        ...         if isinstance(event, OrderCreated):
        ...             await self._handle_created(conn, event)
        ...
        ...     async def _truncate_read_models(self, conn) -> None:
        ...         await conn.execute(text("TRUNCATE TABLE orders"))
        >>>
        >>> # With custom retry policy
        >>> from eventsource.projections.retry import ExponentialBackoffRetryPolicy
        >>> from eventsource.subscriptions.retry import RetryConfig
        >>> policy = ExponentialBackoffRetryPolicy(RetryConfig(max_retries=5))
        >>> projection = OrderProjection(retry_policy=policy, enable_tracing=True)
    """

    # Retry configuration - DEPRECATED: use retry_policy parameter instead
    MAX_RETRIES: int = 3
    RETRY_BACKOFF_BASE: int = 2  # seconds (exponential: 2s, 4s, 8s)

    def __init__(
        self,
        checkpoint_repo: CheckpointRepository | None = None,
        dlq_repo: DLQRepository | None = None,
        retry_policy: RetryPolicy | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the checkpoint-tracking projection.

        Args:
            checkpoint_repo: Repository for checkpoint storage.
                           If None, uses InMemoryCheckpointRepository.
            dlq_repo: Repository for dead letter queue.
                     If None, uses InMemoryDLQRepository.
            retry_policy: Policy for retry behavior.
                         If None, uses ExponentialBackoffRetryPolicy with defaults.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False (tracing off for high-frequency projections).
        """
        self._projection_name = self.__class__.__name__
        # Initialize tracing via TracingMixin (default OFF for projections)
        self._init_tracing(__name__, enable_tracing)

        # Use new managers for checkpoint and DLQ operations
        self._checkpoint_manager = ProjectionCheckpointManager(
            projection_name=self._projection_name,
            checkpoint_repo=checkpoint_repo or InMemoryCheckpointRepository(),
            enable_tracing=enable_tracing,
        )
        self._dlq_manager = ProjectionDLQManager(
            projection_name=self._projection_name,
            dlq_repo=dlq_repo or InMemoryDLQRepository(),
            enable_tracing=enable_tracing,
        )

        # Use retry policy if provided, otherwise create one from class attributes
        # for backward compatibility with MAX_RETRIES and RETRY_BACKOFF_BASE
        if retry_policy is not None:
            self._retry_policy = retry_policy
        else:
            # Use class attributes for backward compatibility
            # Note: Old MAX_RETRIES meant total attempts, new max_retries is retries only
            # So MAX_RETRIES=3 means 2 retries (3 total attempts - 1 initial)
            from eventsource.subscriptions.retry import RetryConfig

            max_retries = max(0, self.MAX_RETRIES - 1)  # Convert to retries count
            # RetryConfig requires initial_delay > 0, use 0.001 for "no backoff" cases
            initial_delay = float(self.RETRY_BACKOFF_BASE) if self.RETRY_BACKOFF_BASE > 0 else 0.001
            self._retry_policy = ExponentialBackoffRetryPolicy(
                config=RetryConfig(
                    max_retries=max_retries,
                    initial_delay=initial_delay,
                    exponential_base=2.0,
                    jitter=0.0 if self.RETRY_BACKOFF_BASE == 0 else 0.1,
                )
            )

        # Keep references for backward compatibility
        self._checkpoint_repo = self._checkpoint_manager.checkpoint_repo
        self._dlq_repo = self._dlq_manager.dlq_repo

    async def handle(self, event: DomainEvent) -> None:
        """
        Handle event with retry logic and DLQ fallback.

        This method wraps the projection logic with:
        1. Retry with exponential backoff for transient failures
        2. Dead letter queue for permanent failures
        3. Checkpoint tracking for successful processing
        4. Optional tracing (when enable_tracing=True)

        Args:
            event: The domain event to process
        """
        with self._create_span_context(
            "eventsource.projection.handle",
            {
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_EVENT_ID: str(event.event_id),
            },
        ) as span:
            await self._handle_with_retry(event, span)

    async def _handle_with_retry(self, event: DomainEvent, span: "Span | None") -> None:
        """
        Internal method that implements retry logic for event handling.

        Uses the configured RetryPolicy for backoff and retry decisions.

        Args:
            event: The domain event to process
            span: Optional OpenTelemetry span for adding attributes
        """
        max_attempts = self._retry_policy.max_retries + 1  # Include initial attempt

        for attempt in range(max_attempts):
            try:
                # Process the event in projection-specific logic
                await self._process_event(event)

                # Update checkpoint after successful processing via manager
                await self._checkpoint_manager.update(event)

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

                    # Send to DLQ via manager
                    await self._dlq_manager.send_to_dlq(event, e, attempt + 1)
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

    async def _send_to_dlq(self, event: DomainEvent, error: Exception, retry_count: int) -> None:
        """
        Send failed event to dead letter queue.

        DEPRECATED: This method is kept for backward compatibility.
        Internally delegates to ProjectionDLQManager.

        Args:
            event: The event that failed processing
            error: The exception that occurred
            retry_count: Number of retry attempts made
        """
        await self._dlq_manager.send_to_dlq(event, error, retry_count)

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
        """
        return await self._checkpoint_manager.get_checkpoint()

    async def get_lag_metrics(self) -> dict[str, Any] | None:
        """
        Get projection lag metrics.

        Returns:
            Dictionary with lag information, or None if no checkpoint exists
        """
        return await self._checkpoint_manager.get_lag_metrics(
            event_types=[et.__name__ for et in self.subscribed_to()],
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

        # Reset checkpoint via manager
        await self._checkpoint_manager.reset()

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


class DeclarativeProjection(CheckpointTrackingProjection):
    """
    Projection that uses declarative event handlers with the @handles decorator.

    This base class automatically discovers handler methods decorated with @handles
    and routes events to them. The subscribed_to() method is auto-generated from
    the @handles decorators, eliminating duplication.

    Subclasses just need to:
    1. Implement handler methods decorated with @handles(EventType)
    2. Optionally override _truncate_read_models() for reset support

    Attributes:
        unregistered_event_handling: Controls behavior when an event has no
            registered handler. Options:
            - "ignore": Silently ignore unhandled events (default, for backwards
              compatibility and forward compatibility with new event types)
            - "warn": Log a warning for unhandled events
            - "error": Raise UnhandledEventError for unhandled events

    Handler Signature:
        Handler methods must be async and accept exactly 2 parameters:
        - conn: Database connection (if using database)
        - event: The domain event to process

        For projections not using database connections, you can use
        a generic parameter name but must maintain the 2-parameter signature.

    Example:
        >>> class OrderProjection(DeclarativeProjection):
        ...     @handles(OrderCreated)
        ...     async def _handle_order_created(self, conn, event: OrderCreated) -> None:
        ...         # Handle the event
        ...         pass
        ...
        ...     @handles(OrderShipped)
        ...     async def _handle_order_shipped(self, conn, event: OrderShipped) -> None:
        ...         # Handle shipping event
        ...         pass
        ...
        ...     async def _truncate_read_models(self, conn) -> None:
        ...         await conn.execute(text("TRUNCATE TABLE orders"))

        >>> # For strict mode (raises error on unhandled events):
        >>> class StrictOrderProjection(DeclarativeProjection):
        ...     unregistered_event_handling = "error"
        ...     # ... handlers ...
    """

    # Class-level configuration for unregistered event handling
    # Options: "ignore" (default), "warn", "error"
    unregistered_event_handling: UnregisteredEventHandling = "ignore"

    def __init__(
        self,
        checkpoint_repo: CheckpointRepository | None = None,
        dlq_repo: DLQRepository | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the declarative projection.

        Discovers all @handles decorated methods and builds a routing map
        using HandlerRegistry for handler management.

        Args:
            checkpoint_repo: Repository for checkpoint storage.
            dlq_repo: Repository for dead letter queue.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False (tracing off for high-frequency projections).
        """
        # Initialize registry before calling super().__init__()
        # in case subscribed_to() is called during parent initialization
        # Note: We use require_async=True since DeclarativeProjection requires async handlers
        self._handler_registry = HandlerRegistry(
            self,
            require_async=True,
            unregistered_event_handling=self.unregistered_event_handling,  # type: ignore[arg-type]
            validate_on_init=True,
        )

        # Keep _handlers dict for backward compatibility
        self._handlers: dict[type[DomainEvent], Callable[..., Coroutine[Any, Any, None]]] = {
            event_type: info.handler  # type: ignore[misc]
            for event_type, info in self._handler_registry.get_all_handlers().items()
        }

        super().__init__(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            enable_tracing=enable_tracing,
        )

    def subscribed_to(self) -> list[type[DomainEvent]]:
        """
        Return list of event types this projection handles.

        Auto-generates from @handles decorators. Subclasses can
        override to customize the subscription list if needed.

        Returns:
            List of event type classes
        """
        return self._handler_registry.get_subscribed_events()

    async def _process_event(self, event: DomainEvent) -> None:
        """
        Route event to appropriate handler method.

        Called by CheckpointTrackingProjection.handle() within a transaction.
        Behavior for unhandled events depends on unregistered_event_handling setting.

        Args:
            event: The domain event to process

        Raises:
            UnhandledEventError: If unregistered_event_handling="error" and no handler found
        """
        handler_info = self._handler_registry.get_handler(type(event))

        if handler_info is None:
            # Delegate unregistered event handling to registry
            await self._handler_registry.dispatch(event, context=None)
            return

        handler_name = handler_info.handler_name

        # Dispatch to handler with optional tracing
        with self._create_span_context(
            "eventsource.projection.handler",
            {
                ATTR_PROJECTION_NAME: self._projection_name,
                ATTR_EVENT_TYPE: type(event).__name__,
                ATTR_HANDLER_NAME: handler_name,
            },
        ):
            # Dispatch via registry, passing None for context
            # Subclasses (DatabaseProjection) override _process_event to provide real connection
            await self._handler_registry.dispatch(event, context=None)


class DatabaseProjection(DeclarativeProjection):
    """
    Projection with database connection support for handlers.

    Extends DeclarativeProjection to provide a real database connection to
    handlers with 2-parameter signatures (conn, event). This enables handlers
    to execute SQL operations within the projection's transaction context.

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

    Attributes:
        _session_factory: SQLAlchemy async session factory for database connections
        _current_connection: Current database connection within handle() context
    """

    def __init__(
        self,
        session_factory: "async_sessionmaker[AsyncSession]",
        checkpoint_repo: CheckpointRepository | None = None,
        dlq_repo: DLQRepository | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the database projection.

        Args:
            session_factory: SQLAlchemy async session factory for creating
                           database sessions. Required for database operations.
            checkpoint_repo: Repository for checkpoint storage.
                           If None, uses InMemoryCheckpointRepository.
            dlq_repo: Repository for dead letter queue.
                     If None, uses InMemoryDLQRepository.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False (tracing off for high-frequency projections).
        """
        super().__init__(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
            enable_tracing=enable_tracing,
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

    async def handle(self, event: DomainEvent) -> None:
        """
        Handle event within a database transaction.

        Wraps the parent handle() in a database session context, ensuring
        all handler operations share the same transaction. On successful
        completion, the transaction is committed. On error, the transaction
        is rolled back.

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
                await super().handle(event)

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
        with self._create_span_context(
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


# Type hints for SQLAlchemy (imported at runtime if available)
try:
    from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, async_sessionmaker
except ImportError:
    # SQLAlchemy not installed - provide type stubs for type checking
    from typing import TYPE_CHECKING

    if TYPE_CHECKING:
        from sqlalchemy.ext.asyncio import (
            AsyncConnection,
            AsyncSession,
            async_sessionmaker,
        )
