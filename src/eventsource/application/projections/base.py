"""
Base classes for projections and event handlers.

Projections build read models from domain events. This module provides:
- Projection: Abstract base class for all projections
- EventHandler: Base class for event handlers
- CheckpointTrackingProjection: Adds checkpoint, retry, and DLQ support
- DeclarativeProjection: Adds @handles decorator support with tenant filtering
- TenantFilter: Type alias for tenant filter parameter

DatabaseProjection now lives in `eventsource.adapters.sql.projection`.

Projections are a core concept in event sourcing, responsible for
maintaining read models optimized for specific query patterns.
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from opentelemetry.trace import Span

from eventsource.application.projections.checkpoints import (
    lag_metrics_dict,
    read_checkpoint,
    record_checkpoint,
    reset_checkpoint,
)
from eventsource.application.projections.dlq import send_to_dlq
from eventsource.application.projections.handlers import HandlerRegistry
from eventsource.application.projections.retry import (
    ExponentialBackoffRetryPolicy,
    RetryPolicy,
)
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository
from eventsource.ports.handlers import EventSubscriber

logger = logging.getLogger(__name__)

# Type alias for unregistered event handling mode
UnregisteredEventHandling = str  # "ignore" | "warn" | "error"

# Tenant filter can be:
# - Static UUID: Always filter by this tenant
# - Callable: Dynamic filter, called per-event (e.g., get_current_tenant)
# - None: No filtering, process all events
type TenantFilter = UUID | Callable[[], UUID | None] | None


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


class CheckpointTrackingProjection(EventSubscriber, ABC):
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
        >>> from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
        >>> from eventsource.application.subscriptions.retry import RetryConfig
        >>> policy = ExponentialBackoffRetryPolicy(RetryConfig(max_retries=5))
        >>> projection = OrderProjection(retry_policy=policy, enable_tracing=True)
    """

    def __init__(
        self,
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        retry_policy: RetryPolicy | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = False,
    ) -> None:
        """
        Initialize the checkpoint-tracking projection.

        Args:
            checkpoint_repo: Repository for checkpoint storage.
                           If None, checkpoint tracking is disabled: no checkpoint
                           is written, and `get_checkpoint()` / `get_lag_metrics()`
                           return None.
            dlq_repo: Repository for dead letter queue.
                     If None, DLQ capture is disabled: permanent failures are
                     logged at critical and re-raised, as before.
            retry_policy: Policy for retry behavior.
                         If None, uses ExponentialBackoffRetryPolicy with defaults.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False (tracing off for high-frequency projections).
                          Ignored if tracer is explicitly provided.
        """
        self._projection_name = self.__class__.__name__
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self._checkpoint_repo: ProjectionCheckpoints | None = checkpoint_repo
        self._dlq_repo: DLQRepository | None = dlq_repo

        # Use provided retry policy or create default
        if retry_policy is not None:
            self._retry_policy = retry_policy
        else:
            from eventsource.application.subscriptions.retry import RetryConfig

            self._retry_policy = ExponentialBackoffRetryPolicy(
                config=RetryConfig(
                    max_retries=2,  # 3 total attempts
                    initial_delay=2.0,  # 2 second base backoff
                    exponential_base=2.0,
                    jitter=0.1,
                )
            )

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
        with self._tracer.span(
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
                # Process the event in projection-specific logic. Nothing else
                # belongs in this `try`: the retry loop treats every exception
                # raised here as "the handler rejected this event", so widening
                # the block makes an unrelated failure look like a poison event.
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

                # Check if we should retry
                if not self._retry_policy.should_retry(attempt, e):
                    # Add retry count to span before final failure
                    if span is not None:
                        span.set_attribute(ATTR_RETRY_COUNT, attempt + 1)

                    # Send to DLQ
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
                    continue

            # The event applied cleanly. Checkpointing happens here, outside
            # the retry loop, because a checkpoint-store failure is a liveness
            # problem, not a poison event: retrying it would re-run
            # `_process_event` (re-applying the read-model mutation), and
            # exhausting the retries would DLQ an event the projection has
            # already applied successfully -- so an operator replaying the DLQ
            # would apply it yet again. `docs/architecture.md` has always
            # described the intended ordering; the code had it in the wrong
            # block.
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

    async def _record_checkpoint_after_success(
        self, event: DomainEvent, span: "Span | None"
    ) -> None:
        """Record the checkpoint for an event the projection already applied.

        Failure is re-raised so the caller (a subscription runner) sees a
        stalled projection rather than silent progress loss, but it is never
        routed to the DLQ and never re-runs the handler.
        """
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
            (including when checkpoint tracking is disabled)
        """
        if self._checkpoint_repo is None:
            return None
        return await read_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)

    async def get_lag_metrics(self) -> dict[str, Any] | None:
        """
        Get projection lag metrics.

        Returns:
            Dictionary with lag information, or None if no checkpoint exists
            (including when checkpoint tracking is disabled)
        """
        if self._checkpoint_repo is None:
            return None
        return await lag_metrics_dict(
            self._checkpoint_repo,
            self._projection_name,
            [et.__name__ for et in self.subscribed_to()],
            self._tracer,
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

        # Reset checkpoint
        if self._checkpoint_repo is not None:
            await reset_checkpoint(self._checkpoint_repo, self._projection_name, self._tracer)

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

    Supports automatic tenant filtering via the tenant_filter parameter.
    When set, only events matching the filter are processed.

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

    Example with static tenant filter:
        >>> # Process only events for a specific tenant
        >>> projection = OrderProjection(tenant_filter=tenant_uuid)

    Example with dynamic filter (context-based):
        >>> from eventsource import get_current_tenant
        >>> # Process events for current request's tenant
        >>> projection = OrderProjection(tenant_filter=get_current_tenant)

    Example without filter (process all):
        >>> projection = OrderProjection()  # tenant_filter=None
    """

    # Class-level configuration for unregistered event handling
    # Options: "ignore" (default), "warn", "error"
    unregistered_event_handling: UnregisteredEventHandling = "ignore"

    def __init__(
        self,
        checkpoint_repo: ProjectionCheckpoints | None = None,
        dlq_repo: DLQRepository | None = None,
        enable_tracing: bool = False,
        *,
        tenant_filter: TenantFilter = None,
    ) -> None:
        """
        Initialize the declarative projection.

        Discovers all @handles decorated methods and builds a routing map
        using HandlerRegistry for handler management.

        Args:
            checkpoint_repo: Repository for checkpoint storage.
                           If None, checkpoint tracking is disabled: no checkpoint
                           is written, and `get_checkpoint()` / `get_lag_metrics()`
                           return None.
            dlq_repo: Repository for dead letter queue.
                     If None, DLQ capture is disabled: permanent failures are
                     logged at critical and re-raised, as before.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Default is False (tracing off for high-frequency projections).
            tenant_filter: Optional tenant filter. Can be:
                - UUID: Static filter, only process events with this tenant_id
                - Callable[[], UUID | None]: Dynamic filter, called per event
                - None: No filtering, process all events (default)
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

        # Store tenant filter
        self._tenant_filter = tenant_filter

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

    def _get_tenant_filter_value(self) -> UUID | None:
        """
        Resolve the current tenant filter value.

        Returns:
            The tenant UUID to filter by, or None for no filtering
        """
        if self._tenant_filter is None:
            return None

        if isinstance(self._tenant_filter, UUID):
            return self._tenant_filter

        # It's a callable - invoke it
        return self._tenant_filter()

    def _should_process_event(self, event: DomainEvent) -> bool:
        """
        Check if event should be processed based on tenant filter.

        Args:
            event: The event to check

        Returns:
            True if event should be processed, False to skip

        Logic:
        - If no filter set (None): Process all events
        - If filter set and event has tenant_id: Must match
        - If filter set and event has no tenant_id: Process (legacy events)
        """
        filter_value = self._get_tenant_filter_value()

        if filter_value is None:
            return True  # No filtering

        event_tenant: UUID | None = getattr(event, "tenant_id", None)

        if event_tenant is None:
            # Event has no tenant_id - process it (legacy/system events)
            return True

        return bool(event_tenant == filter_value)

    async def _process_event(self, event: DomainEvent) -> None:
        """
        Route event to appropriate handler method with tenant filtering.

        Called by CheckpointTrackingProjection.handle() within a transaction.
        If tenant_filter is set and event doesn't match, the event is
        silently skipped. Otherwise, behavior for unhandled events depends
        on unregistered_event_handling setting.

        Args:
            event: The domain event to process

        Raises:
            UnhandledEventError: If unregistered_event_handling="error" and no handler found
        """
        # Check tenant filter first
        if not self._should_process_event(event):
            logger.debug(
                "Skipping event %s: tenant %s doesn't match filter %s",
                event.event_id,
                getattr(event, "tenant_id", None),
                self._get_tenant_filter_value(),
                extra={
                    "projection": self._projection_name,
                    "event_id": str(event.event_id),
                    "event_type": type(event).__name__,
                    "event_tenant_id": str(getattr(event, "tenant_id", None)),
                    "filter_tenant_id": str(self._get_tenant_filter_value()),
                },
            )
            return

        handler_info = self._handler_registry.get_handler(type(event))

        if handler_info is None:
            # Delegate unregistered event handling to registry
            await self._handler_registry.dispatch(event, context=None)
            return

        handler_name = handler_info.handler_name

        # Dispatch to handler with optional tracing
        with self._tracer.span(
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
