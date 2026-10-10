"""
Declarative projection base with @handles discovery and tenant filtering.
"""

from __future__ import annotations

import logging
from uuid import UUID

from eventsource.application.projections.base_checkpoint import CheckpointTrackingProjection
from eventsource.application.projections.base_protocols import (
    TenantFilter,
    UnregisteredEventHandling,
)
from eventsource.application.projections.handlers import HandlerRegistry
from eventsource.application.projections.retry import ProjectionRetryPolicy
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_PROJECTION_NAME,
)
from eventsource.ports.checkpoints import ProjectionCheckpoints
from eventsource.ports.dlq import DLQRepository

logger = logging.getLogger(__name__)


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
        retry_policy: ProjectionRetryPolicy | None = None,
        tracer: Tracer | None = None,
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
                          Ignored if tracer is explicitly provided.
            retry_policy: Policy for retry behavior.
                         If None, uses ExponentialBackoffRetryPolicy with defaults.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
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
            retry_policy=retry_policy,
            tracer=tracer,
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


__all__ = ["DeclarativeProjection"]
