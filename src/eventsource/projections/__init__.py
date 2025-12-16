"""
Projection system for the eventsource library.

This module provides the core abstractions and utilities for building
projections (read models) from domain events.

Public API:
- Projection: Abstract base class for projections
- SyncProjection: Synchronous projection base class
- EventHandlerBase: Base class for event handlers
- CheckpointTrackingProjection: Projection with checkpoint, retry, and DLQ support
- DeclarativeProjection: Projection with @handles decorator support and tenant filtering
- DatabaseProjection: Projection with database connection support for handlers
- TenantFilter: Type alias for tenant filter parameter
- handles: Decorator for marking event handler methods
- get_handled_event_type: Utility to get event type from decorated handler
- is_event_handler: Check if a function is decorated with @handles
- ProjectionRegistry: Registry for managing multiple projections
- ProjectionCoordinator: Coordinates event distribution to projections
- SubscriberRegistry: Registry for EventSubscriber instances
- EventHandler: Protocol for event handlers
- SyncEventHandler: Protocol for sync event handlers
- EventSubscriber: Base class for event subscribers
- AsyncEventHandler: Base class for async event handlers

Example:
    >>> from eventsource.projections import (
    ...     DatabaseProjection,
    ...     handles,
    ...     ProjectionRegistry,
    ... )
    >>>
    >>> class OrderProjection(DatabaseProjection):
    ...     @handles(OrderCreated)
    ...     async def _handle_order_created(self, conn, event: OrderCreated):
    ...         # Handle the event with database connection
    ...         await conn.execute(text("INSERT INTO orders ..."))
    >>>
    >>> projection = OrderProjection(session_factory=async_session_factory)
    >>> await projection.handle(event)

Example with tenant filtering:
    >>> from eventsource.projections import DeclarativeProjection, TenantFilter
    >>> from eventsource.multitenancy import get_current_tenant
    >>>
    >>> # Static tenant filter
    >>> projection = OrderProjection(tenant_filter=tenant_uuid)
    >>>
    >>> # Dynamic tenant filter (context-based)
    >>> projection = OrderProjection(tenant_filter=get_current_tenant)
"""

# Re-export handles from canonical location for backward compatibility
# Users should import from eventsource.handlers instead
from eventsource.handlers import (
    get_handled_event_type,
    handles,
    is_event_handler,
)
from eventsource.projections.base import (
    CheckpointTrackingProjection,
    DatabaseProjection,
    DeclarativeProjection,
    EventHandlerBase,
    Projection,
    SyncProjection,
    TenantFilter,
)
from eventsource.projections.coordinator import (
    ProjectionCoordinator,
    ProjectionRegistry,
    SubscriberRegistry,
)

# Protocols from canonical location (TD-007)
from eventsource.projections.protocols import AsyncEventHandler
from eventsource.protocols import (
    EventHandler,
    EventSubscriber,
    SyncEventHandler,
)

__all__ = [
    # Base classes
    "Projection",
    "SyncProjection",
    "EventHandlerBase",
    "CheckpointTrackingProjection",
    "DeclarativeProjection",
    "DatabaseProjection",
    # Type aliases
    "TenantFilter",
    # Decorators
    "handles",
    "get_handled_event_type",
    "is_event_handler",
    # Coordinators and registries
    "ProjectionRegistry",
    "ProjectionCoordinator",
    "SubscriberRegistry",
    # Protocols
    "EventHandler",
    "SyncEventHandler",
    "EventSubscriber",
    "AsyncEventHandler",
]
