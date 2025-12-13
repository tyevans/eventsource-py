"""
Handler registry for discovering and routing event handlers.

This module provides the HandlerRegistry class which extracts handler
discovery and routing logic from DeclarativeProjection, addressing
the Single Responsibility Principle violation.

The registry handles:
- Discovering @handles decorated methods
- Validating handler signatures
- Routing events to appropriate handlers
- Managing unregistered event handling

Example:
    >>> from eventsource.handlers.registry import HandlerRegistry
    >>>
    >>> class MyProjection:
    ...     @handles(OrderCreated)
    ...     async def _handle_created(self, event: OrderCreated) -> None:
    ...         pass
    >>>
    >>> projection = MyProjection()
    >>> registry = HandlerRegistry(projection)
    >>> registry.get_subscribed_events()
    [<class 'OrderCreated'>]
"""

import inspect
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Literal

from eventsource.events.base import DomainEvent
from eventsource.exceptions import UnhandledEventError
from eventsource.handlers.decorators import get_handled_event_type

logger = logging.getLogger(__name__)

# Type alias for unregistered event handling mode
UnregisteredEventHandling = Literal["ignore", "warn", "error"]


class HandlerSignatureError(ValueError):
    """
    Raised when an event handler has an invalid signature.

    This exception provides detailed guidance on how to fix invalid handler
    signatures, including expected signature patterns and hints for common
    mistakes.

    Attributes:
        handler_name: Name of the handler method
        owner_name: Name of the class containing the handler
        event_type: The event type from @handles decorator
        param_count: Actual number of parameters (excluding self)
        is_async_required: Whether async is required for this handler

    Example:
        >>> from eventsource.handlers import handles, HandlerRegistry
        >>>
        >>> class BadProjection:
        ...     @handles(OrderCreated)
        ...     async def _handle(self, a, b, c, event):  # Too many params
        ...         pass
        ...
        >>> # Raises HandlerSignatureError with helpful message
    """

    def __init__(
        self,
        handler_name: str,
        owner_name: str,
        event_type: type,
        param_count: int,
        is_async_required: bool = True,
    ) -> None:
        self.handler_name = handler_name
        self.owner_name = owner_name
        self.event_type = event_type
        self.param_count = param_count
        self.is_async_required = is_async_required

        event_name = event_type.__name__
        async_prefix = "async " if is_async_required else ""

        message = (
            f"Handler '{handler_name}' in {owner_name} has invalid signature "
            f"for @handles({event_name}).\n\n"
            f"Expected one of:\n"
            f"  {async_prefix}def {handler_name}(self, event: {event_name}) -> None\n"
            f"  {async_prefix}def {handler_name}(self, context, event: {event_name}) -> None\n\n"
            f"Got: {param_count} parameter(s) (excluding self)\n\n"
            f"Hint: Ensure your handler has exactly 1 or 2 parameters after 'self'."
        )

        super().__init__(message)


@dataclass
class HandlerInfo:
    """
    Metadata about a registered event handler.

    Contains all the information needed to validate and invoke a handler.

    Attributes:
        event_type: The DomainEvent subclass this handler processes
        handler_name: Name of the handler method
        handler: The callable handler method
        is_async: Whether the handler is an async coroutine function
        param_count: Number of parameters (1 for event-only, 2 for conn+event)
    """

    event_type: type[DomainEvent]
    handler_name: str
    handler: Callable[..., Coroutine[Any, Any, None] | None]
    is_async: bool
    param_count: int


class HandlerRegistry:
    """
    Registry for discovering, validating, and routing event handlers.

    This class extracts handler management from DeclarativeProjection,
    providing a reusable component for any class that uses @handles
    decorated methods.

    Responsibilities:
    - Discover @handles decorated methods on an owner object
    - Validate handler signatures (async, parameter count, types)
    - Route events to appropriate handlers
    - Handle unregistered events based on policy

    Example:
        >>> class OrderProjection:
        ...     @handles(OrderCreated)
        ...     async def _handle_created(self, event: OrderCreated) -> None:
        ...         pass
        ...
        ...     @handles(OrderShipped)
        ...     async def _handle_shipped(self, event: OrderShipped) -> None:
        ...         pass
        >>>
        >>> projection = OrderProjection()
        >>> registry = HandlerRegistry(
        ...     projection,
        ...     require_async=True,
        ...     unregistered_event_handling="warn",
        ... )
        >>>
        >>> # Get all subscribed event types
        >>> registry.get_subscribed_events()
        [<class 'OrderCreated'>, <class 'OrderShipped'>]
        >>>
        >>> # Dispatch an event
        >>> await registry.dispatch(order_created_event)
    """

    def __init__(
        self,
        owner: Any,
        *,
        require_async: bool = True,
        unregistered_event_handling: UnregisteredEventHandling = "ignore",
        validate_on_init: bool = True,
    ) -> None:
        """
        Initialize the handler registry.

        Args:
            owner: The object containing @handles decorated methods
            require_async: If True, all handlers must be async (default)
            unregistered_event_handling: How to handle events with no handler:
                - "ignore": Silently ignore (default)
                - "warn": Log a warning
                - "error": Raise UnhandledEventError
            validate_on_init: If True, validate handlers during __init__
        """
        self._owner = owner
        self._owner_name = owner.__class__.__name__
        self._require_async = require_async
        self._unregistered_event_handling = unregistered_event_handling
        self._handlers: dict[type[DomainEvent], HandlerInfo] = {}

        # Discover handlers
        self._discover_handlers()

        # Validate if requested
        if validate_on_init:
            self._validate_handlers()

    def _discover_handlers(self) -> None:
        """
        Scan the owner object for @handles decorated methods.

        Discovers both private methods (starting with _) and public methods.
        """
        for attr_name in dir(self._owner):
            # Skip dunder methods
            if attr_name.startswith("__"):
                continue

            attr = getattr(self._owner, attr_name, None)
            if attr is None:
                continue

            # Check if decorated with @handles
            event_type = get_handled_event_type(attr)
            if event_type is None:
                continue

            # Skip if event_type is not a proper type (e.g., mock object)
            if not isinstance(event_type, type):
                continue

            # Get handler info
            is_async = inspect.iscoroutinefunction(attr)

            # Get parameter count (excluding self for bound methods)
            try:
                sig = inspect.signature(attr)
                param_count = len(list(sig.parameters.values()))
            except (ValueError, TypeError):
                # Can't inspect signature - assume 1 parameter
                param_count = 1

            handler_info = HandlerInfo(
                event_type=event_type,
                handler_name=attr_name,
                handler=attr,
                is_async=is_async,
                param_count=param_count,
            )

            self._handlers[event_type] = handler_info

            logger.debug(
                "Registered handler %s for %s",
                attr_name,
                event_type.__name__,
                extra={
                    "owner": self._owner_name,
                    "handler": attr_name,
                    "event_type": event_type.__name__,
                    "is_async": is_async,
                    "param_count": param_count,
                },
            )

    def _validate_handlers(self) -> None:
        """
        Validate all discovered handlers.

        Checks:
        1. Handler is async if require_async=True
        2. Handler has correct parameter count (1 or 2)
        3. Event type annotation matches @handles decorator (warning if mismatch)

        Raises:
            ValueError: If handler has invalid signature
        """
        for handler_info in self._handlers.values():
            self._validate_handler(handler_info)

    def _validate_handler(self, handler_info: HandlerInfo) -> None:
        """
        Validate a single handler's signature.

        Checks that the handler:
        1. Is async if require_async=True
        2. Has correct parameter count (1 or 2 after self)
        3. Has matching event type annotation (warning only)

        Args:
            handler_info: Handler metadata to validate

        Raises:
            HandlerSignatureError: If handler has invalid parameter count
            ValueError: If handler is not async when required
        """
        handler = handler_info.handler
        handler_name = handler_info.handler_name
        event_type = handler_info.event_type

        # Check async requirement
        if self._require_async and not handler_info.is_async:
            event_name = event_type.__name__
            raise ValueError(
                f"Handler '{handler_name}' in {self._owner_name} must be async.\n\n"
                f"Change:\n"
                f"  def {handler_name}(self, ...)\n\n"
                f"To:\n"
                f"  async def {handler_name}(self, event: {event_name}) -> None"
            )

        # Check parameter count (1 for event-only, 2 for context+event)
        if handler_info.param_count < 1 or handler_info.param_count > 2:
            raise HandlerSignatureError(
                handler_name=handler_name,
                owner_name=self._owner_name,
                event_type=event_type,
                param_count=handler_info.param_count,
                is_async_required=self._require_async,
            )

        # Check event type annotation (warning only)
        try:
            sig = inspect.signature(handler)
            params = list(sig.parameters.values())
            event_param = params[-1]  # Last param is the event

            if (
                event_param.annotation != inspect.Parameter.empty
                and isinstance(event_param.annotation, type)
                and event_param.annotation != event_type
            ):
                logger.warning(
                    "Handler %s in %s: Event type annotation %s doesn't match @handles(%s)",
                    handler_name,
                    self._owner_name,
                    event_param.annotation.__name__,
                    event_type.__name__,
                    extra={
                        "owner": self._owner_name,
                        "handler": handler_name,
                        "expected_type": event_type.__name__,
                        "actual_annotation": event_param.annotation.__name__,
                    },
                )
        except (ValueError, TypeError):
            pass  # Can't inspect - skip annotation check

    def get_handler(self, event_type: type[DomainEvent]) -> HandlerInfo | None:
        """
        Get the handler info for a specific event type.

        Args:
            event_type: The event type to get handler for

        Returns:
            HandlerInfo if found, None otherwise
        """
        return self._handlers.get(event_type)

    def has_handler(self, event_type: type[DomainEvent]) -> bool:
        """
        Check if a handler exists for the given event type.

        Args:
            event_type: Event type to check

        Returns:
            True if handler exists, False otherwise
        """
        return event_type in self._handlers

    def get_subscribed_events(self) -> list[type[DomainEvent]]:
        """
        Get list of event types that have handlers.

        Returns:
            List of DomainEvent subclasses
        """
        return list(self._handlers.keys())

    def get_all_handlers(self) -> dict[type[DomainEvent], HandlerInfo]:
        """
        Get all registered handlers.

        Returns:
            Dictionary mapping event types to handler info
        """
        return dict(self._handlers)

    async def dispatch(
        self,
        event: DomainEvent,
        context: Any = None,
    ) -> bool:
        """
        Dispatch an event to its registered handler.

        Routes the event to the appropriate handler based on event type.
        If no handler is found, behavior depends on unregistered_event_handling.

        Args:
            event: The domain event to dispatch
            context: Optional context to pass to 2-param handlers (e.g., db connection)

        Returns:
            True if handler was found and executed, False if no handler

        Raises:
            UnhandledEventError: If unregistered_event_handling="error" and no handler
        """
        event_type = type(event)
        handler_info = self._handlers.get(event_type)

        if handler_info is None:
            self._handle_unregistered_event(event)
            return False

        handler = handler_info.handler

        # Invoke handler based on parameter count
        if handler_info.param_count == 1:
            # Single parameter: just event
            if handler_info.is_async:
                # is_async guarantees handler returns a coroutine
                await handler(event)  # type: ignore[misc]
            else:
                handler(event)
        else:
            # Two parameters: context + event
            if handler_info.is_async:
                # is_async guarantees handler returns a coroutine
                await handler(context, event)  # type: ignore[misc]
            else:
                handler(context, event)

        return True

    def _handle_unregistered_event(self, event: DomainEvent) -> None:
        """
        Handle an event that has no registered handler.

        Behavior depends on unregistered_event_handling setting.

        Args:
            event: The unhandled event

        Raises:
            UnhandledEventError: If unregistered_event_handling="error"
        """
        event_type = type(event)
        available_handlers = [et.__name__ for et in self._handlers]

        if self._unregistered_event_handling == "error":
            raise UnhandledEventError(
                event_type=event_type.__name__,
                event_id=event.event_id,
                handler_class=self._owner_name,
                available_handlers=available_handlers,
            )
        elif self._unregistered_event_handling == "warn":
            logger.warning(
                "No handler registered for event type %s in %s. "
                "Available handlers: %s. "
                "Add @handles(%s) decorator to handle this event.",
                event_type.__name__,
                self._owner_name,
                ", ".join(available_handlers) if available_handlers else "none",
                event_type.__name__,
                extra={
                    "owner": self._owner_name,
                    "event_type": event_type.__name__,
                    "event_id": str(event.event_id),
                    "available_handlers": available_handlers,
                },
            )
        # "ignore" mode: do nothing

    @property
    def owner(self) -> Any:
        """Get the owner object."""
        return self._owner

    @property
    def handler_count(self) -> int:
        """Get the number of registered handlers."""
        return len(self._handlers)

    def __repr__(self) -> str:
        return (
            f"HandlerRegistry({self._owner_name}, "
            f"handlers={self.handler_count}, "
            f"require_async={self._require_async})"
        )


__all__ = [
    "HandlerRegistry",
    "HandlerInfo",
    "HandlerSignatureError",
    "UnregisteredEventHandling",
]
