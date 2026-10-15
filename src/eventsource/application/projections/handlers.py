"""Handler registry for discovering, validating, and routing @handles event handlers."""

import inspect
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Literal

from eventsource.domain.decorators import discover_handlers
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import HandlerSignatureError, UnhandledEventError

logger = logging.getLogger(__name__)

UnregisteredEventHandling = Literal["ignore", "warn", "error"]


@dataclass
class HandlerInfo:
    """Metadata about a registered event handler."""

    event_type: type[DomainEvent]
    handler_name: str
    handler: Callable[..., Coroutine[Any, Any, None] | None]
    is_async: bool
    param_count: int


class HandlerRegistry:
    """Registry for discovering, validating, and routing @handles event handlers."""

    def __init__(
        self,
        owner: Any,
        *,
        require_async: bool = True,
        unregistered_event_handling: UnregisteredEventHandling = "ignore",
        validate_on_init: bool = True,
    ) -> None:
        """Initialize the handler registry and optionally validate discovered handlers."""
        self._owner = owner
        self._owner_name = owner.__class__.__name__
        self._require_async = require_async
        self._unregistered_event_handling = unregistered_event_handling
        self._handlers: dict[type[DomainEvent], HandlerInfo] = {}

        self._discover_handlers()
        if validate_on_init:
            self._validate_handlers()

    def _discover_handlers(self) -> None:
        """Scan the owner object for @handles decorated methods."""
        for event_type, attr_name in discover_handlers(type(self._owner)).items():
            attr = getattr(self._owner, attr_name)
            is_async = inspect.iscoroutinefunction(attr)

            try:
                sig = inspect.signature(attr)
                param_count = len(list(sig.parameters.values()))
            except (ValueError, TypeError):
                param_count = 1

            self._handlers[event_type] = HandlerInfo(
                event_type=event_type,
                handler_name=attr_name,
                handler=attr,
                is_async=is_async,
                param_count=param_count,
            )

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
        """Validate all discovered handlers."""
        for handler_info in self._handlers.values():
            self._validate_handler(handler_info)

    def _validate_handler(self, handler_info: HandlerInfo) -> None:
        """Validate a single handler's signature (async requirement, parameter count, annotation)."""
        handler = handler_info.handler
        handler_name = handler_info.handler_name
        event_type = handler_info.event_type

        if self._require_async and not handler_info.is_async:
            event_name = event_type.__name__
            raise ValueError(
                f"Handler '{handler_name}' in {self._owner_name} must be async.\n\n"
                f"Change:\n  def {handler_name}(self, ...)\n\n"
                f"To:\n  async def {handler_name}(self, event: {event_name}) -> None"
            )

        if handler_info.param_count < 1 or handler_info.param_count > 2:
            raise HandlerSignatureError(
                handler_name=handler_name,
                owner_name=self._owner_name,
                event_type=event_type,
                param_count=handler_info.param_count,
                is_async_required=self._require_async,
            )

        try:
            sig = inspect.signature(handler)
            event_param = list(sig.parameters.values())[-1]
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
            pass

    def get_handler(self, event_type: type[DomainEvent]) -> HandlerInfo | None:
        """Get the handler info for a specific event type, or None if not found."""
        return self._handlers.get(event_type)

    def has_handler(self, event_type: type[DomainEvent]) -> bool:
        """Check if a handler exists for the given event type."""
        return event_type in self._handlers

    def get_subscribed_events(self) -> list[type[DomainEvent]]:
        """Get list of event types that have handlers."""
        return list(self._handlers.keys())

    def get_all_handlers(self) -> dict[type[DomainEvent], HandlerInfo]:
        """Get dictionary mapping event types to handler info."""
        return dict(self._handlers)

    async def dispatch(self, event: DomainEvent, context: Any = None) -> bool:
        """Dispatch an event to its registered handler. Returns True if handled."""
        handler_info = self._handlers.get(type(event))
        if handler_info is None:
            self._handle_unregistered_event(event)
            return False

        args = (event,) if handler_info.param_count == 1 else (context, event)
        if handler_info.is_async:
            await handler_info.handler(*args)  # type: ignore[misc]
        else:
            handler_info.handler(*args)
        return True

    def _handle_unregistered_event(self, event: DomainEvent) -> None:
        """Handle an event that has no registered handler according to configured policy."""
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


__all__ = ["HandlerRegistry", "HandlerInfo", "UnregisteredEventHandling"]
