"""Handler registry for discovering, validating, and routing @handles event handlers."""

import inspect
import logging
from collections.abc import Callable, Coroutine, Iterator
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
        self._owner = owner
        self._owner_name = owner.__class__.__name__
        self._require_async = require_async
        self._unregistered_event_handling = unregistered_event_handling
        self._handlers: dict[type[DomainEvent], HandlerInfo] = {}
        self._discover_handlers()
        if validate_on_init:
            self._validate_handlers()

    def _discover_handlers(self) -> None:
        for event_type, attr_name in discover_handlers(type(self._owner)).items():
            attr = getattr(self._owner, attr_name)
            is_async = inspect.iscoroutinefunction(attr)
            try:
                param_count = len(list(inspect.signature(attr).parameters.values()))
            except (ValueError, TypeError):
                param_count = 1

            self._handlers[event_type] = HandlerInfo(
                event_type, attr_name, attr, is_async, param_count
            )
            extra = {"owner": self._owner_name, "handler": attr_name, "event_type": event_type.__name__, "is_async": is_async, "param_count": param_count}  # fmt: skip
            logger.debug("Registered handler %s for %s", attr_name, event_type.__name__, extra=extra)  # fmt: skip

    def _validate_handlers(self) -> None:
        for handler_info in self._handlers.values():
            self._validate_handler(handler_info)

    def _validate_handler(self, handler_info: HandlerInfo) -> None:
        name, event_type = handler_info.handler_name, handler_info.event_type
        if self._require_async and not handler_info.is_async:
            raise ValueError(
                f"Handler '{name}' in {self._owner_name} must be async.\n\n"
                f"Change:\n  def {name}(self, ...)\n\n"
                f"To:\n  async def {name}(self, event: {event_type.__name__}) -> None"
            )
        if handler_info.param_count < 1 or handler_info.param_count > 2:
            raise HandlerSignatureError(
                name, self._owner_name, event_type, handler_info.param_count, self._require_async
            )
        try:
            ann = list(inspect.signature(handler_info.handler).parameters.values())[-1].annotation
            if ann != inspect.Parameter.empty and isinstance(ann, type) and ann != event_type:
                extra = {"owner": self._owner_name, "handler": name, "expected_type": event_type.__name__, "actual_annotation": ann.__name__}  # fmt: skip
                msg = "Handler %s in %s: Event type annotation %s doesn't match @handles(%s)"
                logger.warning(msg, name, self._owner_name, ann.__name__, event_type.__name__, extra=extra)  # fmt: skip
        except (ValueError, TypeError):
            pass

    def get_handler(self, target: type[DomainEvent] | DomainEvent) -> HandlerInfo | None:
        return self._handlers.get(target if isinstance(target, type) else type(target))

    def has_handler(self, target: type[DomainEvent] | DomainEvent) -> bool:
        return (target if isinstance(target, type) else type(target)) in self._handlers

    def get_subscribed_events(self) -> list[type[DomainEvent]]:
        return list(self._handlers.keys())

    def get_all_handlers(self) -> dict[type[DomainEvent], HandlerInfo]:
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
        event_name = type(event).__name__
        available = [et.__name__ for et in self._handlers]
        if self._unregistered_event_handling == "error":
            raise UnhandledEventError(event_name, event.event_id, self._owner_name, available)
        if self._unregistered_event_handling == "warn":
            avail_str = ", ".join(available) if available else "none"
            extra = {"owner": self._owner_name, "event_type": event_name, "event_id": str(event.event_id), "available_handlers": available}  # fmt: skip
            msg = "No handler registered for event type %s in %s. Available handlers: %s. Add @handles(%s) decorator to handle this event."
            logger.warning(msg, event_name, self._owner_name, avail_str, event_name, extra=extra)

    @property
    def owner(self) -> Any:
        return self._owner

    @property
    def handler_count(self) -> int:
        return len(self._handlers)

    def __len__(self) -> int:
        return len(self._handlers)

    def __bool__(self) -> bool:
        return True

    def __contains__(self, target: type[DomainEvent] | DomainEvent) -> bool:
        return self.has_handler(target)

    def __iter__(self) -> Iterator[type[DomainEvent]]:
        return iter(self._handlers)

    def __repr__(self) -> str:
        return f"HandlerRegistry({self._owner_name}, handlers={self.handler_count}, require_async={self._require_async})"


__all__ = ["HandlerRegistry", "HandlerInfo", "UnregisteredEventHandling"]
