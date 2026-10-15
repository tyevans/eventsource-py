"""Declarative aggregate root with @handles decorator support."""

from __future__ import annotations

import inspect
import logging
from abc import ABC
from typing import ClassVar, cast

from pydantic import BaseModel

from eventsource.domain.aggregate.aggregate_root import AggregateRoot
from eventsource.domain.aggregate.types import UnregisteredEventHandling
from eventsource.domain.decorators import discover_handlers
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import (
    AggregateNotCreatedError,
    HandlerSignatureError,
    UnhandledEventError,
)

logger = logging.getLogger(__name__)


class DeclarativeAggregate[TState: BaseModel](AggregateRoot[TState], ABC):
    """Aggregate that uses @handles decorators to register event handlers.

    Attributes:
        requires_creation_event: When True, _get_initial_state() is optional and
            accessing `state` before a creation event raises AggregateNotCreatedError.
        unregistered_event_handling: Behavior when an event has no handler:
            "error" (default, raises UnhandledEventError), "warn", or "ignore".
    """

    requires_creation_event: ClassVar[bool] = False
    unregistered_event_handling: ClassVar[UnregisteredEventHandling] = "error"
    _event_handlers: ClassVar[dict[type[DomainEvent], str]] = {}

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Discover and validate @handles methods for each subclass."""
        super().__init_subclass__(**kwargs)
        cls._event_handlers = discover_handlers(cls)
        for event_type, name in cls._event_handlers.items():
            method = getattr(cls, name)
            if inspect.iscoroutinefunction(method):
                try:
                    async_params = list(inspect.signature(method).parameters.values())
                    async_param_count = len(async_params) - 1  # exclude self (unbound function)
                except (ValueError, TypeError):
                    async_param_count = 1
                raise HandlerSignatureError(
                    handler_name=name,
                    owner_name=cls.__name__,
                    event_type=event_type,
                    param_count=async_param_count,
                    is_async_required=False,
                    reason=(
                        "aggregate event handlers run synchronously during replay; remove 'async'"
                    ),
                )
            try:
                params = list(inspect.signature(method).parameters.values())
            except (ValueError, TypeError):
                continue
            param_count = len(params) - 1  # exclude self (unbound function)
            if param_count != 1:
                raise HandlerSignatureError(
                    handler_name=name,
                    owner_name=cls.__name__,
                    event_type=event_type,
                    param_count=param_count,
                    is_async_required=False,
                )

    @property
    def state(self) -> TState:
        """Return current state, raising AggregateNotCreatedError if not yet created."""
        if self.requires_creation_event and self._state is None:
            raise AggregateNotCreatedError(
                self.__class__.__name__,
                suggestion=f"Call a creation method on {self.__class__.__name__} first.",
            )
        return cast(TState, self._state)

    @property
    def state_or_none(self) -> TState | None:
        """Return current state, or None if the aggregate has not been created."""
        return self._state

    @property
    def is_created(self) -> bool:
        """Return True if the aggregate has state."""
        return self._state is not None

    def _get_initial_state(self) -> TState | None:
        """Return None when `requires_creation_event` is True; otherwise require override."""
        if self.requires_creation_event:
            return None
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _get_initial_state() "
            f"or set requires_creation_event = True"
        )

    def _apply(self, event: DomainEvent) -> None:
        """Apply event using registered @handles methods."""
        handler_name = self._event_handlers.get(type(event))
        if handler_name:
            getattr(self, handler_name)(event)
        else:
            self._handle_unregistered_event(event)

    def _handle_unregistered_event(self, event: DomainEvent) -> None:
        """Handle an event that has no registered handler according to configuration."""
        event_type = type(event)
        available_handlers = [et.__name__ for et in self._event_handlers]

        if self.unregistered_event_handling == "error":
            raise UnhandledEventError(
                event_type=event_type.__name__,
                event_id=event.event_id,
                handler_class=self.__class__.__name__,
                available_handlers=available_handlers,
            )
        elif self.unregistered_event_handling == "warn":
            logger.warning(
                "No handler registered for event type %s in %s. Available handlers: %s.",
                event_type.__name__,
                self.__class__.__name__,
                ", ".join(available_handlers) if available_handlers else "none",
                extra={
                    "event_type": event_type.__name__,
                    "event_id": str(event.event_id),
                    "handler_class": self.__class__.__name__,
                    "available_handlers": available_handlers,
                },
            )


__all__ = ["DeclarativeAggregate"]
