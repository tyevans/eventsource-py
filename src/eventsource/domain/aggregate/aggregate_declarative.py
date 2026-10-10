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
    """
    Aggregate that uses decorators to register event handlers.

    This class provides an alternative to the basic AggregateRoot that
    uses a declarative pattern with the @handles decorator to register
    event handlers, reducing boilerplate in the _apply method.

    Supports deferred state via `requires_creation_event` class attribute.
    When True, the aggregate doesn't require an initial state implementation
    and will raise AggregateNotCreatedError if state is accessed before
    a creation event is applied.

    Attributes:
        requires_creation_event: When True, the aggregate doesn't require
            _get_initial_state() implementation and state access raises
            AggregateNotCreatedError until a creation event is applied.
            Default is False (backward compatible).
        unregistered_event_handling: Controls behavior when an event has no
            registered handler. Options:
            - "error": Raise UnhandledEventError for unhandled events (default).
              An aggregate is the write model — a silently unapplied event means
              command handlers reason over divergent state.
            - "warn": Log a warning for unhandled events
            - "ignore": Silently ignore unhandled events (explicit opt-down, e.g.
              for forward-compat replay of event types added after this
              aggregate's handlers were written)
    """

    # Class-level attribute for deferred state support
    # When True, aggregate doesn't require _get_initial_state() implementation
    requires_creation_event: ClassVar[bool] = False

    # Class-level configuration for unregistered event handling
    # Options: "error" (default), "warn", "ignore"
    unregistered_event_handling: ClassVar[UnregisteredEventHandling] = "error"

    # Per-subclass handler registry, rebuilt by __init_subclass__.
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
        """
        Get the current state of the aggregate.

        Returns:
            The current aggregate state

        Raises:
            AggregateNotCreatedError: If requires_creation_event=True and
                no events have been applied yet
        """
        if self.requires_creation_event and self._state is None:
            raise AggregateNotCreatedError(
                self.__class__.__name__,
                suggestion=f"Call a creation method on {self.__class__.__name__} first.",
            )
        return cast(TState, self._state)

    @property
    def state_or_none(self) -> TState | None:
        """
        Get the current state without raising on uncreated aggregate.

        This is useful for checking if an aggregate exists or for
        conditional logic based on creation status.

        Returns:
            The current state, or None if aggregate hasn't been created
        """
        return self._state

    @property
    def is_created(self) -> bool:
        """
        Check if the aggregate has been created (has state).

        Returns:
            True if at least one event has been applied, False otherwise
        """
        return self._state is not None

    def _get_initial_state(self) -> TState | None:
        """
        Get initial state for new aggregate.

        Behavior depends on `requires_creation_event`:

        - False (default): Subclasses must implement this method
        - True: Returns None, state is set by first event handler

        Returns:
            Initial state, or None for deferred state aggregates

        Raises:
            NotImplementedError: If requires_creation_event=False and
                not implemented in subclass
        """
        if self.requires_creation_event:
            return None

        raise NotImplementedError(
            f"{self.__class__.__name__} must implement _get_initial_state() "
            f"or set requires_creation_event = True"
        )

    def _apply(self, event: DomainEvent) -> None:
        """
        Apply event using registered handlers.

        Looks up the handler for the event type and calls it.
        Behavior for unhandled events depends on unregistered_event_handling setting.

        Raises:
            UnhandledEventError: If unregistered_event_handling="error" and no handler found
        """
        event_type = type(event)
        handler_name = self._event_handlers.get(event_type)
        if handler_name:
            handler = getattr(self, handler_name)
            handler(event)
        else:
            # No handler found - handle based on configuration
            self._handle_unregistered_event(event)

    def _handle_unregistered_event(self, event: DomainEvent) -> None:
        """
        Handle an event that has no registered handler.

        Behavior depends on the unregistered_event_handling class attribute:
        - "ignore": Do nothing (silent)
        - "warn": Log a warning
        - "error": Raise UnhandledEventError

        Args:
            event: The event that has no handler

        Raises:
            UnhandledEventError: If unregistered_event_handling="error"
        """
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
        # "ignore" mode: do nothing (silent)


__all__ = ["DeclarativeAggregate"]
