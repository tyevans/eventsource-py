"""
Synchronous Given-When-Then harness for decider-style domains.

Supports both single-aggregate and multi-aggregate scenarios without
requiring store or event bus infrastructure.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import CommandRejectedError


class DeciderScenario:
    """
    Synchronous given/when/then harness for decider-style domains.

    Works with a DeciderAggregate subclass or the three functions directly.
    No store, no event loop, no fixtures: ``given`` folds events through
    ``evolve`` from ``initial_state``, partitioned by aggregate ID. ``when``
    evaluates ``decide`` against the target aggregate's state, capturing
    events or the raised exception, and ``then_*`` asserts the outcome.

    Supports multi-aggregate scenarios where events with distinct aggregate IDs
    maintain isolated state.

    Example:
        >>> (DeciderScenario(OrderAggregate)
        ...     .given(OrderCreated(aggregate_id=oid, aggregate_version=1, ...))
        ...     .when(ShipOrder(order_id=oid, tracking_number="T"))
        ...     .then_events(OrderShipped))
    """

    _NON_AGGREGATE_ID_FIELDS = frozenset(
        {
            "command_id",
            "correlation_id",
            "causation_id",
            "tenant_id",
            "actor_id",
        }
    )

    def __init__(
        self,
        aggregate_class: type[Any] | None = None,
        *,
        decide: Callable[[Any, Any], list[DomainEvent]] | None = None,
        evolve: Callable[[Any, DomainEvent], Any] | None = None,
        initial_state: Callable[[], Any] | None = None,
    ) -> None:
        if aggregate_class is not None:
            decide = aggregate_class.decide
            evolve = aggregate_class.evolve
            initial_state = aggregate_class.initial_state
        if decide is None or evolve is None or initial_state is None:
            raise TypeError(
                "DeciderScenario needs an aggregate class or all of "
                "decide=, evolve=, initial_state="
            )
        self._decide = decide
        self._evolve = evolve
        self._initial_state = initial_state
        self._states: dict[UUID, Any] = {}
        self._default_state: Any = initial_state()
        self._events: list[DomainEvent] | None = None
        self._error: BaseException | None = None
        self._last_target_id: UUID | None = None

    @property
    def events(self) -> list[DomainEvent]:
        """Events produced by when(); empty before when() or on rejection."""
        return list(self._events) if self._events is not None else []

    @property
    def state(self) -> Any:
        """Current state for single-aggregate scenarios or initial state."""
        if not self._states:
            return self._default_state
        if len(self._states) == 1:
            return next(iter(self._states.values()))
        if self._last_target_id and self._last_target_id in self._states:
            return self._states[self._last_target_id]
        raise ValueError(
            "Multiple aggregate states exist in scenario. Use get_state(aggregate_id) "
            "or .states to access per-aggregate state."
        )

    @property
    def _state(self) -> Any:
        """Internal/backward-compatible state accessor."""
        if not self._states:
            return self._default_state
        if len(self._states) == 1:
            return next(iter(self._states.values()))
        if self._last_target_id and self._last_target_id in self._states:
            return self._states[self._last_target_id]
        return next(iter(self._states.values()))

    @property
    def states(self) -> dict[UUID, Any]:
        """Dictionary mapping aggregate IDs to their evolved states."""
        return dict(self._states)

    def get_state(self, aggregate_id: UUID | None = None) -> Any:
        """Return state for a specific aggregate ID, or the single/initial state."""
        if aggregate_id is not None:
            return self._states.get(aggregate_id, self._initial_state())
        return self.state

    def given(self, *events: DomainEvent | Iterable[DomainEvent]) -> DeciderScenario:
        """Fold prior events into state via evolve, partitioned by aggregate ID."""
        for item in events:
            if isinstance(item, DomainEvent):
                self._apply_given_event(item)
            elif isinstance(item, Iterable):
                for sub_item in item:
                    if isinstance(sub_item, DomainEvent):
                        self._apply_given_event(sub_item)
        return self

    def _apply_given_event(self, event: DomainEvent) -> None:
        agg_id = getattr(event, "aggregate_id", None)
        if agg_id is None or not isinstance(agg_id, UUID):
            self._default_state = self._evolve(self._default_state, event)
            return

        if agg_id not in self._states:
            self._states[agg_id] = self._initial_state()
        self._states[agg_id] = self._evolve(self._states[agg_id], event)

    def _resolve_target_id(self, command: object, explicit_id: UUID | None = None) -> UUID | None:
        if explicit_id is not None:
            return explicit_id

        # 1. Direct aggregate_id on command
        cmd_agg_id = getattr(command, "aggregate_id", None)
        if isinstance(cmd_agg_id, UUID):
            return cmd_agg_id

        # 2. Inspect command fields / attributes
        field_values: list[tuple[str, Any]] = []
        if isinstance(command, BaseModel):
            field_values = [(k, getattr(command, k)) for k in type(command).model_fields]
        elif hasattr(command, "__dict__"):
            field_values = list(command.__dict__.items())
        elif hasattr(command, "__dataclass_fields__"):
            field_values = [(k, getattr(command, k)) for k in command.__dataclass_fields__]

        uuid_candidates: list[tuple[str, UUID]] = []
        for name, val in field_values:
            if name in self._NON_AGGREGATE_ID_FIELDS:
                continue
            if isinstance(val, UUID):
                if val in self._states:
                    return val
                if name.endswith("_id") or name == "id":
                    uuid_candidates.append((name, val))

        if len(uuid_candidates) == 1:
            return uuid_candidates[0][1]

        # 3. Fallback based on known aggregates
        if len(self._states) == 1:
            return next(iter(self._states.keys()))
        if len(self._states) == 0:
            return None

        raise ValueError(
            f"Multiple aggregates exist in scenario ({list(self._states.keys())}). "
            "Please specify which aggregate to evaluate via when(command, aggregate_id=...) "
            "or set an aggregate ID on the command."
        )

    def when(self, command: object, *, aggregate_id: UUID | None = None) -> DeciderScenario:
        """Run decide, capturing produced events or the raised exception."""
        target_id = self._resolve_target_id(command, explicit_id=aggregate_id)
        self._last_target_id = target_id

        if target_id is not None:
            current_state = self._states.get(target_id, self._initial_state())
        else:
            current_state = self._default_state

        self._events = None
        self._error = None
        try:
            self._events = list(self._decide(command, current_state))
        except Exception as exc:  # noqa: BLE001 - the exception IS the result
            self._error = exc
        return self

    def then_events(self, *event_types: type[DomainEvent]) -> DeciderScenario:
        """Assert the command produced exactly these event types, in order."""
        if self._events is None and self._error is None:
            raise AssertionError("call when() before then_events()")
        if self._error is not None:
            raise AssertionError(
                f"expected events {[t.__name__ for t in event_types]} but the "
                f"command was rejected: {self._error!r}"
            )
        assert self._events is not None  # narrows for mypy
        actual = [type(e).__name__ for e in self._events]
        expected = [t.__name__ for t in event_types]
        if actual != expected:
            raise AssertionError(f"expected events {expected}, got {actual}")
        return self

    def then_rejected(
        self,
        exc_type: type[BaseException] = CommandRejectedError,
        match: str | None = None,
    ) -> DeciderScenario:
        """Assert the command was rejected with exc_type (default CommandRejectedError)."""
        if self._events is None and self._error is None:
            raise AssertionError("call when() before then_rejected()")
        if self._error is None:
            raise AssertionError(f"expected rejection but command produced {self.events!r}")
        if not isinstance(self._error, exc_type):
            raise AssertionError(
                f"expected {exc_type.__name__}, got {type(self._error).__name__}: {self._error}"
            )
        if match is not None and not re.search(match, str(self._error)):
            raise AssertionError(f"rejection message {str(self._error)!r} does not match {match!r}")
        return self


__all__ = ["DeciderScenario"]
