"""Synchronous Given-When-Then harness for decider-style domains."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel

from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import CommandRejectedError
from eventsource.testing.assertions import EventAssertions


class DeciderScenario:
    _NON_AGGREGATE_ID_FIELDS = frozenset(
        {"command_id", "correlation_id", "causation_id", "tenant_id", "actor_id"}
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
            decide, evolve = aggregate_class.decide, aggregate_class.evolve
            initial_state = aggregate_class.initial_state
        if decide is None or evolve is None or initial_state is None:
            msg = "DeciderScenario needs an aggregate class or all of decide=, evolve=, initial_state="
            raise TypeError(msg)
        self._decide, self._evolve, self._initial_state = decide, evolve, initial_state
        self._states: dict[UUID, Any] = {}
        self._default_state: Any = initial_state()
        self._events: list[DomainEvent] | None = None
        self._error: BaseException | None = None
        self._last_target_id: UUID | None = None

    @property
    def events(self) -> list[DomainEvent]:
        return list(self._events) if self._events is not None else []

    @property
    def state(self) -> Any:
        if not self._states:
            return self._default_state
        if len(self._states) == 1:
            return next(iter(self._states.values()))
        if self._last_target_id and self._last_target_id in self._states:
            return self._states[self._last_target_id]
        msg = "Multiple aggregate states exist in scenario. Use get_state(aggregate_id) or .states to access per-aggregate state."
        raise ValueError(msg)

    @property
    def states(self) -> dict[UUID, Any]:
        return dict(self._states)

    def get_state(self, aggregate_id: UUID | None = None) -> Any:
        if aggregate_id is None:
            return self.state
        return self._states.get(aggregate_id, self._initial_state())

    def given(self, *events: DomainEvent | Iterable[DomainEvent]) -> DeciderScenario:
        for item in events:
            for ev in (item,) if isinstance(item, DomainEvent) else item:
                if isinstance(ev, DomainEvent):
                    agg_id = getattr(ev, "aggregate_id", None)
                    if isinstance(agg_id, UUID):
                        self._states[agg_id] = self._evolve(self.get_state(agg_id), ev)
                    else:
                        self._default_state = self._evolve(self._default_state, ev)
        return self

    def given_commands(self, *commands: object, aggregate_id: UUID | None = None) -> Self:
        for command in commands:
            target_id = self._resolve_target_id(command, explicit_id=aggregate_id)
            state = self.get_state(target_id) if target_id else self._default_state
            for event in self._decide(command, state):
                state = self._evolve(state, event)
                ev_id = getattr(event, "aggregate_id", None)
                eff_id = target_id or (ev_id if isinstance(ev_id, UUID) else None)
                if eff_id is not None:
                    self._states[eff_id] = state
                else:
                    self._default_state = state
        return self

    def _resolve_target_id(self, command: object, explicit_id: UUID | None = None) -> UUID | None:
        if explicit_id is not None:
            return explicit_id
        if isinstance(cmd_agg_id := getattr(command, "aggregate_id", None), UUID):
            return cmd_agg_id
        if isinstance(command, BaseModel):
            fields = [(k, getattr(command, k)) for k in type(command).model_fields]
        elif hasattr(command, "__dict__"):
            fields = list(command.__dict__.items())
        else:
            dc_fields = getattr(command, "__dataclass_fields__", ())
            fields = [(k, getattr(command, k)) for k in dc_fields]
        uuid_candidates: list[UUID] = []
        for name, val in fields:
            if name not in self._NON_AGGREGATE_ID_FIELDS and isinstance(val, UUID):
                if val in self._states:
                    return val
                if name.endswith("_id") or name == "id":
                    uuid_candidates.append(val)
        if len(uuid_candidates) == 1:
            return uuid_candidates[0]
        if len(self._states) == 1:
            return next(iter(self._states.keys()))
        if not self._states:
            return None
        msg = f"Multiple aggregates exist in scenario ({list(self._states.keys())}). Please specify which aggregate to evaluate via when(command, aggregate_id=...) or set an aggregate ID on the command."
        raise ValueError(msg)

    def when(self, command: object, *, aggregate_id: UUID | None = None) -> DeciderScenario:
        return self.when_all(command, aggregate_id=aggregate_id)

    def when_all(self, *commands: object, aggregate_id: UUID | None = None) -> DeciderScenario:
        self._events, self._error = None, None
        collected: list[DomainEvent] = []
        for command in commands:
            target_id = self._resolve_target_id(command, explicit_id=aggregate_id)
            self._last_target_id = target_id
            state = self.get_state(target_id) if target_id else self._default_state
            try:
                emitted = list(self._decide(command, state))
            except Exception as exc:  # noqa: BLE001
                self._error = exc
                return self
            for event in emitted:
                state = self._evolve(state, event)
            if target_id is not None:
                self._states[target_id] = state
            else:
                self._default_state = state
            collected.extend(emitted)
        self._events = collected
        return self

    def then_no_error(self) -> DeciderScenario:
        if self._events is None and self._error is None:
            raise AssertionError("call when() before then_no_error()")
        if self._error is not None:
            err = self._error
            raise AssertionError(f"expected command to succeed, but it was rejected: {err!r}")
        return self

    def then_no_events(self) -> Self:
        self.then_no_error()
        if self._events:
            got = [type(e).__name__ for e in self._events]
            raise AssertionError(f"expected no events, got {got}")
        return self

    def then_event(self, event_type: type[DomainEvent], **expected_fields: Any) -> Self:
        self.then_no_error()
        EventAssertions(self.events).assert_event_with_fields(event_type, **expected_fields)
        return self

    def then_state(
        self, expected_or_predicate: Any, *, aggregate_id: UUID | None = None
    ) -> DeciderScenario:
        self.then_no_error()
        actual = self.get_state(aggregate_id) if aggregate_id is not None else self.state
        if callable(expected_or_predicate) and not isinstance(expected_or_predicate, type):
            if not expected_or_predicate(actual):
                raise AssertionError(f"state predicate returned False for state {actual!r}")
        elif actual != expected_or_predicate:
            raise AssertionError(f"expected state {expected_or_predicate!r}, got {actual!r}")
        return self

    def then_events(self, *event_types: type[DomainEvent]) -> DeciderScenario:
        if self._events is None and self._error is None:
            raise AssertionError("call when() before then_events()")
        expected = [t.__name__ for t in event_types]
        if self._error is not None:
            err = self._error
            raise AssertionError(
                f"expected events {expected} but the command was rejected: {err!r}"
            )
        assert self._events is not None
        actual = [type(e).__name__ for e in self._events]
        if actual != expected:
            raise AssertionError(f"expected events {expected}, got {actual}")
        return self

    def then_event_types(self, *expected_types: type[DomainEvent]) -> Self:
        if self._events is None and self._error is None:
            raise AssertionError("call when() before then_event_types()")
        exp_names = [t.__name__ for t in expected_types]
        if self._error is not None:
            err = self._error
            raise AssertionError(f"Expected event types {exp_names}, but command raised {err!r}")
        assert self._events is not None
        actual_types = [type(e) for e in self._events]
        if actual_types != list(expected_types):
            got = [t.__name__ for t in actual_types]
            raise AssertionError(f"Expected event types {exp_names}, got {got}")
        return self

    def then_rejected(
        self, exc_type: type[BaseException] = CommandRejectedError, match: str | None = None
    ) -> DeciderScenario:
        if self._events is None and self._error is None:
            raise AssertionError("call when() before then_rejected()")
        if self._error is None:
            raise AssertionError(f"expected rejection but command produced {self.events!r}")
        if not isinstance(self._error, exc_type):
            got_name = type(self._error).__name__
            raise AssertionError(f"expected {exc_type.__name__}, got {got_name}: {self._error}")
        if match is not None and not re.search(match, str(self._error)):
            raise AssertionError(f"rejection message {str(self._error)!r} does not match {match!r}")
        return self


Scenario = ScenarioResult = DeciderScenario
__all__ = ["DeciderScenario", "Scenario", "ScenarioResult"]
