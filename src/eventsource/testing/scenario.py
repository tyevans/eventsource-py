"""Synchronous Given-When-Then harness for decider-style domains."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable
from typing import Any, Self
from uuid import UUID

from eventsource.domain import CommandRejectedError, DomainEvent
from eventsource.testing.assertions import EventAssertions

_NON_AGG_IDS = frozenset(("command_id", "correlation_id", "causation_id", "tenant_id", "actor_id"))


class DeciderScenario:
    _NON_AGGREGATE_ID_FIELDS = _NON_AGG_IDS
    _events: list[DomainEvent] | None = None
    _error: BaseException | None = None
    _last_target_id: UUID | None = None

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

    @property
    def events(self) -> list[DomainEvent]:
        return list(self._events) if self._events is not None else []

    @property
    def assertions(self) -> EventAssertions:
        return EventAssertions(self.events)

    def __len__(self) -> int:
        return len(self.events)

    def __bool__(self) -> bool:
        return True

    @property
    def state(self) -> Any:
        if len(self._states) <= 1:
            return next(iter(self._states.values()), self._default_state)
        if self._last_target_id and self._last_target_id in self._states:
            return self._states[self._last_target_id]
        msg = "Multiple aggregate states exist in scenario. Use get_state(aggregate_id) or .states to access per-aggregate state."
        raise ValueError(msg)

    @property
    def states(self) -> dict[UUID, Any]:
        return dict(self._states)

    def get_state(self, aggregate_id: UUID | None = None) -> Any:
        aid = aggregate_id
        return self.state if aid is None else self._states.get(aid, self._initial_state())

    def _apply(self, ev: DomainEvent, tid: UUID | None = None) -> None:
        agg = getattr(ev, "aggregate_id", None)
        aid = tid or (agg if isinstance(agg, UUID) else None)
        nxt = self._evolve(self.get_state(aid) if aid else self._default_state, ev)
        self._states.__setitem__(aid, nxt) if aid else setattr(self, "_default_state", nxt)

    def given(self, *events: DomainEvent | Iterable[DomainEvent]) -> DeciderScenario:
        for item in events:
            for ev in (item,) if isinstance(item, DomainEvent) else item:
                if isinstance(ev, DomainEvent):
                    self._apply(ev)
        return self

    def given_commands(self, *commands: object, aggregate_id: UUID | None = None) -> Self:
        for cmd in commands:
            tid = self._resolve_target_id(cmd, explicit_id=aggregate_id)
            for ev in self._decide(cmd, self.get_state(tid) if tid else self._default_state):
                self._apply(ev, tid)
        return self

    def _resolve_target_id(self, cmd: object, explicit_id: UUID | None = None) -> UUID | None:
        if isinstance(aid := explicit_id or getattr(cmd, "aggregate_id", None), UUID):
            return aid
        ks = getattr(type(cmd), "model_fields", ()) or getattr(cmd, "__dict__", ())
        cands: list[UUID] = []
        for k in ks or getattr(cmd, "__dataclass_fields__", ()):
            if k not in _NON_AGG_IDS and isinstance(v := getattr(cmd, k), UUID):
                if v in self._states:
                    return v
                if k.endswith("_id") or k == "id":
                    cands.append(v)
        if len(cands) == 1 or len(self._states) <= 1:
            return cands[0] if len(cands) == 1 else next(iter(self._states), None)
        msg = f"Multiple aggregates exist in scenario ({list(self._states.keys())}). Please specify which aggregate to evaluate via when(command, aggregate_id=...) or set an aggregate ID on the command."
        raise ValueError(msg)

    def when(self, command: object, *, aggregate_id: UUID | None = None) -> DeciderScenario:
        return self.when_all(command, aggregate_id=aggregate_id)

    def when_all(self, *commands: object, aggregate_id: UUID | None = None) -> DeciderScenario:
        self._events, self._error, collected = None, None, []
        for cmd in commands:
            self._last_target_id = tid = self._resolve_target_id(cmd, explicit_id=aggregate_id)
            try:
                state = self.get_state(tid) if tid else self._default_state
                emitted = list(self._decide(cmd, state))
            except Exception as exc:  # noqa: BLE001
                self._error = exc
                return self
            for ev in emitted:
                self._apply(ev, tid)
            collected.extend(emitted)
        self._events = collected
        return self

    def _ensure_acted(self, method: str, err_prefix: str | None = None) -> list[DomainEvent]:
        if (no_act := self._events is None and self._error is None) or (err_prefix and self._error):
            msg = f"call when() before {method}()" if no_act else f"{err_prefix} {self._error!r}"
            raise AssertionError(msg)
        return self._events or []

    def then_no_error(self) -> DeciderScenario:
        self._ensure_acted("then_no_error", "expected command to succeed, but it was rejected:")
        return self

    def then_no_events(self) -> Self:
        if events := self.then_no_error()._events:
            raise AssertionError(f"expected no events, got {[type(e).__name__ for e in events]}")
        return self

    def then_event(self, event_type: type[DomainEvent], **expected_fields: Any) -> Self:
        self.then_no_error()
        self.assertions.assert_event_with_fields(event_type, **expected_fields)
        return self

    def then_state(self, expected: Any, *, aggregate_id: UUID | None = None) -> DeciderScenario:
        self.then_no_error()
        actual = self.get_state(aggregate_id) if aggregate_id is not None else self.state
        is_pred = callable(expected) and not isinstance(expected, type)
        if (is_pred and not expected(actual)) or (not is_pred and actual != expected):
            msg = f"state predicate returned False for state {actual!r}"
            raise AssertionError(msg if is_pred else f"expected state {expected!r}, got {actual!r}")
        return self

    def then_events(self, *event_types: type[DomainEvent]) -> DeciderScenario:
        exp = [t.__name__ for t in event_types]
        msg = f"expected events {exp} but the command was rejected:"
        actual = [type(e).__name__ for e in self._ensure_acted("then_events", msg)]
        if actual != exp:
            raise AssertionError(f"expected events {exp}, got {actual}")
        return self

    def then_event_types(self, *expected_types: type[DomainEvent]) -> Self:
        exp = [t.__name__ for t in expected_types]
        msg = f"Expected event types {exp}, but command raised"
        actual = [type(e) for e in self._ensure_acted("then_event_types", msg)]
        if actual != list(expected_types):
            raise AssertionError(f"Expected event types {exp}, got {[t.__name__ for t in actual]}")
        return self

    def then_rejected(
        self, exc_type: type[BaseException] = CommandRejectedError, match: str | None = None
    ) -> DeciderScenario:
        self._ensure_acted("then_rejected")
        if (err := self._error) is None:
            raise AssertionError(f"expected rejection but command produced {self.events!r}")
        if not isinstance(err, exc_type) or (match is not None and not re.search(match, str(err))):
            got = f"expected {exc_type.__name__}, got {type(err).__name__}: {err}"
            bad = f"rejection message {str(err)!r} does not match {match!r}"
            raise AssertionError(bad if isinstance(err, exc_type) else got)
        return self


Scenario = ScenarioResult = DeciderScenario
__all__ = ["DeciderScenario", "Scenario", "ScenarioResult"]
