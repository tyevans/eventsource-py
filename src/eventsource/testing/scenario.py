"""Synchronous Given-When-Then harness for decider-style domains."""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator
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

    def __init__(self, aggregate_class: type[Any] | None = None, **kw: Any) -> None:
        decide, evolve = (aggregate_class.decide, aggregate_class.evolve) if aggregate_class else (kw.get("decide"), kw.get("evolve"))  # fmt: skip
        if decide is None or evolve is None or (init := aggregate_class.initial_state if aggregate_class else kw.get("initial_state")) is None:  # fmt: skip
            raise TypeError("DeciderScenario needs an aggregate class or all of decide=, evolve=, initial_state=")  # fmt: skip
        self._decide, self._evolve, self._init, self._def_state = decide, evolve, init, init()
        self._states: dict[UUID, Any] = {}

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

    def __iter__(self) -> Iterator[DomainEvent]:
        return iter(self.events)

    def __contains__(self, target: object) -> bool:
        return target in self.assertions

    @property
    def state(self) -> Any:
        s, tid = self._states, self._last_target_id
        if len(s) <= 1 or (tid and tid in s):
            return s[tid] if len(s) > 1 and tid else next(iter(s.values()), self._def_state)
        raise ValueError("Multiple aggregate states exist in scenario. Use get_state(aggregate_id) or .states to access per-aggregate state.")  # fmt: skip

    @property
    def states(self) -> dict[UUID, Any]:
        return dict(self._states)

    def get_state(self, aggregate_id: UUID | None = None) -> Any:
        return self.state if aggregate_id is None else self._states.get(aggregate_id, self._init())

    def _apply(self, ev: DomainEvent, tid: UUID | None = None) -> None:
        aid = tid or (a if isinstance(a := getattr(ev, "aggregate_id", None), UUID) else None)
        self._states.__setitem__(aid, self._evolve(self.get_state(aid), ev)) if aid else setattr(self, "_def_state", self._evolve(self._def_state, ev))  # fmt: skip

    def given(self, *events: DomainEvent | Iterable[DomainEvent]) -> DeciderScenario:
        for item in events:
            for ev in (item,) if isinstance(item, DomainEvent) else item:
                self._apply(ev) if isinstance(ev, DomainEvent) else None
        return self

    def given_commands(self, *commands: object, aggregate_id: UUID | None = None) -> Self:
        for cmd in commands:
            tid = self._resolve_target_id(cmd, explicit_id=aggregate_id)
            for ev in self._decide(cmd, self.get_state(tid) if tid else self._def_state):
                self._apply(ev, tid)
        return self

    def _resolve_target_id(self, cmd: object, explicit_id: UUID | None = None) -> UUID | None:
        if isinstance(aid := explicit_id or getattr(cmd, "aggregate_id", None), UUID):
            return aid
        ks = getattr(type(cmd), "model_fields", ()) or getattr(cmd, "__dict__", ()) or getattr(cmd, "__dataclass_fields__", ())  # fmt: skip
        cands: list[UUID] = []
        for k in (k for k in ks if k not in _NON_AGG_IDS and isinstance(getattr(cmd, k), UUID)):
            if (v := getattr(cmd, k)) in self._states:
                return v  # type: ignore[no-any-return]
            cands.extend([v] if k == "id" or k.endswith("_id") else [])
        if len(cands) == 1 or len(self._states) <= 1:
            return cands[0] if len(cands) == 1 else next(iter(self._states), None)
        raise ValueError(f"Multiple aggregates exist in scenario ({list(self._states.keys())}). Please specify which aggregate to evaluate via when(command, aggregate_id=...) or set an aggregate ID on the command.")  # fmt: skip

    def when(self, command: object, *, aggregate_id: UUID | None = None) -> DeciderScenario:
        return self.when_all(command, aggregate_id=aggregate_id)

    def when_all(self, *commands: object, aggregate_id: UUID | None = None) -> DeciderScenario:
        self._events, self._error, collected = None, None, []
        for cmd in commands:
            self._last_target_id = tid = self._resolve_target_id(cmd, explicit_id=aggregate_id)
            try:
                for ev in (emitted := list(self._decide(cmd, self.get_state(tid) if tid else self._def_state))):  # fmt: skip
                    self._apply(ev, tid)
                collected.extend(emitted)
            except Exception as exc:  # noqa: BLE001
                self._error = exc
                return self
        self._events = collected
        return self

    def _ensure_acted(self, method: str, err_prefix: str | None = None) -> list[DomainEvent]:
        if (no_act := self._events is None and self._error is None) or (err_prefix and self._error):
            raise AssertionError(f"call when() before {method}()" if no_act else f"{err_prefix} {self._error!r}")  # fmt: skip
        return self._events or []

    def then_no_error(self) -> DeciderScenario:
        self._ensure_acted("then_no_error", "expected command to succeed, but it was rejected:")
        return self

    def then_no_events(self) -> Self:
        if events := self.then_no_error()._events:
            raise AssertionError(f"expected no events, got {[type(e).__name__ for e in events]}")
        return self

    def then_event(self, event_type: type[DomainEvent], **expected_fields: Any) -> Self:
        self.then_no_error().assertions.assert_event_with_fields(event_type, **expected_fields)
        return self

    def then_state(self, expected: Any, *, aggregate_id: UUID | None = None) -> DeciderScenario:
        actual, is_pred = self.then_no_error().get_state(aggregate_id), callable(expected) and not isinstance(expected, type)  # fmt: skip
        if (is_pred and not expected(actual)) or (not is_pred and actual != expected):
            raise AssertionError(f"state predicate returned False for state {actual!r}" if is_pred else f"expected state {expected!r}, got {actual!r}")  # fmt: skip
        return self

    def then_events(self, *event_types: type[DomainEvent]) -> DeciderScenario:
        exp = [t.__name__ for t in event_types]
        if (actual := [type(e).__name__ for e in self._ensure_acted("then_events", f"expected events {exp} but the command was rejected:")]) != exp:  # fmt: skip
            raise AssertionError(f"expected events {exp}, got {actual}")
        return self

    def then_event_types(self, *expected_types: type[DomainEvent]) -> Self:
        exp = [t.__name__ for t in expected_types]
        if (actual := [type(e) for e in self._ensure_acted("then_event_types", f"Expected event types {exp}, but command raised")]) != list(expected_types):  # fmt: skip
            raise AssertionError(f"Expected event types {exp}, got {[t.__name__ for t in actual]}")
        return self

    def then_rejected(self, exc_type: type[BaseException] = CommandRejectedError, match: str | None = None) -> DeciderScenario:  # fmt: skip
        self._ensure_acted("then_rejected")
        if (err := self._error) is None or not isinstance(err, exc_type) or (match is not None and not re.search(match, str(err))):  # fmt: skip
            raise AssertionError(f"expected rejection but command produced {self.events!r}" if err is None else (f"rejection message {str(err)!r} does not match {match!r}" if isinstance(err, exc_type) else f"expected {exc_type.__name__}, got {type(err).__name__}: {err}"))  # fmt: skip
        return self


Scenario = ScenarioResult = DeciderScenario
__all__ = ["DeciderScenario", "Scenario", "ScenarioResult"]
