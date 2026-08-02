"""
DeciderAggregate: the decider pattern as a first-class aggregate style.

The domain is three pure functions — initial_state, decide, evolve — and
this class is the imperative shell that adapts them to the AggregateRoot
machinery (repositories, snapshots, replay). See ADR-0022 and
docs/explanation/decider-pattern.md.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent


class DeciderAggregate[TState: BaseModel, TCommand = object](AggregateRoot[TState]):
    """
    Aggregate style built from pure ``decide``/``evolve`` functions.

    Subclasses implement three static methods and set ``aggregate_type``;
    everything else — replay, snapshots, version validation, repository
    integration — is inherited from AggregateRoot.

    Unlike AggregateRoot, state is eagerly initialized: ``state`` is never
    None on this class, so ``decide`` always has a real state to match on.

    Contract note: ``DomainEvent.aggregate_id`` is a required field, so
    ``decide`` must set it. ``initial_state(aggregate_id)`` receives the id
    precisely so state carries the aggregate's identity for decide to use.

    Subscript with two type parameters — ``DeciderAggregate[MyState,
    MyCommandUnion]`` — to get mypy exhaustiveness checking on a userland
    command union in ``decide``/``execute``. Subscripting with one parameter
    — ``DeciderAggregate[MyState]`` — leaves ``TCommand`` defaulted to
    ``object``, preserving the structural typing ADR-0022 established (no
    base command class required).
    """

    def __init__(self, aggregate_id: UUID) -> None:
        super().__init__(aggregate_id)
        self._state = self.initial_state(aggregate_id)

    @staticmethod
    @abstractmethod
    def initial_state(aggregate_id: UUID) -> TState:
        """Return the state of an aggregate before any event has occurred."""

    @staticmethod
    @abstractmethod
    def decide(command: TCommand, state: TState) -> list[DomainEvent]:
        """Given current state, return the events a command produces, or raise."""

    @staticmethod
    @abstractmethod
    def evolve(state: TState, event: DomainEvent) -> TState:
        """Return the next state after an event. Should be total (case _: return state)."""

    @property
    def state(self) -> TState:
        """Current state. Never None: eagerly initialized from initial_state()."""
        if self._state is None:
            raise RuntimeError(
                f"{type(self).__name__} has no state: initial_state() must "
                f"return a non-None state (established in __init__, "
                f"maintained by _apply)."
            )
        return self._state

    def _get_initial_state(self) -> TState:
        return self.initial_state(self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        base = self._state if self._state is not None else self.initial_state(self.aggregate_id)
        self._state = self.evolve(base, event)

    def execute(self, command: TCommand) -> list[DomainEvent]:
        """
        Run decide(), stamp each produced event, and apply it.

        decide() completes before any event is applied, so a rejection
        leaves the aggregate fully untouched. Returns the stamped events.

        Stamping (one model_copy per event; fields decide() set explicitly
        are never overwritten — detected via model_fields_set):
        - always: aggregate_version, aggregate_type
        - when command is a DomainCommand: causation_id (command_id),
          correlation_id, actor_id
        - tenant_id: command value if DomainCommand, else ambient tenant
          context, else untouched — for every command type
        """
        events = self.decide(command, self.state)
        applied: list[DomainEvent] = []
        for event in events:
            stamped = self._stamp(event, command)
            self.apply_event(stamped, is_new=True)
            applied.append(stamped)
        return applied

    def _stamp(self, event: DomainEvent, command: object) -> DomainEvent:
        fields_set = event.model_fields_set
        updates: dict[str, Any] = {}
        if "aggregate_version" not in fields_set:
            updates["aggregate_version"] = self.get_next_version()
        if "aggregate_type" not in fields_set:
            updates["aggregate_type"] = self.aggregate_type
        updates.update(self._provenance_updates(command, fields_set))
        if not updates:
            return event
        return event.model_copy(update=updates)


__all__ = ["DeciderAggregate"]
