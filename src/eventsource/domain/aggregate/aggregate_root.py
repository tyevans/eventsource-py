"""Base class for event-sourced aggregate roots."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, ClassVar
from uuid import UUID

from pydantic import BaseModel

from eventsource.domain.aggregate.provenance import AggregateProvenanceMixin
from eventsource.domain.aggregate.snapshot import AggregateSnapshotMixin
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import AggregateTypeNotSetError, EventVersionError

if TYPE_CHECKING:
    from collections.abc import Iterable

logger = logging.getLogger(__name__)


class AggregateRoot[TState: BaseModel](
    AggregateSnapshotMixin[TState], AggregateProvenanceMixin, ABC
):
    """Base class for event-sourced aggregate roots."""

    # Required stream category; construction raises AggregateTypeNotSetError if unset.
    aggregate_type: ClassVar[str]
    schema_version: int = 1
    validate_versions: bool = True

    def __init__(self, aggregate_id: UUID) -> None:
        """Initialize aggregate root."""
        if not getattr(type(self), "aggregate_type", None):
            raise AggregateTypeNotSetError(type(self).__name__)
        self._aggregate_id = aggregate_id
        self._version = 0
        self._uncommitted_events: list[DomainEvent] = []
        self._state: TState | None = None

    @property
    def aggregate_id(self) -> UUID:
        """Get the unique identifier for this aggregate."""
        return self._aggregate_id

    @property
    def version(self) -> int:
        """Get the current version (number of events applied)."""
        return self._version

    @property
    def state(self) -> TState | None:
        """Get the current state of the aggregate (None before any events are applied)."""
        return self._state

    @property
    def uncommitted_events(self) -> list[DomainEvent]:
        """Get a copy of events that haven't been persisted yet."""
        return self._uncommitted_events.copy()

    @property
    def has_uncommitted_events(self) -> bool:
        """Check if there are events waiting to be persisted."""
        return len(self._uncommitted_events) > 0

    def apply_event(self, event: DomainEvent, is_new: bool = True) -> None:
        """Apply an event to the aggregate, validating version and tracking when `is_new=True`."""
        if is_new:
            self._reject_foreign_aggregate_id(event, None)
            expected = self._version + 1
            if event.aggregate_version != expected:
                if self.validate_versions:
                    raise EventVersionError(
                        expected, event.aggregate_version, event.event_id, self._aggregate_id
                    )
                logger.warning(
                    "Version mismatch (validation disabled): expected %d, got %d "
                    "for aggregate %s, event %s",
                    expected,
                    event.aggregate_version,
                    self._aggregate_id,
                    event.event_id,
                    extra={
                        "aggregate_id": str(self._aggregate_id),
                        "expected_version": expected,
                        "actual_version": event.aggregate_version,
                        "event_id": str(event.event_id),
                    },
                )
        self._version = event.aggregate_version
        self._apply(event)
        if is_new:
            self._uncommitted_events.append(event)

    def apply_events(self, *events: DomainEvent, is_new: bool = True) -> None:
        """Apply multiple events sequentially to the aggregate."""
        for event in events:
            self.apply_event(event, is_new=is_new)

    @abstractmethod
    def _apply(self, event: DomainEvent) -> None:
        """Apply event to update aggregate state."""

    @abstractmethod
    def _get_initial_state(self) -> TState | None:
        """Get the initial state for a new aggregate."""

    def mark_events_as_committed(self) -> None:
        """Mark all uncommitted events as committed."""
        self._uncommitted_events.clear()

    def load_from_history(self, events: Iterable[DomainEvent]) -> None:
        """Reconstitute aggregate state from chronological event history."""
        for event in events:
            self.apply_event(event, is_new=False)

    def get_next_version(self) -> int:
        """Get the version number for the next event (`version + 1`)."""
        return self._version + 1

    def clear_uncommitted_events(self) -> list[DomainEvent]:
        """Clear and return all uncommitted events."""
        events = self._uncommitted_events.copy()
        self._uncommitted_events.clear()
        return events

    def _raise_event(self, event: DomainEvent) -> None:
        """Alias for `apply_event(event, is_new=True)`."""
        self.apply_event(event, is_new=True)

    def __repr__(self) -> str:
        """String representation of aggregate."""
        return (
            f"{self.__class__.__name__}(id={self._aggregate_id}, "
            f"version={self._version}, uncommitted={len(self._uncommitted_events)})"
        )

    def __eq__(self, other: object) -> bool:
        """Check equality based on aggregate ID."""
        return (
            self._aggregate_id == other._aggregate_id
            if isinstance(other, AggregateRoot)
            else NotImplemented
        )

    def __hash__(self) -> int:
        """Hash based on aggregate ID."""
        return hash(self._aggregate_id)


__all__ = ["AggregateRoot"]
