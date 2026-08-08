"""Shared event and stream-id factories for the port conformance suites.

Kept sqlalchemy-free: only `eventsource.domain` and
`eventsource.ports.readmodels` are imported here, so this module stays usable
from the Tier 0 surface.
"""

from uuid import UUID, uuid4

from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry
from eventsource.ports.readmodels import ReadModel


class ConformanceEvent(DomainEvent):
    """Minimal domain event used across all port conformance suites."""

    aggregate_type: str = "Conformance"
    payload: str = "conformance"


def make_stream(category: str = "Conformance", aggregate_id: UUID | None = None) -> StreamId:
    """Build a fresh `StreamId` for a conformance test case."""
    return StreamId(aggregate_id=aggregate_id or uuid4(), category=category)


def make_event(aggregate_id: UUID, payload: str = "conformance") -> ConformanceEvent:
    """Build a `ConformanceEvent` for the given aggregate id."""
    return ConformanceEvent(aggregate_id=aggregate_id, payload=payload)


def make_conformance_registry() -> EventRegistry:
    """Fresh registry with `ConformanceEvent` registered and nothing else.

    `ConformanceEvent` is deliberately absent from `default_registry` --
    registration is explicit in this library, never automatic. Every store
    adapter's conformance fixture builds its store on one of these, because
    every store now resolves event types through a registry: the serializing
    backends on read, the in-memory ones on append.

    `UnregisteredEvent` must never be added here; `AppenderConformance`
    depends on it being absent.
    """
    registry = EventRegistry()
    registry.register(ConformanceEvent)
    return registry


class UnregisteredEvent(DomainEvent):
    """Event class that must NEVER be registered in any registry.

    Used by `AppenderConformance` to pin that every store rejects an event
    whose `event_type` cannot be resolved. Registering this class anywhere
    -- including in `default_registry` via `@register_event` -- silently
    disables that conformance case.
    """

    aggregate_type: str = "Conformance"
    payload: str = "unregistered"


class ConformanceReadModel(ReadModel):
    """Minimal read model used by `ReadModelRepositoryConformance`.

    Three custom fields: one text, one integer, one nullable text -- enough
    to exercise filtering, ordering, and update-visibility without depending
    on any dialect's handling of decimals, JSON, or dates.

    `note` is nullable on purpose. NULL is where the filter dispatch used to
    diverge (Python's `None != "x"` is true; SQL's `NULL != 'x'` is unknown),
    so the operator matrix needs a column that can actually hold one.
    """

    name: str = "conformance"
    count: int = 0
    note: str | None = None
