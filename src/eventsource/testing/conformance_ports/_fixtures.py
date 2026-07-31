"""Shared event and stream-id factories for the port conformance suites.

Kept sqlalchemy-free: only `eventsource.domain`, `eventsource.events`, and
`eventsource.ports.readmodels` are imported here, so this module stays usable
from the Tier 0 surface.
"""

from uuid import UUID, uuid4

from eventsource.domain import StreamId
from eventsource.events import DomainEvent
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


class ConformanceReadModel(ReadModel):
    """Minimal read model used by `ReadModelRepositoryConformance`.

    Two custom fields only: one text, one integer -- enough to exercise
    filtering, ordering, and update-visibility without depending on any
    dialect's handling of decimals, JSON, or dates.
    """

    name: str = "conformance"
    count: int = 0
