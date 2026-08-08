"""Append-time registry validation for stores that never serialize.

The SQL-family adapters round-trip events through JSON: on read they call
`EventRegistry.get(event_type)` and raise `EventTypeNotFoundError` when the
class was never registered. The in-memory stores hold live `DomainEvent`
objects, so a missing `@register_event` is invisible to them -- a test that
passes against the memory store then fails on the first read against SQLite
or PostgreSQL.

`check_registered` closes that gap by validating at append time, so the
in-memory stores fail where the SQL stores would. Detection is earlier (write
rather than read) but the exception type is the same, which is what the
`AppenderConformance` case pins.
"""

from collections.abc import Sequence

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry


def check_registered(events: Sequence[DomainEvent], registry: EventRegistry) -> None:
    """Raise `EventTypeNotFoundError` if any event's type is unregistered.

    Args:
        events: The batch about to be appended.
        registry: The registry the store would deserialize through if it
            serialized at all.

    Raises:
        EventTypeNotFoundError: If any event's `event_type` is absent from
            `registry`.
    """
    for event in events:
        registry.get(event.event_type)


__all__ = ["check_registered"]
