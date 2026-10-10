"""
Base classes for event-sourced aggregates.

Aggregates are the consistency boundaries in event sourcing.
They maintain their state by applying events and emit new events
when commands are executed.
"""

from __future__ import annotations

from eventsource.domain.aggregate.aggregate_declarative import DeclarativeAggregate
from eventsource.domain.aggregate.aggregate_root import AggregateRoot
from eventsource.domain.aggregate.provenance import AggregateProvenanceMixin
from eventsource.domain.aggregate.snapshot import AggregateSnapshotMixin
from eventsource.domain.aggregate.types import (
    EventHandler,
    TEvent,
    UnregisteredEventHandling,
)

__all__ = [
    "AggregateProvenanceMixin",
    "AggregateRoot",
    "AggregateSnapshotMixin",
    "DeclarativeAggregate",
    "EventHandler",
    "TEvent",
    "UnregisteredEventHandling",
]
