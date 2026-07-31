"""Aggregate pattern implementations for the eventsource library."""

from eventsource.aggregates.repository import (
    AggregateRepository,
    TAggregate,
)
from eventsource.domain.aggregate import (
    AggregateRoot,
    DeclarativeAggregate,
)
from eventsource.types import TState

__all__ = [
    "AggregateRoot",
    "AggregateRepository",
    "DeclarativeAggregate",
    "TAggregate",
    "TState",
]
