"""Aggregate pattern implementations for the eventsource library."""

from eventsource.aggregates.base import (
    AggregateRoot,
    DeclarativeAggregate,
    handles,
)
from eventsource.aggregates.repository import (
    AggregateRepository,
    TAggregate,
)
from eventsource.types import TState

__all__ = [
    "AggregateRoot",
    "AggregateRepository",
    "DeclarativeAggregate",
    "handles",
    "TAggregate",
    "TState",
]
