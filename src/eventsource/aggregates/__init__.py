"""Aggregate pattern implementations for the eventsource library."""

from eventsource.aggregates.base import (
    AggregateRoot,
    DeclarativeAggregate,
)
from eventsource.aggregates.decider import DeciderAggregate
from eventsource.aggregates.repository import (
    AggregateRepository,
    TAggregate,
)
from eventsource.types import TState

__all__ = [
    "AggregateRoot",
    "AggregateRepository",
    "DeciderAggregate",
    "DeclarativeAggregate",
    "TAggregate",
    "TState",
]
