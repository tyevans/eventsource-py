"""Aggregate use cases: repository and snapshotting collaborators."""

from eventsource.application.aggregates.repository import AggregateRepository, TAggregate
from eventsource.application.aggregates.snapshotting import (
    BackgroundScheduler,
    EveryNEvents,
    ImmediateScheduler,
    Never,
    SnapshotPolicy,
    SnapshotScheduler,
    read_valid_snapshot,
    take_snapshot,
)

__all__ = [
    "AggregateRepository",
    "BackgroundScheduler",
    "EveryNEvents",
    "ImmediateScheduler",
    "Never",
    "SnapshotPolicy",
    "SnapshotScheduler",
    "TAggregate",
    "read_valid_snapshot",
    "take_snapshot",
]
