"""Aggregate use cases: repository and snapshotting collaborators."""

from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.application.aggregates.snapshotting import (
    BackgroundScheduler,
    EveryNEvents,
    ImmediateScheduler,
    Never,
    SnapshotMissReason,
    SnapshotPolicy,
    SnapshotScheduler,
    read_valid_snapshot,
    record_snapshot_miss,
    reset_snapshot_miss_counts,
    snapshot_miss_counts,
    take_snapshot,
)

__all__ = [
    "AggregateRepository",
    "BackgroundScheduler",
    "EveryNEvents",
    "ImmediateScheduler",
    "Never",
    "SnapshotMissReason",
    "SnapshotPolicy",
    "SnapshotScheduler",
    "read_valid_snapshot",
    "record_snapshot_miss",
    "reset_snapshot_miss_counts",
    "snapshot_miss_counts",
    "take_snapshot",
]
