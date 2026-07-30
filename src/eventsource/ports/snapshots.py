# TRANSITION: alias module. The snapshot contract is unchanged by the ports
# spec; this re-exports the existing implementation from
# eventsource.snapshots.interface until sub-project 3 physically relocates
# it into this package.
"""Snapshot port re-exports."""

from eventsource.snapshots.interface import Snapshot, SnapshotStore

__all__ = ["Snapshot", "SnapshotStore"]
