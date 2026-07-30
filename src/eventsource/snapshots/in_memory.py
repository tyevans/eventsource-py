"""
TRANSITION: In-memory snapshot store moved to adapters.memory.snapshots.

This module re-exports from the new location for backward compatibility.
Imports should migrate to eventsource.adapters.memory.snapshots over time.
"""

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore

__all__ = ["InMemorySnapshotStore"]
