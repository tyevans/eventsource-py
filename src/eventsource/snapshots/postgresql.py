"""
TRANSITION: PostgreSQL snapshot store moved to adapters.postgresql.snapshots.

This module re-exports from the new location for backward compatibility.
Imports should migrate to eventsource.adapters.postgresql.snapshots over time.
"""

from eventsource.adapters.postgresql.snapshots import PostgreSQLSnapshotStore

__all__ = ["PostgreSQLSnapshotStore"]
