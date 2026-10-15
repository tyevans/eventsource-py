"""Row mapping helpers for SQLDLQRepository."""

from __future__ import annotations

from typing import Any

from eventsource.adapters._sql.dialect import (
    Dialect,
    json_result,
    ts_result,
    uuid_result,
)
from eventsource.ports.dlq import DLQEntry, DLQStats, ProjectionFailureCount


def row_to_dlq_entry(row: Any, dialect: Dialect) -> DLQEntry:
    """Convert a database result row into a DLQEntry domain model."""
    return DLQEntry(
        id=row[0],
        event_id=uuid_result(row[1]),  # type: ignore[arg-type]
        projection_name=row[2],
        event_type=row[3],
        event_data=json_result(row[4]),
        error_message=row[5],
        error_stacktrace=row[6],
        retry_count=row[7],
        first_failed_at=ts_result(row[8]),
        last_failed_at=ts_result(row[9]),
        status=row[10],
    )


def row_to_dlq_stats(row: Any) -> DLQStats:
    """Convert an aggregate statistics row into DLQStats."""
    oldest_failure = ts_result(row[3]) if row else None
    return DLQStats(
        total_failed=row[0] if row and row[0] else 0,
        total_retrying=row[1] if row and row[1] else 0,
        affected_projections=row[2] if row and row[2] else 0,
        oldest_failure=oldest_failure.isoformat() if oldest_failure else None,
    )


def rows_to_projection_failure_counts(rows: list[Any]) -> list[ProjectionFailureCount]:
    """Convert grouped projection failure rows into ProjectionFailureCount models."""
    counts = []
    for row in rows:
        oldest = ts_result(row[2])
        most_recent = ts_result(row[3])
        counts.append(
            ProjectionFailureCount(
                projection_name=row[0],
                failure_count=row[1],
                oldest_failure=oldest.isoformat() if oldest else None,
                most_recent_failure=most_recent.isoformat() if most_recent else None,
            )
        )
    return counts


__all__ = [
    "row_to_dlq_entry",
    "row_to_dlq_stats",
    "rows_to_projection_failure_counts",
]
