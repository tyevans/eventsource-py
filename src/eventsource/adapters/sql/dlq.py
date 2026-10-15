"""SQLAlchemy DLQ adapter, serving both PostgreSQL and SQLite."""

import traceback
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters._sql.dialect import (
    dialect_of,
    json_param,
    ts_param,
    ts_result,
    uuid_param,
)
from eventsource.adapters.sql.dlq_helpers import (
    row_to_dlq_entry,
    row_to_dlq_stats,
    rows_to_projection_failure_counts,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_ERROR_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.ports.dlq import DLQEntry, DLQStats, ProjectionFailureCount

_BASE_COLS = (
    "id, event_id, projection_name, event_type, event_data, error_message, "
    "error_stacktrace, retry_count, first_failed_at, last_failed_at, status"
)


class SQLDLQRepository:
    """SQLAlchemy-backed DLQ repository (`dead_letter_queue`) for PostgreSQL and SQLite."""

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """Initialize the DLQ repository with a database connection/engine and optional tracer."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self.conn = conn

    _row_to_entry = staticmethod(row_to_dlq_entry)

    async def add_failed_event(
        self,
        event_id: UUID,
        projection_name: str,
        event_type: str,
        event_data: dict[str, Any],
        error: Exception,
        retry_count: int = 0,
    ) -> None:
        """Add or update a failed event in the DLQ."""
        attrs = {
            ATTR_EVENT_ID: str(event_id),
            ATTR_EVENT_TYPE: event_type,
            ATTR_PROJECTION_NAME: projection_name,
            ATTR_ERROR_TYPE: str(error)[:100],
            ATTR_RETRY_COUNT: retry_count,
        }
        with self._tracer.span("eventsource.dlq.add", attrs):
            now = datetime.now(UTC)
            query = text(
                "INSERT INTO dead_letter_queue (event_id, projection_name, event_type, "
                "event_data, error_message, error_stacktrace, retry_count, first_failed_at, "
                "last_failed_at, status) VALUES (:event_id, :projection_name, :event_type, "
                ":event_data, :error_message, :error_stacktrace, :retry_count, :now, :now, "
                "'failed') ON CONFLICT (event_id, projection_name) DO UPDATE SET "
                "retry_count = EXCLUDED.retry_count, last_failed_at = EXCLUDED.last_failed_at, "
                "error_message = EXCLUDED.error_message, "
                "error_stacktrace = EXCLUDED.error_stacktrace, status = 'failed'"
            )
            async with sql_connection(self.conn, write=True) as conn:
                dialect = dialect_of(conn)
                await conn.execute(
                    query,
                    {
                        "event_id": uuid_param(event_id, dialect),
                        "projection_name": projection_name,
                        "event_type": event_type,
                        "event_data": json_param(event_data, dialect),
                        "error_message": str(error),
                        "error_stacktrace": traceback.format_exc(),
                        "retry_count": retry_count,
                        "now": ts_param(now, dialect),
                    },
                )

    async def get_failed_events(
        self,
        projection_name: str | None = None,
        status: str = "failed",
        limit: int = 100,
    ) -> list[DLQEntry]:
        """Get failed events from the DLQ filtered by optional projection_name and status."""
        span_attrs: dict[str, Any] = {"limit": limit, "status_filter": status}
        if projection_name:
            span_attrs[ATTR_PROJECTION_NAME] = projection_name
        with self._tracer.span("eventsource.dlq.get", span_attrs):
            where_sql = "status = :status"
            params: dict[str, Any] = {"status": status, "limit": limit}
            if projection_name:
                where_sql += " AND projection_name = :projection_name"
                params["projection_name"] = projection_name
            sql = f"SELECT {_BASE_COLS} FROM dead_letter_queue WHERE {where_sql} ORDER BY first_failed_at DESC LIMIT :limit"  # nosec B608
            async with sql_connection(self.conn, write=False) as conn:
                dialect = dialect_of(conn)
                rows = (await conn.execute(text(sql), params)).fetchall()
            return [self._row_to_entry(row, dialect) for row in rows]

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        """Get a specific failed event by its DLQ ID, or None if not found."""
        with self._tracer.span("eventsource.dlq.get_by_id", {"dlq.id": str(dlq_id)}):
            sql = f"SELECT {_BASE_COLS}, resolved_at, resolved_by FROM dead_letter_queue WHERE id = :dlq_id"  # nosec B608
            async with sql_connection(self.conn, write=False) as conn:
                dialect = dialect_of(conn)
                row = (await conn.execute(text(sql), {"dlq_id": dlq_id})).fetchone()
            if not row:
                return None
            entry = self._row_to_entry(row, dialect)
            entry.resolved_at, entry.resolved_by = ts_result(row[11]), row[12]
            return entry

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        """Mark a DLQ entry as resolved."""
        resolved_by_str = str(resolved_by) if resolved_by else None
        attrs = {"dlq.id": str(dlq_id), "resolved_by": resolved_by_str}
        with self._tracer.span("eventsource.dlq.resolve", attrs):
            sql = "UPDATE dead_letter_queue SET status = 'resolved', resolved_at = :now, resolved_by = :resolved_by WHERE id = :dlq_id"
            async with sql_connection(self.conn, write=True) as conn:
                now_param = ts_param(datetime.now(UTC), dialect_of(conn))
                params = {"now": now_param, "resolved_by": resolved_by_str, "dlq_id": dlq_id}
                await conn.execute(text(sql), params)

    async def mark_retrying(self, dlq_id: int | str) -> None:
        """Mark a DLQ entry as being retried."""
        with self._tracer.span("eventsource.dlq.retry", {"dlq.id": str(dlq_id)}):
            query = text("UPDATE dead_letter_queue SET status = 'retrying' WHERE id = :dlq_id")
            async with sql_connection(self.conn, write=True) as conn:
                await conn.execute(query, {"dlq_id": dlq_id})

    async def get_failure_stats(self) -> DLQStats:
        """Get aggregate statistics about DLQ health."""
        with self._tracer.span("eventsource.dlq.get_stats", {}):
            query = text(
                "SELECT SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as total_failed, "
                "SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END) as total_retrying, "
                "COUNT(DISTINCT projection_name) as affected_projections, "
                "MIN(first_failed_at) as oldest_failure "
                "FROM dead_letter_queue WHERE status IN ('failed', 'retrying')"
            )
            async with sql_connection(self.conn, write=False) as conn:
                return row_to_dlq_stats((await conn.execute(query)).fetchone())

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        """Get failure counts grouped by projection."""
        with self._tracer.span("eventsource.dlq.get_projection_counts", {}):
            query = text(
                "SELECT projection_name, COUNT(*) as failure_count, "
                "MIN(first_failed_at) as oldest_failure, "
                "MAX(last_failed_at) as most_recent_failure "
                "FROM dead_letter_queue WHERE status IN ('failed', 'retrying') "
                "GROUP BY projection_name ORDER BY failure_count DESC"
            )
            async with sql_connection(self.conn, write=False) as conn:
                return rows_to_projection_failure_counts(
                    list((await conn.execute(query)).fetchall())
                )

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        """Delete resolved events older than specified days and return deleted count."""
        attrs = {"older_than_days": older_than_days}
        with self._tracer.span("eventsource.dlq.delete_resolved", attrs):
            cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
            sql = "DELETE FROM dead_letter_queue WHERE status = 'resolved' AND resolved_at < :cutoff RETURNING id"
            async with sql_connection(self.conn, write=True) as conn:
                params = {"cutoff": ts_param(cutoff, dialect_of(conn))}
                return len((await conn.execute(text(sql), params)).fetchall())


__all__ = ["SQLDLQRepository"]
