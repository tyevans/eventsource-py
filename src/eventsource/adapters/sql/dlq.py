"""SQLAlchemy DLQ adapter, serving both PostgreSQL and SQLite."""

from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._common.dlq_tenant import refuse_cleanup_under_scope
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
from eventsource.adapters.sql.dlq_sql import (
    BASE_COLS,
    counts_sql,
    owner_column,
    scope_clause,
    stats_sql,
    upsert_sql,
)
from eventsource.adapters.sql.dlq_tenant_column import DLQTenantColumn
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_ERROR_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
)
from eventsource.observability.error_summary import error_summary
from eventsource.observability.stack_trace import frames_only_trace
from eventsource.ports.dlq import DLQEntry, DLQStats, ProjectionFailureCount


class SQLDLQRepository:
    """SQLAlchemy-backed DLQ repository (`dead_letter_queue`) for PostgreSQL and SQLite."""

    def __init__(self, conn: AsyncConnection | AsyncEngine, tracer: Tracer | None = None, enable_tracing: bool = True):  # fmt: skip
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing, self.conn, self._owner = self._tracer.enabled, conn, DLQTenantColumn()

    async def add_failed_event(self, event_id: UUID, projection_name: str, event_type: str, event_data: dict[str, Any], error: Exception, retry_count: int = 0, *, tenant_id: UUID | None = None) -> None:  # fmt: skip
        has_owner = await self._owner.ensure(self.conn)
        attrs = {
            ATTR_EVENT_ID: str(event_id), ATTR_EVENT_TYPE: event_type,
            ATTR_PROJECTION_NAME: projection_name, ATTR_ERROR_TYPE: error_summary(error)[:100],
            ATTR_RETRY_COUNT: retry_count,
        }  # fmt: skip
        with self._tracer.span("eventsource.dlq.add", attrs):
            async with sql_connection(self.conn, write=True) as conn:
                d = dialect_of(conn)
                params: dict[str, Any] = {
                    "event_id": uuid_param(event_id, d),
                    "projection_name": projection_name,
                    "event_type": event_type,
                    "event_data": json_param(event_data, d),
                    "error_message": error_summary(error),
                    "error_stacktrace": frames_only_trace(),
                    "retry_count": retry_count,
                    "now": ts_param(datetime.now(UTC), d),
                }
                if has_owner:
                    params["tenant_id"] = uuid_param(tenant_id, d)
                await conn.execute(text(upsert_sql(has_owner)), params)

    async def get_failed_events(
        self, projection_name: str | None = None, status: str = "failed", limit: int = 100
    ) -> list[DLQEntry]:
        has_owner = await self._owner.ensure(self.conn)
        span_attrs: dict[str, Any] = {"limit": limit, "status_filter": status}
        if projection_name:
            span_attrs[ATTR_PROJECTION_NAME] = projection_name
        with self._tracer.span("eventsource.dlq.get", span_attrs):
            where_sql = "status = :status"
            params: dict[str, Any] = {"status": status, "limit": limit}
            if projection_name:
                where_sql += " AND projection_name = :projection_name"
                params["projection_name"] = projection_name
            async with sql_connection(self.conn, write=False) as conn:
                dialect = dialect_of(conn)
                scope_sql, scope_params = scope_clause(dialect, has_owner)
                sql = f"SELECT {BASE_COLS}, {owner_column(has_owner)} FROM dead_letter_queue WHERE {where_sql}{scope_sql} ORDER BY first_failed_at DESC LIMIT :limit"  # nosec B608
                rows = (await conn.execute(text(sql), {**params, **scope_params})).fetchall()
            return [row_to_dlq_entry(row, dialect) for row in rows]

    async def get_failed_event_by_id(self, dlq_id: int | str) -> DLQEntry | None:
        has_owner = await self._owner.ensure(self.conn)
        with self._tracer.span("eventsource.dlq.get_by_id", {"dlq.id": str(dlq_id)}):
            async with sql_connection(self.conn, write=False) as conn:
                dialect = dialect_of(conn)
                scope_sql, scope_params = scope_clause(dialect, has_owner)
                sql = f"SELECT {BASE_COLS}, {owner_column(has_owner)}, resolved_at, resolved_by FROM dead_letter_queue WHERE id = :dlq_id{scope_sql}"  # nosec B608
                row = (await conn.execute(text(sql), {"dlq_id": dlq_id, **scope_params})).fetchone()
            if not row:
                return None
            entry = row_to_dlq_entry(row, dialect)
            entry.resolved_at, entry.resolved_by = ts_result(row[12]), row[13]
            return entry

    async def mark_resolved(self, dlq_id: int | str, resolved_by: str | UUID) -> None:
        has_owner = await self._owner.ensure(self.conn)
        resolved_by_str = str(resolved_by) if resolved_by else None
        attrs = {"dlq.id": str(dlq_id), "resolved_by": resolved_by_str}
        with self._tracer.span("eventsource.dlq.resolve", attrs):
            async with sql_connection(self.conn, write=True) as conn:
                d = dialect_of(conn)
                scope_sql, scope_params = scope_clause(d, has_owner)
                sql = f"UPDATE dead_letter_queue SET status = 'resolved', resolved_at = :now, resolved_by = :resolved_by WHERE id = :dlq_id{scope_sql}"  # nosec B608
                params = {"now": ts_param(datetime.now(UTC), d), "resolved_by": resolved_by_str, "dlq_id": dlq_id}  # fmt: skip
                await conn.execute(text(sql), {**params, **scope_params})

    async def mark_retrying(self, dlq_id: int | str) -> None:
        has_owner = await self._owner.ensure(self.conn)
        with self._tracer.span("eventsource.dlq.retry", {"dlq.id": str(dlq_id)}):
            async with sql_connection(self.conn, write=True) as conn:
                scope_sql, scope_params = scope_clause(dialect_of(conn), has_owner)
                sql = f"UPDATE dead_letter_queue SET status = 'retrying' WHERE id = :dlq_id{scope_sql}"  # nosec B608
                await conn.execute(text(sql), {"dlq_id": dlq_id, **scope_params})

    async def get_failure_stats(self) -> DLQStats:
        has_owner = await self._owner.ensure(self.conn)
        with self._tracer.span("eventsource.dlq.get_stats", {}):
            async with sql_connection(self.conn, write=False) as conn:
                scope_sql, scope_params = scope_clause(dialect_of(conn), has_owner)
                row = (await conn.execute(stats_sql(scope_sql), scope_params)).fetchone()
                return row_to_dlq_stats(row)

    async def get_projection_failure_counts(self) -> list[ProjectionFailureCount]:
        has_owner = await self._owner.ensure(self.conn)
        with self._tracer.span("eventsource.dlq.get_projection_counts", {}):
            async with sql_connection(self.conn, write=False) as conn:
                scope_sql, scope_params = scope_clause(dialect_of(conn), has_owner)
                rows = list((await conn.execute(counts_sql(scope_sql), scope_params)).fetchall())
                return rows_to_projection_failure_counts(rows)

    async def delete_resolved_events(self, older_than_days: int = 30) -> int:
        refuse_cleanup_under_scope()
        attrs = {"older_than_days": older_than_days}
        with self._tracer.span("eventsource.dlq.delete_resolved", attrs):
            cutoff = datetime.now(UTC) - timedelta(days=older_than_days)
            sql = "DELETE FROM dead_letter_queue WHERE status = 'resolved' AND resolved_at < :cutoff RETURNING id"
            async with sql_connection(self.conn, write=True) as conn:
                params = {"cutoff": ts_param(cutoff, dialect_of(conn))}
                return len((await conn.execute(text(sql), params)).fetchall())


__all__ = ["SQLDLQRepository"]
