"""SQL text builders for `SQLDLQRepository` (ADR-0114).

The scope predicate is the SQL spelling of `visible_under_scope()` in
`adapters/_common/dlq_tenant.py`: equality with the active tenant. A NULL owner
never equals a tenant, so an untenanted entry stays invisible under a scope.
Without the owner column no entry has an owner, so the predicate is false.
"""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from eventsource.adapters._sql.dialect import Dialect, uuid_param
from eventsource.domain.tenant_context import get_current_tenant

BASE_COLS = (
    "id, event_id, projection_name, event_type, event_data, error_message, "
    "error_stacktrace, retry_count, first_failed_at, last_failed_at, status"
)
_UPSERT_COLS = (
    "event_id, projection_name, event_type, event_data, error_message, error_stacktrace, "
    "retry_count, first_failed_at, last_failed_at, status"
)
_UPSERT_VALUES = (
    ":event_id, :projection_name, :event_type, :event_data, :error_message, "
    ":error_stacktrace, :retry_count, :now, :now, 'failed'"
)
_UPSERT_UPDATE = (
    "retry_count = EXCLUDED.retry_count, last_failed_at = EXCLUDED.last_failed_at, "
    "error_message = EXCLUDED.error_message, error_stacktrace = EXCLUDED.error_stacktrace, "
    "status = 'failed'"
)
_STATS_BASE = (
    "SELECT SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as total_failed, "
    "SUM(CASE WHEN status = 'retrying' THEN 1 ELSE 0 END) as total_retrying, "
    "COUNT(DISTINCT projection_name) as affected_projections, MIN(first_failed_at) as oldest_failure "
    "FROM dead_letter_queue WHERE status IN ('failed', 'retrying')"
)
_COUNTS_BASE = (
    "SELECT projection_name, COUNT(*) as failure_count, MIN(first_failed_at) as oldest_failure, "
    "MAX(last_failed_at) as most_recent_failure FROM dead_letter_queue "
    "WHERE status IN ('failed', 'retrying')"
)


def owner_column(has_column: bool) -> str:
    """Select list entry for the owner: the column, or NULL when it is absent."""
    return "tenant_id" if has_column else "NULL AS tenant_id"


# SQL spelling of visible_under_scope() in adapters/_common/dlq_tenant.py; keep them in step.
def scope_clause(dialect: Dialect, has_column: bool) -> tuple[str, dict[str, Any]]:
    """Return the SQL predicate and bind parameters for the active tenant scope.

    Returns an empty predicate when no scope is active.
    """
    active = get_current_tenant()
    if active is None:
        return "", {}
    if not has_column:
        return " AND 1 = 0", {}
    return " AND tenant_id = :active_tenant", {"active_tenant": uuid_param(active, dialect)}


def upsert_sql(has_column: bool) -> str:
    """Build the insert-or-refresh statement. The owner is set once and never replaced."""
    cols, values, update = _UPSERT_COLS, _UPSERT_VALUES, _UPSERT_UPDATE
    if has_column:
        cols += ", tenant_id"
        values += ", :tenant_id"
        update += ", tenant_id = COALESCE(dead_letter_queue.tenant_id, EXCLUDED.tenant_id)"
    # Every fragment is a module constant; no caller value reaches this text.
    return (
        f"INSERT INTO dead_letter_queue ({cols}) VALUES ({values}) "  # nosec B608
        f"ON CONFLICT (event_id, projection_name) DO UPDATE SET {update}"
    )


def stats_sql(scope_sql: str) -> Any:
    """Aggregate statistics statement, narrowed by the scope predicate."""
    return text(_STATS_BASE + scope_sql)


def counts_sql(scope_sql: str) -> Any:
    """Per-projection failure count statement, narrowed by the scope predicate."""
    return text(_COUNTS_BASE + scope_sql + " GROUP BY projection_name ORDER BY failure_count DESC")


__all__ = ["BASE_COLS", "counts_sql", "owner_column", "scope_clause", "stats_sql", "upsert_sql"]
