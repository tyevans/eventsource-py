"""MigrationAuditLogRepository - Data access for migration audit logs."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters.serialization import json_dumps
from eventsource.adapters.sql.migration.audit_log_helpers import row_to_audit_entry
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_DB_SYSTEM
from eventsource.ports.migration.models import AuditEventType, MigrationAuditEntry

_SELECT_COLS = (
    "SELECT id, migration_id, event_type, old_phase, new_phase, details, operator, occurred_at "
    "FROM migration_audit_log"
)


class PostgreSQLMigrationAuditLogRepository:
    """PostgreSQL implementation of MigrationAuditLogRepository."""

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn

    async def record(self, entry: MigrationAuditEntry) -> int:
        """Record an immutable audit log entry and return its generated ID."""
        attrs = {
            "migration.id": str(entry.migration_id),
            "audit.event_type": entry.event_type.value,
            ATTR_DB_SYSTEM: "postgresql",
        }
        with self._tracer.span("eventsource.audit_log_repo.record", attrs):
            query = text(
                "INSERT INTO migration_audit_log ("
                "migration_id, event_type, old_phase, new_phase, details, operator, occurred_at"
                ") VALUES ("
                ":migration_id, :event_type, :old_phase, :new_phase, :details, :operator, :occurred_at"
                ") RETURNING id"
            )
            params = {
                "migration_id": entry.migration_id,
                "event_type": entry.event_type.value,
                "old_phase": entry.old_phase.value if entry.old_phase else None,
                "new_phase": entry.new_phase.value if entry.new_phase else None,
                "details": json_dumps(entry.details) if entry.details else None,
                "operator": entry.operator,
                "occurred_at": entry.occurred_at,
            }
            async with sql_connection(self._conn, write=True) as conn:
                row = (await conn.execute(query, params)).fetchone()
            if row is None:
                raise RuntimeError("Failed to record audit entry - no row returned")
            return int(row[0])

    async def get_by_migration(
        self,
        migration_id: UUID,
        event_types: list[AuditEventType] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[MigrationAuditEntry]:
        """Get audit entries for a migration ordered by occurred_at ascending."""
        attrs = {"migration.id": str(migration_id), ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.audit_log_repo.get_by_migration", attrs):
            conditions = ["migration_id = :migration_id"]
            params: dict[str, Any] = {"migration_id": migration_id}
            if event_types:
                conditions.append("event_type = ANY(:event_types)")
                params["event_types"] = [et.value for et in event_types]
            if since:
                conditions.append("occurred_at >= :since")
                params["since"] = since
            if until:
                conditions.append("occurred_at <= :until")
                params["until"] = until

            where_clause = " AND ".join(conditions)
            limit_clause = f"LIMIT {limit}" if limit else ""
            sql = f"{_SELECT_COLS} WHERE {where_clause} ORDER BY occurred_at ASC {limit_clause}"  # nosec B608
            async with sql_connection(self._conn, write=False) as conn:
                rows = (await conn.execute(text(sql), params)).fetchall()
            return [self._row_to_entry(row) for row in rows]

    async def get_by_id(self, entry_id: int) -> MigrationAuditEntry | None:
        """Get an audit entry by ID, or None if not found."""
        attrs = {"audit.entry_id": entry_id, ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.audit_log_repo.get_by_id", attrs):
            query = text(f"{_SELECT_COLS} WHERE id = :id")  # nosec B608
            async with sql_connection(self._conn, write=False) as conn:
                row = (await conn.execute(query, {"id": entry_id})).fetchone()
            return None if row is None else self._row_to_entry(row)

    async def get_latest(
        self, migration_id: UUID, event_type: AuditEventType | None = None
    ) -> MigrationAuditEntry | None:
        """Get the most recent audit entry for a migration, or None if none exist."""
        attrs = {"migration.id": str(migration_id), ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.audit_log_repo.get_latest", attrs):
            params: dict[str, Any] = {"migration_id": migration_id}
            where = "WHERE migration_id = :migration_id"
            if event_type:
                where += " AND event_type = :event_type"
                params["event_type"] = event_type.value
            query = text(f"{_SELECT_COLS} {where} ORDER BY occurred_at DESC LIMIT 1")  # nosec B608
            async with sql_connection(self._conn, write=False) as conn:
                row = (await conn.execute(query, params)).fetchone()
            return None if row is None else self._row_to_entry(row)

    async def count_by_migration(
        self, migration_id: UUID, event_type: AuditEventType | None = None
    ) -> int:
        """Count audit entries for a migration, optionally filtered by event type."""
        attrs = {"migration.id": str(migration_id), ATTR_DB_SYSTEM: "postgresql"}
        with self._tracer.span("eventsource.audit_log_repo.count_by_migration", attrs):
            params: dict[str, Any] = {"migration_id": migration_id}
            where = "WHERE migration_id = :migration_id"
            if event_type:
                where += " AND event_type = :event_type"
                params["event_type"] = event_type.value
            query = text(f"SELECT COUNT(*) FROM migration_audit_log {where}")  # nosec B608
            async with sql_connection(self._conn, write=False) as conn:
                row = (await conn.execute(query, params)).fetchone()
            return 0 if row is None else int(row[0])

    def _row_to_entry(self, row: Sequence[Any]) -> MigrationAuditEntry:
        return row_to_audit_entry(row)


__all__ = ["PostgreSQLMigrationAuditLogRepository"]
