"""
MigrationAuditLogRepository - Data access for migration audit logs.

The MigrationAuditLogRepository provides append-only operations for
recording migration lifecycle events for compliance and debugging purposes.

This module implements the MigrationAuditLogRepository protocol and provides
a PostgreSQL implementation for production use.

Responsibilities:
    - Record migration lifecycle events (start, phase changes, completion)
    - Record operator actions (pause, resume, abort)
    - Record errors and cutover events
    - Query audit history by migration ID
    - Support filtering by event type and time range

Database Table:
    Uses the `migration_audit_log` table defined in PREREQ-002.

Usage:
    >>> from eventsource.migration.repositories import (
    ...     MigrationAuditLogRepository,
    ...     PostgreSQLMigrationAuditLogRepository,
    ... )
    >>>
    >>> repo = PostgreSQLMigrationAuditLogRepository(conn)
    >>>
    >>> # Record an audit event
    >>> entry = MigrationAuditEntry.phase_change(
    ...     migration_id=migration_id,
    ...     old_phase=MigrationPhase.BULK_COPY,
    ...     new_phase=MigrationPhase.DUAL_WRITE,
    ...     occurred_at=datetime.now(UTC),
    ...     operator="admin@example.com",
    ... )
    >>> entry_id = await repo.record(entry)
    >>>
    >>> # Query audit history
    >>> entries = await repo.get_by_migration(migration_id)

See Also:
    - Task: P4-001-audit-log.md
    - Schema: PREREQ-002-migration-schema.md
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationPhase,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_DB_SYSTEM
from eventsource.repositories._connection import execute_with_connection


@runtime_checkable
class MigrationAuditLogRepository(Protocol):
    """
    Protocol for migration audit log persistence.

    Provides append-only operations for recording audit events and
    query operations for compliance reporting and debugging.

    Implementations must ensure:
    - Audit entries are immutable once written
    - Timestamps are accurate and use UTC
    - All required fields are properly validated
    """

    async def record(self, entry: MigrationAuditEntry) -> int:
        """
        Record an audit log entry.

        Args:
            entry: The audit entry to record (id field will be ignored)

        Returns:
            The generated ID for the audit entry
        """
        ...

    async def get_by_migration(
        self,
        migration_id: UUID,
        event_types: list[AuditEventType] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[MigrationAuditEntry]:
        """
        Get audit entries for a migration.

        Args:
            migration_id: The migration ID to query
            event_types: Optional filter by event types
            since: Optional filter for entries after this time
            until: Optional filter for entries before this time
            limit: Optional maximum number of entries to return

        Returns:
            List of audit entries, ordered by occurred_at ascending
        """
        ...

    async def get_by_id(self, entry_id: int) -> MigrationAuditEntry | None:
        """
        Get an audit entry by ID.

        Args:
            entry_id: The audit entry ID

        Returns:
            The audit entry or None if not found
        """
        ...

    async def get_latest(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> MigrationAuditEntry | None:
        """
        Get the most recent audit entry for a migration.

        Args:
            migration_id: The migration ID to query
            event_type: Optional filter by event type

        Returns:
            The most recent audit entry or None if none exist
        """
        ...

    async def count_by_migration(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> int:
        """
        Count audit entries for a migration.

        Args:
            migration_id: The migration ID to query
            event_type: Optional filter by event type

        Returns:
            Number of matching audit entries
        """
        ...


class PostgreSQLMigrationAuditLogRepository:
    """
    PostgreSQL implementation of MigrationAuditLogRepository.

    Persists audit entries to the `migration_audit_log` table.
    Provides append-only write operations and flexible query
    capabilities with full observability through OpenTelemetry tracing.

    Example:
        >>> async with engine.begin() as conn:
        ...     repo = PostgreSQLMigrationAuditLogRepository(conn)
        ...     entry = MigrationAuditEntry.migration_started(
        ...         migration_id=migration_id,
        ...         occurred_at=datetime.now(UTC),
        ...         operator="system",
        ...         details={"config": config.to_dict()},
        ...     )
        ...     entry_id = await repo.record(entry)
        ...
        >>> # Query audit history
        >>> entries = await repo.get_by_migration(migration_id)
        >>> for e in entries:
        ...     print(f"{e.occurred_at}: {e.event_type.value}")
    """

    def __init__(
        self,
        conn: AsyncConnection | AsyncEngine,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ):
        """
        Initialize the repository.

        Args:
            conn: Database connection or engine
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._conn = conn

    async def record(self, entry: MigrationAuditEntry) -> int:
        """
        Record an audit log entry.

        Creates a new immutable audit entry in the database. The entry's
        id field is ignored and a new ID is generated.

        Args:
            entry: The audit entry to record

        Returns:
            The generated ID for the audit entry
        """
        with self._tracer.span(
            "eventsource.audit_log_repo.record",
            {
                "migration.id": str(entry.migration_id),
                "audit.event_type": entry.event_type.value,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                INSERT INTO migration_audit_log (
                    migration_id, event_type, old_phase, new_phase,
                    details, operator, occurred_at
                ) VALUES (
                    :migration_id, :event_type, :old_phase, :new_phase,
                    :details, :operator, :occurred_at
                )
                RETURNING id
            """)

            params = {
                "migration_id": entry.migration_id,
                "event_type": entry.event_type.value,
                "old_phase": entry.old_phase.value if entry.old_phase else None,
                "new_phase": entry.new_phase.value if entry.new_phase else None,
                "details": json.dumps(entry.details) if entry.details else None,
                "operator": entry.operator,
                "occurred_at": entry.occurred_at,
            }

            async with execute_with_connection(self._conn, transactional=True) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()

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
        """
        Get audit entries for a migration.

        Queries the audit log with optional filters for event types and
        time ranges. Results are ordered by occurred_at ascending.

        Args:
            migration_id: The migration ID to query
            event_types: Optional filter by event types
            since: Optional filter for entries after this time
            until: Optional filter for entries before this time
            limit: Optional maximum number of entries to return

        Returns:
            List of audit entries, ordered by occurred_at ascending
        """
        with self._tracer.span(
            "eventsource.audit_log_repo.get_by_migration",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            # Build dynamic query with filters
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

            # where_clause built from hardcoded conditions; limit_clause is an integer
            query = text(f"""
                SELECT
                    id, migration_id, event_type, old_phase, new_phase,
                    details, operator, occurred_at
                FROM migration_audit_log
                WHERE {where_clause}
                ORDER BY occurred_at ASC
                {limit_clause}
            """)  # nosec B608 - no user input in SQL construction

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, params)
                rows = result.fetchall()

            return [self._row_to_entry(row) for row in rows]

    async def get_by_id(self, entry_id: int) -> MigrationAuditEntry | None:
        """
        Get an audit entry by ID.

        Args:
            entry_id: The audit entry ID

        Returns:
            The audit entry or None if not found
        """
        with self._tracer.span(
            "eventsource.audit_log_repo.get_by_id",
            {
                "audit.entry_id": entry_id,
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            query = text("""
                SELECT
                    id, migration_id, event_type, old_phase, new_phase,
                    details, operator, occurred_at
                FROM migration_audit_log
                WHERE id = :id
            """)

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, {"id": entry_id})
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_entry(row)

    async def get_latest(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> MigrationAuditEntry | None:
        """
        Get the most recent audit entry for a migration.

        Useful for finding the last recorded state or event for a migration.

        Args:
            migration_id: The migration ID to query
            event_type: Optional filter by event type

        Returns:
            The most recent audit entry or None if none exist
        """
        with self._tracer.span(
            "eventsource.audit_log_repo.get_latest",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            if event_type:
                query = text("""
                    SELECT
                        id, migration_id, event_type, old_phase, new_phase,
                        details, operator, occurred_at
                    FROM migration_audit_log
                    WHERE migration_id = :migration_id
                      AND event_type = :event_type
                    ORDER BY occurred_at DESC
                    LIMIT 1
                """)
                params: dict[str, Any] = {
                    "migration_id": migration_id,
                    "event_type": event_type.value,
                }
            else:
                query = text("""
                    SELECT
                        id, migration_id, event_type, old_phase, new_phase,
                        details, operator, occurred_at
                    FROM migration_audit_log
                    WHERE migration_id = :migration_id
                    ORDER BY occurred_at DESC
                    LIMIT 1
                """)
                params = {"migration_id": migration_id}

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()

            if row is None:
                return None

            return self._row_to_entry(row)

    async def count_by_migration(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> int:
        """
        Count audit entries for a migration.

        Useful for metrics and progress tracking.

        Args:
            migration_id: The migration ID to query
            event_type: Optional filter by event type

        Returns:
            Number of matching audit entries
        """
        with self._tracer.span(
            "eventsource.audit_log_repo.count_by_migration",
            {
                "migration.id": str(migration_id),
                ATTR_DB_SYSTEM: "postgresql",
            },
        ):
            if event_type:
                query = text("""
                    SELECT COUNT(*)
                    FROM migration_audit_log
                    WHERE migration_id = :migration_id
                      AND event_type = :event_type
                """)
                params: dict[str, Any] = {
                    "migration_id": migration_id,
                    "event_type": event_type.value,
                }
            else:
                query = text("""
                    SELECT COUNT(*)
                    FROM migration_audit_log
                    WHERE migration_id = :migration_id
                """)
                params = {"migration_id": migration_id}

            async with execute_with_connection(self._conn, transactional=False) as conn:
                result = await conn.execute(query, params)
                row = result.fetchone()

            if row is None:
                return 0
            return int(row[0])

    # =========================================================================
    # Helper methods
    # =========================================================================

    def _row_to_entry(self, row: Sequence[Any]) -> MigrationAuditEntry:
        """
        Convert database row to MigrationAuditEntry instance.

        Handles JSON deserialization for the details field and converts
        string values to enum types.

        Args:
            row: Database row tuple

        Returns:
            MigrationAuditEntry instance
        """
        # Parse details field (may be JSON string or dict or None)
        details = row[5]
        if details is not None and isinstance(details, str):
            details = json.loads(details)

        # Parse old_phase (may be None)
        old_phase = None
        if row[3]:
            old_phase = MigrationPhase(row[3])

        # Parse new_phase (may be None)
        new_phase = None
        if row[4]:
            new_phase = MigrationPhase(row[4])

        return MigrationAuditEntry(
            id=row[0],
            migration_id=row[1],
            event_type=AuditEventType(row[2]),
            old_phase=old_phase,
            new_phase=new_phase,
            details=details,
            operator=row[6],
            occurred_at=row[7],
        )


# Type alias for backwards compatibility
MigrationAuditLogRepositoryProtocol = MigrationAuditLogRepository
