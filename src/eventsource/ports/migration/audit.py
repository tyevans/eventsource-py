"""
Protocol for migration audit log persistence.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Protocol, runtime_checkable
from uuid import UUID

if TYPE_CHECKING:
    from eventsource.ports.migration.models import (
        AuditEventType,
        MigrationAuditEntry,
    )


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


__all__ = [
    "MigrationAuditLogRepository",
]
