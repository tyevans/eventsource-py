"""In-memory MigrationAuditLogRepository double for coordinator audit tests."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
)


class FakeAuditLogRepository:
    """In-memory MigrationAuditLogRepository double.

    Not the standalone-only anti-pattern: this is injected into a real
    MigrationCoordinator and exercised through the coordinator's public
    methods, not called directly by any test.
    """

    def __init__(self) -> None:
        self.entries: list[MigrationAuditEntry] = []

    async def record(self, entry: MigrationAuditEntry) -> int:
        entry_id = len(self.entries) + 1
        self.entries.append(entry)
        return entry_id

    async def get_by_migration(
        self,
        migration_id: UUID,
        event_types: list[AuditEventType] | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int | None = None,
    ) -> list[MigrationAuditEntry]:
        return [e for e in self.entries if e.migration_id == migration_id]

    async def get_by_id(self, entry_id: int) -> MigrationAuditEntry | None:
        for i, entry in enumerate(self.entries, start=1):
            if i == entry_id:
                return entry
        return None

    async def get_latest(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> MigrationAuditEntry | None:
        matches = [
            e
            for e in self.entries
            if e.migration_id == migration_id and (event_type is None or e.event_type == event_type)
        ]
        return matches[-1] if matches else None

    async def count_by_migration(
        self,
        migration_id: UUID,
        event_type: AuditEventType | None = None,
    ) -> int:
        return len(
            [
                e
                for e in self.entries
                if e.migration_id == migration_id
                and (event_type is None or e.event_type == event_type)
            ]
        )


__all__ = ["FakeAuditLogRepository"]
