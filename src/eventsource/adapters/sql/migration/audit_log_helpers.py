"""Helper functions for migration audit log repository.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from eventsource.adapters.serialization import json_loads
from eventsource.ports.migration.models import (
    AuditEventType,
    MigrationAuditEntry,
    MigrationPhase,
)


def row_to_audit_entry(row: Sequence[Any]) -> MigrationAuditEntry:
    """Convert database row to MigrationAuditEntry instance.

    Handles JSON deserialization via orjson (`json_loads`) for the details field
    and converts string values to enum types.

    Args:
        row: Database row tuple

    Returns:
        MigrationAuditEntry instance
    """
    details = row[5]
    if details is not None and isinstance(details, str):
        details = json_loads(details)

    old_phase = MigrationPhase(row[3]) if row[3] else None
    new_phase = MigrationPhase(row[4]) if row[4] else None

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


__all__ = ["row_to_audit_entry"]
