"""Helper functions and state machine definitions for migration repository.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from eventsource.ports.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
)
from eventsource.ports.positions import Position


def _token(position: Position | None) -> str | None:
    """Render a position as its persisted token, or None."""
    return position.to_str() if position is not None else None


def _position(token: Any) -> Position | None:
    """Decode a persisted position token, or None when the column is null."""
    return Position.from_str(token) if token else None


# Valid phase transitions for the migration state machine
VALID_TRANSITIONS: dict[MigrationPhase, set[MigrationPhase]] = {
    MigrationPhase.PENDING: {
        MigrationPhase.BULK_COPY,
        MigrationPhase.ABORTED,
    },
    MigrationPhase.BULK_COPY: {
        MigrationPhase.DUAL_WRITE,
        MigrationPhase.ABORTED,
        MigrationPhase.FAILED,
    },
    MigrationPhase.DUAL_WRITE: {
        MigrationPhase.CUTOVER,
        MigrationPhase.ABORTED,
        MigrationPhase.FAILED,
    },
    MigrationPhase.CUTOVER: {
        MigrationPhase.COMPLETED,
        MigrationPhase.DUAL_WRITE,  # Rollback
        MigrationPhase.FAILED,
    },
    MigrationPhase.COMPLETED: set(),  # Terminal
    MigrationPhase.ABORTED: set(),  # Terminal
    MigrationPhase.FAILED: set(),  # Terminal
}


def row_to_migration(row: Sequence[Any]) -> Migration:
    """Convert database row to Migration instance.

    Handles JSON deserialization for the config field and converts
    string phase values to MigrationPhase enum.

    Args:
        row: Database row tuple

    Returns:
        Migration instance
    """
    config_data = row[15] if isinstance(row[15], dict) else json.loads(row[15])

    return Migration(
        id=row[0],
        tenant_id=row[1],
        source_store_id=row[2],
        target_store_id=row[3],
        phase=MigrationPhase(row[4]),
        events_total=row[5] or 0,
        events_copied=row[6] or 0,
        last_source_position=_position(row[7]),
        last_target_position=_position(row[8]),
        started_at=row[9],
        bulk_copy_started_at=row[10],
        bulk_copy_completed_at=row[11],
        dual_write_started_at=row[12],
        cutover_started_at=row[13],
        completed_at=row[14],
        config=MigrationConfig.from_dict(config_data),
        error_count=row[16] or 0,
        last_error=row[17],
        last_error_at=row[18],
        is_paused=row[19] or False,
        paused_at=row[20],
        pause_reason=row[21],
        created_at=row[22],
        updated_at=row[23],
        created_by=row[24],
    )


def get_phase_timestamp_updates(phase: MigrationPhase) -> str:
    """Get SQL for updating phase-specific timestamps.

    Different phases have different timestamp fields that need
    to be updated when transitioning to that phase.

    Args:
        phase: The target phase

    Returns:
        SQL fragment for timestamp updates
    """
    timestamp_map = {
        MigrationPhase.BULK_COPY: ("started_at = :phase_time, bulk_copy_started_at = :phase_time,"),
        MigrationPhase.DUAL_WRITE: (
            "bulk_copy_completed_at = :phase_time, dual_write_started_at = :phase_time,"
        ),
        MigrationPhase.CUTOVER: "cutover_started_at = :phase_time,",
        MigrationPhase.COMPLETED: "completed_at = :phase_time,",
        MigrationPhase.ABORTED: "completed_at = :phase_time,",
        MigrationPhase.FAILED: "completed_at = :phase_time,",
    }
    return timestamp_map.get(phase, "")


def get_phase_timestamp_params(
    phase: MigrationPhase,
    now: datetime,
) -> dict[str, Any]:
    """Get params for phase-specific timestamps.

    Args:
        phase: The target phase
        now: Current timestamp

    Returns:
        Dictionary of timestamp parameters
    """
    if phase in (
        MigrationPhase.BULK_COPY,
        MigrationPhase.DUAL_WRITE,
        MigrationPhase.CUTOVER,
        MigrationPhase.COMPLETED,
        MigrationPhase.ABORTED,
        MigrationPhase.FAILED,
    ):
        return {"phase_time": now}
    return {}


__all__ = [
    "VALID_TRANSITIONS",
    "_position",
    "_token",
    "get_phase_timestamp_params",
    "get_phase_timestamp_updates",
    "row_to_migration",
]
