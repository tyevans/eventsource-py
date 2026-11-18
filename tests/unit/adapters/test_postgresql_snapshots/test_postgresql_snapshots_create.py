"""Helper functions for creating mock results and snapshot rows."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

from eventsource.ports.snapshots import Snapshot


def create_mock_result(rows: list[Any] | None = None, rowcount: int = 0) -> MagicMock:
    """Create a mock SQLAlchemy result."""
    result = MagicMock()
    if rows is not None:
        result.fetchone.return_value = rows[0] if rows else None
        result.fetchall.return_value = rows
    result.rowcount = rowcount
    result.scalar.return_value = None
    return result


def create_scalar_result(value: Any) -> MagicMock:
    """Create a mock result for scalar queries."""
    result = MagicMock()
    result.scalar.return_value = value
    return result


def create_snapshot_row(snapshot: Snapshot) -> MagicMock:
    """Create a mock database row from a snapshot."""
    row = MagicMock()
    row.aggregate_id = snapshot.aggregate_id
    row.aggregate_type = snapshot.aggregate_type
    row.version = snapshot.version
    row.schema_version = snapshot.schema_version
    row.state = snapshot.state  # Can be dict or JSON string
    row.created_at = snapshot.created_at
    return row


TENANT_PROBE_MARKER = "information_schema"


def route_tenant_probe(session: AsyncMock, *, column_present: bool) -> None:
    """Answer the snapshots.tenant_id probe; other statements get ``execute.return_value``."""
    probe = MagicMock()
    probe.scalar.return_value = column_present

    async def execute(statement: object, *args: object, **kwargs: object) -> object:
        if TENANT_PROBE_MARKER in str(statement):
            return probe
        return session.execute.return_value

    session.execute.side_effect = execute
