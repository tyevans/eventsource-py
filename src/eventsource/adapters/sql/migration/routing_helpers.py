"""Helper functions for tenant routing repository."""

from __future__ import annotations

import sys
from collections.abc import Sequence
from typing import Any

from eventsource.ports.migration.models import (
    TenantMigrationState,
    TenantRouting,
)


def _get_sql_connection() -> Any:
    """Get the sql_connection context manager dynamically.

    Allows unit test mocking of `eventsource.adapters.sql.migration.routing.sql_connection`
    without requiring circular imports.
    """
    mod = sys.modules.get("eventsource.adapters.sql.migration.routing")
    if mod is not None and hasattr(mod, "sql_connection"):
        return mod.sql_connection
    from eventsource.adapters._sql.connection import sql_connection

    return sql_connection


def row_to_routing(row: Sequence[Any]) -> TenantRouting:
    """Convert database row tuple to TenantRouting instance."""
    return TenantRouting(
        tenant_id=row[0],
        store_id=row[1],
        migration_state=TenantMigrationState(row[2]),
        active_migration_id=row[3],
        created_at=row[4],
        updated_at=row[5],
    )


__all__ = ["_get_sql_connection", "row_to_routing"]
