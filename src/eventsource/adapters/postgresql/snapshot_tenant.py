"""Upgrade-safety probe for the snapshots.tenant_id column (ADR-0113).

Databases created before ADR-0113 have no ``snapshots.tenant_id`` column. The
column arrives through ``updates/005_add_snapshot_tenant.sql``, which the
operator runs by hand. Until then the snapshot store must keep working, so it
checks once per instance whether the column exists and, if not, reads and
writes snapshots without it.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

# Scoped to the search_path, so a same-named table in another schema is ignored.
_TENANT_PROBE_SQL = text(
    "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
    "WHERE table_schema = ANY(current_schemas(false)) "
    "AND table_name = 'snapshots' AND column_name = 'tenant_id')"
)

# Column-less variants used while the probe reports the column as absent.
# The select aliases NULL so the row shape matches the tenant-aware query.
UPSERT_WITHOUT_TENANT_SQL = text(
    "INSERT INTO snapshots (aggregate_id, aggregate_type, version, schema_version, state, "
    "created_at) "
    "VALUES (:aggregate_id, :aggregate_type, :version, :schema_version, :state, :created_at) "
    "ON CONFLICT (aggregate_id, aggregate_type) DO UPDATE SET "
    "version = EXCLUDED.version, schema_version = EXCLUDED.schema_version, "
    "state = EXCLUDED.state, created_at = EXCLUDED.created_at"
)
SELECT_WITHOUT_TENANT_SQL = text(
    "SELECT aggregate_id, aggregate_type, version, schema_version, state, created_at, "
    "NULL AS tenant_id FROM snapshots WHERE aggregate_id = :aggregate_id "
    "AND aggregate_type = :aggregate_type"
)

_MISSING_COLUMN_WARNING = (
    "snapshots.tenant_id is missing. The PostgreSQL snapshot store is saving and "
    "reading snapshots without tenant isolation. Run "
    "src/eventsource/adapters/sql/schemas/updates/005_add_snapshot_tenant.sql, "
    "then restart this process."
)


class SnapshotTenantColumnProbe:
    """Caches whether ``snapshots.tenant_id`` exists. Probes once per instance."""

    def __init__(self) -> None:
        self._has_tenant_column: bool | None = None
        self._lock = asyncio.Lock()

    async def has_tenant_column(self, session: AsyncSession) -> bool:
        """Return True when the column exists, probing the database on first use only."""
        if self._has_tenant_column is None:
            async with self._lock:
                if self._has_tenant_column is None:
                    result = await session.execute(_TENANT_PROBE_SQL)
                    present = bool(result.scalar())
                    if not present:
                        logger.warning(_MISSING_COLUMN_WARNING)
                    self._has_tenant_column = present
        return self._has_tenant_column
