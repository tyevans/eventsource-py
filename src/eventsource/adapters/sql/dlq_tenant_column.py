"""Owner-column probe for the SQL DLQ adapter (ADR-0114).

A database created before ADR-0114 has no ``dead_letter_queue.tenant_id``. SQLite
gets the column added on first use, as the snapshot store does. PostgreSQL gets
it from ``updates/007_add_dlq_tenant.sql``, which the operator runs by hand. Until
then the adapter probes once and degrades: it writes and reads without the owner.
Under a scope it returns nothing, because an entry with no owner is invisible.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from eventsource.adapters._sql.connection import sql_connection
from eventsource.adapters._sql.dialect import Dialect, dialect_of

logger = logging.getLogger(__name__)

# Scoped to the search_path, so a same-named table in another schema is ignored.
_PG_PROBE_SQL = text(
    "SELECT EXISTS (SELECT 1 FROM information_schema.columns "
    "WHERE table_schema = ANY(current_schemas(false)) "
    "AND table_name = 'dead_letter_queue' AND column_name = 'tenant_id')"
)
_SQLITE_COLUMNS_SQL = text("PRAGMA table_info(dead_letter_queue)")
_SQLITE_ADD_SQL = text("ALTER TABLE dead_letter_queue ADD COLUMN tenant_id TEXT")
_SQLITE_INDEX_SQL = text(
    "CREATE INDEX IF NOT EXISTS idx_dlq_tenant_id ON dead_letter_queue (tenant_id)"
)

_MISSING_COLUMN_WARNING = (
    "dead_letter_queue.tenant_id is missing. The PostgreSQL DLQ adapter is saving and "
    "reading entries without an owner tenant, so tenant scope does not apply to them. "
    "Run src/eventsource/adapters/sql/schemas/updates/007_add_dlq_tenant.sql, "
    "then restart this process."
)


async def _sqlite_ensure_column(conn: AsyncConnection) -> bool:
    """Add the owner column to a SQLite table that predates ADR-0114. Always True."""
    rows = (await conn.execute(_SQLITE_COLUMNS_SQL)).fetchall()
    if not any(row[1] == "tenant_id" for row in rows):
        await conn.execute(_SQLITE_ADD_SQL)
        await conn.execute(_SQLITE_INDEX_SQL)
    return True


async def _postgresql_has_column(conn: AsyncConnection) -> bool:
    """Report whether the owner column exists, warning once when it does not."""
    present = bool((await conn.execute(_PG_PROBE_SQL)).scalar())
    if not present:
        logger.warning(_MISSING_COLUMN_WARNING)
    return present


class DLQTenantColumn:
    """Caches whether ``dead_letter_queue.tenant_id`` exists. Probes once per instance."""

    def __init__(self) -> None:
        self.present: bool | None = None
        self._lock = asyncio.Lock()

    async def ensure(self, conn: AsyncConnection | AsyncEngine) -> bool:
        """Return whether the owner column exists. Probes in a write block on first use."""
        if self.present is None:
            async with sql_connection(conn, write=True) as live:
                await self.probe(live, dialect_of(live))
        return bool(self.present)

    async def probe(self, conn: AsyncConnection, dialect: Dialect) -> bool:
        """Return the cached answer, probing the database on first use only."""
        if self.present is None:
            async with self._lock:
                if self.present is None:
                    if dialect is Dialect.SQLITE:
                        self.present = await _sqlite_ensure_column(conn)
                    else:
                        self.present = await _postgresql_has_column(conn)
        return self.present


__all__ = ["DLQTenantColumn"]
