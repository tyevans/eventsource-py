"""Schema bootstrap for the SQLite snapshot store."""

from __future__ import annotations

from typing import TYPE_CHECKING

from eventsource.adapters.sql.schemas import get_schema

if TYPE_CHECKING:
    import aiosqlite


async def _ensure_tenant_column(conn: aiosqlite.Connection) -> None:
    """Add the additive tenant_id column to a snapshots table created before ADR-0113.

    SQLite has no ADD COLUMN IF NOT EXISTS, so the column is checked first.
    The same addition is documented as updates/005_add_snapshot_tenant_sqlite.sql.
    """
    async with conn.execute("PRAGMA table_info(snapshots)") as cursor:
        columns = {row[1] for row in await cursor.fetchall()}
    if "tenant_id" not in columns:
        await conn.execute("ALTER TABLE snapshots ADD COLUMN tenant_id TEXT")


async def ensure_snapshot_schema(conn: aiosqlite.Connection) -> None:
    """Create the snapshots table, then add the additive tenant_id column if missing."""
    await conn.executescript(get_schema("snapshots", backend="sqlite", additive=False))
    await _ensure_tenant_column(conn)


__all__ = ["ensure_snapshot_schema"]
