"""SQLite snapshot store implementation using async aiosqlite."""

from __future__ import annotations

import asyncio
import logging
import warnings
from datetime import datetime
from uuid import UUID

from eventsource.adapters.serialization import json_dumps, json_loads
from eventsource.adapters.sql.schemas import get_schema
from eventsource.domain.exceptions import SnapshotDeserializationError
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)
from eventsource.ports.snapshots import Snapshot

try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False

logger = logging.getLogger(__name__)


class SQLiteNotAvailableError(ImportError):
    """Raised when aiosqlite is not installed."""

    def __init__(self) -> None:
        super().__init__(
            "aiosqlite is required for SQLiteSnapshotStore. "
            "Install it with: pip install eventsource[sqlite]"
        )


class SQLiteSnapshotStore:
    """SQLite implementation of `SnapshotStore`."""

    def __init__(
        self,
        database_path: str,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        busy_timeout: int = 5000,
    ) -> None:
        """Initialize the SQLite snapshot store."""
        if not AIOSQLITE_AVAILABLE:
            raise SQLiteNotAvailableError()
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._database_path, self._busy_timeout = database_path, busy_timeout
        self._connection: aiosqlite.Connection | None = None
        self._lock, self._init_lock = asyncio.Lock(), asyncio.Lock()
        logger.debug("SQLiteSnapshotStore initialized with %s", database_path)

    async def _conn(self) -> aiosqlite.Connection:
        """Return the live connection, opening and initializing it on first use."""
        if self._connection is not None:
            return self._connection
        async with self._init_lock:
            if self._connection is not None:
                return self._connection
            conn = await aiosqlite.connect(self._database_path)
            await conn.execute(f"PRAGMA busy_timeout = {self._busy_timeout}")
            conn.row_factory = aiosqlite.Row
            await conn.executescript(get_schema("snapshots", backend="sqlite"))
            await conn.commit()
            self._connection = conn
            return conn

    async def close(self) -> None:
        """Close the underlying connection, if open. Safe to call multiple times."""
        if self._connection is not None:
            await self._connection.close()
            self._connection = None

    async def __aenter__(self) -> SQLiteSnapshotStore:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        await self.close()

    def __del__(self) -> None:
        if getattr(self, "_connection", None) is not None:
            db = getattr(self, "_database_path", "<unknown>")
            msg = f"Unclosed {type(self).__name__} ({db!r}) {self!r}; call 'await store.close()' or use 'async with'"
            warnings.warn(msg, ResourceWarning, stacklevel=2, source=self)

    def _agg_attrs(self, aggregate_id: UUID, aggregate_type: str) -> dict[str, str]:
        return {ATTR_AGGREGATE_ID: str(aggregate_id), ATTR_AGGREGATE_TYPE: aggregate_type}

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """Save or update a snapshot using INSERT OR REPLACE."""
        s = snapshot
        attrs = {**self._agg_attrs(s.aggregate_id, s.aggregate_type), ATTR_VERSION: s.version}
        sql = (
            "INSERT OR REPLACE INTO snapshots (aggregate_id, aggregate_type, "
            "version, schema_version, state, created_at) VALUES (?, ?, ?, ?, ?, ?)"
        )
        params = (
            str(s.aggregate_id),
            s.aggregate_type,
            s.version,
            s.schema_version,
            json_dumps(s.state),
            s.created_at.isoformat(),
        )
        with self._tracer.span("eventsource.snapshot.save", attrs):
            conn = await self._conn()
            async with self._lock:
                await conn.execute(sql, params)
                await conn.commit()
            logger.debug("Saved snapshot for %s/%s", s.aggregate_type, s.aggregate_id)

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        """Get the snapshot for an aggregate, or None if not found."""
        attrs = self._agg_attrs(aggregate_id, aggregate_type)
        sql = (
            "SELECT aggregate_id, aggregate_type, version, schema_version, state, created_at "
            "FROM snapshots WHERE aggregate_id = ? AND aggregate_type = ?"
        )
        with self._tracer.span("eventsource.snapshot.get", attrs):
            conn = await self._conn()
            async with self._lock:
                cursor = await conn.execute(sql, (str(aggregate_id), aggregate_type))
                row = await cursor.fetchone()
            if row is None:
                logger.debug("No snapshot found for %s/%s", aggregate_type, aggregate_id)
                return None
            r_id, r_type = UUID(row["aggregate_id"]), row["aggregate_type"]
            try:
                state = json_loads(row["state"])
            except (ValueError, TypeError) as e:
                raise SnapshotDeserializationError(r_id, r_type, e) from e
            logger.debug("Retrieved snapshot for %s/%s", aggregate_type, aggregate_id)
            created = datetime.fromisoformat(row["created_at"])
            return Snapshot(r_id, r_type, row["version"], state, row["schema_version"], created)

    async def delete_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        """Delete the snapshot for an aggregate; returns True if deleted."""
        attrs = self._agg_attrs(aggregate_id, aggregate_type)
        sql = "DELETE FROM snapshots WHERE aggregate_id = ? AND aggregate_type = ?"
        with self._tracer.span("eventsource.snapshot.delete", attrs):
            conn = await self._conn()
            async with self._lock:
                cursor = await conn.execute(sql, (str(aggregate_id), aggregate_type))
                await conn.commit()
                deleted: bool = cursor.rowcount > 0
            logger.debug(
                "%s snapshot for %s/%s",
                "Deleted" if deleted else "No",
                aggregate_type,
                aggregate_id,
            )
            return deleted

    async def snapshot_exists(self, aggregate_id: UUID, aggregate_type: str) -> bool:
        """Check if a snapshot exists for the given aggregate."""
        attrs = self._agg_attrs(aggregate_id, aggregate_type)
        sql = (
            "SELECT EXISTS (SELECT 1 FROM snapshots WHERE aggregate_id = ? AND aggregate_type = ?)"
        )
        with self._tracer.span("eventsource.snapshot.exists", attrs):
            conn = await self._conn()
            async with self._lock:
                cursor = await conn.execute(sql, (str(aggregate_id), aggregate_type))
                row = await cursor.fetchone()
                return bool(row[0]) if row else False

    async def delete_snapshots_by_type(
        self, aggregate_type: str, schema_version_below: int | None = None
    ) -> int:
        """Delete all snapshots for a given aggregate type."""
        attrs = {ATTR_AGGREGATE_TYPE: aggregate_type}
        with self._tracer.span("eventsource.snapshot.delete_by_type", attrs):
            conn = await self._conn()
            async with self._lock:
                if schema_version_below is not None:
                    sql = "DELETE FROM snapshots WHERE aggregate_type = ? AND schema_version < ?"
                    cursor = await conn.execute(sql, (aggregate_type, schema_version_below))
                else:
                    sql = "DELETE FROM snapshots WHERE aggregate_type = ?"
                    cursor = await conn.execute(sql, (aggregate_type,))
                await conn.commit()
                count: int = cursor.rowcount
            if count > 0:
                sfx = f" (schema_version < {schema_version_below})" if schema_version_below else ""
                logger.info(
                    "Deleted %d snapshots for aggregate type %s%s", count, aggregate_type, sfx
                )
            return count

    @property
    def database_path(self) -> str:
        """Get the database path."""
        return self._database_path
