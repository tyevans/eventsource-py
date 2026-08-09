"""
SQLite snapshot store implementation.

Provides lightweight, embedded snapshot storage using SQLite
with async aiosqlite driver.

This implementation is suitable for:
- Development and testing environments
- Single-instance deployments
- Embedded applications
- Edge computing scenarios

For high-concurrency production workloads, consider PostgreSQLSnapshotStore.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from uuid import UUID

from eventsource.adapters.sql.schemas import get_schema
from eventsource.domain.exceptions import SnapshotDeserializationError
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_VERSION,
)
from eventsource.ports.snapshots import Snapshot

# Optional dependency handling. Named AIOSQLITE_AVAILABLE (not
# SQLITE_AVAILABLE) because it is the aiosqlite driver being guarded here,
# not Python's always-available sqlite3 stdlib module -- and to match
# store.py's identically-guarded import of the same package, so the two
# don't spell the same fact two different ways (recurring-defects §2). This
# module's own copy is intentional (each guarded import needs its own
# try/except), but only store.py's copy is re-exported from
# adapters/sqlite/__init__.py -- see that module's docstring.
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    AIOSQLITE_AVAILABLE = False
    aiosqlite = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class SQLiteNotAvailableError(ImportError):
    """Raised when aiosqlite is not installed."""

    def __init__(self) -> None:
        super().__init__(
            "aiosqlite is required for SQLiteSnapshotStore. "
            "Install it with: pip install eventsource[sqlite]"
        )


class SQLiteSnapshotStore:
    """
    SQLite implementation of SnapshotStore.

    Uses aiosqlite for async database operations. Ideal for:
    - Development and testing
    - Single-process applications
    - Embedded systems
    - Desktop applications

    Features:
    - File-based persistence
    - No external database server required
    - Async operations via aiosqlite
    - Upsert using INSERT OR REPLACE
    - Optional OpenTelemetry tracing

    Connection discipline mirrors `SQLiteEventStore`: a single connection
    is opened lazily on first use, the `snapshots` schema is applied to it
    (idempotently), and it is reused for the store's lifetime. Required
    for `":memory:"` databases, whose contents live only as long as the
    connection that created them. Every statement runs under `self._lock`,
    so a `commit()` never lands mid-way through another operation.

    The connection is a resource the store owns, so the store implements
    `SupportsClose` -- `aiosqlite` backs each connection with a
    **non-daemon** thread, which keeps the interpreter alive at shutdown
    until someone closes it. Callers that build a store must close it.

    Example:
        >>> from eventsource.adapters.sqlite.snapshots import SQLiteSnapshotStore
        >>>
        >>> store = SQLiteSnapshotStore("snapshots.db")
        >>>
        >>> # Use with repository
        >>> repo = AggregateRepository(
        ...     event_store=event_store,
        ...     aggregate_factory=OrderAggregate,
        ...     aggregate_type="Order",
        ...     snapshot_store=store,
        ... )
        >>>
        >>> # ... and release the connection when done
        >>> await store.close()  # doctest: +SKIP

    Note:
        - SQLite is single-writer, so concurrent writes may be slower
        - For production workloads, consider PostgreSQLSnapshotStore
    """

    def __init__(
        self,
        database_path: str,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        busy_timeout: int = 5000,
    ) -> None:
        """
        Initialize the SQLite snapshot store.

        Args:
            database_path: Path to the SQLite database file.
                          Use ":memory:" for in-memory database.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing (default True).
                          Ignored if tracer is explicitly provided.
            busy_timeout: Milliseconds to wait for a competing writer to
                         release the database lock (default 5000).

        Raises:
            SQLiteNotAvailableError: If aiosqlite is not installed.
        """
        if not AIOSQLITE_AVAILABLE:
            raise SQLiteNotAvailableError()

        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._database_path = database_path
        self._busy_timeout = busy_timeout
        self._connection: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        # Distinct from `self._lock`, which is held around each statement:
        # `_conn()` is called before that lock is taken, and `asyncio.Lock`
        # is not reentrant.
        self._init_lock = asyncio.Lock()
        logger.debug("SQLiteSnapshotStore initialized with %s", database_path)

    async def _conn(self) -> aiosqlite.Connection:
        """Return the live connection, opening and initializing it on first use.

        Double-checked locking around `self._init_lock`: without it two
        concurrent first-callers each open an `aiosqlite.connect()` -- a
        non-daemon background thread each -- and the loser's connection is
        discarded, leaking its thread for the process lifetime.
        """
        if self._connection is not None:
            return self._connection

        async with self._init_lock:
            if self._connection is not None:
                return self._connection

            conn = await aiosqlite.connect(self._database_path)
            # A snapshot store commonly shares its file with a
            # SQLiteEventStore, which holds its own connection; without a
            # busy timeout the second writer fails immediately rather than
            # waiting out the first one's write transaction.
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

    async def save_snapshot(self, snapshot: Snapshot) -> None:
        """
        Save or update a snapshot using INSERT OR REPLACE.

        Args:
            snapshot: The snapshot to save
        """
        with self._tracer.span(
            "eventsource.snapshot.save",
            {
                ATTR_AGGREGATE_ID: str(snapshot.aggregate_id),
                ATTR_AGGREGATE_TYPE: snapshot.aggregate_type,
                ATTR_VERSION: snapshot.version,
            },
        ):
            conn = await self._conn()
            async with self._lock:
                await conn.execute(
                    """
                    INSERT OR REPLACE INTO snapshots (
                        aggregate_id,
                        aggregate_type,
                        version,
                        schema_version,
                        state,
                        created_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(snapshot.aggregate_id),
                        snapshot.aggregate_type,
                        snapshot.version,
                        snapshot.schema_version,
                        json.dumps(snapshot.state),
                        snapshot.created_at.isoformat(),
                    ),
                )
                await conn.commit()

            logger.debug(
                "Saved snapshot for %s/%s at version %d",
                snapshot.aggregate_type,
                snapshot.aggregate_id,
                snapshot.version,
            )

    async def get_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> Snapshot | None:
        """
        Get the snapshot for an aggregate.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            The snapshot if found, None otherwise

        Raises:
            SnapshotDeserializationError: If the stored ``state`` column is
                not valid JSON. SQLite stores it as plain ``TEXT`` (no
                server-side JSON validation on write, unlike PostgreSQL's
                ``JSONB`` column), so on-disk corruption or a hand-edited
                database can genuinely produce this. Per ADR 0017, callers
                should generally go through ``read_valid_snapshot()``, which
                catches this and falls back to full event replay -- raising
                here is what lets it do that instead of letting a bare
                ``json.JSONDecodeError`` escape.
        """
        with self._tracer.span(
            "eventsource.snapshot.get",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            conn = await self._conn()
            async with self._lock:
                cursor = await conn.execute(
                    """
                    SELECT
                        aggregate_id,
                        aggregate_type,
                        version,
                        schema_version,
                        state,
                        created_at
                    FROM snapshots
                    WHERE aggregate_id = ?
                      AND aggregate_type = ?
                    """,
                    (str(aggregate_id), aggregate_type),
                )
                row = await cursor.fetchone()

            if row is None:
                logger.debug(
                    "No snapshot found for %s/%s",
                    aggregate_type,
                    aggregate_id,
                )
                return None

            try:
                state = json.loads(row["state"])
            except (json.JSONDecodeError, TypeError) as e:
                raise SnapshotDeserializationError(
                    aggregate_id=UUID(row["aggregate_id"]),
                    aggregate_type=row["aggregate_type"],
                    original_error=e,
                ) from e

            snapshot = Snapshot(
                aggregate_id=UUID(row["aggregate_id"]),
                aggregate_type=row["aggregate_type"],
                version=row["version"],
                schema_version=row["schema_version"],
                state=state,
                created_at=datetime.fromisoformat(row["created_at"]),
            )

            logger.debug(
                "Retrieved snapshot for %s/%s at version %d",
                aggregate_type,
                aggregate_id,
                snapshot.version,
            )

            return snapshot

    async def delete_snapshot(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Delete the snapshot for an aggregate.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if a snapshot was deleted, False otherwise
        """
        with self._tracer.span(
            "eventsource.snapshot.delete",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            conn = await self._conn()
            async with self._lock:
                cursor = await conn.execute(
                    """
                    DELETE FROM snapshots
                    WHERE aggregate_id = ?
                      AND aggregate_type = ?
                    """,
                    (str(aggregate_id), aggregate_type),
                )
                await conn.commit()
                deleted: bool = cursor.rowcount > 0

            if deleted:
                logger.debug(
                    "Deleted snapshot for %s/%s",
                    aggregate_type,
                    aggregate_id,
                )
            else:
                logger.debug(
                    "No snapshot to delete for %s/%s",
                    aggregate_type,
                    aggregate_id,
                )

            return deleted

    async def snapshot_exists(
        self,
        aggregate_id: UUID,
        aggregate_type: str,
    ) -> bool:
        """
        Check if a snapshot exists.

        Args:
            aggregate_id: Unique identifier of the aggregate
            aggregate_type: Type name of the aggregate

        Returns:
            True if snapshot exists, False otherwise
        """
        with self._tracer.span(
            "eventsource.snapshot.exists",
            {
                ATTR_AGGREGATE_ID: str(aggregate_id),
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            conn = await self._conn()
            async with self._lock:
                cursor = await conn.execute(
                    """
                    SELECT EXISTS (
                        SELECT 1 FROM snapshots
                        WHERE aggregate_id = ?
                          AND aggregate_type = ?
                    )
                    """,
                    (str(aggregate_id), aggregate_type),
                )
                row = await cursor.fetchone()
                return bool(row[0]) if row else False

    async def delete_snapshots_by_type(
        self,
        aggregate_type: str,
        schema_version_below: int | None = None,
    ) -> int:
        """
        Delete all snapshots for a given aggregate type.

        Args:
            aggregate_type: Type name of aggregates
            schema_version_below: If provided, only delete snapshots with
                                 schema_version < this value

        Returns:
            Number of snapshots deleted
        """
        with self._tracer.span(
            "eventsource.snapshot.delete_by_type",
            {
                ATTR_AGGREGATE_TYPE: aggregate_type,
            },
        ):
            conn = await self._conn()
            async with self._lock:
                if schema_version_below is not None:
                    cursor = await conn.execute(
                        """
                        DELETE FROM snapshots
                        WHERE aggregate_type = ?
                          AND schema_version < ?
                        """,
                        (aggregate_type, schema_version_below),
                    )
                else:
                    cursor = await conn.execute(
                        """
                        DELETE FROM snapshots
                        WHERE aggregate_type = ?
                        """,
                        (aggregate_type,),
                    )
                await conn.commit()
                count: int = cursor.rowcount

            if count > 0:
                logger.info(
                    "Deleted %d snapshots for aggregate type %s%s",
                    count,
                    aggregate_type,
                    f" (schema_version < {schema_version_below})" if schema_version_below else "",
                )

            return count

    @property
    def database_path(self) -> str:
        """Get the database path."""
        return self._database_path
