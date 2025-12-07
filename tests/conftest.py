"""Shared pytest fixtures for the eventsource library tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    import aiosqlite

# ============================================================================
# SQLite Availability Check
# ============================================================================

AIOSQLITE_AVAILABLE = False
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]


# ============================================================================
# Pytest Configuration
# ============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for tests."""
    config.addinivalue_line("markers", "sqlite: marks tests that require SQLite (aiosqlite)")


# ============================================================================
# Skip Condition
# ============================================================================

skip_if_no_aiosqlite = pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")


# ============================================================================
# SQLite Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def sqlite_connection() -> AsyncGenerator[Any, None]:
    """
    Provide a raw aiosqlite connection to an in-memory database.

    Creates a fresh in-memory SQLite database for each test.
    The connection is automatically closed after the test.

    Yields:
        aiosqlite.Connection: Raw database connection
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    conn = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row

    yield conn

    await conn.close()


@pytest_asyncio.fixture
async def sqlite_event_store() -> AsyncGenerator[Any, None]:
    """
    Provide an initialized SQLiteEventStore with in-memory database.

    Creates a fresh in-memory SQLite event store for each test.
    The store is automatically initialized with the schema and
    cleaned up after the test.

    Yields:
        SQLiteEventStore: Initialized event store ready for use
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource import EventRegistry
    from eventsource.stores.sqlite import SQLiteEventStore

    # Create fresh registry for tests
    registry = EventRegistry()

    store = SQLiteEventStore(
        database=":memory:",
        event_registry=registry,
        wal_mode=False,  # WAL mode not supported in-memory
        busy_timeout=5000,
    )

    async with store:
        await store.initialize()
        yield store


@pytest_asyncio.fixture
async def sqlite_checkpoint_repo(
    sqlite_connection: aiosqlite.Connection,
) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLiteCheckpointRepository with schema initialized.

    Creates the projection_checkpoints and events tables in the
    in-memory database for checkpoint testing.

    Args:
        sqlite_connection: Raw aiosqlite connection fixture

    Yields:
        SQLiteCheckpointRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.repositories.checkpoint import SQLiteCheckpointRepository

    # Create the required tables
    await sqlite_connection.execute("""
        CREATE TABLE IF NOT EXISTS projection_checkpoints (
            projection_name TEXT PRIMARY KEY,
            last_event_id TEXT,
            last_event_type TEXT,
            last_processed_at TEXT,
            events_processed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    await sqlite_connection.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            tenant_id TEXT,
            actor_id TEXT,
            version INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    await sqlite_connection.commit()

    repo = SQLiteCheckpointRepository(sqlite_connection)
    yield repo


@pytest_asyncio.fixture
async def sqlite_outbox_repo(
    sqlite_connection: aiosqlite.Connection,
) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLiteOutboxRepository with schema initialized.

    Creates the event_outbox table in the in-memory database
    for outbox testing.

    Args:
        sqlite_connection: Raw aiosqlite connection fixture

    Yields:
        SQLiteOutboxRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.repositories.outbox import SQLiteOutboxRepository

    # Create the event_outbox table
    await sqlite_connection.execute("""
        CREATE TABLE IF NOT EXISTS event_outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            tenant_id TEXT,
            event_data TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            published_at TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            CHECK (status IN ('pending', 'published', 'failed'))
        )
    """)
    await sqlite_connection.commit()

    repo = SQLiteOutboxRepository(sqlite_connection)
    yield repo


@pytest_asyncio.fixture
async def sqlite_dlq_repo(
    sqlite_connection: aiosqlite.Connection,
) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLiteDLQRepository with schema initialized.

    Creates the dead_letter_queue table in the in-memory database
    for DLQ testing.

    Args:
        sqlite_connection: Raw aiosqlite connection fixture

    Yields:
        SQLiteDLQRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.repositories.dlq import SQLiteDLQRepository

    # Create the dead_letter_queue table
    await sqlite_connection.execute("""
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            projection_name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_data TEXT NOT NULL,
            error_message TEXT NOT NULL,
            error_stacktrace TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            first_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
            status TEXT NOT NULL DEFAULT 'failed',
            resolved_at TEXT,
            resolved_by TEXT,
            CHECK (status IN ('failed', 'retrying', 'resolved')),
            UNIQUE (event_id, projection_name)
        )
    """)
    await sqlite_connection.commit()

    repo = SQLiteDLQRepository(sqlite_connection)
    yield repo
