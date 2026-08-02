"""
Unit tests for SQLiteOutboxRepository.

Covers both fixture-based tests against the shared `sqlite_outbox_repo`
fixture (`tests/conftest.py`, schema created via the migration templates)
and standalone tests against a locally-created in-memory SQLite database,
carried over from `tests/repositories/test_sqlite_repos.py` and
`tests/unit/test_outbox_repository.py` respectively.
"""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
from eventsource.domain.event import DomainEvent
from eventsource.ports.outbox import OutboxRepository
from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    import aiosqlite


pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


class TestSampleEvent(DomainEvent):
    """Sample event for repository tests."""

    aggregate_type: str = "TestSampleAggregate"
    data: str = "test_data"


# ============================================================================
# SQLiteOutboxRepository Tests (fixture-based, tests/conftest.py)
# ============================================================================


class TestSQLiteOutboxRepositoryProtocol:
    """Verify SQLiteOutboxRepository implements the protocol."""

    def test_implements_protocol(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test that SQLiteOutboxRepository implements OutboxRepository."""
        assert isinstance(sqlite_outbox_repo, OutboxRepository)


class TestSQLiteOutboxRepositoryAddEvent:
    """Tests for add_event method."""

    async def test_add_event(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test adding an event to the outbox."""
        event = TestSampleEvent(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
        )

        outbox_id = await sqlite_outbox_repo.add_event(event)

        assert outbox_id is not None

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].event_type == "TestSampleEvent"

    async def test_add_event_preserves_tenant_id(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test that tenant_id is preserved in outbox."""
        tenant_id = uuid4()
        event = TestSampleEvent(
            aggregate_id=uuid4(),
            tenant_id=tenant_id,
        )

        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending[0].tenant_id == tenant_id

    async def test_add_event_without_tenant_id(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test adding an event without tenant_id."""
        event = TestSampleEvent(
            aggregate_id=uuid4(),
            tenant_id=None,
        )

        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending[0].tenant_id is None


class TestSQLiteOutboxRepositoryGetPendingEvents:
    """Tests for get_pending_events method."""

    async def test_get_pending_events_empty(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test getting pending events when none exist."""
        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending == []

    async def test_get_pending_events_order(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test that pending events are returned in FIFO order."""
        events = []
        for i in range(3):
            event = TestSampleEvent(
                aggregate_id=uuid4(),
                data=f"event_{i}",
            )
            events.append(event)
            await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 3

        # Verify FIFO order
        assert "event_0" in pending[0].event_data
        assert "event_1" in pending[1].event_data
        assert "event_2" in pending[2].event_data

    async def test_get_pending_events_limit(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test limiting number of pending events returned."""
        for _ in range(5):
            event = TestSampleEvent(aggregate_id=uuid4())
            await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events(limit=3)
        assert len(pending) == 3


class TestSQLiteOutboxRepositoryMarkPublished:
    """Tests for mark_published method."""

    async def test_mark_published(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test marking an event as published."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id

        await sqlite_outbox_repo.mark_published(outbox_id)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 0

        stats = await sqlite_outbox_repo.get_stats()
        assert stats.published_count == 1


class TestSQLiteOutboxRepositoryMarkFailed:
    """Tests for mark_failed method."""

    async def test_mark_failed(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test marking an event as failed."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id

        await sqlite_outbox_repo.mark_failed(outbox_id, "Connection error")

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 0

        stats = await sqlite_outbox_repo.get_stats()
        assert stats.failed_count == 1


class TestSQLiteOutboxRepositoryIncrementRetry:
    """Tests for increment_retry method."""

    async def test_increment_retry(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test incrementing retry count."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id

        await sqlite_outbox_repo.increment_retry(outbox_id, "First retry error")
        await sqlite_outbox_repo.increment_retry(outbox_id, "Second retry error")

        pending = await sqlite_outbox_repo.get_pending_events()
        assert pending[0].retry_count == 2


class TestSQLiteOutboxRepositoryCleanupPublished:
    """Tests for cleanup_published method."""

    async def test_cleanup_published(
        self,
        sqlite_outbox_repo: SQLiteOutboxRepository,
        sqlite_connection: aiosqlite.Connection,
    ) -> None:
        """Test cleaning up old published events."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id
        await sqlite_outbox_repo.mark_published(outbox_id)

        # Manually backdate the published_at
        await sqlite_connection.execute(
            """
            UPDATE event_outbox
            SET published_at = datetime('now', '-10 days')
            WHERE id = ?
            """,
            (str(outbox_id),),
        )
        await sqlite_connection.commit()

        deleted = await sqlite_outbox_repo.cleanup_published(days=7)
        assert deleted == 1

    async def test_cleanup_keeps_recent(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test that recent published events are not cleaned up."""
        event = TestSampleEvent(aggregate_id=uuid4())
        await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_id = pending[0].id
        await sqlite_outbox_repo.mark_published(outbox_id)

        deleted = await sqlite_outbox_repo.cleanup_published(days=7)
        assert deleted == 0

    async def test_cleanup_published_days_zero_deletes_entry_published_moments_ago(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Regression: `days=0` must delete an entry published moments ago.

        `published_at` is written via `datetime.now(UTC).isoformat()`
        ('T'-separated, with microseconds and a UTC offset). If
        `cleanup_published` compares that string against SQLite's
        `datetime('now', ...)` (space-separated, no microseconds) as raw
        TEXT, 'T' (0x54) sorts after ' ' (0x20) and the comparison never
        matches for a cutoff computed within the same wall-clock second --
        so a `days=0` cleanup silently deletes nothing, no matter how long
        the entry has been published. `cleanup_published` must compare
        against a Python-computed cutoff in the same isoformat() shape.
        """
        event = TestSampleEvent(aggregate_id=uuid4())
        outbox_id = await sqlite_outbox_repo.add_event(event)
        await sqlite_outbox_repo.mark_published(outbox_id)

        deleted = await sqlite_outbox_repo.cleanup_published(days=0)

        assert deleted == 1


class TestSQLiteOutboxRepositoryGetStats:
    """Tests for get_stats method."""

    async def test_get_stats_empty(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test stats when outbox is empty."""
        stats = await sqlite_outbox_repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
        assert stats.avg_retries == 0.0
        assert stats.oldest_pending is None

    async def test_get_stats_with_data(self, sqlite_outbox_repo: SQLiteOutboxRepository) -> None:
        """Test stats with various event states."""
        # Add multiple events
        for _ in range(5):
            event = TestSampleEvent(aggregate_id=uuid4())
            await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        outbox_ids = [p.id for p in pending]

        # Publish 2, fail 1, leave 2 pending
        await sqlite_outbox_repo.mark_published(outbox_ids[0])
        await sqlite_outbox_repo.mark_published(outbox_ids[1])
        await sqlite_outbox_repo.mark_failed(outbox_ids[4], "Error")

        stats = await sqlite_outbox_repo.get_stats()
        assert stats.pending_count == 2
        assert stats.published_count == 2
        assert stats.failed_count == 1
        assert stats.oldest_pending is not None


class TestSQLiteOutboxRepositoryMultipleEvents:
    """Tests for handling multiple events."""

    async def test_multiple_events_same_aggregate(
        self, sqlite_outbox_repo: SQLiteOutboxRepository
    ) -> None:
        """Test adding multiple events for the same aggregate."""
        aggregate_id = uuid4()

        for i in range(3):
            event = TestSampleEvent(
                aggregate_id=aggregate_id,
                data=f"event_{i}",
            )
            await sqlite_outbox_repo.add_event(event)

        pending = await sqlite_outbox_repo.get_pending_events()
        assert len(pending) == 3

        for entry in pending:
            assert entry.aggregate_id == aggregate_id


# ============================================================================
# SQLiteOutboxRepository Tests (standalone in-memory database)
# ============================================================================


class SampleEvent(DomainEvent):
    """Sample event for the standalone SQLite outbox tests below."""

    aggregate_type: str = "SampleAggregate"
    test_field: str = "test_value"


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteOutboxRepositoryStandalone:
    """Tests for SQLiteOutboxRepository using in-memory SQLite database."""

    @pytest.fixture
    async def db_connection(self) -> aiosqlite.Connection:
        """Create an in-memory SQLite database with schema for each test."""
        conn = await aiosqlite.connect(":memory:")

        # Create the event_outbox table (matching the SQLite schema)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS event_outbox (
                id TEXT PRIMARY KEY,
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
        await conn.commit()

        yield conn

        await conn.close()

    @pytest.fixture
    def repo(self, db_connection: aiosqlite.Connection) -> SQLiteOutboxRepository:
        """Create a SQLiteOutboxRepository for each test."""
        return SQLiteOutboxRepository(db_connection)

    @pytest.fixture
    def sample_event(self) -> SampleEvent:
        """Create a sample event for testing."""
        return SampleEvent(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            test_field="test_data",
        )

    @pytest.mark.asyncio
    async def test_add_event(self, repo: SQLiteOutboxRepository, sample_event: SampleEvent):
        """Test adding an event to the outbox."""
        outbox_id = await repo.add_event(sample_event)

        assert outbox_id is not None
        assert isinstance(outbox_id, UUID)

        # Verify event is in pending queue
        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].event_type == "SampleEvent"
        assert pending[0].aggregate_type == "SampleAggregate"

    @pytest.mark.asyncio
    async def test_add_event_serializes_data(
        self, repo: SQLiteOutboxRepository, sample_event: SampleEvent
    ):
        """Test that event data is properly serialized."""
        await repo.add_event(sample_event)

        pending = await repo.get_pending_events()
        event_data = pending[0].event_data

        # Should be a JSON string
        assert isinstance(event_data, str)
        assert "test_data" in event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_order(self, repo: SQLiteOutboxRepository):
        """Test that pending events are returned in FIFO order."""
        # Add events
        events = []
        for i in range(3):
            event = SampleEvent(
                aggregate_id=uuid4(),
                test_field=f"event_{i}",
            )
            events.append(event)
            await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert len(pending) == 3

        # Should be in order of creation (oldest first)
        assert "event_0" in pending[0].event_data
        assert "event_1" in pending[1].event_data
        assert "event_2" in pending[2].event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_limit(self, repo: SQLiteOutboxRepository):
        """Test limiting number of pending events returned."""
        for i in range(5):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        pending = await repo.get_pending_events(limit=3)
        assert len(pending) == 3

    @pytest.mark.asyncio
    async def test_get_pending_events_empty(self, repo: SQLiteOutboxRepository):
        """Test getting pending events when none exist."""
        pending = await repo.get_pending_events()
        assert pending == []

    @pytest.mark.asyncio
    async def test_mark_published(self, repo: SQLiteOutboxRepository, sample_event: SampleEvent):
        """Test marking an event as published."""
        await repo.add_event(sample_event)

        # Get the outbox id from the database
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id

        await repo.mark_published(outbox_id)

        # Should no longer be in pending
        pending = await repo.get_pending_events()
        assert len(pending) == 0

        # Check stats
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 1

    @pytest.mark.asyncio
    async def test_mark_failed(self, repo: SQLiteOutboxRepository, sample_event: SampleEvent):
        """Test marking an event as failed."""
        await repo.add_event(sample_event)

        # Get the outbox id from the database
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id

        await repo.mark_failed(outbox_id, "Connection refused")

        # Should no longer be in pending
        pending = await repo.get_pending_events()
        assert len(pending) == 0

        # Check stats
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.failed_count == 1

    @pytest.mark.asyncio
    async def test_increment_retry(self, repo: SQLiteOutboxRepository, sample_event: SampleEvent):
        """Test incrementing retry count."""
        await repo.add_event(sample_event)

        # Get the outbox id from the database
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id

        await repo.increment_retry(outbox_id, "Temporary error")
        await repo.increment_retry(outbox_id, "Another error")

        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].retry_count == 2

    @pytest.mark.asyncio
    async def test_cleanup_published(
        self, repo: SQLiteOutboxRepository, db_connection: aiosqlite.Connection
    ):
        """Test cleaning up old published events."""
        # Add and publish an event
        event = SampleEvent(aggregate_id=uuid4(), test_field="test")
        await repo.add_event(event)

        # Get the outbox id and mark as published
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id
        await repo.mark_published(outbox_id)

        # Manually update published_at to be old (10 days ago)
        await db_connection.execute(
            """
            UPDATE event_outbox
            SET published_at = datetime('now', '-10 days')
            WHERE id = ?
            """,
            (str(outbox_id),),
        )
        await db_connection.commit()

        # Cleanup events older than 7 days
        deleted = await repo.cleanup_published(days=7)
        assert deleted == 1

    @pytest.mark.asyncio
    async def test_cleanup_keeps_recent_published(self, repo: SQLiteOutboxRepository):
        """Test that recent published events are not cleaned up."""
        event = SampleEvent(aggregate_id=uuid4(), test_field="test")
        await repo.add_event(event)

        # Get the outbox id and mark as published
        pending = await repo.get_pending_events()
        outbox_id = pending[0].id
        await repo.mark_published(outbox_id)

        # Try to cleanup (event is recent, should not be deleted)
        deleted = await repo.cleanup_published(days=7)
        assert deleted == 0

    @pytest.mark.asyncio
    async def test_get_stats(self, repo: SQLiteOutboxRepository):
        """Test getting outbox statistics."""
        # Add some events
        for i in range(5):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        # Get all pending events to get their IDs
        pending = await repo.get_pending_events()
        outbox_ids = [p.id for p in pending]

        # Mark some as published, some as failed
        await repo.mark_published(outbox_ids[0])
        await repo.mark_published(outbox_ids[1])
        await repo.mark_failed(outbox_ids[4], "Error")

        stats = await repo.get_stats()
        assert stats.pending_count == 2
        assert stats.published_count == 2
        assert stats.failed_count == 1
        assert stats.oldest_pending is not None
        assert stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, repo: SQLiteOutboxRepository):
        """Test getting stats when outbox is empty."""
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
        assert stats.oldest_pending is None
        assert stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_avg_retries(self, repo: SQLiteOutboxRepository):
        """Test average retries calculation in stats."""
        # Add events with different retry counts
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        # Get pending events to get their IDs
        pending = await repo.get_pending_events()

        # Increment retries: 0, 2, 4
        for i, p in enumerate(pending):
            outbox_id = p.id
            for _ in range(i * 2):
                await repo.increment_retry(outbox_id, "Error")

        stats = await repo.get_stats()
        # Average of 0, 2, 4 = 2.0
        assert stats.avg_retries == 2.0

    @pytest.mark.asyncio
    async def test_event_with_tenant_id(self, repo: SQLiteOutboxRepository):
        """Test that tenant_id is preserved in outbox."""
        tenant_id = uuid4()
        event = SampleEvent(
            aggregate_id=uuid4(),
            tenant_id=tenant_id,
            test_field="test",
        )

        await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert pending[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_event_without_tenant_id(self, repo: SQLiteOutboxRepository):
        """Test that events without tenant_id are handled."""
        event = SampleEvent(
            aggregate_id=uuid4(),
            tenant_id=None,
            test_field="test",
        )

        await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert pending[0].tenant_id is None

    @pytest.mark.asyncio
    async def test_multiple_events_same_aggregate(self, repo: SQLiteOutboxRepository):
        """Test adding multiple events for the same aggregate."""
        aggregate_id = uuid4()

        for i in range(3):
            event = SampleEvent(
                aggregate_id=aggregate_id,
                test_field=f"event_{i}",
            )
            await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert len(pending) == 3

        # All should have same aggregate_id
        for entry in pending:
            assert entry.aggregate_id == aggregate_id

    @pytest.mark.asyncio
    async def test_pending_event_fields(self, repo: SQLiteOutboxRepository):
        """Test that pending events have all expected fields."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        event = SampleEvent(
            aggregate_id=aggregate_id,
            tenant_id=tenant_id,
            test_field="test",
        )

        await repo.add_event(event)

        pending = await repo.get_pending_events()
        assert len(pending) == 1

        entry = pending[0]
        assert entry.id is not None
        assert entry.event_id == event.event_id
        assert entry.event_type == "SampleEvent"
        assert entry.aggregate_id == aggregate_id
        assert entry.aggregate_type == "SampleAggregate"
        assert entry.tenant_id == tenant_id
        assert entry.event_data is not None
        assert entry.created_at is not None
        assert entry.retry_count == 0


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteOutboxRepositoryStandaloneProtocol:
    """Tests to verify SQLiteOutboxRepository implements the protocol."""

    @pytest.fixture
    async def db_connection(self) -> aiosqlite.Connection:
        """Create an in-memory SQLite database with schema."""
        conn = await aiosqlite.connect(":memory:")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS event_outbox (
                id TEXT PRIMARY KEY,
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
        await conn.commit()

        yield conn

        await conn.close()

    def test_implements_protocol(self, db_connection: aiosqlite.Connection):
        """Test that SQLiteOutboxRepository implements OutboxRepository protocol."""
        repo = SQLiteOutboxRepository(db_connection)
        # The protocol is runtime checkable
        assert isinstance(repo, OutboxRepository)
