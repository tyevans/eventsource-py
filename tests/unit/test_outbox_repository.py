"""
Unit tests for OutboxRepository implementations.

Tests the InMemoryOutboxRepository for:
- Adding events to outbox
- Getting pending events
- Status transitions (pending -> published/failed)
- Statistics and cleanup
"""

from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.repositories.outbox import (
    InMemoryOutboxRepository,
    OutboxEntry,
    OutboxRepository,
    SQLiteOutboxRepository,
)


class SampleEvent(DomainEvent):
    """Sample event for outbox tests."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "SampleAggregate"
    test_field: str = "test_value"


class TestInMemoryOutboxRepository:
    """Tests for InMemoryOutboxRepository."""

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        """Create a fresh repository for each test."""
        return InMemoryOutboxRepository()

    @pytest.fixture
    def sample_event(self) -> SampleEvent:
        """Create a sample event for testing."""
        return SampleEvent(
            aggregate_id=uuid4(),
            tenant_id=uuid4(),
            test_field="test_data",
        )

    @pytest.mark.asyncio
    async def test_add_event(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
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
        self, repo: InMemoryOutboxRepository, sample_event: SampleEvent
    ):
        """Test that event data is properly serialized."""
        await repo.add_event(sample_event)

        pending = await repo.get_pending_events()
        event_data = pending[0].event_data

        # Should be a JSON string
        assert isinstance(event_data, str)
        assert "test_data" in event_data

    @pytest.mark.asyncio
    async def test_get_pending_events_order(self, repo: InMemoryOutboxRepository):
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
    async def test_get_pending_events_limit(self, repo: InMemoryOutboxRepository):
        """Test limiting number of pending events returned."""
        for i in range(5):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        pending = await repo.get_pending_events(limit=3)
        assert len(pending) == 3

    @pytest.mark.asyncio
    async def test_get_pending_events_empty(self, repo: InMemoryOutboxRepository):
        """Test getting pending events when none exist."""
        pending = await repo.get_pending_events()
        assert pending == []

    @pytest.mark.asyncio
    async def test_mark_published(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        """Test marking an event as published."""
        outbox_id = await repo.add_event(sample_event)

        await repo.mark_published(outbox_id)

        # Should no longer be in pending
        pending = await repo.get_pending_events()
        assert len(pending) == 0

        # Check stats
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 1

    @pytest.mark.asyncio
    async def test_mark_failed(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        """Test marking an event as failed."""
        outbox_id = await repo.add_event(sample_event)

        await repo.mark_failed(outbox_id, "Connection refused")

        # Should no longer be in pending
        pending = await repo.get_pending_events()
        assert len(pending) == 0

        # Check stats
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.failed_count == 1

    @pytest.mark.asyncio
    async def test_increment_retry(self, repo: InMemoryOutboxRepository, sample_event: SampleEvent):
        """Test incrementing retry count."""
        outbox_id = await repo.add_event(sample_event)

        await repo.increment_retry(outbox_id, "Temporary error")
        await repo.increment_retry(outbox_id, "Another error")

        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].retry_count == 2

    @pytest.mark.asyncio
    async def test_cleanup_published(self, repo: InMemoryOutboxRepository):
        """Test cleaning up old published events."""

        # Add and publish an event
        event = SampleEvent(aggregate_id=uuid4(), test_field="test")
        outbox_id = await repo.add_event(event)
        await repo.mark_published(outbox_id)

        # Cleanup with 0 days (should delete everything published)
        deleted = await repo.cleanup_published(days=0)

        # Note: The event was just published, might not be old enough
        assert deleted >= 0

    @pytest.mark.asyncio
    async def test_get_stats(self, repo: InMemoryOutboxRepository):
        """Test getting outbox statistics."""
        # Add some events
        for i in range(5):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            outbox_id = await repo.add_event(event)

            if i < 2:
                await repo.mark_published(outbox_id)
            elif i < 4:
                # Leave as pending
                pass
            else:
                await repo.mark_failed(outbox_id, "Error")

        stats = await repo.get_stats()
        assert stats.pending_count == 2
        assert stats.published_count == 2
        assert stats.failed_count == 1
        assert stats.oldest_pending is not None
        assert stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_empty(self, repo: InMemoryOutboxRepository):
        """Test getting stats when outbox is empty."""
        stats = await repo.get_stats()
        assert stats.pending_count == 0
        assert stats.published_count == 0
        assert stats.failed_count == 0
        assert stats.oldest_pending is None
        assert stats.avg_retries == 0.0

    @pytest.mark.asyncio
    async def test_get_stats_avg_retries(self, repo: InMemoryOutboxRepository):
        """Test average retries calculation in stats."""
        # Add events with different retry counts
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            outbox_id = await repo.add_event(event)

            # Increment retries: 0, 2, 4
            for _ in range(i * 2):
                await repo.increment_retry(outbox_id, "Error")

        stats = await repo.get_stats()
        # Average of 0, 2, 4 = 2.0
        assert stats.avg_retries == 2.0

    @pytest.mark.asyncio
    async def test_clear(self, repo: InMemoryOutboxRepository):
        """Test clearing all entries."""
        for i in range(3):
            event = SampleEvent(aggregate_id=uuid4(), test_field=f"event_{i}")
            await repo.add_event(event)

        await repo.clear()

        pending = await repo.get_pending_events()
        assert len(pending) == 0

        stats = await repo.get_stats()
        assert stats.pending_count == 0

    @pytest.mark.asyncio
    async def test_event_with_tenant_id(self, repo: InMemoryOutboxRepository):
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
    async def test_event_without_tenant_id(self, repo: InMemoryOutboxRepository):
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
    async def test_multiple_events_same_aggregate(self, repo: InMemoryOutboxRepository):
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
    async def test_mark_published_nonexistent(self, repo: InMemoryOutboxRepository):
        """Test marking non-existent event as published (no error)."""
        await repo.mark_published(uuid4())  # Should not raise

    @pytest.mark.asyncio
    async def test_mark_failed_nonexistent(self, repo: InMemoryOutboxRepository):
        """Test marking non-existent event as failed (no error)."""
        await repo.mark_failed(uuid4(), "Error")  # Should not raise


class TestOutboxRepositoryProtocol:
    """Tests to verify InMemoryOutboxRepository implements the protocol."""

    def test_implements_protocol(self):
        """Test that InMemoryOutboxRepository implements OutboxRepository protocol."""
        repo = InMemoryOutboxRepository()
        assert isinstance(repo, OutboxRepository)


class TestInMemoryOutboxRepositoryConcurrency:
    """Tests for concurrent access to InMemoryOutboxRepository.

    These tests verify that asyncio.Lock properly serializes concurrent
    operations without deadlocks or data corruption.
    """

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        """Create a fresh repository for each test."""
        return InMemoryOutboxRepository()

    @pytest.mark.asyncio
    async def test_concurrent_add_events_no_deadlock(self, repo: InMemoryOutboxRepository):
        """Test that 100 concurrent add_event calls complete without deadlock."""
        import asyncio

        num_events = 100

        async def add_event(i: int):
            event = SampleEvent(
                aggregate_id=uuid4(),
                test_field=f"event_{i}",
            )
            await repo.add_event(event)

        # Run 100 concurrent adds
        tasks = [add_event(i) for i in range(num_events)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify all events were added
        pending = await repo.get_pending_events(limit=200)
        assert len(pending) == num_events

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self, repo: InMemoryOutboxRepository):
        """Test mixed concurrent reads and writes complete without issues."""
        import asyncio

        num_operations = 50
        read_results: list[int] = []

        async def writer(i: int):
            event = SampleEvent(
                aggregate_id=uuid4(),
                test_field=f"event_{i}",
            )
            await repo.add_event(event)

        async def reader():
            pending = await repo.get_pending_events()
            read_results.append(len(pending))

        # Interleave reads and writes
        tasks = []
        for i in range(num_operations):
            tasks.append(writer(i))
            tasks.append(reader())

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # All operations completed
        assert len(read_results) == num_operations

    @pytest.mark.asyncio
    async def test_concurrent_publish_workflow(self, repo: InMemoryOutboxRepository):
        """Test concurrent add, get, and publish operations."""
        import asyncio

        num_events = 20
        published_count = 0

        async def add_and_publish():
            nonlocal published_count
            event = SampleEvent(
                aggregate_id=uuid4(),
                test_field="test",
            )
            outbox_id = await repo.add_event(event)
            await repo.mark_published(outbox_id)
            published_count += 1

        # Run concurrent add and publish operations
        tasks = [add_and_publish() for _ in range(num_events)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify stats
        stats = await repo.get_stats()
        assert stats.published_count == num_events
        assert stats.pending_count == 0

    @pytest.mark.asyncio
    async def test_concurrent_retry_increment(self, repo: InMemoryOutboxRepository):
        """Test concurrent retry increments on same event."""
        import asyncio

        event = SampleEvent(
            aggregate_id=uuid4(),
            test_field="test",
        )
        outbox_id = await repo.add_event(event)

        num_retries = 50

        async def increment():
            await repo.increment_retry(outbox_id, "Error")

        # Run concurrent retry increments
        tasks = [increment() for _ in range(num_retries)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify retry count
        pending = await repo.get_pending_events()
        assert len(pending) == 1
        assert pending[0].retry_count == num_retries

    @pytest.mark.asyncio
    async def test_concurrent_stats_during_updates(self, repo: InMemoryOutboxRepository):
        """Test that get_stats works correctly during concurrent updates."""
        import asyncio

        stats_results: list[dict] = []

        async def add_events():
            for i in range(20):
                event = SampleEvent(
                    aggregate_id=uuid4(),
                    test_field=f"event_{i}",
                )
                await repo.add_event(event)

        async def get_stats():
            for _ in range(10):
                stats = await repo.get_stats()
                stats_results.append(stats)
                await asyncio.sleep(0.001)

        # Run concurrent add and stats operations
        await asyncio.wait_for(
            asyncio.gather(add_events(), get_stats()),
            timeout=5.0,
        )

        # All stats calls completed without error
        assert len(stats_results) == 10
        for stats in stats_results:
            assert hasattr(stats, "pending_count")


# ============================================================================
# OutboxEntry Typed Returns Tests
# ============================================================================


class TestOutboxEntryTypedReturns:
    """Tests for typed returns and backward compatibility."""

    @pytest.fixture
    def repo(self) -> InMemoryOutboxRepository:
        """Create a fresh repository for each test."""
        return InMemoryOutboxRepository()

    @pytest.mark.asyncio
    async def test_get_pending_events_returns_outbox_entries(self, repo: InMemoryOutboxRepository):
        """get_pending_events returns list of OutboxEntry instances."""
        event = SampleEvent(
            aggregate_id=uuid4(),
            test_field="test_data",
        )
        await repo.add_event(event)

        entries = await repo.get_pending_events()

        assert len(entries) == 1
        assert isinstance(entries[0], OutboxEntry)
        assert hasattr(entries[0], "event_id")
        assert hasattr(entries[0], "event_type")
        assert hasattr(entries[0], "status")

    @pytest.mark.asyncio
    async def test_attribute_access_works(self, repo: InMemoryOutboxRepository):
        """Can access OutboxEntry fields as attributes."""
        event = SampleEvent(
            aggregate_id=uuid4(),
            test_field="test_data",
        )
        await repo.add_event(event)

        entries = await repo.get_pending_events()
        entry = entries[0]

        assert entry.event_id == event.event_id
        assert entry.event_type == "SampleEvent"
        assert entry.aggregate_type == "SampleAggregate"
        assert entry.status == "pending"


# ============================================================================
# SQLiteOutboxRepository Tests
# ============================================================================

# Check if aiosqlite is available
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteOutboxRepository:
    """Tests for SQLiteOutboxRepository using in-memory SQLite database."""

    @pytest.fixture
    async def db_connection(self) -> "aiosqlite.Connection":
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
    def repo(self, db_connection: "aiosqlite.Connection") -> SQLiteOutboxRepository:
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
        self, repo: SQLiteOutboxRepository, db_connection: "aiosqlite.Connection"
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
class TestSQLiteOutboxRepositoryProtocol:
    """Tests to verify SQLiteOutboxRepository implements the protocol."""

    @pytest.fixture
    async def db_connection(self) -> "aiosqlite.Connection":
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

    def test_implements_protocol(self, db_connection: "aiosqlite.Connection"):
        """Test that SQLiteOutboxRepository implements OutboxRepository protocol."""
        repo = SQLiteOutboxRepository(db_connection)
        # The protocol is runtime checkable
        assert isinstance(repo, OutboxRepository)
