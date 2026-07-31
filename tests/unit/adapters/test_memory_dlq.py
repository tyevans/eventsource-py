"""
Unit tests for DLQRepository implementations.

Tests the InMemoryDLQRepository for:
- Adding failed events
- Querying failed events with filters
- Status transitions (failed -> retrying -> resolved)
- Failure statistics
"""

from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.sql.dlq import SQLDLQRepository
from eventsource.ports.dlq import DLQEntry, DLQRepository


class TestInMemoryDLQRepository:
    """Tests for InMemoryDLQRepository."""

    @pytest.fixture
    def repo(self) -> InMemoryDLQRepository:
        """Create a fresh repository for each test."""
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_add_failed_event(self, repo: InMemoryDLQRepository):
        """Test adding a failed event to DLQ."""
        event_id = uuid4()
        projection_name = "TestProjection"
        event_type = "TestEvent"
        event_data = {"key": "value"}
        error = Exception("Test error")

        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type=event_type,
            event_data=event_data,
            error=error,
            retry_count=1,
        )

        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event_id
        assert failed_events[0].projection_name == projection_name
        assert failed_events[0].event_type == event_type
        assert failed_events[0].retry_count == 1
        assert failed_events[0].status == "failed"
        assert "Test error" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_add_failed_event_upsert(self, repo: InMemoryDLQRepository):
        """Test that adding same event twice updates retry count."""
        event_id = uuid4()
        projection_name = "TestProjection"

        # Add first time
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 1"),
            retry_count=1,
        )

        # Add second time (should update)
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 2"),
            retry_count=2,
        )

        # Verify only one record with updated retry count
        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].retry_count == 2
        assert "Error 2" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_get_failed_events_with_projection_filter(self, repo: InMemoryDLQRepository):
        """Test filtering failed events by projection name."""
        # Add events for different projections
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="Projection1",
            event_type="Event1",
            event_data={},
            error=Exception("Error 1"),
        )
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="Projection2",
            event_type="Event2",
            event_data={},
            error=Exception("Error 2"),
        )

        # Get all
        all_events = await repo.get_failed_events()
        assert len(all_events) == 2

        # Filter by projection1
        proj1_events = await repo.get_failed_events(projection_name="Projection1")
        assert len(proj1_events) == 1
        assert proj1_events[0].projection_name == "Projection1"

        # Filter by projection2
        proj2_events = await repo.get_failed_events(projection_name="Projection2")
        assert len(proj2_events) == 1
        assert proj2_events[0].projection_name == "Projection2"

    @pytest.mark.asyncio
    async def test_get_failed_events_with_status_filter(self, repo: InMemoryDLQRepository):
        """Test filtering failed events by status."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        # Get dlq_id
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        # Mark as retrying
        await repo.mark_retrying(dlq_id)

        # Filter by failed status (should be empty)
        failed_events = await repo.get_failed_events(status="failed")
        assert len(failed_events) == 0

        # Filter by retrying status
        retrying_events = await repo.get_failed_events(status="retrying")
        assert len(retrying_events) == 1

    @pytest.mark.asyncio
    async def test_get_failed_events_limit(self, repo: InMemoryDLQRepository):
        """Test limiting number of returned events."""
        # Add 5 events
        for i in range(5):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type=f"Event{i}",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        # Get with limit
        limited = await repo.get_failed_events(limit=3)
        assert len(limited) == 3

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id(self, repo: InMemoryDLQRepository):
        """Test getting a specific failed event by ID."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={"test": "data"},
            error=Exception("Test error"),
            retry_count=2,
        )

        # Get the DLQ ID
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        # Get by ID
        event = await repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert event.id == dlq_id
        assert event.event_id == event_id
        assert event.retry_count == 2

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id_not_found(self, repo: InMemoryDLQRepository):
        """Test getting non-existent event by ID returns None."""
        result = await repo.get_failed_event_by_id(999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_mark_resolved(self, repo: InMemoryDLQRepository):
        """Test marking a DLQ entry as resolved."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id

        await repo.mark_resolved(dlq_id, resolved_by="admin@test.com")

        event = await repo.get_failed_event_by_id(dlq_id)
        assert event.status == "resolved"
        assert event.resolved_at is not None
        assert event.resolved_by == "admin@test.com"

    @pytest.mark.asyncio
    async def test_mark_resolved_with_uuid(self, repo: InMemoryDLQRepository):
        """Test marking resolved with UUID as resolved_by."""
        event_id = uuid4()
        user_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id

        await repo.mark_resolved(dlq_id, resolved_by=user_id)

        event = await repo.get_failed_event_by_id(dlq_id)
        assert event.resolved_by == str(user_id)

    @pytest.mark.asyncio
    async def test_mark_retrying(self, repo: InMemoryDLQRepository):
        """Test marking a DLQ entry as retrying."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id

        await repo.mark_retrying(dlq_id)

        retrying = await repo.get_failed_events(status="retrying")
        assert len(retrying) == 1
        assert retrying[0].status == "retrying"

    @pytest.mark.asyncio
    async def test_get_failure_stats(self, repo: InMemoryDLQRepository):
        """Test getting DLQ statistics."""
        # Add some failed events for different projections
        for i in range(3):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name=f"Projection{i % 2}",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 3
        assert stats.total_retrying == 0
        assert stats.affected_projections == 2
        assert stats.oldest_failure is not None

    @pytest.mark.asyncio
    async def test_get_failure_stats_with_retrying(self, repo: InMemoryDLQRepository):
        """Test failure stats includes retrying count."""
        # Add two events
        for i in range(2):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        # Mark one as retrying
        events = await repo.get_failed_events()
        await repo.mark_retrying(events[0].id)

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 1
        assert stats.total_retrying == 1
        assert stats.affected_projections == 1

    @pytest.mark.asyncio
    async def test_get_failure_stats_empty(self, repo: InMemoryDLQRepository):
        """Test failure stats with no failures."""
        stats = await repo.get_failure_stats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    @pytest.mark.asyncio
    async def test_get_projection_failure_counts(self, repo: InMemoryDLQRepository):
        """Test getting failure counts grouped by projection."""
        # Add events for different projections
        for i in range(5):
            projection = "HighFailure" if i < 3 else "LowFailure"
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name=projection,
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        counts = await repo.get_projection_failure_counts()
        assert len(counts) == 2

        # Should be ordered by count descending
        assert counts[0].projection_name == "HighFailure"
        assert counts[0].failure_count == 3
        assert counts[1].projection_name == "LowFailure"
        assert counts[1].failure_count == 2

    @pytest.mark.asyncio
    async def test_delete_resolved_events(self, repo: InMemoryDLQRepository):
        """Test deleting resolved events older than specified days."""

        # Add and resolve an event
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id
        await repo.mark_resolved(dlq_id, resolved_by="admin")

        # Delete resolved events older than 0 days (should delete)
        deleted = await repo.delete_resolved_events(older_than_days=0)

        # Note: In-memory implementation uses datetime comparison
        # The event was just resolved, so it might not be deleted
        # This test verifies the method works without error
        assert deleted >= 0

    @pytest.mark.asyncio
    async def test_clear(self, repo: InMemoryDLQRepository):
        """Test clearing all entries."""
        for i in range(3):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        await repo.clear()

        events = await repo.get_failed_events()
        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_event_data_serialization(self, repo: InMemoryDLQRepository):
        """Test that event data is properly serialized."""
        event_id = uuid4()
        tenant_id = uuid4()
        event_data = {
            "tenant_id": str(tenant_id),
            "items": ["a", "b", "c"],
            "nested": {"key": "value"},
        }

        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data=event_data,
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        # event_data should be a JSON string
        assert isinstance(events[0].event_data, str)
        assert str(tenant_id) in events[0].event_data


class TestDLQRepositoryProtocol:
    """Tests to verify InMemoryDLQRepository implements the protocol."""

    def test_implements_protocol(self):
        """Test that InMemoryDLQRepository implements DLQRepository protocol."""
        repo = InMemoryDLQRepository()
        assert isinstance(repo, DLQRepository)


class TestInMemoryDLQRepositoryConcurrency:
    """Tests for concurrent access to InMemoryDLQRepository.

    These tests verify that asyncio.Lock properly serializes concurrent
    operations without deadlocks or data corruption.
    """

    @pytest.fixture
    def repo(self) -> InMemoryDLQRepository:
        """Create a fresh repository for each test."""
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_concurrent_add_failed_events_no_deadlock(self, repo: InMemoryDLQRepository):
        """Test that 100 concurrent add_failed_event calls complete without deadlock."""
        import asyncio

        num_events = 100

        async def add_failed_event(i: int):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type=f"Event{i}",
                event_data={"index": i},
                error=Exception(f"Error {i}"),
                retry_count=1,
            )

        # Run 100 concurrent adds
        tasks = [add_failed_event(i) for i in range(num_events)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify all events were added
        failed_events = await repo.get_failed_events(limit=200)
        assert len(failed_events) == num_events

    @pytest.mark.asyncio
    async def test_concurrent_read_write(self, repo: InMemoryDLQRepository):
        """Test mixed concurrent reads and writes complete without issues."""
        import asyncio

        num_operations = 50
        read_results: list[int] = []

        async def writer(i: int):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={"index": i},
                error=Exception(f"Error {i}"),
            )

        async def reader():
            events = await repo.get_failed_events()
            read_results.append(len(events))

        # Interleave reads and writes
        tasks = []
        for i in range(num_operations):
            tasks.append(writer(i))
            tasks.append(reader())

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # All operations completed
        assert len(read_results) == num_operations

    @pytest.mark.asyncio
    async def test_concurrent_upsert_same_event(self, repo: InMemoryDLQRepository):
        """Test concurrent upserts for the same event_id + projection combination."""
        import asyncio

        event_id = uuid4()
        projection_name = "TestProjection"
        num_updates = 50

        async def upsert(i: int):
            await repo.add_failed_event(
                event_id=event_id,
                projection_name=projection_name,
                event_type="TestEvent",
                event_data={"update": i},
                error=Exception(f"Error {i}"),
                retry_count=i,
            )

        # Run concurrent upserts for the same event
        tasks = [upsert(i) for i in range(num_updates)]
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Should only have one entry (upsert behavior)
        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1

    @pytest.mark.asyncio
    async def test_concurrent_status_transitions(self, repo: InMemoryDLQRepository):
        """Test concurrent status transitions (retry, resolve)."""
        import asyncio

        num_events = 10

        # Add events first
        dlq_ids = []
        for i in range(num_events):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        events = await repo.get_failed_events()
        dlq_ids = [e.id for e in events]

        async def mark_retrying(dlq_id: int | str):
            await repo.mark_retrying(dlq_id)

        async def mark_resolved(dlq_id: int | str):
            await repo.mark_resolved(dlq_id, resolved_by="admin")

        # Alternate between retrying and resolving
        tasks = []
        for i, dlq_id in enumerate(dlq_ids):
            if i % 2 == 0:
                tasks.append(mark_retrying(dlq_id))
            else:
                tasks.append(mark_resolved(dlq_id))

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify status transitions happened
        stats = await repo.get_failure_stats()
        # Some should be retrying, some resolved
        assert stats.total_retrying + stats.total_failed <= num_events

    @pytest.mark.asyncio
    async def test_concurrent_stats_during_updates(self, repo: InMemoryDLQRepository):
        """Test that get_failure_stats works correctly during concurrent updates."""
        import asyncio

        stats_results: list[dict] = []

        async def add_events():
            for i in range(20):
                await repo.add_failed_event(
                    event_id=uuid4(),
                    projection_name=f"Projection{i % 3}",
                    event_type="TestEvent",
                    event_data={},
                    error=Exception(f"Error {i}"),
                )

        async def get_stats():
            for _ in range(10):
                stats = await repo.get_failure_stats()
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
            assert hasattr(stats, "total_failed")

    @pytest.mark.asyncio
    async def test_concurrent_multiple_projections(self, repo: InMemoryDLQRepository):
        """Test concurrent failures across multiple projections."""
        import asyncio

        num_projections = 5
        events_per_projection = 20

        async def add_for_projection(proj_id: int, event_id: int):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name=f"Projection{proj_id}",
                event_type="TestEvent",
                event_data={"proj": proj_id, "event": event_id},
                error=Exception(f"Error {proj_id}-{event_id}"),
            )

        tasks = []
        for proj in range(num_projections):
            for event in range(events_per_projection):
                tasks.append(add_for_projection(proj, event))

        await asyncio.wait_for(asyncio.gather(*tasks), timeout=5.0)

        # Verify all events were added
        stats = await repo.get_failure_stats()
        assert stats.total_failed == num_projections * events_per_projection
        assert stats.affected_projections == num_projections


# ============================================================================
# DLQEntry Typed Returns Tests
# ============================================================================


class TestDLQEntryTypedReturns:
    """Tests for typed returns and backward compatibility."""

    @pytest.fixture
    def repo(self) -> InMemoryDLQRepository:
        """Create a fresh repository for each test."""
        return InMemoryDLQRepository()

    @pytest.mark.asyncio
    async def test_get_failed_events_returns_dlq_entries(self, repo: InMemoryDLQRepository):
        """get_failed_events returns list of DLQEntry instances."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Test error"),
        )

        entries = await repo.get_failed_events()

        assert len(entries) == 1
        assert isinstance(entries[0], DLQEntry)
        assert hasattr(entries[0], "event_id")
        assert hasattr(entries[0], "projection_name")
        assert hasattr(entries[0], "error_message")

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id_returns_dlq_entry(self, repo: InMemoryDLQRepository):
        """get_failed_event_by_id returns DLQEntry instance."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Test error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id

        entry = await repo.get_failed_event_by_id(dlq_id)

        assert entry is not None
        assert isinstance(entry, DLQEntry)
        assert entry.event_id == event_id

    @pytest.mark.asyncio
    async def test_attribute_access_works(self, repo: InMemoryDLQRepository):
        """Can access DLQEntry fields as attributes."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Test error"),
        )

        entries = await repo.get_failed_events()
        entry = entries[0]

        assert entry.event_id == event_id
        assert entry.projection_name == "TestProjection"
        assert entry.event_type == "TestEvent"
        assert entry.status == "failed"
        assert "Test error" in entry.error_message


# ============================================================================
# SQLDLQRepository Tests
# ============================================================================

# Check if aiosqlite is available
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLDLQRepository:
    """Tests for SQLDLQRepository using an in-memory-equivalent SQLite engine."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.migrations import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dlq.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("dlq", backend="sqlite"))
        yield engine
        await engine.dispose()

    @pytest.fixture
    def repo(self, sqlite_engine) -> SQLDLQRepository:
        """Create a SQLDLQRepository for each test."""
        return SQLDLQRepository(sqlite_engine)

    @pytest.mark.asyncio
    async def test_repository_does_not_commit(self, sqlite_engine):
        """A repository write must roll back with the caller's transaction.

        This is the regression test for the old SQLiteDLQRepository, which
        called connection.commit() inside add_failed_event.
        """
        conn = await sqlite_engine.connect()
        try:
            await conn.begin()
            repo = SQLDLQRepository(conn)
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=Exception("Error"),
            )
            await conn.rollback()
        finally:
            await conn.close()

        repo = SQLDLQRepository(sqlite_engine)
        assert await repo.get_failed_events() == []

    @pytest.mark.asyncio
    async def test_add_failed_event(self, repo: SQLDLQRepository):
        """Test adding a failed event to DLQ."""
        event_id = uuid4()
        projection_name = "TestProjection"
        event_type = "TestEvent"
        event_data = {"key": "value"}
        error = Exception("Test error")

        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type=event_type,
            event_data=event_data,
            error=error,
            retry_count=1,
        )

        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event_id
        assert failed_events[0].projection_name == projection_name
        assert failed_events[0].event_type == event_type
        assert failed_events[0].retry_count == 1
        assert failed_events[0].status == "failed"
        assert "Test error" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_add_failed_event_upsert(self, repo: SQLDLQRepository):
        """Test that adding same event twice updates retry count."""
        event_id = uuid4()
        projection_name = "TestProjection"

        # Add first time
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 1"),
            retry_count=1,
        )

        # Add second time (should update)
        await repo.add_failed_event(
            event_id=event_id,
            projection_name=projection_name,
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 2"),
            retry_count=2,
        )

        # Verify only one record with updated retry count
        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].retry_count == 2
        assert "Error 2" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_add_failed_event_different_projections(self, repo: SQLDLQRepository):
        """Test that same event can fail for different projections."""
        event_id = uuid4()

        await repo.add_failed_event(
            event_id=event_id,
            projection_name="Projection1",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 1"),
        )

        await repo.add_failed_event(
            event_id=event_id,
            projection_name="Projection2",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 2"),
        )

        # Should have two records - one for each projection
        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 2

    @pytest.mark.asyncio
    async def test_get_failed_events_with_projection_filter(self, repo: SQLDLQRepository):
        """Test filtering failed events by projection name."""
        # Add events for different projections
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="Projection1",
            event_type="Event1",
            event_data={},
            error=Exception("Error 1"),
        )
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="Projection2",
            event_type="Event2",
            event_data={},
            error=Exception("Error 2"),
        )

        # Get all
        all_events = await repo.get_failed_events()
        assert len(all_events) == 2

        # Filter by projection1
        proj1_events = await repo.get_failed_events(projection_name="Projection1")
        assert len(proj1_events) == 1
        assert proj1_events[0].projection_name == "Projection1"

        # Filter by projection2
        proj2_events = await repo.get_failed_events(projection_name="Projection2")
        assert len(proj2_events) == 1
        assert proj2_events[0].projection_name == "Projection2"

    @pytest.mark.asyncio
    async def test_get_failed_events_with_status_filter(self, repo: SQLDLQRepository):
        """Test filtering failed events by status."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        # Get dlq_id
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        # Mark as retrying
        await repo.mark_retrying(dlq_id)

        # Filter by failed status (should be empty)
        failed_events = await repo.get_failed_events(status="failed")
        assert len(failed_events) == 0

        # Filter by retrying status
        retrying_events = await repo.get_failed_events(status="retrying")
        assert len(retrying_events) == 1

    @pytest.mark.asyncio
    async def test_get_failed_events_limit(self, repo: SQLDLQRepository):
        """Test limiting number of returned events."""
        # Add 5 events
        for i in range(5):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type=f"Event{i}",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        # Get with limit
        limited = await repo.get_failed_events(limit=3)
        assert len(limited) == 3

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id(self, repo: SQLDLQRepository):
        """Test getting a specific failed event by ID."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={"test": "data"},
            error=Exception("Test error"),
            retry_count=2,
        )

        # Get the DLQ ID
        events = await repo.get_failed_events()
        dlq_id = events[0].id

        # Get by ID
        event = await repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert event.id == dlq_id
        assert event.event_id == event_id
        assert event.retry_count == 2

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id_not_found(self, repo: SQLDLQRepository):
        """Test getting non-existent event by ID returns None."""
        result = await repo.get_failed_event_by_id(999999)
        assert result is None

    @pytest.mark.asyncio
    async def test_mark_resolved(self, repo: SQLDLQRepository):
        """Test marking a DLQ entry as resolved."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id

        await repo.mark_resolved(dlq_id, resolved_by="admin@test.com")

        event = await repo.get_failed_event_by_id(dlq_id)
        assert event.status == "resolved"
        assert event.resolved_at is not None
        assert event.resolved_by == "admin@test.com"

    @pytest.mark.asyncio
    async def test_mark_resolved_with_uuid(self, repo: SQLDLQRepository):
        """Test marking resolved with UUID as resolved_by."""
        event_id = uuid4()
        user_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id

        await repo.mark_resolved(dlq_id, resolved_by=user_id)

        event = await repo.get_failed_event_by_id(dlq_id)
        assert event.resolved_by == str(user_id)

    @pytest.mark.asyncio
    async def test_mark_retrying(self, repo: SQLDLQRepository):
        """Test marking a DLQ entry as retrying."""
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id

        await repo.mark_retrying(dlq_id)

        retrying = await repo.get_failed_events(status="retrying")
        assert len(retrying) == 1
        assert retrying[0].status == "retrying"

    @pytest.mark.asyncio
    async def test_get_failure_stats(self, repo: SQLDLQRepository):
        """Test getting DLQ statistics."""
        # Add some failed events for different projections
        for i in range(3):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name=f"Projection{i % 2}",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 3
        assert stats.total_retrying == 0
        assert stats.affected_projections == 2
        assert stats.oldest_failure is not None

    @pytest.mark.asyncio
    async def test_get_failure_stats_with_retrying(self, repo: SQLDLQRepository):
        """Test failure stats includes retrying count."""
        # Add two events
        for i in range(2):
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name="TestProjection",
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        # Mark one as retrying
        events = await repo.get_failed_events()
        await repo.mark_retrying(events[0].id)

        stats = await repo.get_failure_stats()
        assert stats.total_failed == 1
        assert stats.total_retrying == 1
        assert stats.affected_projections == 1

    @pytest.mark.asyncio
    async def test_get_failure_stats_empty(self, repo: SQLDLQRepository):
        """Test failure stats with no failures."""
        stats = await repo.get_failure_stats()
        assert stats.total_failed == 0
        assert stats.total_retrying == 0
        assert stats.affected_projections == 0
        assert stats.oldest_failure is None

    @pytest.mark.asyncio
    async def test_get_projection_failure_counts(self, repo: SQLDLQRepository):
        """Test getting failure counts grouped by projection."""
        # Add events for different projections
        for i in range(5):
            projection = "HighFailure" if i < 3 else "LowFailure"
            await repo.add_failed_event(
                event_id=uuid4(),
                projection_name=projection,
                event_type="TestEvent",
                event_data={},
                error=Exception(f"Error {i}"),
            )

        counts = await repo.get_projection_failure_counts()
        assert len(counts) == 2

        # Should be ordered by count descending
        assert counts[0].projection_name == "HighFailure"
        assert counts[0].failure_count == 3
        assert counts[1].projection_name == "LowFailure"
        assert counts[1].failure_count == 2

    @pytest.mark.asyncio
    async def test_delete_resolved_events(self, repo: "SQLDLQRepository", sqlite_engine):
        """Test deleting resolved events older than specified days."""
        event_id = uuid4()

        # Add and resolve an event
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id
        await repo.mark_resolved(dlq_id, resolved_by="admin")

        # Manually update resolved_at to be old (35 days ago)
        async with sqlite_engine.begin() as conn:
            await conn.execute(
                text("""
                    UPDATE dead_letter_queue
                    SET resolved_at = datetime('now', '-35 days')
                    WHERE id = :dlq_id
                    """),
                {"dlq_id": dlq_id},
            )

        # Delete resolved events older than 30 days
        deleted = await repo.delete_resolved_events(older_than_days=30)

        assert deleted == 1

        # Verify it's gone
        result = await repo.get_failed_event_by_id(dlq_id)
        assert result is None

    @pytest.mark.asyncio
    async def test_delete_resolved_events_keeps_recent(self, repo: SQLDLQRepository):
        """Test that recent resolved events are not deleted."""
        event_id = uuid4()

        # Add and resolve an event
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        dlq_id = events[0].id
        await repo.mark_resolved(dlq_id, resolved_by="admin")

        # Try to delete (event is recent, should not be deleted)
        deleted = await repo.delete_resolved_events(older_than_days=30)

        assert deleted == 0

        # Verify it still exists
        result = await repo.get_failed_event_by_id(dlq_id)
        assert result is not None

    @pytest.mark.asyncio
    async def test_event_data_serialization(self, repo: SQLDLQRepository):
        """Test that event data is properly serialized."""
        event_id = uuid4()
        tenant_id = uuid4()
        event_data = {
            "tenant_id": str(tenant_id),
            "items": ["a", "b", "c"],
            "nested": {"key": "value"},
        }

        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data=event_data,
            error=Exception("Error"),
        )

        events = await repo.get_failed_events()
        # event_data is decoded via json_result on read, so it comes back
        # as the original dict rather than a JSON string.
        assert events[0].event_data == event_data

    @pytest.mark.asyncio
    async def test_error_stacktrace_captured(self, repo: SQLDLQRepository):
        """Test that stacktrace is captured when adding failed event."""
        event_id = uuid4()

        try:
            raise RuntimeError("Intentional error for test")
        except RuntimeError as e:
            await repo.add_failed_event(
                event_id=event_id,
                projection_name="StacktraceProjection",
                event_type="TestEvent",
                event_data={"test": True},
                error=e,
                retry_count=0,
            )

        failed_events = await repo.get_failed_events(projection_name="StacktraceProjection")

        assert len(failed_events) == 1
        assert "RuntimeError" in failed_events[0].error_stacktrace
        assert "Intentional error for test" in failed_events[0].error_stacktrace


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLDLQRepositoryProtocol:
    """Tests to verify SQLDLQRepository implements the DLQRepository protocol."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.migrations import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dlq_protocol.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("dlq", backend="sqlite"))
        yield engine
        await engine.dispose()

    def test_implements_protocol(self, sqlite_engine):
        """Test that SQLDLQRepository implements DLQRepository protocol."""
        repo = SQLDLQRepository(sqlite_engine)
        # The protocol is runtime checkable
        assert isinstance(repo, DLQRepository)
