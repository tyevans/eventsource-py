"""CRUD and transaction tests for SQLDLQRepository."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.sql.dlq import SQLDLQRepository
from eventsource.ports.dlq import DLQRepository

# Check if aiosqlite is available
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLDLQRepositoryProtocol:
    """Tests to verify SQLDLQRepository implements the DLQRepository protocol."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.adapters.sql.schemas import get_schema

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


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLDLQRepositoryCrud:
    """Tests for SQLDLQRepository CRUD and transaction management."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.adapters.sql.schemas import get_schema

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
        assert event is not None
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
        assert event is not None
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
