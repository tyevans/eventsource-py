"""CRUD and transaction tests for SQLDLQRepository."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.sql.dlq import SQLDLQRepository
from eventsource.ports.dlq import DLQRepository

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
        repo = SQLDLQRepository(sqlite_engine)
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
        return SQLDLQRepository(sqlite_engine)

    @pytest.mark.asyncio
    async def test_repository_does_not_commit(self, sqlite_engine):
        """A repository write must roll back with the caller's transaction."""
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
        assert await SQLDLQRepository(sqlite_engine).get_failed_events() == []

    @pytest.mark.asyncio
    async def test_add_failed_event(self, repo: SQLDLQRepository):
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={"key": "value"},
            error=Exception("Test error"),
            retry_count=1,
        )
        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].event_id == event_id
        assert failed_events[0].projection_name == "TestProjection"
        assert failed_events[0].event_type == "TestEvent"
        assert failed_events[0].retry_count == 1
        assert failed_events[0].status == "failed"
        assert "Test error" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_add_failed_event_upsert(self, repo: SQLDLQRepository):
        event_id = uuid4()
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 1"),
            retry_count=1,
        )
        await repo.add_failed_event(
            event_id=event_id,
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error 2"),
            retry_count=2,
        )
        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0].retry_count == 2
        assert "Error 2" in failed_events[0].error_message

    @pytest.mark.asyncio
    async def test_add_failed_event_different_projections(self, repo: SQLDLQRepository):
        event_id = uuid4()
        for proj, msg in (("Projection1", "Error 1"), ("Projection2", "Error 2")):
            await repo.add_failed_event(
                event_id=event_id,
                projection_name=proj,
                event_type="TestEvent",
                event_data={},
                error=Exception(msg),
            )
        failed_events = await repo.get_failed_events()
        assert len(failed_events) == 2

    @pytest.mark.asyncio
    async def test_mark_resolved(self, repo: SQLDLQRepository):
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )
        dlq_id = (await repo.get_failed_events())[0].id
        await repo.mark_resolved(dlq_id, resolved_by="admin@test.com")
        event = await repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert event.status == "resolved"
        assert event.resolved_at is not None
        assert event.resolved_by == "admin@test.com"

    @pytest.mark.asyncio
    async def test_mark_resolved_with_uuid(self, repo: SQLDLQRepository):
        user_id = uuid4()
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )
        dlq_id = (await repo.get_failed_events())[0].id
        await repo.mark_resolved(dlq_id, resolved_by=user_id)
        event = await repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert event.resolved_by == str(user_id)

    @pytest.mark.asyncio
    async def test_mark_retrying(self, repo: SQLDLQRepository):
        await repo.add_failed_event(
            event_id=uuid4(),
            projection_name="TestProjection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Error"),
        )
        dlq_id = (await repo.get_failed_events())[0].id
        await repo.mark_retrying(dlq_id)
        retrying = await repo.get_failed_events(status="retrying")
        assert len(retrying) == 1
        assert retrying[0].status == "retrying"
