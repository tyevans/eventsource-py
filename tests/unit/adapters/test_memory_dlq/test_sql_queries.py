"""Query, statistics, and maintenance tests for SQLDLQRepository."""

from __future__ import annotations

from uuid import uuid4

import pytest
from sqlalchemy import text

from eventsource.adapters.sql.dlq import SQLDLQRepository

try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLDLQRepositoryQueries:
    """Tests for SQLDLQRepository queries, statistics, and maintenance."""

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

    async def _seed_default(self, repo: SQLDLQRepository, **kwargs) -> None:
        defaults = {
            "event_id": uuid4(),
            "projection_name": "TestProjection",
            "event_type": "TestEvent",
            "event_data": {},
            "error": Exception("Error"),
        }
        await repo.add_failed_event(**{**defaults, **kwargs})

    @pytest.mark.asyncio
    async def test_get_failed_events_with_projection_filter(self, repo: SQLDLQRepository):
        for i in (1, 2):
            await self._seed_default(
                repo,
                projection_name=f"Projection{i}",
                event_type=f"Event{i}",
                error=Exception(f"Error {i}"),
            )
        assert len(await repo.get_failed_events()) == 2
        proj1 = await repo.get_failed_events(projection_name="Projection1")
        assert len(proj1) == 1 and proj1[0].projection_name == "Projection1"
        proj2 = await repo.get_failed_events(projection_name="Projection2")
        assert len(proj2) == 1 and proj2[0].projection_name == "Projection2"

    @pytest.mark.asyncio
    async def test_get_failed_events_with_status_filter(self, repo: SQLDLQRepository):
        await self._seed_default(repo)
        events = await repo.get_failed_events()
        await repo.mark_retrying(events[0].id)
        assert len(await repo.get_failed_events(status="failed")) == 0
        assert len(await repo.get_failed_events(status="retrying")) == 1

    @pytest.mark.asyncio
    async def test_get_failed_events_limit(self, repo: SQLDLQRepository):
        for i in range(5):
            await self._seed_default(repo, event_type=f"Event{i}", error=Exception(f"Error {i}"))
        assert len(await repo.get_failed_events(limit=3)) == 3

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id(self, repo: SQLDLQRepository):
        event_id = uuid4()
        await self._seed_default(
            repo,
            event_id=event_id,
            event_data={"test": "data"},
            error=Exception("Test error"),
            retry_count=2,
        )
        dlq_id = (await repo.get_failed_events())[0].id
        event = await repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert (event.id, event.event_id, event.retry_count) == (dlq_id, event_id, 2)

    @pytest.mark.asyncio
    async def test_get_failed_event_by_id_not_found(self, repo: SQLDLQRepository):
        assert await repo.get_failed_event_by_id(999999) is None

    @pytest.mark.asyncio
    async def test_get_failure_stats(self, repo: SQLDLQRepository):
        for i in range(3):
            await self._seed_default(
                repo, projection_name=f"Projection{i % 2}", error=Exception(f"Error {i}")
            )
        stats = await repo.get_failure_stats()
        assert (stats.total_failed, stats.total_retrying, stats.affected_projections) == (3, 0, 2)
        assert stats.oldest_failure is not None

    @pytest.mark.asyncio
    async def test_get_failure_stats_with_retrying(self, repo: SQLDLQRepository):
        for i in range(2):
            await self._seed_default(repo, error=Exception(f"Error {i}"))
        events = await repo.get_failed_events()
        await repo.mark_retrying(events[0].id)
        stats = await repo.get_failure_stats()
        assert (stats.total_failed, stats.total_retrying, stats.affected_projections) == (1, 1, 1)

    @pytest.mark.asyncio
    async def test_get_failure_stats_empty(self, repo: SQLDLQRepository):
        stats = await repo.get_failure_stats()
        assert (stats.total_failed, stats.total_retrying, stats.affected_projections) == (0, 0, 0)
        assert stats.oldest_failure is None

    @pytest.mark.asyncio
    async def test_get_projection_failure_counts(self, repo: SQLDLQRepository):
        for i in range(5):
            await self._seed_default(
                repo,
                projection_name="HighFailure" if i < 3 else "LowFailure",
                error=Exception(f"Error {i}"),
            )
        counts = await repo.get_projection_failure_counts()
        assert len(counts) == 2
        assert (counts[0].projection_name, counts[0].failure_count) == ("HighFailure", 3)
        assert (counts[1].projection_name, counts[1].failure_count) == ("LowFailure", 2)

    @pytest.mark.asyncio
    async def test_delete_resolved_events(self, repo: SQLDLQRepository, sqlite_engine):
        await self._seed_default(repo)
        dlq_id = (await repo.get_failed_events())[0].id
        await repo.mark_resolved(dlq_id, resolved_by="admin")
        sql = "UPDATE dead_letter_queue SET resolved_at = datetime('now', '-35 days') WHERE id = :dlq_id"
        async with sqlite_engine.begin() as conn:
            await conn.execute(text(sql), {"dlq_id": dlq_id})
        assert await repo.delete_resolved_events(older_than_days=30) == 1
        assert await repo.get_failed_event_by_id(dlq_id) is None

    @pytest.mark.asyncio
    async def test_delete_resolved_events_keeps_recent(self, repo: SQLDLQRepository):
        await self._seed_default(repo)
        dlq_id = (await repo.get_failed_events())[0].id
        await repo.mark_resolved(dlq_id, resolved_by="admin")
        assert await repo.delete_resolved_events(older_than_days=30) == 0
        assert await repo.get_failed_event_by_id(dlq_id) is not None

    @pytest.mark.asyncio
    async def test_event_data_serialization(self, repo: SQLDLQRepository):
        event_data = {
            "tenant_id": str(uuid4()),
            "items": ["a", "b", "c"],
            "nested": {"key": "value"},
        }
        await self._seed_default(repo, event_data=event_data)
        assert (await repo.get_failed_events())[0].event_data == event_data

    @pytest.mark.asyncio
    async def test_error_stacktrace_captured(self, repo: SQLDLQRepository):
        try:
            raise RuntimeError("Intentional error for test")
        except RuntimeError as e:
            await self._seed_default(
                repo,
                projection_name="StacktraceProjection",
                event_data={"test": True},
                error=e,
                retry_count=0,
            )
        failed = await repo.get_failed_events(projection_name="StacktraceProjection")
        assert len(failed) == 1 and "RuntimeError" in failed[0].error_stacktrace
        assert "Intentional error for test" in failed[0].error_stacktrace
