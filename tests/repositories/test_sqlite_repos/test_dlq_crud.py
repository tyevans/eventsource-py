"""Tests for SQLDLQRepository CRUD operations against SQLite."""

from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

import pytest

from tests.conftest import AIOSQLITE_AVAILABLE, skip_if_no_aiosqlite

if AIOSQLITE_AVAILABLE:
    from eventsource.adapters.sql.dlq import SQLDLQRepository
    from eventsource.ports.dlq import DLQRepository

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]


async def _add_event(
    repo: SQLDLQRepository,
    *,
    event_id: UUID | None = None,
    projection_name: str = "TestProjection",
    event_type: str = "TestEvent",
    event_data: dict[str, Any] | None = None,
    error: str = "Error",
    retry_count: int = 1,
) -> UUID:
    eid = event_id or uuid4()
    await repo.add_failed_event(
        event_id=eid,
        projection_name=projection_name,
        event_type=event_type,
        event_data=event_data or {},
        error=Exception(error),
        retry_count=retry_count,
    )
    return eid


class TestSQLDLQRepositoryProtocol:
    def test_implements_protocol(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        assert isinstance(sqlite_dlq_repo, DLQRepository)


class TestSQLDLQRepositoryAddFailedEvent:
    async def test_add_failed_event(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        event_id = await _add_event(
            sqlite_dlq_repo, event_data={"key": "value"}, error="Test error", retry_count=1
        )
        failed = await sqlite_dlq_repo.get_failed_events()
        assert len(failed) == 1
        assert (failed[0].event_id, failed[0].projection_name) == (event_id, "TestProjection")
        assert failed[0].retry_count == 1 and "Test error" in failed[0].error_message

    async def test_add_failed_event_upsert(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        event_id = await _add_event(sqlite_dlq_repo, error="Error 1", retry_count=1)
        await _add_event(sqlite_dlq_repo, event_id=event_id, error="Error 2", retry_count=2)
        failed = await sqlite_dlq_repo.get_failed_events()
        assert len(failed) == 1
        assert failed[0].retry_count == 2 and "Error 2" in failed[0].error_message

    async def test_add_failed_event_different_projections(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        event_id = uuid4()
        for proj, err in [("Projection1", "Error 1"), ("Projection2", "Error 2")]:
            await _add_event(sqlite_dlq_repo, event_id=event_id, projection_name=proj, error=err)
        assert len(await sqlite_dlq_repo.get_failed_events()) == 2


class TestSQLDLQRepositoryGetFailedEvents:
    async def test_get_failed_events_empty(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        assert await sqlite_dlq_repo.get_failed_events() == []

    async def test_get_failed_events_with_projection_filter(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        for proj, evt, err in [
            ("Projection1", "Event1", "Error 1"),
            ("Projection2", "Event2", "Error 2"),
        ]:
            await _add_event(sqlite_dlq_repo, projection_name=proj, event_type=evt, error=err)
        filtered = await sqlite_dlq_repo.get_failed_events(projection_name="Projection1")
        assert len(filtered) == 1 and filtered[0].projection_name == "Projection1"

    async def test_get_failed_events_with_status_filter(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        await _add_event(sqlite_dlq_repo)
        events = await sqlite_dlq_repo.get_failed_events()
        await sqlite_dlq_repo.mark_retrying(events[0].id)

        assert len(await sqlite_dlq_repo.get_failed_events(status="failed")) == 0
        assert len(await sqlite_dlq_repo.get_failed_events(status="retrying")) == 1

    async def test_get_failed_events_limit(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        for i in range(5):
            await _add_event(sqlite_dlq_repo, event_type=f"Event{i}", error=f"Error {i}")
        assert len(await sqlite_dlq_repo.get_failed_events(limit=3)) == 3


class TestSQLDLQRepositoryGetFailedEventById:
    async def test_get_failed_event_by_id(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        event_id = await _add_event(
            sqlite_dlq_repo, event_data={"test": "data"}, error="Test error", retry_count=2
        )
        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id
        event = await sqlite_dlq_repo.get_failed_event_by_id(dlq_id)
        assert event is not None
        assert (event.id, event.event_id, event.retry_count) == (dlq_id, event_id, 2)

    async def test_get_failed_event_by_id_not_found(
        self, sqlite_dlq_repo: SQLDLQRepository
    ) -> None:
        assert await sqlite_dlq_repo.get_failed_event_by_id(999999) is None


class TestSQLDLQRepositoryMarkResolved:
    async def test_mark_resolved(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        await _add_event(sqlite_dlq_repo)
        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id
        await sqlite_dlq_repo.mark_resolved(dlq_id, resolved_by="admin@test.com")
        event = await sqlite_dlq_repo.get_failed_event_by_id(dlq_id)
        assert event.status == "resolved" and event.resolved_at is not None
        assert event.resolved_by == "admin@test.com"

    async def test_mark_resolved_with_uuid(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        user_id = uuid4()
        await _add_event(sqlite_dlq_repo)
        events = await sqlite_dlq_repo.get_failed_events()
        dlq_id = events[0].id
        await sqlite_dlq_repo.mark_resolved(dlq_id, resolved_by=user_id)
        event = await sqlite_dlq_repo.get_failed_event_by_id(dlq_id)
        assert event.resolved_by == str(user_id)


class TestSQLDLQRepositoryMarkRetrying:
    async def test_mark_retrying(self, sqlite_dlq_repo: SQLDLQRepository) -> None:
        await _add_event(sqlite_dlq_repo)
        events = await sqlite_dlq_repo.get_failed_events()
        await sqlite_dlq_repo.mark_retrying(events[0].id)
        retrying = await sqlite_dlq_repo.get_failed_events(status="retrying")
        assert len(retrying) == 1 and retrying[0].status == "retrying"
