"""Integration tests for cross-dialect agreement in SQL DLQ repository."""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from eventsource import SQLDLQRepository, create_async_engine
from eventsource.adapters.sql.schemas import get_schema
from tests.integration.conftest import skip_if_no_postgres_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestSQLDLQRepositoryCrossDialectAgreement:
    """The unified repository must give identical answers on both dialects.

    Runs the same sequence of calls against a PostgreSQL-backed repository
    and a SQLite-backed repository and asserts the results agree, rather
    than testing each dialect in isolation -- this is what would have
    caught the unconditional-dialect-branch defect found in Task 3's
    get_lag_metrics (a dialect branch taken unconditionally where the
    other dialect's path was guarded on a non-empty collection).
    """

    async def test_get_failure_stats_agrees_across_dialects(
        self,
        postgres_dlq_repo: SQLDLQRepository,
        tmp_path: Path,
    ) -> None:
        sqlite_engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dlq_cross.db")
        async with sqlite_engine.begin() as conn:
            raw = await conn.get_raw_connection()
            assert raw.driver_connection is not None
            await raw.driver_connection.executescript(get_schema("dlq", backend="sqlite"))
        sqlite_repo = SQLDLQRepository(sqlite_engine)

        try:
            for repo in (postgres_dlq_repo, sqlite_repo):
                for i in range(3):
                    await repo.add_failed_event(
                        event_id=uuid4(),
                        projection_name=f"Projection{i % 2}",
                        event_type="TestEvent",
                        event_data={},
                        error=ValueError(f"Error {i}"),
                    )
                events = await repo.get_failed_events(limit=1000)
                # Mark one entry retrying on each backend, identically.
                await repo.mark_retrying(events[0].id)

            pg_stats = await postgres_dlq_repo.get_failure_stats()
            sqlite_stats = await sqlite_repo.get_failure_stats()

            assert pg_stats.total_failed == sqlite_stats.total_failed
            assert pg_stats.total_retrying == sqlite_stats.total_retrying
            assert pg_stats.affected_projections == sqlite_stats.affected_projections
            assert (pg_stats.oldest_failure is None) == (sqlite_stats.oldest_failure is None)
        finally:
            await sqlite_engine.dispose()

    async def test_get_failed_events_empty_filters_agree_across_dialects(
        self,
        postgres_dlq_repo: SQLDLQRepository,
        tmp_path: Path,
    ) -> None:
        """Calling get_failed_events with no projection filter must behave
        identically on both dialects -- empty/None filters are exactly
        where an unconditional dialect branch would silently diverge."""
        sqlite_engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dlq_cross2.db")
        async with sqlite_engine.begin() as conn:
            raw = await conn.get_raw_connection()
            assert raw.driver_connection is not None
            await raw.driver_connection.executescript(get_schema("dlq", backend="sqlite"))
        sqlite_repo = SQLDLQRepository(sqlite_engine)

        try:
            for repo in (postgres_dlq_repo, sqlite_repo):
                await repo.add_failed_event(
                    event_id=uuid4(),
                    projection_name="OnlyProjection",
                    event_type="TestEvent",
                    event_data={},
                    error=ValueError("Error"),
                )

            pg_events = await postgres_dlq_repo.get_failed_events(projection_name=None)
            sqlite_events = await sqlite_repo.get_failed_events(projection_name=None)

            assert len(pg_events) == len(sqlite_events) == 1
        finally:
            await sqlite_engine.dispose()
