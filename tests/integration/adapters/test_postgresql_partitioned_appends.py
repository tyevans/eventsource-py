"""Append guarantees on the partitioned events schema (events_partitioned.sql).

The partitioned table cannot enforce event_id uniqueness or stream version
uniqueness on its own, because the timestamp partition key must be part of
every unique constraint. These tests check the two guarantees the adapter
restores: the `event_ids` side table and the per-stream advisory lock.

Each event uses a different occurred_at. Under the old schema, rows that
differ only by timestamp were not rejected.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from eventsource.adapters.postgresql import PostgreSQLEventStore
from eventsource.adapters.sql.schemas import get_schema
from eventsource.domain.event_registry import EventRegistry
from eventsource.domain.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports import ExpectedVersion
from eventsource.testing.conformance_ports._fixtures import ConformanceEvent, make_stream

from ..conftest import skip_if_no_postgres_infra

pytestmark = [pytest.mark.integration, pytest.mark.postgres, skip_if_no_postgres_infra]

_BASE_TIME = datetime(2026, 1, 15, 12, 0, tzinfo=UTC)


def _registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(ConformanceEvent)
    return registry


async def _apply_partitioned_schema(url: str) -> AsyncEngine:
    """Drop and recreate the events table from the partitioned template."""
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS events, event_ids CASCADE"))
        await conn.execute(text("DROP SEQUENCE IF EXISTS events_global_position_seq"))
    async with engine.connect() as conn:
        raw = await conn.get_raw_connection()
        driver_connection = raw.driver_connection
        assert driver_connection is not None
        await driver_connection.execute(get_schema("events_partitioned"))
        await conn.commit()
    return engine


def _event(aggregate_id, *, event_id=None, seconds: int = 0) -> ConformanceEvent:
    kwargs = {"event_id": event_id} if event_id is not None else {}
    return ConformanceEvent(
        aggregate_id=aggregate_id,
        payload="p",
        occurred_at=_BASE_TIME + timedelta(seconds=seconds),
        **kwargs,
    )


class TestPartitionedSameVersion:
    async def test_concurrent_appends_at_same_version_yield_one_winner(
        self, ports_postgres_connection_url: str
    ) -> None:
        engine = await _apply_partitioned_schema(ports_postgres_connection_url)
        try:
            store = PostgreSQLEventStore(engine, event_registry=_registry())
            stream = make_stream(aggregate_id=uuid4())
            first = _event(stream.aggregate_id, seconds=1)
            second = _event(stream.aggregate_id, seconds=2)

            results = await asyncio.gather(
                store.append(stream, [first], ExpectedVersion.no_stream()),
                store.append(stream, [second], ExpectedVersion.no_stream()),
                return_exceptions=True,
            )

            successes = [r for r in results if not isinstance(r, BaseException)]
            failures = [r for r in results if isinstance(r, BaseException)]
            assert len(successes) == 1
            assert len(failures) == 1
            assert isinstance(failures[0], OptimisticLockError)
            assert await store.get_stream_version(stream) == 1
        finally:
            await engine.dispose()


class TestPartitionedEventIdUniqueness:
    async def test_same_event_id_with_different_timestamp_raises_duplicate(
        self, ports_postgres_connection_url: str
    ) -> None:
        engine = await _apply_partitioned_schema(ports_postgres_connection_url)
        try:
            store = PostgreSQLEventStore(engine, event_registry=_registry())
            event_id = uuid4()
            first_stream = make_stream(aggregate_id=uuid4())
            second_stream = make_stream(aggregate_id=uuid4())
            await store.append(
                first_stream,
                [_event(first_stream.aggregate_id, event_id=event_id, seconds=1)],
                ExpectedVersion.no_stream(),
            )

            with pytest.raises(DuplicateEventError):
                await store.append(
                    second_stream,
                    [_event(second_stream.aggregate_id, event_id=event_id, seconds=2)],
                    ExpectedVersion.no_stream(),
                )

            assert await store.get_stream_version(second_stream) == 0
        finally:
            await engine.dispose()

    async def test_concurrent_same_event_id_in_two_streams_stores_one(
        self, ports_postgres_connection_url: str
    ) -> None:
        engine = await _apply_partitioned_schema(ports_postgres_connection_url)
        try:
            store = PostgreSQLEventStore(engine, event_registry=_registry())
            event_id = uuid4()
            stream_a = make_stream(aggregate_id=uuid4())
            stream_b = make_stream(aggregate_id=uuid4())

            results = await asyncio.gather(
                store.append(
                    stream_a,
                    [_event(stream_a.aggregate_id, event_id=event_id, seconds=1)],
                    ExpectedVersion.no_stream(),
                ),
                store.append(
                    stream_b,
                    [_event(stream_b.aggregate_id, event_id=event_id, seconds=2)],
                    ExpectedVersion.no_stream(),
                ),
                return_exceptions=True,
            )

            successes = [r for r in results if not isinstance(r, BaseException)]
            duplicates = [r for r in results if isinstance(r, DuplicateEventError)]
            assert len(successes) == 1
            assert len(duplicates) == 1
            async with engine.connect() as conn:
                count = await conn.scalar(
                    text("SELECT COUNT(*) FROM events WHERE event_id = :event_id"),
                    {"event_id": event_id},
                )
            assert count == 1
        finally:
            await engine.dispose()
