"""No-skip mutation-killer for the safe-horizon `read_all` predicate.

Two concurrent writers: writer A begins a transaction, inserts (acquiring
global_position N), and parks pre-commit on an asyncio.Event; writer B then
inserts and commits (position N+1). The reader reads the feed: it must NOT
see N+1 while N is uncommitted (the horizon holds it back). Writer A is then
released and commits; the reader resumes from its checkpoint and must see N
then N+1, in order, with nothing lost.
"""

import asyncio
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from eventsource.adapters.postgresql import PostgreSQLEventStore
from eventsource.adapters.serialization import json_dumps
from eventsource.domain.event_registry import EventRegistry
from eventsource.ports import ExpectedVersion, collect
from eventsource.testing.conformance_ports._fixtures import ConformanceEvent, make_stream

from ..conftest import skip_if_no_postgres_infra

pytestmark = [pytest.mark.integration, pytest.mark.postgres, skip_if_no_postgres_infra]


def _make_registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(ConformanceEvent)
    return registry


async def _run_no_skip_scenario(ports_postgres_connection_url: str) -> None:
    engine = create_async_engine(ports_postgres_connection_url)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS events CASCADE"))

    store = PostgreSQLEventStore(
        engine, event_registry=_make_registry(), create_schema=True, owns_engine=True
    )
    # Force schema creation before the two writers race.
    await store.current_position()

    a_parked = asyncio.Event()
    a_release = asyncio.Event()
    stream_a = make_stream(aggregate_id=uuid4())
    stream_b = make_stream(aggregate_id=uuid4())

    writer_a_engine = create_async_engine(ports_postgres_connection_url)

    async def writer_a() -> None:
        event = ConformanceEvent(aggregate_id=stream_a.aggregate_id, payload="a")
        async with writer_a_engine.connect() as conn:
            # SQLAlchemy async connections auto-begin a transaction on first
            # execute (commit-as-you-go); no explicit BEGIN needed or wanted.
            await conn.execute(
                text(
                    """
                    INSERT INTO events (
                        event_id, event_type, aggregate_type, aggregate_id,
                        tenant_id, actor_id, version, timestamp, payload, created_at
                    )
                    VALUES (
                        :event_id, :event_type, :aggregate_type, :aggregate_id,
                        NULL, NULL, 1, :timestamp, :payload, NOW()
                    )
                    """
                ),
                {
                    "event_id": event.event_id,
                    "event_type": event.event_type,
                    "aggregate_type": stream_a.category,
                    "aggregate_id": stream_a.aggregate_id,
                    "timestamp": event.occurred_at,
                    "payload": json_dumps(event.model_dump(mode="json")),
                },
            )
            a_parked.set()
            await a_release.wait()
            await conn.commit()

    async def writer_b() -> None:
        await a_parked.wait()
        await store.append(
            stream_b,
            [ConformanceEvent(aggregate_id=stream_b.aggregate_id, payload="b")],
            ExpectedVersion.any_(),
        )

    task_a = asyncio.create_task(writer_a())
    task_b = asyncio.create_task(writer_b())
    await asyncio.wait_for(a_parked.wait(), timeout=5)
    await asyncio.wait_for(task_b, timeout=5)

    # Reader must not see writer B's event while writer A's lower-position
    # transaction is still uncommitted -- horizon holds it back.
    visible = await collect(store.read_all())
    assert visible == [], (
        "safe-horizon predicate failed: read_all returned events past an "
        "uncommitted lower global_position"
    )
    assert await store.current_position() is None

    # Release writer A; it commits.
    a_release.set()
    await asyncio.wait_for(task_a, timeout=5)

    # Resume from empty checkpoint: must see both events, in position order,
    # nothing lost.
    resumed = await collect(store.read_all())
    assert [e.event.payload for e in resumed] == ["a", "b"]  # type: ignore[attr-defined]
    positions = [e.position for e in resumed]
    assert positions == sorted(positions)

    await writer_a_engine.dispose()
    await store.close()


async def test_no_skip_horizon_holds_back_uncommitted_lower_position(
    ports_postgres_connection_url: str,
) -> None:
    await asyncio.wait_for(_run_no_skip_scenario(ports_postgres_connection_url), timeout=10)
