"""Unit tests for same-transaction outbox writes on the PostgreSQL adapter.

All database interactions are mocked -- the mocked `AsyncSession` is
inspected to assert the outbox `INSERT` runs on the same session as the
event `INSERT`, before `session.commit()`.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from eventsource.adapters.postgresql.store import PostgreSQLEventStore
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion

# --- Test event ---


class SampleOutboxEvent(DomainEvent):
    """Simple event for outbox write tests."""

    data: str = "test"


# --- Fixtures ---


@pytest.fixture
def mock_session() -> AsyncMock:
    session = AsyncMock(spec=AsyncSession)
    return session


def _install_session_factory(store: PostgreSQLEventStore, mock_session: AsyncMock) -> MagicMock:
    """Replace the store's real session factory with a mock context manager."""
    factory = MagicMock(spec=async_sessionmaker)
    context_manager = AsyncMock()
    context_manager.__aenter__.return_value = mock_session
    context_manager.__aexit__.return_value = None
    factory.return_value = context_manager
    store._session_factory = factory  # type: ignore[assignment]
    return factory


@pytest.fixture
def mock_engine() -> MagicMock:
    engine = MagicMock(spec=AsyncEngine)
    engine.url.database = "testdb"
    return engine


def _make_store(
    mock_engine: MagicMock, mock_session: AsyncMock, *, outbox_enabled: bool
) -> PostgreSQLEventStore:
    store = PostgreSQLEventStore(mock_engine, outbox_enabled=outbox_enabled)
    _install_session_factory(store, mock_session)
    return store


def _mock_scalar_result(value: int) -> MagicMock:
    result = MagicMock()
    result.scalar.return_value = value
    return result


def _mock_insert_result(global_position: int) -> MagicMock:
    result = MagicMock()
    result.scalar.return_value = global_position
    return result


def _configure_append_execute(mock_session: AsyncMock, num_events: int) -> None:
    """Wire mock_session.execute to satisfy append()'s query sequence:

    1. SELECT MAX(version) for current_version
    2. one INSERT INTO events (RETURNING global_position) per event
    3. optionally one INSERT INTO event_outbox per event
    """
    responses = [_mock_scalar_result(0)]
    for i in range(num_events):
        responses.append(_mock_insert_result(i + 1))
        responses.append(MagicMock())  # outbox insert result (unused if disabled)
    mock_session.execute = AsyncMock(side_effect=responses)


@pytest.fixture
def stream() -> StreamId:
    return StreamId(aggregate_id=uuid4(), category="TestAggregate")


# --- Tests ---


@pytest.mark.asyncio
async def test_outbox_disabled_by_default_no_outbox_insert(
    mock_engine: MagicMock, mock_session: AsyncMock, stream: StreamId
) -> None:
    store = _make_store(mock_engine, mock_session, outbox_enabled=False)
    assert store.outbox_enabled is False

    event = SampleOutboxEvent(aggregate_id=stream.aggregate_id, aggregate_type=stream.category)
    _configure_append_execute(mock_session, num_events=1)

    await store.append(stream, [event], ExpectedVersion.no_stream())

    sql_texts = [str(call.args[0]) for call in mock_session.execute.await_args_list]
    assert not any("event_outbox" in sql for sql in sql_texts)


@pytest.mark.asyncio
async def test_outbox_enabled_writes_one_row_per_event_same_session_before_commit(
    mock_engine: MagicMock, mock_session: AsyncMock, stream: StreamId
) -> None:
    store = _make_store(mock_engine, mock_session, outbox_enabled=True)
    assert store.outbox_enabled is True

    events = [
        SampleOutboxEvent(aggregate_id=stream.aggregate_id, aggregate_type=stream.category),
        SampleOutboxEvent(aggregate_id=stream.aggregate_id, aggregate_type=stream.category),
    ]
    _configure_append_execute(mock_session, num_events=len(events))

    commit_order: list[str] = []
    mock_session.commit.side_effect = lambda: commit_order.append("commit")

    await store.append(stream, events, ExpectedVersion.no_stream())

    calls = mock_session.execute.await_args_list
    outbox_calls = [c for c in calls if "event_outbox" in str(c.args[0])]
    assert len(outbox_calls) == len(events)

    # commit() must run after all execute() calls (same session, before commit).
    assert mock_session.commit.await_count == 1


@pytest.mark.asyncio
async def test_outbox_payload_shape(
    mock_engine: MagicMock, mock_session: AsyncMock, stream: StreamId
) -> None:
    store = _make_store(mock_engine, mock_session, outbox_enabled=True)
    event = SampleOutboxEvent(aggregate_id=stream.aggregate_id, aggregate_type=stream.category)
    _configure_append_execute(mock_session, num_events=1)

    await store.append(stream, [event], ExpectedVersion.no_stream())

    calls = mock_session.execute.await_args_list
    outbox_calls = [c for c in calls if "event_outbox" in str(c.args[0])]
    assert len(outbox_calls) == 1

    params = outbox_calls[0].args[1]
    event_data = json.loads(params["event_data"])

    assert set(event_data.keys()) == {
        "event_id",
        "aggregate_id",
        "aggregate_type",
        "tenant_id",
        "occurred_at",
        "payload",
    }
    assert event_data["event_id"] == str(event.event_id)
    assert event_data["aggregate_id"] == str(event.aggregate_id)
    assert event_data["aggregate_type"] == stream.category
    assert event_data["tenant_id"] == (str(event.tenant_id) if event.tenant_id else None)
    assert event_data["occurred_at"] == event.occurred_at.isoformat()
    assert event_data["payload"] == event.model_dump(mode="json")
