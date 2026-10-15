"""Unit tests for SQLCheckpointRepository position methods."""

from __future__ import annotations

from datetime import datetime
from uuid import uuid4

import pytest

from eventsource.ports.checkpoints import CheckpointData

from ._helpers import pos

try:
    import aiosqlite

    from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    SQLCheckpointRepository = None  # type: ignore[assignment,misc]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLCheckpointRepositoryPosition:
    """Tests for SQLCheckpointRepository position methods."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        from eventsource import create_async_engine
        from eventsource.adapters.sql.schemas import get_schema

        engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/position.db")
        async with engine.begin() as conn:
            raw = await conn.get_raw_connection()
            await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
        yield engine
        await engine.dispose()

    @pytest.fixture
    def repo(self, sqlite_engine) -> SQLCheckpointRepository:
        return SQLCheckpointRepository(sqlite_engine)

    @pytest.mark.asyncio
    async def test_get_position_returns_none_for_nonexistent(self, repo: SQLCheckpointRepository):
        result = await repo.get_position("NonExistentSubscription")
        assert result is None

    @pytest.mark.asyncio
    async def test_save_position_creates_new_checkpoint(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        event_id = uuid4()
        event_type = "TestEvent"
        position = pos(100)

        await repo.save_position(subscription_id, position, event_id, event_type)
        result = await repo.get_position(subscription_id)
        assert result == position

    @pytest.mark.asyncio
    async def test_save_position_updates_existing_checkpoint(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        first_position = pos(100)
        second_position = pos(200)

        await repo.save_position(subscription_id, first_position, uuid4(), "Event1")
        await repo.save_position(subscription_id, second_position, uuid4(), "Event2")

        result = await repo.get_position(subscription_id)
        assert result == second_position

    @pytest.mark.asyncio
    async def test_save_position_increments_events_processed(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        for i in range(3):
            await repo.save_position(subscription_id, pos(i * 100), uuid4(), f"Event{i}")

        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_save_position_updates_event_id_and_type(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        event_id = uuid4()
        event_type = "TestEventType"
        position = pos(100)

        await repo.save_position(subscription_id, position, event_id, event_type)
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]
        assert checkpoint.last_event_id == event_id
        assert checkpoint.last_event_type == event_type

    @pytest.mark.asyncio
    async def test_get_position_returns_correct_value_after_save(
        self, repo: SQLCheckpointRepository
    ):
        subscription_id = "TestSubscription"
        positions = [pos(n) for n in (10, 50, 100, 500, 1000)]

        for position in positions:
            await repo.save_position(subscription_id, position, uuid4(), "Event")
            result = await repo.get_position(subscription_id)
            assert result == position

    @pytest.mark.asyncio
    async def test_checkpoint_data_includes_global_position(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        position = pos(42)

        await repo.save_position(subscription_id, position, uuid4(), "TestEvent")
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        checkpoint = checkpoints[0]
        assert isinstance(checkpoint, CheckpointData)
        assert checkpoint.position == position

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_independent_positions(
        self, repo: SQLCheckpointRepository
    ):
        sub1 = "Subscription1"
        sub2 = "Subscription2"
        pos1 = pos(100)
        pos2 = pos(200)

        await repo.save_position(sub1, pos1, uuid4(), "Event1")
        await repo.save_position(sub2, pos2, uuid4(), "Event2")

        assert await repo.get_position(sub1) == pos1
        assert await repo.get_position(sub2) == pos2

    @pytest.mark.asyncio
    async def test_get_position_returns_none_after_reset(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        await repo.save_position(subscription_id, pos(100), uuid4(), "Event")
        assert await repo.get_position(subscription_id) == pos(100)

        await repo.reset_checkpoint(subscription_id)
        assert await repo.get_position(subscription_id) is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_does_not_set_position(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        event_id = uuid4()

        await repo.update_checkpoint(subscription_id, event_id, "TestEvent")
        result = await repo.get_position(subscription_id)
        assert result is None

        checkpoint = await repo.get_checkpoint(subscription_id)
        assert checkpoint == event_id

    @pytest.mark.asyncio
    async def test_save_position_after_update_checkpoint(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        await repo.update_checkpoint(subscription_id, uuid4(), "Event1")
        assert await repo.get_position(subscription_id) is None

        position = pos(500)
        await repo.save_position(subscription_id, position, uuid4(), "Event2")
        assert await repo.get_position(subscription_id) == position

        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].events_processed == 2

    @pytest.mark.asyncio
    async def test_save_position_sets_last_processed_at(self, repo: SQLCheckpointRepository):
        subscription_id = "TestSubscription"
        await repo.save_position(subscription_id, pos(100), uuid4(), "Event")

        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].last_processed_at is not None
        assert isinstance(checkpoints[0].last_processed_at, datetime)

    @pytest.mark.asyncio
    async def test_sqlitestore_position_round_trip_through_checkpoint_repo(
        self, repo: SQLCheckpointRepository, tmp_path
    ) -> None:
        from eventsource.adapters.sqlite import SQLiteEventStore
        from eventsource.domain.event_registry import EventRegistry
        from eventsource.ports import ExpectedVersion
        from eventsource.testing.conformance_ports._fixtures import (
            ConformanceEvent,
            make_stream,
        )

        registry = EventRegistry()
        registry.register(ConformanceEvent)
        db_path = str(tmp_path / "store.db")
        store = SQLiteEventStore(db_path, event_registry=registry)

        try:
            stream1 = make_stream()
            result1 = await store.append(
                stream1,
                [ConformanceEvent(aggregate_id=stream1.aggregate_id, payload="first")],
                ExpectedVersion.any_(),
            )
            position1 = result1.position
            assert position1 is not None

            stream2 = make_stream()
            result2 = await store.append(
                stream2,
                [ConformanceEvent(aggregate_id=stream2.aggregate_id, payload="second")],
                ExpectedVersion.any_(),
            )
            position2 = result2.position
            assert position2 is not None

            await repo.save_position("S", position1, uuid4(), "Created")
            read_position = await repo.get_position("S")

            assert read_position is not None
            assert read_position == position1
            assert read_position.store_id == position1.store_id
            assert position1 < position2
            assert read_position < position2
        finally:
            await store.close()


class TestIntPositionCodecStrictToken:
    """Verify IntPositionCodec strictly requires encoded Position tokens."""

    def test_bare_integer_string_raises_decode_error(self) -> None:
        from eventsource.adapters._sql.positions import IntPositionCodec
        from eventsource.ports.exceptions import PositionDecodeError

        codec = IntPositionCodec(store_id="pg:main")
        with pytest.raises(PositionDecodeError):
            codec.decode("12345")

    def test_valid_encoded_position_round_trips(self) -> None:
        from eventsource.adapters._sql.positions import IntPositionCodec

        codec = IntPositionCodec(store_id="pg:main")
        encoded = codec.encode(42)
        assert codec.decode(encoded.to_str()) == encoded
        assert codec.value_of(encoded) == 42
