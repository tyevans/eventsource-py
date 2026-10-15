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
        assert await repo.get_position("NonExistentSubscription") is None

    @pytest.mark.asyncio
    async def test_save_position_creates_new_checkpoint(self, repo: SQLCheckpointRepository):
        position = pos(100)
        await repo.save_position("TestSubscription", position, uuid4(), "TestEvent")
        assert await repo.get_position("TestSubscription") == position

    @pytest.mark.asyncio
    async def test_save_position_updates_existing_checkpoint(self, repo: SQLCheckpointRepository):
        await repo.save_position("TestSubscription", pos(100), uuid4(), "Event1")
        await repo.save_position("TestSubscription", pos(200), uuid4(), "Event2")
        assert await repo.get_position("TestSubscription") == pos(200)

    @pytest.mark.asyncio
    async def test_save_position_increments_events_processed(self, repo: SQLCheckpointRepository):
        for i in range(3):
            await repo.save_position("TestSubscription", pos(i * 100), uuid4(), f"Event{i}")
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].events_processed == 3

    @pytest.mark.asyncio
    async def test_save_position_updates_event_id_and_type(self, repo: SQLCheckpointRepository):
        event_id = uuid4()
        await repo.save_position("TestSubscription", pos(100), event_id, "TestEventType")
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert checkpoints[0].last_event_id == event_id
        assert checkpoints[0].last_event_type == "TestEventType"

    @pytest.mark.asyncio
    async def test_get_position_returns_correct_value_after_save(
        self, repo: SQLCheckpointRepository
    ):
        for position in (pos(n) for n in (10, 50, 100, 500, 1000)):
            await repo.save_position("TestSubscription", position, uuid4(), "Event")
            assert await repo.get_position("TestSubscription") == position

    @pytest.mark.asyncio
    async def test_checkpoint_data_includes_global_position(self, repo: SQLCheckpointRepository):
        position = pos(42)
        await repo.save_position("TestSubscription", position, uuid4(), "TestEvent")
        checkpoints = await repo.get_all_checkpoints()
        assert len(checkpoints) == 1
        assert isinstance(checkpoints[0], CheckpointData)
        assert checkpoints[0].position == position

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_independent_positions(
        self, repo: SQLCheckpointRepository
    ):
        pos1, pos2 = pos(100), pos(200)
        await repo.save_position("Subscription1", pos1, uuid4(), "Event1")
        await repo.save_position("Subscription2", pos2, uuid4(), "Event2")
        assert await repo.get_position("Subscription1") == pos1
        assert await repo.get_position("Subscription2") == pos2

    @pytest.mark.asyncio
    async def test_get_position_returns_none_after_reset(self, repo: SQLCheckpointRepository):
        await repo.save_position("TestSubscription", pos(100), uuid4(), "Event")
        assert await repo.get_position("TestSubscription") == pos(100)
        await repo.reset_checkpoint("TestSubscription")
        assert await repo.get_position("TestSubscription") is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_does_not_set_position(self, repo: SQLCheckpointRepository):
        event_id = uuid4()
        await repo.update_checkpoint("TestSubscription", event_id, "TestEvent")
        assert await repo.get_position("TestSubscription") is None
        assert await repo.get_checkpoint("TestSubscription") == event_id

    @pytest.mark.asyncio
    async def test_save_position_after_update_checkpoint(self, repo: SQLCheckpointRepository):
        await repo.update_checkpoint("TestSubscription", uuid4(), "Event1")
        assert await repo.get_position("TestSubscription") is None
        position = pos(500)
        await repo.save_position("TestSubscription", position, uuid4(), "Event2")
        assert await repo.get_position("TestSubscription") == position
        checkpoints = await repo.get_all_checkpoints()
        assert checkpoints[0].events_processed == 2

    @pytest.mark.asyncio
    async def test_save_position_sets_last_processed_at(self, repo: SQLCheckpointRepository):
        await repo.save_position("TestSubscription", pos(100), uuid4(), "Event")
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
        from eventsource.testing.conformance_ports._fixtures import ConformanceEvent, make_stream

        registry = EventRegistry()
        registry.register(ConformanceEvent)
        store = SQLiteEventStore(str(tmp_path / "store.db"), event_registry=registry)
        try:
            stream1, stream2 = make_stream(), make_stream()
            result1 = await store.append(
                stream1,
                [ConformanceEvent(aggregate_id=stream1.aggregate_id, payload="first")],
                ExpectedVersion.any_(),
            )
            result2 = await store.append(
                stream2,
                [ConformanceEvent(aggregate_id=stream2.aggregate_id, payload="second")],
                ExpectedVersion.any_(),
            )
            position1, position2 = result1.position, result2.position
            assert position1 is not None and position2 is not None
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
