"""Tests for `LegacyStoreAdapter`, wrapping `MemoryEventStore` behind the old `EventStore` ABC."""

from uuid import uuid4

import pytest

from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.adapters.memory import MemoryEventStore
from eventsource.events import DomainEvent
from eventsource.exceptions import OptimisticLockError
from eventsource.stores.interface import ExpectedVersion, ReadDirection, ReadOptions
from eventsource.stores.legacy import LegacyStoreAdapter, _expected_from_int


class ThingHappened(DomainEvent):
    aggregate_type: str = "Thing"


@pytest.fixture
def legacy() -> LegacyStoreAdapter:
    return LegacyStoreAdapter(MemoryEventStore(store_id="legacy-test"), store_id="legacy-test")


class TestExpectedVersionMapping:
    def test_any_by_name(self) -> None:
        assert _expected_from_int(ExpectedVersion.ANY).kind == "any"

    def test_no_stream_by_name(self) -> None:
        assert _expected_from_int(ExpectedVersion.NO_STREAM).kind == "no_stream"

    def test_stream_exists_by_name(self) -> None:
        assert _expected_from_int(ExpectedVersion.STREAM_EXISTS).kind == "stream_exists"

    def test_nonnegative_int_is_exact(self) -> None:
        result = _expected_from_int(5)
        assert result.kind == "exact"
        assert result.version == 5

    def test_zero_maps_to_no_stream_not_exact(self) -> None:
        # NOTE: ExpectedVersion.NO_STREAM == 0 too, so 0 maps to no_stream
        # via the sentinel branch (checked before the generic >= 0 branch).
        assert _expected_from_int(0).kind == "no_stream"


class TestAppendEvents:
    async def test_new_stream_returns_success_result(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        result = await legacy.append_events(
            aggregate_id,
            "Thing",
            [ThingHappened(aggregate_id=aggregate_id)],
            ExpectedVersion.NO_STREAM,
        )
        assert result.success is True
        assert result.new_version == 1
        assert result.conflict is False
        assert result.global_position == 1

    async def test_conflict_raises_optimistic_lock_error(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        await legacy.append_events(
            aggregate_id,
            "Thing",
            [ThingHappened(aggregate_id=aggregate_id)],
            ExpectedVersion.NO_STREAM,
        )
        with pytest.raises(OptimisticLockError):
            await legacy.append_events(
                aggregate_id,
                "Thing",
                [ThingHappened(aggregate_id=aggregate_id)],
                ExpectedVersion.NO_STREAM,
            )

    async def test_any_expected_version_skips_check(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        await legacy.append_events(
            aggregate_id,
            "Thing",
            [ThingHappened(aggregate_id=aggregate_id)],
            ExpectedVersion.NO_STREAM,
        )
        result = await legacy.append_events(
            aggregate_id,
            "Thing",
            [ThingHappened(aggregate_id=aggregate_id)],
            ExpectedVersion.ANY,
        )
        assert result.success is True
        assert result.new_version == 2


class TestGetEvents:
    async def test_round_trips_events(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        event = ThingHappened(aggregate_id=aggregate_id)
        await legacy.append_events(aggregate_id, "Thing", [event], ExpectedVersion.NO_STREAM)
        stream = await legacy.get_events(aggregate_id, "Thing")
        assert stream.aggregate_id == aggregate_id
        assert stream.aggregate_type == "Thing"
        assert stream.version == 1
        assert stream.events == [event]

    async def test_missing_aggregate_type_raises(self, legacy: LegacyStoreAdapter) -> None:
        with pytest.raises(ValueError):
            await legacy.get_events(uuid4())

    async def test_empty_stream(self, legacy: LegacyStoreAdapter) -> None:
        stream = await legacy.get_events(uuid4(), "Thing")
        assert stream.is_empty
        assert stream.version == 0

    async def test_from_version_skips_leading_events(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        e1 = ThingHappened(aggregate_id=aggregate_id)
        e2 = ThingHappened(aggregate_id=aggregate_id)
        await legacy.append_events(aggregate_id, "Thing", [e1], ExpectedVersion.NO_STREAM)
        await legacy.append_events(aggregate_id, "Thing", [e2], ExpectedVersion.ANY)
        stream = await legacy.get_events(aggregate_id, "Thing", from_version=1)
        assert stream.events == [e2]


class TestEventExistsAndVersion:
    async def test_event_exists(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        event = ThingHappened(aggregate_id=aggregate_id)
        assert await legacy.event_exists(event.event_id) is False
        await legacy.append_events(aggregate_id, "Thing", [event], ExpectedVersion.NO_STREAM)
        assert await legacy.event_exists(event.event_id) is True

    async def test_get_stream_version(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        assert await legacy.get_stream_version(aggregate_id, "Thing") == 0
        await legacy.append_events(
            aggregate_id,
            "Thing",
            [ThingHappened(aggregate_id=aggregate_id)],
            ExpectedVersion.NO_STREAM,
        )
        assert await legacy.get_stream_version(aggregate_id, "Thing") == 1


class TestGetEventsByType:
    async def test_returns_events_of_type(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        event = ThingHappened(aggregate_id=aggregate_id)
        await legacy.append_events(aggregate_id, "Thing", [event], ExpectedVersion.NO_STREAM)
        events = await legacy.get_events_by_type("Thing")
        assert events == [event]

    async def test_naive_from_timestamp_rejected(self, legacy: LegacyStoreAdapter) -> None:
        from datetime import datetime

        with pytest.raises(ValueError, match="timezone-aware"):
            await legacy.get_events_by_type("Thing", from_timestamp=datetime(2024, 1, 1))


class TestReadAll:
    async def test_yields_stored_events_with_int_positions(
        self, legacy: LegacyStoreAdapter
    ) -> None:
        aggregate_id = uuid4()
        event = ThingHappened(aggregate_id=aggregate_id)
        await legacy.append_events(aggregate_id, "Thing", [event], ExpectedVersion.NO_STREAM)

        results = [se async for se in legacy.read_all()]
        assert len(results) == 1
        stored = results[0]
        assert stored.event == event
        assert stored.global_position == 1
        assert stored.stream_position == 1
        assert stored.stream_id == f"{aggregate_id}:Thing"

    async def test_backward_direction_reverses_order(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        e1 = ThingHappened(aggregate_id=aggregate_id)
        e2 = ThingHappened(aggregate_id=aggregate_id)
        e3 = ThingHappened(aggregate_id=aggregate_id)
        await legacy.append_events(aggregate_id, "Thing", [e1], ExpectedVersion.NO_STREAM)
        await legacy.append_events(aggregate_id, "Thing", [e2], ExpectedVersion.ANY)
        await legacy.append_events(aggregate_id, "Thing", [e3], ExpectedVersion.ANY)

        results = [
            se async for se in legacy.read_all(ReadOptions(direction=ReadDirection.BACKWARD))
        ]
        assert [se.event for se in results] == [e3, e2, e1]

    async def test_backward_with_limit_keeps_most_recent_n(
        self, legacy: LegacyStoreAdapter
    ) -> None:
        # Mirrors stores/in_memory.py's _do_read_all: filters are applied in
        # forward order first, then direction is reversed, THEN limit is
        # applied -- so limit=2 backward keeps the two most recent events,
        # still in descending (newest-first) order, not the two oldest.
        aggregate_id = uuid4()
        e1 = ThingHappened(aggregate_id=aggregate_id)
        e2 = ThingHappened(aggregate_id=aggregate_id)
        e3 = ThingHappened(aggregate_id=aggregate_id)
        await legacy.append_events(aggregate_id, "Thing", [e1], ExpectedVersion.NO_STREAM)
        await legacy.append_events(aggregate_id, "Thing", [e2], ExpectedVersion.ANY)
        await legacy.append_events(aggregate_id, "Thing", [e3], ExpectedVersion.ANY)

        results = [
            se
            async for se in legacy.read_all(ReadOptions(direction=ReadDirection.BACKWARD, limit=2))
        ]
        assert [se.event for se in results] == [e3, e2]

    async def test_forward_with_limit_keeps_earliest_n(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        e1 = ThingHappened(aggregate_id=aggregate_id)
        e2 = ThingHappened(aggregate_id=aggregate_id)
        e3 = ThingHappened(aggregate_id=aggregate_id)
        await legacy.append_events(aggregate_id, "Thing", [e1], ExpectedVersion.NO_STREAM)
        await legacy.append_events(aggregate_id, "Thing", [e2], ExpectedVersion.ANY)
        await legacy.append_events(aggregate_id, "Thing", [e3], ExpectedVersion.ANY)

        results = [se async for se in legacy.read_all(ReadOptions(limit=2))]
        assert [se.event for se in results] == [e1, e2]


class TestGetGlobalPosition:
    async def test_empty_store_returns_zero(self, legacy: LegacyStoreAdapter) -> None:
        assert await legacy.get_global_position() == 0

    async def test_round_trips_through_codec(self, legacy: LegacyStoreAdapter) -> None:
        aggregate_id = uuid4()
        await legacy.append_events(
            aggregate_id,
            "Thing",
            [ThingHappened(aggregate_id=aggregate_id)],
            ExpectedVersion.NO_STREAM,
        )
        assert await legacy.get_global_position() == 1


class TestCodecIsolation:
    def test_codec_store_id_matches_wrapped_adapter(self) -> None:
        adapter = MemoryEventStore(store_id="isolated")
        legacy = LegacyStoreAdapter(adapter, store_id="isolated")
        assert legacy._codec == IntPositionCodec("isolated")


class TestStoreIdDefaulting:
    async def test_omitted_store_id_defaults_to_adapter_public_attribute(self) -> None:
        adapter = MemoryEventStore(store_id="auto-detected")
        legacy = LegacyStoreAdapter(adapter)
        assert legacy._codec == IntPositionCodec("auto-detected")

        aggregate_id = uuid4()
        await legacy.append_events(
            aggregate_id,
            "Thing",
            [ThingHappened(aggregate_id=aggregate_id)],
            ExpectedVersion.NO_STREAM,
        )
        assert await legacy.get_global_position() == 1

    def test_missing_store_id_and_no_public_attribute_raises(self) -> None:
        class NoStoreIdAdapter:
            max_append_batch = None

        with pytest.raises(TypeError):
            LegacyStoreAdapter(NoStoreIdAdapter())  # type: ignore[arg-type]

    def test_explicit_store_id_overrides_adapter_attribute(self) -> None:
        adapter = MemoryEventStore(store_id="adapter-default")
        legacy = LegacyStoreAdapter(adapter, store_id="explicit-override")
        assert legacy._codec == IntPositionCodec("explicit-override")
