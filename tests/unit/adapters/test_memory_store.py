from uuid import uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.events import DomainEvent
from eventsource.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports import ExpectedVersion, collect


class ThingHappened(DomainEvent):
    aggregate_type: str = "Thing"


def sid() -> StreamId:
    return StreamId(aggregate_id=uuid4(), category="Thing")


@pytest.fixture
def store() -> InMemoryEventStore:
    return InMemoryEventStore()


class TestAppend:
    async def test_append_returns_one_based_version(self, store: InMemoryEventStore) -> None:
        stream = sid()
        result = await store.append(
            stream,
            [ThingHappened(aggregate_id=stream.aggregate_id)],
            ExpectedVersion.no_stream(),
        )
        assert result.new_version == 1
        assert await store.get_stream_version(stream) == 1

    async def test_absent_stream_version_zero(self, store: InMemoryEventStore) -> None:
        assert await store.get_stream_version(sid()) == 0

    async def test_exact_conflict_raises(self, store: InMemoryEventStore) -> None:
        stream = sid()
        await store.append(
            stream,
            [ThingHappened(aggregate_id=stream.aggregate_id)],
            ExpectedVersion.no_stream(),
        )
        with pytest.raises(OptimisticLockError):
            await store.append(
                stream,
                [ThingHappened(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.exact(0),
            )

    async def test_duplicate_event_id_rejected_atomically(self, store: InMemoryEventStore) -> None:
        stream = sid()
        event = ThingHappened(aggregate_id=stream.aggregate_id)
        await store.append(stream, [event], ExpectedVersion.no_stream())
        fresh = ThingHappened(aggregate_id=stream.aggregate_id)
        with pytest.raises(DuplicateEventError):
            await store.append(stream, [fresh, event], ExpectedVersion.exact(1))
        assert await store.get_stream_version(stream) == 1  # atomic: fresh not written

    async def test_empty_batch_rejected(self, store: InMemoryEventStore) -> None:
        with pytest.raises(ValueError):
            await store.append(sid(), [], ExpectedVersion.any_())


class TestFeed:
    async def test_exclusive_resumption(self, store: InMemoryEventStore) -> None:
        stream = sid()
        await store.append(
            stream,
            [ThingHappened(aggregate_id=stream.aggregate_id) for _ in range(3)],
            ExpectedVersion.no_stream(),
        )
        first_two = [env async for env in store.read_all()][:2]
        resumed = [env async for env in store.read_all(from_position=first_two[-1].position)]
        assert len(resumed) == 1

    async def test_current_position_none_when_empty(self, store: InMemoryEventStore) -> None:
        assert await store.current_position() is None


class TestStreamRead:
    async def test_read_stream_forward(self, store: InMemoryEventStore) -> None:
        stream = sid()
        await store.append(
            stream,
            [ThingHappened(aggregate_id=stream.aggregate_id) for _ in range(3)],
            ExpectedVersion.no_stream(),
        )
        envs = await collect(store.read_stream(stream))
        assert [e.stream_version for e in envs] == [1, 2, 3]

    async def test_read_stream_backward(self, store: InMemoryEventStore) -> None:
        from eventsource.ports import ReadDirection, StreamReadOptions

        stream = sid()
        await store.append(
            stream,
            [ThingHappened(aggregate_id=stream.aggregate_id) for _ in range(3)],
            ExpectedVersion.no_stream(),
        )
        envs = await collect(
            store.read_stream(stream, StreamReadOptions(direction=ReadDirection.BACKWARD))
        )
        assert [e.stream_version for e in envs] == [3, 2, 1]

    async def test_event_exists(self, store: InMemoryEventStore) -> None:
        stream = sid()
        event = ThingHappened(aggregate_id=stream.aggregate_id)
        assert await store.event_exists(event.event_id) is False
        await store.append(stream, [event], ExpectedVersion.no_stream())
        assert await store.event_exists(event.event_id) is True


class TestCategory:
    async def test_read_category(self, store: InMemoryEventStore) -> None:
        stream = sid()
        await store.append(
            stream,
            [ThingHappened(aggregate_id=stream.aggregate_id)],
            ExpectedVersion.no_stream(),
        )
        envs = await collect(store.read_category("Thing"))
        assert len(envs) == 1
