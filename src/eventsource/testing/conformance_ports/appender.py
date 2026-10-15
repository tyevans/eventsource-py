"""Conformance suite for the `EventAppender` port."""

from abc import ABC, abstractmethod
from typing import Protocol
from uuid import uuid4

import pytest

from eventsource.domain.exceptions import (
    DuplicateEventError,
    EventTypeNotFoundError,
    OptimisticLockError,
)
from eventsource.ports import AppendResult, ExpectedVersion
from eventsource.ports.store import EventAppender, StreamReader
from eventsource.testing.conformance_ports._fixtures import (
    UnregisteredEvent,
    make_event,
    make_stream,
)


class _AppenderUnderTest(EventAppender, StreamReader, Protocol):
    """Adapter surface needed by this suite: append plus version lookup."""


class AppenderConformance(ABC):
    """Conformance suite for `EventAppender` implementations (`positions_expected` toggles position checks)."""

    positions_expected: bool = True

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `EventAppender`."""
        raise NotImplementedError

    async def test_no_stream_append_to_fresh_stream_succeeds(
        self, store: _AppenderUnderTest
    ) -> None:
        stream = make_stream()
        result = await store.append(
            stream, [make_event(stream.aggregate_id)], ExpectedVersion.no_stream()
        )
        assert result.new_version == 1

    async def test_no_stream_append_to_existing_stream_conflicts(
        self, store: _AppenderUnderTest
    ) -> None:
        stream = make_stream()
        await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.no_stream())
        with pytest.raises(OptimisticLockError):
            await store.append(
                stream, [make_event(stream.aggregate_id)], ExpectedVersion.no_stream()
            )

    async def test_any_append_to_fresh_stream_succeeds(self, store: _AppenderUnderTest) -> None:
        stream = make_stream()
        result = await store.append(
            stream, [make_event(stream.aggregate_id)], ExpectedVersion.any_()
        )
        assert result.new_version == 1

    async def test_any_append_to_existing_stream_succeeds(self, store: _AppenderUnderTest) -> None:
        stream = make_stream()
        await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.any_())
        result = await store.append(
            stream, [make_event(stream.aggregate_id)], ExpectedVersion.any_()
        )
        assert result.new_version == 2

    async def test_stream_exists_append_to_absent_stream_raises(
        self, store: _AppenderUnderTest
    ) -> None:
        stream = make_stream()
        with pytest.raises(OptimisticLockError):
            await store.append(
                stream, [make_event(stream.aggregate_id)], ExpectedVersion.stream_exists()
            )

    async def test_stream_exists_append_to_existing_stream_succeeds(
        self, store: _AppenderUnderTest
    ) -> None:
        stream = make_stream()
        await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.no_stream())
        result = await store.append(
            stream, [make_event(stream.aggregate_id)], ExpectedVersion.stream_exists()
        )
        assert result.new_version == 2

    async def test_exact_append_matching_version_succeeds(self, store: _AppenderUnderTest) -> None:
        stream = make_stream()
        await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.exact(0))
        result = await store.append(
            stream, [make_event(stream.aggregate_id)], ExpectedVersion.exact(1)
        )
        assert result.new_version == 2

    async def test_exact_append_mismatched_version_conflicts(
        self, store: _AppenderUnderTest
    ) -> None:
        stream = make_stream()
        with pytest.raises(OptimisticLockError):
            await store.append(stream, [make_event(stream.aggregate_id)], ExpectedVersion.exact(1))

    async def test_duplicate_event_id_raises_and_batch_is_atomic(
        self, store: _AppenderUnderTest
    ) -> None:
        stream = make_stream()
        first = make_event(stream.aggregate_id)
        await store.append(stream, [first], ExpectedVersion.no_stream())
        duplicate_batch = [make_event(stream.aggregate_id), first]
        with pytest.raises(DuplicateEventError):
            await store.append(stream, duplicate_batch, ExpectedVersion.any_())
        assert await store.get_stream_version(stream) == 1

    async def test_empty_batch_raises_value_error(self, store: _AppenderUnderTest) -> None:
        stream = make_stream()
        with pytest.raises(ValueError, match="empty"):
            await store.append(stream, [], ExpectedVersion.any_())

    async def test_append_result_position_non_none_and_strictly_increasing(
        self, store: _AppenderUnderTest
    ) -> None:
        stream_a, stream_b = make_stream(aggregate_id=uuid4()), make_stream(aggregate_id=uuid4())
        result_a = await store.append(
            stream_a, [make_event(stream_a.aggregate_id)], ExpectedVersion.any_()
        )
        result_b = await store.append(
            stream_b, [make_event(stream_b.aggregate_id)], ExpectedVersion.any_()
        )
        if not self.positions_expected:
            assert result_a.position is None
            assert result_b.position is None
            return
        assert result_a.position is not None
        assert result_b.position is not None
        assert result_b.position > result_a.position

    async def test_unregistered_event_type_is_rejected(self, store: _AppenderUnderTest) -> None:
        """An event whose `event_type` is not in the store's registry fails on append or read."""
        stream = make_stream()
        event = UnregisteredEvent(aggregate_id=stream.aggregate_id)
        with pytest.raises(EventTypeNotFoundError):
            await store.append(stream, [event], ExpectedVersion.no_stream())
            async for _ in store.read_stream(stream):
                pass

    async def test_append_result_type(self, store: _AppenderUnderTest) -> None:
        stream = make_stream()
        result = await store.append(
            stream, [make_event(stream.aggregate_id)], ExpectedVersion.any_()
        )
        assert isinstance(result, AppendResult)
        assert result.stream == stream
