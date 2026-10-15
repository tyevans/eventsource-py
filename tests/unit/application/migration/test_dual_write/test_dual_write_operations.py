"""Unit tests for DualWriteInterceptor read operations, router integration, concurrency, and edge cases."""

import asyncio
from unittest.mock import MagicMock
from uuid import UUID, uuid4

from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.domain import StreamId
from eventsource.ports import AppendResult, ExpectedVersion, FullEventStore, Position

from ._fixtures import TestEvent, async_generator_mock, sid


def _new_stream_and_event(data: str | None = None) -> tuple[StreamId, TestEvent]:
    agg_id = uuid4()
    ev = TestEvent(aggregate_id=agg_id, data=data) if data else TestEvent(aggregate_id=agg_id)
    return StreamId(aggregate_id=agg_id, category="TestAggregate"), ev


class TestReadOperations:
    """Tests for read operations (should delegate to source store)."""

    async def test_read_category_delegates_to_source(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        called = {"source": False, "target": False}
        source_store.read_category = lambda *a, **kw: (
            called.update(source=True) or async_generator_mock([])
        )
        target_store.read_category = lambda *a, **kw: (
            called.update(target=True) or async_generator_mock([])
        )
        assert [e async for e in interceptor.read_category("TestAggregate")] == []
        assert called == {"source": True, "target": False}

    async def test_event_exists_delegates_to_source(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        event_id = uuid4()
        await interceptor.event_exists(event_id)
        source_store.event_exists.assert_called_once_with(event_id)
        target_store.event_exists.assert_not_called()

    async def test_get_stream_version_delegates_to_source(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        stream = sid()
        await interceptor.get_stream_version(stream)
        source_store.get_stream_version.assert_called_once_with(stream)
        target_store.get_stream_version.assert_not_called()

    async def test_current_position_delegates_to_source(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        result = await interceptor.current_position()
        assert result == Position(store_id="source", key=(100,))
        source_store.current_position.assert_called_once()
        target_store.current_position.assert_not_called()

    async def test_read_stream_delegates_to_source(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock
    ) -> None:
        assert [event async for event in interceptor.read_stream(sid())] == []

    async def test_read_all_delegates_to_source(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock
    ) -> None:
        assert [event async for event in interceptor.read_all()] == []


class TestRouterIntegration:
    """Tests for the interceptor as a `FullEventStore` structural replacement."""

    async def test_interceptor_stands_in_for_full_event_store(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        interceptor_inst = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        _: FullEventStore = interceptor_inst  # mypy-checked structural conformance
        stream, event = _new_stream_and_event()

        await interceptor_inst.append(stream, [event], ExpectedVersion.no_stream())
        assert [e async for e in interceptor_inst.read_stream(stream)] == []
        assert [e async for e in interceptor_inst.read_category("TestAggregate")] == []
        await interceptor_inst.event_exists(event.event_id)
        await interceptor_inst.get_stream_version(stream)
        assert [e async for e in interceptor_inst.read_all()] == []
        await interceptor_inst.current_position()


class TestConcurrentOperations:
    """Tests for concurrent operation handling."""

    async def test_concurrent_writes(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        interceptor_inst = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        async def do_write(index: int):
            stream, event = _new_stream_and_event(f"data-{index}")
            return await interceptor_inst.append(stream, [event], ExpectedVersion.no_stream())

        results = await asyncio.gather(*(do_write(i) for i in range(10)))
        assert len(results) == 10 and all(isinstance(r, AppendResult) for r in results)
        assert source_store.append.call_count == 10 and target_store.append.call_count == 10

    async def test_concurrent_writes_with_target_failures(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        interceptor_inst = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        call_count = [0]

        async def intermittent_failure(stream, events, expected):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                raise Exception("Intermittent failure")
            return AppendResult(
                stream=stream, new_version=1, position=Position(store_id="target", key=(1,))
            )

        target_store.append = intermittent_failure

        async def do_write(index: int):
            stream, event = _new_stream_and_event()
            return await interceptor_inst.append(stream, [event], ExpectedVersion.no_stream())

        results = await asyncio.gather(*(do_write(i) for i in range(10)))
        assert len(results) == 10 and all(isinstance(r, AppendResult) for r in results)
        assert interceptor_inst.get_failure_stats().total_failures == 5


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    async def test_multiple_events_single_write(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event-{i}") for i in range(5)]
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        result = await interceptor.append(stream, events, ExpectedVersion.no_stream())
        assert result.stream == stream
        source_store.append.assert_called_once_with(stream, events, ExpectedVersion.no_stream())
        target_store.append.assert_called_once_with(stream, events, ExpectedVersion.exact(0))

    async def test_failure_tracking_multiple_events(
        self, interceptor: DualWriteInterceptor, target_store: MagicMock
    ) -> None:
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event-{i}") for i in range(3)]
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        target_store.append.side_effect = Exception("Error")
        await interceptor.append(stream, events, ExpectedVersion.no_stream())
        failed_writes = interceptor.get_failed_writes()
        assert len(failed_writes) == 1 and len(failed_writes[0].event_ids) == 3
        assert interceptor.get_failure_stats().total_events_failed == 3

    async def test_source_position_tracked_in_failure(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        stream, event = _new_stream_and_event()
        expected_position = Position(store_id="source", key=(500,))
        source_store.append.side_effect = None
        source_store.append.return_value = AppendResult(
            stream=stream, new_version=1, position=expected_position
        )
        target_store.append.side_effect = Exception("Error")
        await interceptor.append(stream, [event], ExpectedVersion.no_stream())
        assert interceptor.get_failed_writes()[0].source_position == expected_position


__all__ = [  # pragma: allowlist secret
    "TestConcurrentOperations",
    "TestEdgeCases",
    "TestReadOperations",
    "TestRouterIntegration",
]
