"""Unit tests for BulkCopier batch writing, counting, and streaming."""

from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.domain import StreamId
from eventsource.domain.exceptions import DuplicateEventError
from eventsource.ports import AppendResult
from tests.unit.application.migration.test_bulk_copier._fixtures import _pos, create_test_events


@pytest.mark.asyncio
class TestBulkCopierWriteBatch:
    """Tests for BulkCopier._write_batch() method."""

    async def test_write_batch_groups_by_aggregate(self) -> None:
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        tenant_id, agg1_id, agg2_id = uuid4(), uuid4(), uuid4()
        stream1 = StreamId(aggregate_id=agg1_id, category="TestAggregate")
        stream2 = StreamId(aggregate_id=agg2_id, category="TestAggregate")
        target_store.append = AsyncMock(
            side_effect=[
                AppendResult(stream=stream1, new_version=2, position=_pos("target", 2)),
                AppendResult(stream=stream2, new_version=3, position=_pos("target", 5)),
            ]
        )
        copier = BulkCopier(MagicMock(), target_store, MagicMock(), enable_tracing=False)
        events = create_test_events(2, tenant_id, agg1_id, start=1) + create_test_events(
            3, tenant_id, agg2_id, start=3
        )
        last_pos = await copier._write_batch(uuid4(), tenant_id, events)
        assert target_store.append.call_count == 2 and last_pos == _pos("target", 5)

    async def test_write_batch_with_position_mapper(self) -> None:
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        tenant_id, agg_id, migration_id = uuid4(), uuid4(), uuid4()
        stream = StreamId(aggregate_id=agg_id, category="TestAggregate")
        target_positions = [_pos("target", i) for i in (1, 2, 3)]
        target_store.append = AsyncMock(
            side_effect=[
                AppendResult(stream=stream, new_version=i + 1, position=target_positions[i])
                for i in range(3)
            ]
        )
        position_mapper = AsyncMock()
        copier = BulkCopier(
            MagicMock(),
            target_store,
            MagicMock(),
            position_mapper=position_mapper,
            enable_tracing=False,
        )
        source_positions = [_pos("source", i) for i in (1, 2, 3)]
        events = create_test_events(3, tenant_id, agg_id, start=1)
        last_pos = await copier._write_batch(migration_id, tenant_id, events)
        assert target_store.append.call_count == 3
        assert position_mapper.record_mapping.call_count == 3
        for i, call in enumerate(position_mapper.record_mapping.call_args_list):
            assert call.args == (
                migration_id,
                source_positions[i],
                target_positions[i],
                events[i].event.event_id,
            )
        assert last_pos == target_positions[-1]

    async def test_write_batch_handles_missing_stream_version(self) -> None:
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(side_effect=Exception("Stream not found"))
        tenant_id, agg_id = uuid4(), uuid4()
        stream = StreamId(aggregate_id=agg_id, category="TestAggregate")
        target_store.append = AsyncMock(
            return_value=AppendResult(stream=stream, new_version=1, position=_pos("target", 1))
        )
        copier = BulkCopier(MagicMock(), target_store, MagicMock(), enable_tracing=False)
        events = create_test_events(1, tenant_id, agg_id, start=1)
        last_pos = await copier._write_batch(uuid4(), tenant_id, events)
        assert last_pos == _pos("target", 1)
        expected_version = target_store.append.call_args.args[2]
        assert expected_version.kind == "exact" and expected_version.version == 0

    async def test_write_batch_duplicate_event_counts_as_copied_and_continues(self) -> None:
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        tenant_id, agg1_id, agg2_id = uuid4(), uuid4(), uuid4()
        stream1 = StreamId(aggregate_id=agg1_id, category="TestAggregate")
        stream2 = StreamId(aggregate_id=agg2_id, category="TestAggregate")
        target_store.append = AsyncMock(
            side_effect=[
                DuplicateEventError("already present"),
                DuplicateEventError("already present"),
                AppendResult(stream=stream2, new_version=1, position=_pos("target", 9)),
            ]
        )
        copier = BulkCopier(MagicMock(), target_store, MagicMock(), enable_tracing=False)
        events = [
            create_test_events(1, tenant_id, agg1_id, start=1, stream_id=stream1)[0],
            create_test_events(1, tenant_id, agg2_id, start=2, stream_id=stream2)[0],
        ]
        last_pos = await copier._write_batch(uuid4(), tenant_id, events)
        assert target_store.append.call_count == 3 and last_pos == _pos("target", 9)

    async def test_write_batch_all_duplicates_returns_none(self) -> None:
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(side_effect=DuplicateEventError("already present"))
        tenant_id, agg_id = uuid4(), uuid4()
        copier = BulkCopier(MagicMock(), target_store, MagicMock(), enable_tracing=False)
        events = create_test_events(1, tenant_id, agg_id, start=1)
        assert await copier._write_batch(uuid4(), tenant_id, events) is None

    async def test_duplicate_on_per_event_append_records_no_mapping(self) -> None:
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        tenant_id, agg_id, migration_id = uuid4(), uuid4(), uuid4()
        stream = StreamId(aggregate_id=agg_id, category="TestAggregate")
        target_store.append = AsyncMock(
            side_effect=[
                DuplicateEventError("already present"),
                AppendResult(stream=stream, new_version=1, position=_pos("target", 7)),
            ]
        )
        position_mapper = AsyncMock()
        copier = BulkCopier(
            MagicMock(),
            target_store,
            MagicMock(),
            position_mapper=position_mapper,
            enable_tracing=False,
        )
        events = create_test_events(2, tenant_id, agg_id, start=1)
        await copier._write_batch(migration_id, tenant_id, events)
        assert position_mapper.record_mapping.call_count == 1
        assert position_mapper.record_mapping.call_args.args == (
            migration_id,
            events[1].position,
            _pos("target", 7),
            events[1].event.event_id,
        )


@pytest.mark.asyncio
class TestBulkCopierCountTenantEvents:
    """Tests for BulkCopier._count_tenant_events() method."""

    async def test_count_tenant_events(self) -> None:
        source_store = AsyncMock()

        async def event_generator(from_position, options):
            for _ in range(15):
                yield MagicMock()

        source_store.read_all = event_generator
        copier = BulkCopier(source_store, MagicMock(), MagicMock(), enable_tracing=False)
        assert await copier._count_tenant_events(uuid4()) == 15

    async def test_count_tenant_events_empty(self) -> None:
        source_store = AsyncMock()

        async def empty_generator(from_position, options):
            return
            yield

        source_store.read_all = empty_generator
        copier = BulkCopier(source_store, MagicMock(), MagicMock(), enable_tracing=False)
        assert await copier._count_tenant_events(uuid4()) == 0


@pytest.mark.asyncio
class TestBulkCopierStreamTenantEvents:
    """Tests for BulkCopier._stream_tenant_events() method."""

    async def test_stream_tenant_events_with_from_position(self) -> None:
        tenant_id = uuid4()
        source_store = AsyncMock()
        captured_from_position, captured_options = "unset", None

        async def event_generator(from_position, options):
            nonlocal captured_from_position, captured_options
            captured_from_position, captured_options = from_position, options
            yield MagicMock()

        source_store.read_all = event_generator
        copier = BulkCopier(source_store, MagicMock(), MagicMock(), enable_tracing=False)
        from_position = _pos("source", 100)
        async for _ in copier._stream_tenant_events(tenant_id, from_position):
            pass
        assert captured_from_position == from_position
        assert captured_options is not None and captured_options.tenant_id == tenant_id
