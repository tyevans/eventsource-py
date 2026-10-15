"""Unit tests for DualWriteInterceptor initialization, append success, and FailedWrite."""

from datetime import UTC, datetime
from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.dual_write import DualWriteInterceptor, FailedWrite
from eventsource.domain import StreamId
from eventsource.ports import AppendResult, ExpectedVersion, Position

from ._fixtures import TestEvent, sid


class TestFailedWrite:
    def test_failed_write_creation(self) -> None:
        aggregate_id, event_ids = uuid4(), [uuid4(), uuid4()]
        timestamp = datetime.now(UTC)
        position = Position(store_id="source", key=(100,))

        failed_write = FailedWrite(
            timestamp=timestamp,
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            event_ids=event_ids,
            error_message="Connection refused",
            source_position=position,
        )

        assert failed_write.timestamp == timestamp
        assert failed_write.aggregate_id == aggregate_id
        assert failed_write.aggregate_type == "Order"
        assert failed_write.event_ids == event_ids
        assert failed_write.error_message == "Connection refused"
        assert failed_write.source_position == position

    def test_failed_write_with_no_position(self) -> None:
        failed_write = FailedWrite(
            timestamp=datetime.now(UTC),
            aggregate_id=uuid4(),
            aggregate_type="Order",
            event_ids=[uuid4()],
            error_message="Connection refused",
            source_position=None,
        )
        assert failed_write.source_position is None


class TestDualWriteInterceptorInit:
    def test_init_with_defaults(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        interceptor_inst = DualWriteInterceptor(
            source_store=source_store, target_store=target_store, tenant_id=tenant_id
        )
        assert interceptor_inst.source_store == source_store
        assert interceptor_inst.target_store == target_store
        assert interceptor_inst.tenant_id == tenant_id
        assert interceptor_inst._max_failure_history == 1000

    def test_init_with_custom_max_failure_history(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        interceptor_inst = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            max_failure_history=500,
        )
        assert interceptor_inst._max_failure_history == 500

    def test_init_with_tracing_disabled(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        interceptor_inst = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        assert interceptor_inst._enable_tracing is False

    def test_properties(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: UUID,
    ) -> None:
        assert interceptor.source_store is source_store
        assert interceptor.target_store is target_store
        assert interceptor.tenant_id == tenant_id


class TestAppendSuccess:
    """Tests for successful append operations."""

    async def test_append_writes_to_both_stores(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that events are written to both stores."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        result = await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        assert result.stream == stream
        source_store.append.assert_called_once_with(stream, [event], ExpectedVersion.no_stream())
        # The mirror does not forward the caller's expectation: it appends
        # at the exact pre-append source version, so it only ever extends
        # a converged target stream.
        target_store.append.assert_called_once_with(stream, [event], ExpectedVersion.exact(0))

    async def test_append_returns_source_result(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
    ) -> None:
        """Test that source result is returned, identity-checked (no arithmetic)."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        expected_result = AppendResult(
            stream=stream,
            new_version=5,
            position=Position(store_id="source", key=(200,)),
        )
        source_store.append.side_effect = None
        source_store.append.return_value = expected_result

        result = await interceptor.append(stream, [event], ExpectedVersion.exact(4))

        assert result is expected_result
        assert result.new_version == 5
        assert result.position == Position(store_id="source", key=(200,))

    async def test_append_empty_list_raises(
        self,
        interceptor: DualWriteInterceptor,
    ) -> None:
        """Test that empty event list raises ValueError."""
        with pytest.raises(ValueError, match="Cannot append empty event list"):
            await interceptor.append(sid(), [], ExpectedVersion.any_())

    async def test_append_no_failures_tracked(
        self,
        interceptor: DualWriteInterceptor,
    ) -> None:
        """Test that successful writes don't track failures."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        assert len(interceptor.get_failed_writes()) == 0
        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 0


__all__ = [  # pragma: allowlist secret
    "TestAppendSuccess",
    "TestDualWriteInterceptorInit",
    "TestFailedWrite",
]
