"""Unit tests for DualWriteInterceptor failure handling, failure tracking, and FailureStats."""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.dual_write import (
    DualWriteInterceptor,
    FailedWrite,
    FailureStats,
)
from eventsource.application.migration.metrics import clear_metrics_registry, get_migration_metrics
from eventsource.domain import StreamId
from eventsource.domain.exceptions import OptimisticLockError
from eventsource.ports import AppendResult, ExpectedVersion, Position

from ._fixtures import TestEvent


def _make_event_and_stream(
    aggregate_id: UUID | None = None,
) -> tuple[UUID, TestEvent, StreamId]:
    agg_id = aggregate_id or uuid4()
    return agg_id, TestEvent(aggregate_id=agg_id), StreamId(agg_id, "TestAggregate")


class TestFailureStats:
    def test_empty_stats(self) -> None:
        stats = FailureStats()
        vals = (stats.total_failures, stats.total_events_failed, stats.unique_aggregates_affected)
        assert (
            vals == (0, 0, 0) and stats.first_failure_at is None and stats.last_failure_at is None
        )

    def test_stats_to_dict(self) -> None:
        ts = datetime.now(UTC)
        stats = FailureStats(
            total_failures=5,
            total_events_failed=10,
            first_failure_at=ts,
            last_failure_at=ts,
            unique_aggregates_affected=3,
        )
        res = stats.to_dict()
        assert res["total_failures"] == 5 and res["total_events_failed"] == 10
        assert res["first_failure_at"] == ts.isoformat()
        assert res["last_failure_at"] == ts.isoformat() and res["unique_aggregates_affected"] == 3

    def test_stats_to_dict_with_none_timestamps(self) -> None:
        result = FailureStats().to_dict()
        assert result["first_failure_at"] is None and result["last_failure_at"] is None


class TestAppendSourceFailure:
    async def test_source_failure_propagates(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        aggregate_id, event, stream = _make_event_and_stream()
        source_store.append.side_effect = OptimisticLockError(aggregate_id, 0, 5)
        with pytest.raises(OptimisticLockError):
            await interceptor.append(stream, [event], ExpectedVersion.no_stream())
        target_store.append.assert_not_called()


class TestAppendTargetFailure:
    async def test_target_failure_operation_succeeds(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        _aggregate_id, event, stream = _make_event_and_stream()
        expected = AppendResult(
            stream=stream, new_version=1, position=Position(store_id="source", key=(1,))
        )
        source_store.append.side_effect = None
        source_store.append.return_value = expected
        target_store.append.side_effect = Exception("Connection refused")
        assert await interceptor.append(stream, [event], ExpectedVersion.no_stream()) is expected

    async def test_target_failure_tracked(
        self, interceptor: DualWriteInterceptor, source_store: MagicMock, target_store: MagicMock
    ) -> None:
        aggregate_id, event, stream = _make_event_and_stream()
        target_store.append.side_effect = Exception("Connection refused")
        await interceptor.append(stream, [event], ExpectedVersion.no_stream())
        failed_writes = interceptor.get_failed_writes()
        assert len(failed_writes) == 1 and failed_writes[0].aggregate_id == aggregate_id
        assert failed_writes[0].aggregate_type == "TestAggregate"
        assert event.event_id in failed_writes[0].event_ids
        assert "Connection refused" in failed_writes[0].error_message

    async def test_target_failure_logs_warning(
        self, interceptor: DualWriteInterceptor, target_store: MagicMock
    ) -> None:
        aggregate_id, event, stream = _make_event_and_stream()
        target_store.append.side_effect = Exception("Target unavailable")
        with patch("eventsource.application.migration.dual_write.logger") as mock_logger:
            await interceptor.append(stream, [event], ExpectedVersion.no_stream())
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "Target write failed" in warning_msg and str(aggregate_id) in warning_msg

    async def test_target_failure_records_metric_when_migration_id_set(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        clear_metrics_registry()
        migration_id = uuid4()
        inst = DualWriteInterceptor(
            source_store, target_store, tenant_id, migration_id=migration_id, enable_tracing=False
        )
        _aggregate_id, event, stream = _make_event_and_stream()
        target_store.append.side_effect = Exception("Connection refused")
        await inst.append(stream, [event], ExpectedVersion.no_stream())
        snapshot = get_migration_metrics(str(migration_id), str(tenant_id)).get_snapshot()
        assert snapshot.failed_target_writes == 1
        clear_metrics_registry()

    async def test_multiple_target_failures_tracked(
        self, interceptor: DualWriteInterceptor, target_store: MagicMock
    ) -> None:
        target_store.append.side_effect = Exception("Connection refused")
        for _ in range(3):
            _aggregate_id, event, stream = _make_event_and_stream()
            await interceptor.append(stream, [event], ExpectedVersion.no_stream())
        assert len(interceptor.get_failed_writes()) == 3
        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 3 and stats.unique_aggregates_affected == 3


class TestFailureTracking:
    async def test_failure_stats_aggregation(
        self, interceptor: DualWriteInterceptor, target_store: MagicMock
    ) -> None:
        target_store.append.side_effect = Exception("Error")
        aggregate_id, _event, stream = _make_event_and_stream()
        for i in range(3):
            expected = ExpectedVersion.no_stream() if i == 0 else ExpectedVersion.exact(i)
            await interceptor.append(stream, [TestEvent(aggregate_id=aggregate_id)], expected)
        stats = interceptor.get_failure_stats()
        vals = (stats.total_failures, stats.total_events_failed, stats.unique_aggregates_affected)
        assert vals == (3, 3, 1)

    async def test_failure_timestamps_tracked(
        self, interceptor: DualWriteInterceptor, target_store: MagicMock
    ) -> None:
        target_store.append.side_effect = Exception("Error")
        _aggregate_id, event, stream = _make_event_and_stream()
        await interceptor.append(stream, [event], ExpectedVersion.no_stream())
        stats = interceptor.get_failure_stats()
        assert stats.first_failure_at is not None and stats.last_failure_at is not None
        assert stats.first_failure_at <= stats.last_failure_at

    def test_clear_failure_history(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        inst = DualWriteInterceptor(source_store, target_store, tenant_id, enable_tracing=False)
        pos = Position(store_id="source", key=(1,))
        inst._failed_writes.append(
            FailedWrite(datetime.now(UTC), uuid4(), "Test", [uuid4()], "Error", pos)
        )
        inst._affected_aggregates.add(uuid4())
        assert inst.clear_failure_history() == 1
        assert len(inst.get_failed_writes()) == 0 and len(inst._affected_aggregates) == 0

    async def test_failure_history_trimming(
        self, source_store: MagicMock, target_store: MagicMock, tenant_id: UUID
    ) -> None:
        max_history = 5
        inst = DualWriteInterceptor(
            source_store,
            target_store,
            tenant_id,
            enable_tracing=False,
            max_failure_history=max_history,
        )
        target_store.append.side_effect = Exception("Error")
        for _i in range(max_history + 3):
            _aggregate_id, event, stream = _make_event_and_stream()
            await inst.append(stream, [event], ExpectedVersion.no_stream())
        assert len(inst.get_failed_writes()) == max_history


__all__ = [  # pragma: allowlist secret
    "TestAppendSourceFailure",
    "TestAppendTargetFailure",
    "TestFailureStats",
    "TestFailureTracking",
]
