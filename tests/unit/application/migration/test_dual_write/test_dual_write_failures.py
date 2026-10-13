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
from eventsource.domain import StreamId
from eventsource.domain.exceptions import OptimisticLockError
from eventsource.ports import AppendResult, ExpectedVersion, Position

from ._fixtures import (
    TestEvent,
)


class TestFailureStats:
    """Tests for FailureStats dataclass."""

    def test_empty_stats(self) -> None:
        """Test default empty statistics."""
        stats = FailureStats()

        assert stats.total_failures == 0
        assert stats.total_events_failed == 0
        assert stats.first_failure_at is None
        assert stats.last_failure_at is None
        assert stats.unique_aggregates_affected == 0

    def test_stats_to_dict(self) -> None:
        """Test converting stats to dictionary."""
        timestamp = datetime.now(UTC)
        stats = FailureStats(
            total_failures=5,
            total_events_failed=10,
            first_failure_at=timestamp,
            last_failure_at=timestamp,
            unique_aggregates_affected=3,
        )

        result = stats.to_dict()

        assert result["total_failures"] == 5
        assert result["total_events_failed"] == 10
        assert result["first_failure_at"] == timestamp.isoformat()
        assert result["last_failure_at"] == timestamp.isoformat()
        assert result["unique_aggregates_affected"] == 3

    def test_stats_to_dict_with_none_timestamps(self) -> None:
        """Test converting stats with None timestamps."""
        stats = FailureStats()

        result = stats.to_dict()

        assert result["first_failure_at"] is None
        assert result["last_failure_at"] is None


class TestAppendSourceFailure:
    """Tests for source failure scenarios."""

    async def test_source_failure_propagates(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that source failures (a version conflict) propagate to the caller."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        source_store.append.side_effect = OptimisticLockError(aggregate_id, 0, 5)

        with pytest.raises(OptimisticLockError):
            await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        # Target should not be called if source fails
        target_store.append.assert_not_called()


class TestAppendTargetFailure:
    """Tests for target failure scenarios."""

    async def test_target_failure_operation_succeeds(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that target failures don't fail the operation."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        expected_result = AppendResult(
            stream=stream,
            new_version=1,
            position=Position(store_id="source", key=(1,)),
        )
        source_store.append.side_effect = None
        source_store.append.return_value = expected_result
        target_store.append.side_effect = Exception("Connection refused")

        result = await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        # Operation should succeed (source wrote successfully) -- the source
        # result is returned unchanged despite the target failure.
        assert result is expected_result

    async def test_target_failure_tracked(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that target failures are tracked."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        target_store.append.side_effect = Exception("Connection refused")

        await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        failed_writes = interceptor.get_failed_writes()
        assert len(failed_writes) == 1
        assert failed_writes[0].aggregate_id == aggregate_id
        assert failed_writes[0].aggregate_type == "TestAggregate"
        assert event.event_id in failed_writes[0].event_ids
        assert "Connection refused" in failed_writes[0].error_message

    async def test_target_failure_logs_warning(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that target failures are logged as warnings."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        target_store.append.side_effect = Exception("Target unavailable")

        with patch("eventsource.application.migration.dual_write.logger") as mock_logger:
            await interceptor.append(stream, [event], ExpectedVersion.no_stream())

            # Check warning was logged
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "Target write failed" in warning_msg
            assert str(aggregate_id) in warning_msg

    async def test_target_failure_records_metric_when_migration_id_set(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: UUID,
    ) -> None:
        """A mirror failure reports through the real MigrationMetrics
        instance for this migration when migration_id is set -- not a
        standalone metrics object the test constructs and calls directly."""
        from eventsource.application.migration.metrics import (
            clear_metrics_registry,
            get_migration_metrics,
        )

        clear_metrics_registry()
        migration_id = uuid4()
        interceptor_with_migration = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            migration_id=migration_id,
            enable_tracing=False,
        )
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        target_store.append.side_effect = Exception("Connection refused")

        await interceptor_with_migration.append(stream, [event], ExpectedVersion.no_stream())

        snapshot = get_migration_metrics(str(migration_id), str(tenant_id)).get_snapshot()
        assert snapshot.failed_target_writes == 1

        clear_metrics_registry()

    async def test_multiple_target_failures_tracked(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that multiple target failures are tracked."""
        target_store.append.side_effect = Exception("Connection refused")

        # Perform multiple writes
        for _ in range(3):
            aggregate_id = uuid4()
            event = TestEvent(aggregate_id=aggregate_id)
            stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
            await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        failed_writes = interceptor.get_failed_writes()
        assert len(failed_writes) == 3

        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 3
        assert stats.unique_aggregates_affected == 3


class TestFailureTracking:
    """Tests for failure tracking functionality."""

    async def test_failure_stats_aggregation(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that failure statistics are correctly aggregated."""
        target_store.append.side_effect = Exception("Error")

        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        # Write 3 events to the same aggregate
        for i in range(3):
            event = TestEvent(aggregate_id=aggregate_id)
            expected = ExpectedVersion.no_stream() if i == 0 else ExpectedVersion.exact(i)
            await interceptor.append(stream, [event], expected)

        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 3
        assert stats.total_events_failed == 3
        assert stats.unique_aggregates_affected == 1  # Same aggregate

    async def test_failure_timestamps_tracked(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that first and last failure timestamps are tracked."""
        target_store.append.side_effect = Exception("Error")

        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        stats = interceptor.get_failure_stats()
        assert stats.first_failure_at is not None
        assert stats.last_failure_at is not None
        assert stats.first_failure_at <= stats.last_failure_at

    def test_clear_failure_history(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: UUID,
    ) -> None:
        """Test clearing failure history."""
        interceptor_inst = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Manually add some failures
        interceptor_inst._failed_writes.append(
            FailedWrite(
                timestamp=datetime.now(UTC),
                aggregate_id=uuid4(),
                aggregate_type="Test",
                event_ids=[uuid4()],
                error_message="Error",
                source_position=Position(store_id="source", key=(1,)),
            )
        )
        interceptor_inst._affected_aggregates.add(uuid4())

        count = interceptor_inst.clear_failure_history()

        assert count == 1
        assert len(interceptor_inst.get_failed_writes()) == 0
        assert len(interceptor_inst._affected_aggregates) == 0

    async def test_failure_history_trimming(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: UUID,
    ) -> None:
        """Test that failure history is trimmed when exceeding max."""
        max_history = 5
        interceptor_inst = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
            max_failure_history=max_history,
        )

        target_store.append.side_effect = Exception("Error")

        # Write more than max_history events
        for _i in range(max_history + 3):
            aggregate_id = uuid4()
            event = TestEvent(aggregate_id=aggregate_id)
            stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
            await interceptor_inst.append(stream, [event], ExpectedVersion.no_stream())

        # Should be trimmed to max_history
        assert len(interceptor_inst.get_failed_writes()) == max_history


__all__ = [  # pragma: allowlist secret
    "TestAppendSourceFailure",
    "TestAppendTargetFailure",
    "TestFailureStats",
    "TestFailureTracking",
]
