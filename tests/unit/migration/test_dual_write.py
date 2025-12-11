"""
Unit tests for DualWriteInterceptor.

Tests cover:
- DualWriteInterceptor initialization
- Dual-write semantics (source-first, target best-effort)
- Source failure propagation
- Target failure handling (logged but operation succeeds)
- Failure tracking and statistics
- EventStore protocol implementation (read operations)
- Tracing integration
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.exceptions import OptimisticLockError
from eventsource.migration.dual_write import (
    DualWriteInterceptor,
    FailedWrite,
    FailureStats,
)
from eventsource.stores.interface import (
    AppendResult,
    EventStream,
)

# =============================================================================
# Test Events
# =============================================================================


class TestEvent(DomainEvent):
    """Test event for unit tests."""

    event_type: str = "TestEvent"
    aggregate_type: str = "TestAggregate"
    data: str = "test"


# =============================================================================
# Test Fixtures
# =============================================================================


def create_mock_store(global_position: int = 100) -> MagicMock:
    """Create a mock event store with proper async support."""
    store = MagicMock()
    store.append_events = AsyncMock(
        return_value=AppendResult.successful(new_version=1, global_position=global_position)
    )
    store.get_events = AsyncMock(
        return_value=EventStream(
            aggregate_id=uuid4(),
            aggregate_type="TestAggregate",
            events=[],
            version=0,
        )
    )
    store.get_events_by_type = AsyncMock(return_value=[])
    store.event_exists = AsyncMock(return_value=False)
    store.get_stream_version = AsyncMock(return_value=0)
    store.get_global_position = AsyncMock(return_value=global_position)

    # For async generators
    async def mock_read_stream(*args, **kwargs):
        return
        yield

    async def mock_read_all(*args, **kwargs):
        return
        yield

    store.read_stream = mock_read_stream
    store.read_all = mock_read_all

    return store


@pytest.fixture
def source_store() -> MagicMock:
    """Create a mock source event store."""
    return create_mock_store(global_position=100)


@pytest.fixture
def target_store() -> MagicMock:
    """Create a mock target event store."""
    return create_mock_store(global_position=50)


@pytest.fixture
def tenant_id() -> uuid4:
    """Create a tenant ID for testing."""
    return uuid4()


@pytest.fixture
def interceptor(
    source_store: MagicMock,
    target_store: MagicMock,
    tenant_id: uuid4,
) -> DualWriteInterceptor:
    """Create a DualWriteInterceptor with mock dependencies."""
    return DualWriteInterceptor(
        source_store=source_store,
        target_store=target_store,
        tenant_id=tenant_id,
        enable_tracing=False,
    )


# =============================================================================
# Test FailedWrite Dataclass
# =============================================================================


class TestFailedWrite:
    """Tests for FailedWrite dataclass."""

    def test_failed_write_creation(self) -> None:
        """Test creating a FailedWrite record."""
        aggregate_id = uuid4()
        event_ids = [uuid4(), uuid4()]
        timestamp = datetime.now(UTC)

        failed_write = FailedWrite(
            timestamp=timestamp,
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            event_ids=event_ids,
            error_message="Connection refused",
            source_position=100,
        )

        assert failed_write.timestamp == timestamp
        assert failed_write.aggregate_id == aggregate_id
        assert failed_write.aggregate_type == "Order"
        assert failed_write.event_ids == event_ids
        assert failed_write.error_message == "Connection refused"
        assert failed_write.source_position == 100


# =============================================================================
# Test FailureStats Dataclass
# =============================================================================


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


# =============================================================================
# Test DualWriteInterceptor Initialization
# =============================================================================


class TestDualWriteInterceptorInit:
    """Tests for DualWriteInterceptor initialization."""

    def test_init_with_defaults(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test initialization with default parameters."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
        )

        assert interceptor.source_store == source_store
        assert interceptor.target_store == target_store
        assert interceptor.tenant_id == tenant_id
        assert interceptor._max_failure_history == 1000

    def test_init_with_custom_max_failure_history(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test initialization with custom max failure history."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            max_failure_history=500,
        )

        assert interceptor._max_failure_history == 500

    def test_init_with_tracing_disabled(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test initialization with tracing disabled."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        assert interceptor._enable_tracing is False

    def test_properties(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test property accessors."""
        assert interceptor.source_store is source_store
        assert interceptor.target_store is target_store
        assert interceptor.tenant_id == tenant_id


# =============================================================================
# Test Append Events - Success Cases
# =============================================================================


class TestAppendEventsSuccess:
    """Tests for successful append_events operations."""

    @pytest.mark.asyncio
    async def test_append_events_writes_to_both_stores(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that events are written to both stores."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        result = await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        assert result.success is True
        source_store.append_events.assert_called_once_with(
            aggregate_id, "TestAggregate", [event], 0
        )
        target_store.append_events.assert_called_once_with(
            aggregate_id, "TestAggregate", [event], 0
        )

    @pytest.mark.asyncio
    async def test_append_events_returns_source_result(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
    ) -> None:
        """Test that source result is returned."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        expected_result = AppendResult.successful(new_version=5, global_position=200)
        source_store.append_events.return_value = expected_result

        result = await interceptor.append_events(aggregate_id, "TestAggregate", [event], 4)

        assert result.success is True
        assert result.new_version == 5
        assert result.global_position == 200

    @pytest.mark.asyncio
    async def test_append_events_empty_list_raises(
        self,
        interceptor: DualWriteInterceptor,
    ) -> None:
        """Test that empty event list raises ValueError."""
        with pytest.raises(ValueError, match="Cannot append empty event list"):
            await interceptor.append_events(uuid4(), "TestAggregate", [], 0)

    @pytest.mark.asyncio
    async def test_append_events_no_failures_tracked(
        self,
        interceptor: DualWriteInterceptor,
    ) -> None:
        """Test that successful writes don't track failures."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        assert len(interceptor.get_failed_writes()) == 0
        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 0


# =============================================================================
# Test Append Events - Source Failure
# =============================================================================


class TestAppendEventsSourceFailure:
    """Tests for source failure scenarios."""

    @pytest.mark.asyncio
    async def test_source_failure_propagates(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that source failures propagate to caller."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        source_store.append_events.side_effect = OptimisticLockError(aggregate_id, 0, 5)

        with pytest.raises(OptimisticLockError):
            await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        # Target should not be called if source fails
        target_store.append_events.assert_not_called()

    @pytest.mark.asyncio
    async def test_source_conflict_returns_immediately(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that source conflict result returns without target write."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        source_store.append_events.return_value = AppendResult.conflicted(5)

        result = await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        assert result.success is False
        assert result.conflict is True
        target_store.append_events.assert_not_called()


# =============================================================================
# Test Append Events - Target Failure
# =============================================================================


class TestAppendEventsTargetFailure:
    """Tests for target failure scenarios."""

    @pytest.mark.asyncio
    async def test_target_failure_operation_succeeds(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that target failures don't fail the operation."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        target_store.append_events.side_effect = Exception("Connection refused")

        result = await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        # Operation should succeed (source wrote successfully)
        assert result.success is True

    @pytest.mark.asyncio
    async def test_target_failure_tracked(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that target failures are tracked."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        target_store.append_events.side_effect = Exception("Connection refused")

        await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        failed_writes = interceptor.get_failed_writes()
        assert len(failed_writes) == 1
        assert failed_writes[0].aggregate_id == aggregate_id
        assert failed_writes[0].aggregate_type == "TestAggregate"
        assert event.event_id in failed_writes[0].event_ids
        assert "Connection refused" in failed_writes[0].error_message

    @pytest.mark.asyncio
    async def test_target_failure_logs_warning(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that target failures are logged as warnings."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        target_store.append_events.side_effect = Exception("Target unavailable")

        with patch("eventsource.migration.dual_write.logger") as mock_logger:
            await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

            # Check warning was logged
            mock_logger.warning.assert_called_once()
            warning_msg = mock_logger.warning.call_args[0][0]
            assert "Target write failed" in warning_msg
            assert str(aggregate_id) in warning_msg

    @pytest.mark.asyncio
    async def test_multiple_target_failures_tracked(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that multiple target failures are tracked."""
        target_store.append_events.side_effect = Exception("Connection refused")

        # Perform multiple writes
        for _ in range(3):
            aggregate_id = uuid4()
            event = TestEvent(aggregate_id=aggregate_id)
            await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        failed_writes = interceptor.get_failed_writes()
        assert len(failed_writes) == 3

        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 3
        assert stats.unique_aggregates_affected == 3


# =============================================================================
# Test Failure Tracking
# =============================================================================


class TestFailureTracking:
    """Tests for failure tracking functionality."""

    @pytest.mark.asyncio
    async def test_failure_stats_aggregation(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that failure statistics are correctly aggregated."""
        target_store.append_events.side_effect = Exception("Error")

        aggregate_id = uuid4()

        # Write 3 events to the same aggregate
        for i in range(3):
            event = TestEvent(aggregate_id=aggregate_id)
            await interceptor.append_events(aggregate_id, "TestAggregate", [event], i)

        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 3
        assert stats.total_events_failed == 3
        assert stats.unique_aggregates_affected == 1  # Same aggregate

    @pytest.mark.asyncio
    async def test_failure_timestamps_tracked(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that first and last failure timestamps are tracked."""
        target_store.append_events.side_effect = Exception("Error")

        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        stats = interceptor.get_failure_stats()
        assert stats.first_failure_at is not None
        assert stats.last_failure_at is not None
        assert stats.first_failure_at <= stats.last_failure_at

    def test_clear_failure_history(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test clearing failure history."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Manually add some failures
        interceptor._failed_writes.append(
            FailedWrite(
                timestamp=datetime.now(UTC),
                aggregate_id=uuid4(),
                aggregate_type="Test",
                event_ids=[uuid4()],
                error_message="Error",
                source_position=1,
            )
        )
        interceptor._affected_aggregates.add(uuid4())

        count = interceptor.clear_failure_history()

        assert count == 1
        assert len(interceptor.get_failed_writes()) == 0
        assert len(interceptor._affected_aggregates) == 0

    @pytest.mark.asyncio
    async def test_failure_history_trimming(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test that failure history is trimmed when exceeding max."""
        max_history = 5
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
            max_failure_history=max_history,
        )

        target_store.append_events.side_effect = Exception("Error")

        # Write more than max_history events
        for _i in range(max_history + 3):
            aggregate_id = uuid4()
            event = TestEvent(aggregate_id=aggregate_id)
            await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        # Should be trimmed to max_history
        assert len(interceptor.get_failed_writes()) == max_history


# =============================================================================
# Test Read Operations (Delegation to Source)
# =============================================================================


class TestReadOperations:
    """Tests for read operations (should delegate to source store)."""

    @pytest.mark.asyncio
    async def test_get_events_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that get_events delegates to source store."""
        aggregate_id = uuid4()

        await interceptor.get_events(aggregate_id, "TestAggregate")

        source_store.get_events.assert_called_once()
        target_store.get_events.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_events_by_type_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that get_events_by_type delegates to source store."""
        await interceptor.get_events_by_type("TestAggregate")

        source_store.get_events_by_type.assert_called_once()
        target_store.get_events_by_type.assert_not_called()

    @pytest.mark.asyncio
    async def test_event_exists_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that event_exists delegates to source store."""
        event_id = uuid4()

        await interceptor.event_exists(event_id)

        source_store.event_exists.assert_called_once_with(event_id)
        target_store.event_exists.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_stream_version_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that get_stream_version delegates to source store."""
        aggregate_id = uuid4()

        await interceptor.get_stream_version(aggregate_id, "TestAggregate")

        source_store.get_stream_version.assert_called_once_with(aggregate_id, "TestAggregate")
        target_store.get_stream_version.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_global_position_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that get_global_position delegates to source store."""
        result = await interceptor.get_global_position()

        assert result == 100
        source_store.get_global_position.assert_called_once()
        target_store.get_global_position.assert_not_called()

    @pytest.mark.asyncio
    async def test_read_stream_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
    ) -> None:
        """Test that read_stream delegates to source store."""
        stream_id = f"{uuid4()}:TestAggregate"
        events_read = []

        async for event in interceptor.read_stream(stream_id):
            events_read.append(event)

        # Verify we iterated through source's generator
        assert events_read == []

    @pytest.mark.asyncio
    async def test_read_all_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
    ) -> None:
        """Test that read_all delegates to source store."""
        events_read = []

        async for event in interceptor.read_all():
            events_read.append(event)

        assert events_read == []


# =============================================================================
# Test Integration with TenantStoreRouter Pattern
# =============================================================================


class TestRouterIntegration:
    """Tests for integration patterns with TenantStoreRouter."""

    @pytest.mark.asyncio
    async def test_interceptor_works_as_eventstore_replacement(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test that interceptor can be used as EventStore replacement."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Verify all EventStore methods are available
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        # Write operations
        result = await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)
        assert result.success is True

        # Read operations
        await interceptor.get_events(aggregate_id)
        await interceptor.get_events_by_type("TestAggregate")
        await interceptor.event_exists(uuid4())
        await interceptor.get_stream_version(aggregate_id, "TestAggregate")
        await interceptor.get_global_position()

        # Streaming operations
        async for _ in interceptor.read_stream(f"{aggregate_id}:TestAggregate"):
            pass

        async for _ in interceptor.read_all():
            pass


# =============================================================================
# Test Concurrent Operations
# =============================================================================


class TestConcurrentOperations:
    """Tests for concurrent operation handling."""

    @pytest.mark.asyncio
    async def test_concurrent_writes(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test that concurrent writes work correctly."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        async def do_write(index: int):
            aggregate_id = uuid4()
            event = TestEvent(aggregate_id=aggregate_id, data=f"data-{index}")
            return await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        # Run multiple concurrent writes
        tasks = [asyncio.create_task(do_write(i)) for i in range(10)]
        results = await asyncio.gather(*tasks)

        # All writes should succeed
        assert all(r.success for r in results)
        assert source_store.append_events.call_count == 10
        assert target_store.append_events.call_count == 10

    @pytest.mark.asyncio
    async def test_concurrent_writes_with_target_failures(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """Test concurrent writes when target fails intermittently."""
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Make target fail every other call
        call_count = [0]

        async def intermittent_failure(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                raise Exception("Intermittent failure")
            return AppendResult.successful(1, 1)

        target_store.append_events = intermittent_failure

        async def do_write(index: int):
            aggregate_id = uuid4()
            event = TestEvent(aggregate_id=aggregate_id)
            return await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        # Run concurrent writes
        tasks = [asyncio.create_task(do_write(i)) for i in range(10)]
        results = await asyncio.gather(*tasks)

        # All source writes should succeed
        assert all(r.success for r in results)

        # Some target writes should have failed
        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 5


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_multiple_events_single_write(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test writing multiple events in single append."""
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event-{i}") for i in range(5)]

        result = await interceptor.append_events(aggregate_id, "TestAggregate", events, 0)

        assert result.success is True
        source_store.append_events.assert_called_once_with(aggregate_id, "TestAggregate", events, 0)
        target_store.append_events.assert_called_once_with(aggregate_id, "TestAggregate", events, 0)

    @pytest.mark.asyncio
    async def test_failure_tracking_multiple_events(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that multiple events in failed write are all tracked."""
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event-{i}") for i in range(3)]

        target_store.append_events.side_effect = Exception("Error")

        await interceptor.append_events(aggregate_id, "TestAggregate", events, 0)

        failed_writes = interceptor.get_failed_writes()
        assert len(failed_writes) == 1
        assert len(failed_writes[0].event_ids) == 3

        stats = interceptor.get_failure_stats()
        assert stats.total_events_failed == 3

    @pytest.mark.asyncio
    async def test_source_position_tracked_in_failure(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that source position is tracked when target fails."""
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)

        source_store.append_events.return_value = AppendResult.successful(1, 500)
        target_store.append_events.side_effect = Exception("Error")

        await interceptor.append_events(aggregate_id, "TestAggregate", [event], 0)

        failed_writes = interceptor.get_failed_writes()
        assert failed_writes[0].source_position == 500
