"""
Unit tests for DualWriteInterceptor.

Tests cover:
- DualWriteInterceptor initialization
- Dual-write semantics (source-first, target best-effort)
- Source failure propagation
- Target failure handling (logged but operation succeeds)
- Failure tracking and statistics
- FullEventStore port (structural) implementation (read operations)
- Tracing integration
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.application.migration.dual_write import (
    DualWriteInterceptor,
    FailedWrite,
    FailureStats,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import OptimisticLockError
from eventsource.ports import AppendResult, ExpectedVersion, FullEventStore, Position

# =============================================================================
# Test Events
# =============================================================================


class TestEvent(DomainEvent):
    """Test event for unit tests."""

    aggregate_type: str = "TestAggregate"
    data: str = "test"


def sid(aggregate_id=None, category: str = "TestAggregate") -> StreamId:
    """Build a StreamId for tests."""
    return StreamId(aggregate_id=aggregate_id or uuid4(), category=category)


# =============================================================================
# Test Fixtures
# =============================================================================


def create_mock_store(store_id: str = "store", position_key: tuple = (100,)) -> MagicMock:
    """Create a mock FullEventStore with proper async support."""
    store = MagicMock()
    store.append = AsyncMock(
        side_effect=lambda stream, events, expected: AppendResult(
            stream=stream,
            new_version=len(events),
            position=Position(store_id=store_id, key=(1,)),
        )
    )
    store.event_exists = AsyncMock(return_value=False)
    store.get_stream_version = AsyncMock(return_value=0)
    store.current_position = AsyncMock(return_value=Position(store_id=store_id, key=position_key))

    # For async generators, we need to return the generator itself, not a coroutine
    def mock_read_stream(*args, **kwargs):
        return async_generator_mock([])

    def mock_read_all(*args, **kwargs):
        return async_generator_mock([])

    def mock_read_category(*args, **kwargs):
        return async_generator_mock([])

    store.read_stream = mock_read_stream
    store.read_all = mock_read_all
    store.read_category = mock_read_category

    return store


async def async_generator_mock(items: list):
    """Helper to create async generators for mocking."""
    for item in items:
        yield item


@pytest.fixture
def source_store() -> MagicMock:
    """Create a mock source event store."""
    return create_mock_store(store_id="source", position_key=(100,))


@pytest.fixture
def target_store() -> MagicMock:
    """Create a mock target event store."""
    return create_mock_store(store_id="target", position_key=(50,))


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
        """Test creating a FailedWrite for a feedless source store."""
        failed_write = FailedWrite(
            timestamp=datetime.now(UTC),
            aggregate_id=uuid4(),
            aggregate_type="Order",
            event_ids=[uuid4()],
            error_message="Connection refused",
            source_position=None,
        )

        assert failed_write.source_position is None


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
# Test Append - Success Cases
# =============================================================================


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


# =============================================================================
# Test Append - Source Failure
# =============================================================================


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


# =============================================================================
# Test Append - Target Failure
# =============================================================================


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
        tenant_id: uuid4,
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


# =============================================================================
# Test Failure Tracking
# =============================================================================


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
                source_position=Position(store_id="source", key=(1,)),
            )
        )
        interceptor._affected_aggregates.add(uuid4())

        count = interceptor.clear_failure_history()

        assert count == 1
        assert len(interceptor.get_failed_writes()) == 0
        assert len(interceptor._affected_aggregates) == 0

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

        target_store.append.side_effect = Exception("Error")

        # Write more than max_history events
        for _i in range(max_history + 3):
            aggregate_id = uuid4()
            event = TestEvent(aggregate_id=aggregate_id)
            stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
            await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        # Should be trimmed to max_history
        assert len(interceptor.get_failed_writes()) == max_history


# =============================================================================
# Test Read Operations (Delegation to Source)
# =============================================================================


class TestReadOperations:
    """Tests for read operations (should delegate to source store)."""

    async def test_read_category_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that read_category delegates to source store."""
        source_called = False
        target_called = False

        def source_read_category(*args, **kwargs):
            nonlocal source_called
            source_called = True
            return async_generator_mock([])

        def target_read_category(*args, **kwargs):
            nonlocal target_called
            target_called = True
            return async_generator_mock([])

        source_store.read_category = source_read_category
        target_store.read_category = target_read_category

        async for _ in interceptor.read_category("TestAggregate"):
            pass

        assert source_called is True
        assert target_called is False

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

    async def test_get_stream_version_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that get_stream_version delegates to source store."""
        stream = sid()

        await interceptor.get_stream_version(stream)

        source_store.get_stream_version.assert_called_once_with(stream)
        target_store.get_stream_version.assert_not_called()

    async def test_current_position_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that current_position delegates to source store."""
        result = await interceptor.current_position()

        assert result == Position(store_id="source", key=(100,))
        source_store.current_position.assert_called_once()
        target_store.current_position.assert_not_called()

    async def test_read_stream_delegates_to_source(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
    ) -> None:
        """Test that read_stream delegates to source store."""
        stream = sid()
        events_read = []

        async for event in interceptor.read_stream(stream):
            events_read.append(event)

        # Verify we iterated through source's generator
        assert events_read == []

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
# Test Structural Conformance / Router Integration
# =============================================================================


class TestRouterIntegration:
    """Tests for the interceptor as a `FullEventStore` structural replacement."""

    async def test_interceptor_stands_in_for_full_event_store(
        self,
        source_store: MagicMock,
        target_store: MagicMock,
        tenant_id: uuid4,
    ) -> None:
        """The interceptor is a drop-in wherever a `FullEventStore` is expected.

        This deliberately uses a concrete category throughout -- the old
        EventStore capability of looking a stream up by aggregate_id alone
        (omitting aggregate_type/category) is dropped under the ports shape
        and is not replaced by a cross-type port.
        """
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        _: FullEventStore = interceptor  # mypy-checked structural conformance

        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        event = TestEvent(aggregate_id=aggregate_id)

        # Write operation
        await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        # Read operations -- one call through each remaining member
        async for _ in interceptor.read_stream(stream):
            pass
        async for _ in interceptor.read_category("TestAggregate"):
            pass
        await interceptor.event_exists(event.event_id)
        await interceptor.get_stream_version(stream)
        async for _ in interceptor.read_all():
            pass
        await interceptor.current_position()


# =============================================================================
# Test Concurrent Operations
# =============================================================================


class TestConcurrentOperations:
    """Tests for concurrent operation handling."""

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
            stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
            return await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        # Run multiple concurrent writes
        tasks = [asyncio.create_task(do_write(i)) for i in range(10)]
        results = await asyncio.gather(*tasks)

        # All writes should succeed (no exception raised, sensible results)
        assert len(results) == 10
        assert all(isinstance(r, AppendResult) for r in results)
        assert source_store.append.call_count == 10
        assert target_store.append.call_count == 10

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

        async def intermittent_failure(stream, events, expected):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                raise Exception("Intermittent failure")
            return AppendResult(
                stream=stream,
                new_version=1,
                position=Position(store_id="target", key=(1,)),
            )

        target_store.append = intermittent_failure

        async def do_write(index: int):
            aggregate_id = uuid4()
            event = TestEvent(aggregate_id=aggregate_id)
            stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
            return await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        # Run concurrent writes
        tasks = [asyncio.create_task(do_write(i)) for i in range(10)]
        results = await asyncio.gather(*tasks)

        # All source writes should succeed
        assert len(results) == 10
        assert all(isinstance(r, AppendResult) for r in results)

        # Some target writes should have failed
        stats = interceptor.get_failure_stats()
        assert stats.total_failures == 5


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    async def test_multiple_events_single_write(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test writing multiple events in single append."""
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event-{i}") for i in range(5)]
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        result = await interceptor.append(stream, events, ExpectedVersion.no_stream())

        assert result.stream == stream
        source_store.append.assert_called_once_with(stream, events, ExpectedVersion.no_stream())
        # Derived mirror expectation: new_version (5) minus the batch (5).
        target_store.append.assert_called_once_with(stream, events, ExpectedVersion.exact(0))

    async def test_failure_tracking_multiple_events(
        self,
        interceptor: DualWriteInterceptor,
        target_store: MagicMock,
    ) -> None:
        """Test that multiple events in failed write are all tracked."""
        aggregate_id = uuid4()
        events = [TestEvent(aggregate_id=aggregate_id, data=f"event-{i}") for i in range(3)]
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        target_store.append.side_effect = Exception("Error")

        await interceptor.append(stream, events, ExpectedVersion.no_stream())

        failed_writes = interceptor.get_failed_writes()
        assert len(failed_writes) == 1
        assert len(failed_writes[0].event_ids) == 3

        stats = interceptor.get_failure_stats()
        assert stats.total_events_failed == 3

    async def test_source_position_tracked_in_failure(
        self,
        interceptor: DualWriteInterceptor,
        source_store: MagicMock,
        target_store: MagicMock,
    ) -> None:
        """Test that the FIRST event's source position is tracked when target fails.

        `AppendResult.position` is the position of the first appended event,
        not the last -- this asserts identity, not arithmetic.
        """
        aggregate_id = uuid4()
        event = TestEvent(aggregate_id=aggregate_id)
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        expected_position = Position(store_id="source", key=(500,))
        source_store.append.side_effect = None
        source_store.append.return_value = AppendResult(
            stream=stream,
            new_version=1,
            position=expected_position,
        )
        target_store.append.side_effect = Exception("Error")

        await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        failed_writes = interceptor.get_failed_writes()
        assert failed_writes[0].source_position == expected_position
