"""
Unit tests for BulkCopier implementation.

Tests cover:
- BulkCopyProgress dataclass
- BulkCopyResult dataclass
- RateLimiter functionality
- BulkCopier initialization
- BulkCopier.run() method
- Pause/resume/cancel functionality
- Error handling
- Progress tracking and callbacks
- Event batching
- Position mapping integration
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import MemoryEventStore
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.migration.bulk_copier import (
    BulkCopier,
    BulkCopyProgress,
    BulkCopyResult,
    RateLimiter,
)
from eventsource.migration.exceptions import BulkCopyError
from eventsource.migration.models import Migration, MigrationConfig, MigrationPhase
from eventsource.ports import AppendResult, EventEnvelope, ExpectedVersion, Position


# Test event class for testing
class TestEvent(DomainEvent):
    """Test event for unit tests."""

    event_type: str = "TestEvent"
    aggregate_type: str = "TestAggregate"
    value: str = "test"


def _pos(store_id: str, *key: int | str) -> Position:
    return Position(store_id=store_id, key=key)


class TestBulkCopyProgress:
    """Tests for BulkCopyProgress dataclass."""

    def test_progress_percent_with_events(self) -> None:
        """Test progress percentage calculation with events."""
        progress = BulkCopyProgress(
            migration_id=uuid4(),
            events_copied=500,
            events_total=1000,
            last_source_position=_pos("source", 500),
            last_target_position=_pos("target", 250),
            events_per_second=100.0,
            estimated_remaining_seconds=5.0,
            is_complete=False,
        )
        assert progress.progress_percent == 50.0

    def test_progress_percent_with_zero_total(self) -> None:
        """Test progress percentage with zero total events."""
        progress = BulkCopyProgress(
            migration_id=uuid4(),
            events_copied=0,
            events_total=0,
            last_source_position=None,
            last_target_position=None,
            events_per_second=0.0,
            estimated_remaining_seconds=None,
            is_complete=False,
        )
        assert progress.progress_percent == 0.0

    def test_progress_percent_capped_at_100(self) -> None:
        """Test progress percentage is capped at 100."""
        progress = BulkCopyProgress(
            migration_id=uuid4(),
            events_copied=1500,  # More than total (shouldn't happen but handle it)
            events_total=1000,
            last_source_position=_pos("source", 1500),
            last_target_position=_pos("target", 750),
            events_per_second=100.0,
            estimated_remaining_seconds=0.0,
            is_complete=True,
        )
        assert progress.progress_percent == 100.0

    def test_progress_is_frozen(self) -> None:
        """Test BulkCopyProgress is immutable."""
        progress = BulkCopyProgress(
            migration_id=uuid4(),
            events_copied=100,
            events_total=1000,
            last_source_position=_pos("source", 100),
            last_target_position=_pos("target", 50),
            events_per_second=10.0,
            estimated_remaining_seconds=90.0,
            is_complete=False,
        )
        with pytest.raises(AttributeError):
            progress.events_copied = 200  # type: ignore


class TestBulkCopyResult:
    """Tests for BulkCopyResult dataclass."""

    def test_successful_result(self) -> None:
        """Test creating a successful result."""
        result = BulkCopyResult(
            success=True,
            events_copied=1000,
            last_source_position=_pos("source", 1000),
            last_target_position=_pos("target", 500),
            duration_seconds=10.5,
        )
        assert result.success is True
        assert result.events_copied == 1000
        assert result.error_message is None

    def test_failed_result(self) -> None:
        """Test creating a failed result."""
        result = BulkCopyResult(
            success=False,
            events_copied=500,
            last_source_position=_pos("source", 500),
            last_target_position=_pos("target", 250),
            duration_seconds=5.0,
            error_message="Connection failed",
        )
        assert result.success is False
        assert result.error_message == "Connection failed"


class TestRateLimiter:
    """Tests for RateLimiter class."""

    @pytest.mark.asyncio
    async def test_rate_limiter_allows_within_capacity(self) -> None:
        """Test rate limiter allows requests within capacity."""
        limiter = RateLimiter(max_rate=1000)
        # Should not wait for small count
        start = asyncio.get_event_loop().time()
        await limiter.wait(100)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.1  # Should be nearly instant

    @pytest.mark.asyncio
    async def test_rate_limiter_waits_when_exceeding_capacity(self) -> None:
        """Test rate limiter waits when exceeding capacity."""
        limiter = RateLimiter(max_rate=100)
        # Consume all tokens
        await limiter.wait(100)
        # Now should wait for more tokens
        start = asyncio.get_event_loop().time()
        await limiter.wait(10)
        elapsed = asyncio.get_event_loop().time() - start
        # Should wait approximately 0.1 seconds (10/100)
        assert elapsed >= 0.05  # Allow some tolerance

    @pytest.mark.asyncio
    async def test_rate_limiter_with_zero_rate(self) -> None:
        """Test rate limiter with zero rate does not wait."""
        limiter = RateLimiter(max_rate=0)
        start = asyncio.get_event_loop().time()
        await limiter.wait(1000)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.1  # Should be nearly instant

    @pytest.mark.asyncio
    async def test_rate_limiter_refills_tokens_over_time(self) -> None:
        """Test rate limiter refills tokens over time."""
        limiter = RateLimiter(max_rate=1000)
        # Consume all tokens
        await limiter.wait(1000)
        # Wait for tokens to refill
        await asyncio.sleep(0.5)  # Should add ~500 tokens
        start = asyncio.get_event_loop().time()
        await limiter.wait(400)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.1  # Should be nearly instant since tokens refilled


class TestBulkCopierInit:
    """Tests for BulkCopier initialization."""

    def test_init_with_required_args(self) -> None:
        """Test initialization with required arguments."""
        source_store = MagicMock()
        target_store = MagicMock()
        migration_repo = MagicMock()

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
        )

        assert copier._source == source_store
        assert copier._target == target_store
        assert copier._migration_repo == migration_repo
        assert copier._position_mapper is None
        assert copier.is_cancelled is False
        assert copier.is_paused is False

    def test_init_with_position_mapper(self) -> None:
        """Test initialization with position mapper."""
        source_store = MagicMock()
        target_store = MagicMock()
        migration_repo = MagicMock()
        position_mapper = MagicMock()

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            position_mapper=position_mapper,
        )

        assert copier._position_mapper == position_mapper

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        source_store = MagicMock()
        target_store = MagicMock()
        migration_repo = MagicMock()

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        assert copier._enable_tracing is False


class TestBulkCopierPauseResumeCancel:
    """Tests for BulkCopier pause/resume/cancel functionality."""

    def test_cancel(self) -> None:
        """Test cancel sets flag."""
        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        assert copier.is_cancelled is False
        copier.cancel()
        assert copier.is_cancelled is True

    def test_pause(self) -> None:
        """Test pause sets flag and clears event."""
        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        assert copier.is_paused is False
        assert copier._pause_event.is_set()

        copier.pause()

        assert copier.is_paused is True
        assert not copier._pause_event.is_set()

    def test_resume(self) -> None:
        """Test resume clears flag and sets event."""
        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        copier.pause()
        assert copier.is_paused is True

        copier.resume()

        assert copier.is_paused is False
        assert copier._pause_event.is_set()


class TestBulkCopierRun:
    """Tests for BulkCopier.run() method."""

    @pytest.fixture
    def migration(self) -> Migration:
        """Create a sample migration for testing."""
        return Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            config=MigrationConfig(batch_size=10, max_bulk_copy_rate=10000),
        )

    @pytest.fixture
    def source_store(self) -> AsyncMock:
        """Create a mock source store."""
        return AsyncMock()

    @pytest.fixture
    def target_store(self) -> AsyncMock:
        """Create a mock target store."""
        return AsyncMock()

    @pytest.fixture
    def migration_repo(self) -> AsyncMock:
        """Create a mock migration repository."""
        return AsyncMock()

    def _create_events(
        self,
        count: int,
        tenant_id: UUID,
        aggregate_id: UUID | None = None,
        start: int = 1,
        store_id: str = "source",
    ) -> list[EventEnvelope]:
        """Create a list of EventEnvelope instances for testing."""
        events = []
        agg_id = aggregate_id or uuid4()
        stream_id = StreamId(aggregate_id=agg_id, category="TestAggregate")
        for i in range(count):
            event = TestEvent(
                aggregate_id=agg_id,
                tenant_id=tenant_id,
                value=f"test_{i}",
            )
            envelope = EventEnvelope(
                event=event,
                stream_id=stream_id,
                stream_version=i + 1,
                position=_pos(store_id, start + i),
                stored_at=datetime.now(UTC),
            )
            events.append(envelope)
        return events

    @pytest.mark.asyncio
    async def test_run_with_no_events(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run with no events to copy."""

        async def empty_generator(from_position, options):
            return
            yield  # Make it an async generator

        source_store.read_all = empty_generator
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        progress_updates = []
        async for progress in copier.run(migration):
            progress_updates.append(progress)

        # Should yield at least final progress
        assert len(progress_updates) >= 1
        final = progress_updates[-1]
        assert final.is_complete is True
        assert final.events_copied == 0

    @pytest.mark.asyncio
    async def test_run_with_events_single_batch(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run with events fitting in single batch."""
        events = self._create_events(5, migration.tenant_id)

        async def event_generator(from_position, options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            return_value=AppendResult(
                stream=events[0].stream_id, new_version=5, position=_pos("target", 5)
            )
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        progress_updates = []
        async for progress in copier.run(migration):
            progress_updates.append(progress)

        # Should yield final progress (batch size 10 > 5 events)
        assert len(progress_updates) >= 1
        final = progress_updates[-1]
        assert final.is_complete is True
        assert final.events_copied == 5

    @pytest.mark.asyncio
    async def test_run_with_events_multiple_batches(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run with events spanning multiple batches."""
        events = self._create_events(25, migration.tenant_id)

        async def event_generator(from_position, options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            side_effect=[
                AppendResult(
                    stream=events[0].stream_id, new_version=10, position=_pos("target", 10)
                ),
                AppendResult(
                    stream=events[0].stream_id, new_version=20, position=_pos("target", 20)
                ),
                AppendResult(
                    stream=events[0].stream_id, new_version=25, position=_pos("target", 25)
                ),
            ]
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        # Set batch size to 10
        migration.config = MigrationConfig(batch_size=10, max_bulk_copy_rate=100000)

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        progress_updates = []
        async for progress in copier.run(migration):
            progress_updates.append(progress)

        # Should yield progress after each batch (2 full batches) + final
        assert len(progress_updates) >= 2
        assert progress_updates[-1].is_complete is True
        assert progress_updates[-1].events_copied == 25

    @pytest.mark.asyncio
    async def test_run_with_progress_callback(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run calls progress callback."""
        events = self._create_events(5, migration.tenant_id)

        async def event_generator(from_position, options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            return_value=AppendResult(
                stream=events[0].stream_id, new_version=5, position=_pos("target", 5)
            )
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        callback_results = []

        def callback(progress: BulkCopyProgress) -> None:
            callback_results.append(progress)

        async for _ in copier.run(migration, progress_callback=callback):
            pass

        # Callback should have been called
        assert len(callback_results) >= 1

    @pytest.mark.asyncio
    async def test_run_respects_cancellation(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run stops when cancelled."""
        events = self._create_events(100, migration.tenant_id)

        async def event_generator(from_position, options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            return_value=AppendResult(
                stream=events[0].stream_id, new_version=10, position=_pos("target", 10)
            )
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        migration.config = MigrationConfig(batch_size=10, max_bulk_copy_rate=100000)

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        progress_updates = []
        async for progress in copier.run(migration):
            progress_updates.append(progress)
            if len(progress_updates) == 1:
                copier.cancel()

        # Should have stopped early
        final = progress_updates[-1]
        assert final.is_complete is False  # Cancelled, not complete
        assert final.events_copied < 100

    @pytest.mark.asyncio
    async def test_run_resumes_from_checkpoint(
        self,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run resumes from last checkpoint."""
        tenant_id = uuid4()
        events = self._create_events(25, tenant_id, start=11)

        async def event_generator(from_position, options):
            # Should only return events strictly after from_position
            for event in events:
                if from_position is None or event.position > from_position:
                    yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=10)
        target_store.append = AsyncMock(
            return_value=AppendResult(
                stream=events[0].stream_id, new_version=35, position=_pos("target", 35)
            )
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        # Migration with checkpoint at position 10
        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            events_total=35,  # Already set
            events_copied=10,  # Already copied 10
            last_source_position=_pos("source", 10),  # Checkpoint
            last_target_position=_pos("target", 5),
            config=MigrationConfig(batch_size=100, max_bulk_copy_rate=100000),
        )

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        progress_updates = []
        async for progress in copier.run(migration):
            progress_updates.append(progress)

        final = progress_updates[-1]
        assert final.events_copied == 10 + 25  # Previous + new

    @pytest.mark.asyncio
    async def test_run_handles_error(
        self,
        migration: Migration,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run handles errors and raises BulkCopyError."""
        events = self._create_events(5, migration.tenant_id)

        async def event_generator(from_position, options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(side_effect=Exception("Connection failed"))
        migration_repo.set_events_total = AsyncMock()
        migration_repo.record_error = AsyncMock()

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        with pytest.raises(BulkCopyError) as exc_info:
            async for _ in copier.run(migration):
                pass

        assert exc_info.value.migration_id == migration.id
        assert "Connection failed" in exc_info.value.original_error

        # Should have recorded error
        migration_repo.record_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_counts_events_when_total_not_set(
        self,
        source_store: AsyncMock,
        target_store: AsyncMock,
        migration_repo: AsyncMock,
    ) -> None:
        """Test run counts events when total is not set."""
        tenant_id = uuid4()
        events = self._create_events(15, tenant_id)

        async def event_generator(from_position, options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(
            return_value=AppendResult(
                stream=events[0].stream_id, new_version=15, position=_pos("target", 15)
            )
        )
        migration_repo.set_events_total = AsyncMock()
        migration_repo.update_progress = AsyncMock()

        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            events_total=0,  # Not set
            config=MigrationConfig(batch_size=100, max_bulk_copy_rate=100000),
        )

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        async for _ in copier.run(migration):
            pass

        # Should have called set_events_total
        migration_repo.set_events_total.assert_called_once_with(
            migration.id,
            15,  # Count of events
        )


class TestBulkCopierWriteBatch:
    """Tests for BulkCopier._write_batch() method."""

    @pytest.mark.asyncio
    async def test_write_batch_groups_by_aggregate(self) -> None:
        """Test write batch groups events by aggregate (batched path, no position_mapper)."""
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)

        tenant_id = uuid4()
        agg1_id = uuid4()
        agg2_id = uuid4()
        stream1 = StreamId(aggregate_id=agg1_id, category="TestAggregate")
        stream2 = StreamId(aggregate_id=agg2_id, category="TestAggregate")

        target_store.append = AsyncMock(
            side_effect=[
                AppendResult(stream=stream1, new_version=2, position=_pos("target", 2)),
                AppendResult(stream=stream2, new_version=3, position=_pos("target", 5)),
            ]
        )

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        # Create events for two different aggregates
        events = []
        for i in range(2):
            event = TestEvent(aggregate_id=agg1_id, tenant_id=tenant_id)
            events.append(
                EventEnvelope(
                    event=event,
                    stream_id=stream1,
                    stream_version=i + 1,
                    position=_pos("source", i + 1),
                    stored_at=datetime.now(UTC),
                )
            )
        for i in range(3):
            event = TestEvent(aggregate_id=agg2_id, tenant_id=tenant_id)
            events.append(
                EventEnvelope(
                    event=event,
                    stream_id=stream2,
                    stream_version=i + 1,
                    position=_pos("source", i + 3),
                    stored_at=datetime.now(UTC),
                )
            )

        migration_id = uuid4()
        last_pos = await copier._write_batch(migration_id, tenant_id, events)

        # Should have called append twice (once per aggregate stream, batched)
        assert target_store.append.call_count == 2
        assert last_pos == _pos("target", 5)

    @pytest.mark.asyncio
    async def test_write_batch_with_position_mapper(self) -> None:
        """Test write batch appends one event at a time and records mappings."""
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)

        tenant_id = uuid4()
        agg_id = uuid4()
        migration_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="TestAggregate")

        target_positions = [_pos("target", 1), _pos("target", 2), _pos("target", 3)]
        target_store.append = AsyncMock(
            side_effect=[
                AppendResult(stream=stream, new_version=i + 1, position=target_positions[i])
                for i in range(3)
            ]
        )

        position_mapper = AsyncMock()
        position_mapper.record_mapping = AsyncMock()

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            position_mapper=position_mapper,
            enable_tracing=False,
        )

        source_positions = [_pos("source", 1), _pos("source", 2), _pos("source", 3)]
        events = []
        for i in range(3):
            event = TestEvent(aggregate_id=agg_id, tenant_id=tenant_id)
            events.append(
                EventEnvelope(
                    event=event,
                    stream_id=stream,
                    stream_version=i + 1,
                    position=source_positions[i],
                    stored_at=datetime.now(UTC),
                )
            )

        last_pos = await copier._write_batch(migration_id, tenant_id, events)

        # Per-event appends: one append call per event.
        assert target_store.append.call_count == 3
        # Should have recorded 3 position mappings, one per event, in
        # ascending source order.
        assert position_mapper.record_mapping.call_count == 3
        for i, call in enumerate(position_mapper.record_mapping.call_args_list):
            args = call.args
            assert args[0] == migration_id
            assert args[1] == source_positions[i]
            assert args[2] == target_positions[i]
            assert args[3] == events[i].event.event_id

        # Last append wins.
        assert last_pos == target_positions[-1]

    @pytest.mark.asyncio
    async def test_write_batch_handles_missing_stream_version(self) -> None:
        """Test write batch handles exception when getting stream version."""
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(side_effect=Exception("Stream not found"))

        tenant_id = uuid4()
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="TestAggregate")
        target_store.append = AsyncMock(
            return_value=AppendResult(stream=stream, new_version=1, position=_pos("target", 1))
        )

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        event = TestEvent(aggregate_id=agg_id, tenant_id=tenant_id)
        envelope = EventEnvelope(
            event=event,
            stream_id=stream,
            stream_version=1,
            position=_pos("source", 1),
            stored_at=datetime.now(UTC),
        )

        # Should not raise, should use version 0
        last_pos = await copier._write_batch(uuid4(), tenant_id, [envelope])
        assert last_pos == _pos("target", 1)

        # Should have called append with ExpectedVersion.exact(0)
        call_args = target_store.append.call_args
        expected_version = call_args.args[2]
        assert expected_version.kind == "exact"
        assert expected_version.version == 0

    @pytest.mark.asyncio
    async def test_write_batch_duplicate_event_counts_as_copied_and_continues(self) -> None:
        """A DuplicateEventError on append means the event was already copied;
        the copy continues rather than failing."""
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)

        tenant_id = uuid4()
        agg1_id = uuid4()
        agg2_id = uuid4()
        stream1 = StreamId(aggregate_id=agg1_id, category="TestAggregate")
        stream2 = StreamId(aggregate_id=agg2_id, category="TestAggregate")

        # First stream's batch append raises DuplicateEventError; the
        # copier then retries that stream per event (the group could have
        # straddled absent events) and the per-event append confirms the
        # duplicate. Second stream succeeds batched.
        target_store.append = AsyncMock(
            side_effect=[
                DuplicateEventError("already present"),
                DuplicateEventError("already present"),
                AppendResult(stream=stream2, new_version=1, position=_pos("target", 9)),
            ]
        )

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        event1 = TestEvent(aggregate_id=agg1_id, tenant_id=tenant_id)
        event2 = TestEvent(aggregate_id=agg2_id, tenant_id=tenant_id)
        events = [
            EventEnvelope(
                event=event1,
                stream_id=stream1,
                stream_version=1,
                position=_pos("source", 1),
                stored_at=datetime.now(UTC),
            ),
            EventEnvelope(
                event=event2,
                stream_id=stream2,
                stream_version=1,
                position=_pos("source", 2),
                stored_at=datetime.now(UTC),
            ),
        ]

        # Should not raise -- duplicate is swallowed and counted as copied.
        last_pos = await copier._write_batch(uuid4(), tenant_id, events)

        # Batched attempt + per-event confirmation for stream1, batched
        # success for stream2.
        assert target_store.append.call_count == 3
        # Only the successful (non-duplicate) stream contributes a position.
        assert last_pos == _pos("target", 9)

    @pytest.mark.asyncio
    async def test_write_batch_all_duplicates_returns_none(self) -> None:
        """An all-duplicate batch appends nothing and reports no position."""
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(side_effect=DuplicateEventError("already present"))

        tenant_id = uuid4()
        agg_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="TestAggregate")

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        events = [
            EventEnvelope(
                event=TestEvent(aggregate_id=agg_id, tenant_id=tenant_id),
                stream_id=stream,
                stream_version=1,
                position=_pos("source", 1),
                stored_at=datetime.now(UTC),
            )
        ]

        assert await copier._write_batch(uuid4(), tenant_id, events) is None

    @pytest.mark.asyncio
    async def test_duplicate_on_per_event_append_records_no_mapping(self) -> None:
        """A duplicate must not produce a position mapping.

        The event is already in the target at a position this run never
        observed, so recording a mapping would invent one. Checkpoint
        translation depends on that not happening.
        """
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)

        tenant_id = uuid4()
        agg_id = uuid4()
        migration_id = uuid4()
        stream = StreamId(aggregate_id=agg_id, category="TestAggregate")

        # First event is already present, second appends cleanly.
        target_store.append = AsyncMock(
            side_effect=[
                DuplicateEventError("already present"),
                AppendResult(stream=stream, new_version=1, position=_pos("target", 7)),
            ]
        )

        position_mapper = AsyncMock()
        position_mapper.record_mapping = AsyncMock()

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            position_mapper=position_mapper,
            enable_tracing=False,
        )

        events = [
            EventEnvelope(
                event=TestEvent(aggregate_id=agg_id, tenant_id=tenant_id),
                stream_id=stream,
                stream_version=i + 1,
                position=_pos("source", i + 1),
                stored_at=datetime.now(UTC),
            )
            for i in range(2)
        ]

        await copier._write_batch(migration_id, tenant_id, events)

        # Only the appended event is mapped; the duplicate contributes none.
        assert position_mapper.record_mapping.call_count == 1
        recorded = position_mapper.record_mapping.call_args
        assert recorded.args[1] == events[1].position
        assert recorded.args[2] == _pos("target", 7)
        assert recorded.args[3] == events[1].event.event_id


class TestBulkCopierRunProgress:
    """Progress reporting across batches."""

    @pytest.mark.asyncio
    async def test_all_duplicate_batch_keeps_earlier_target_position(self) -> None:
        """A later all-duplicate batch must not null a real position."""
        tenant_id = uuid4()
        migration_id = uuid4()
        agg_ids = [uuid4(), uuid4()]
        streams = [StreamId(aggregate_id=a, category="TestAggregate") for a in agg_ids]

        source_store = MagicMock()

        async def event_generator(from_position=None, options=None):
            for i in range(2):
                yield EventEnvelope(
                    event=TestEvent(aggregate_id=agg_ids[i], tenant_id=tenant_id),
                    stream_id=streams[i],
                    stream_version=1,
                    position=_pos("source", i + 1),
                    stored_at=datetime.now(UTC),
                )

        source_store.read_all = event_generator

        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        # First batch appends for real; second batch is entirely duplicate
        # (the batched attempt raises, then the per-event retry confirms
        # the one event really is a duplicate).
        target_store.append = AsyncMock(
            side_effect=[
                AppendResult(stream=streams[0], new_version=1, position=_pos("target", 4)),
                DuplicateEventError("already present"),
                DuplicateEventError("already present"),
            ]
        )

        migration_repo = AsyncMock()
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="source",
            target_store_id="target",
            phase=MigrationPhase.BULK_COPY,
            events_total=2,
            config=MigrationConfig(batch_size=1),
        )

        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=migration_repo,
            enable_tracing=False,
        )

        progresses = [p async for p in copier.run(migration)]

        # The all-duplicate second batch leaves the real position standing.
        assert progresses[-1].last_target_position == _pos("target", 4)


class TestBulkCopierOverlapWithLiveMirror:
    """The copy pass now runs with the dual-write interceptor installed,
    so the copier and the mirror can race on the same stream. The copier
    must tolerate events the mirror already landed without ever skipping
    events the mirror did not."""

    @pytest.mark.asyncio
    async def test_mixed_group_falls_back_per_event_and_copies_absent_events(self) -> None:
        """A batched group straddling an already-present event must not be
        skipped wholesale: the absent events still reach the target, in
        source stream order."""
        target_store = MemoryEventStore("target")
        tenant_id = uuid4()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")

        mirrored = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, value="mirrored")
        absent = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, value="absent")

        # A dual-write mirror already landed the first event.
        await target_store.append(stream, [mirrored], ExpectedVersion.no_stream())

        envelopes = [
            EventEnvelope(
                event=mirrored,
                stream_id=stream,
                stream_version=1,
                position=_pos("source", 1),
                stored_at=datetime.now(UTC),
            ),
            EventEnvelope(
                event=absent,
                stream_id=stream,
                stream_version=2,
                position=_pos("source", 2),
                stored_at=datetime.now(UTC),
            ),
        ]

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        last_pos = await copier._write_batch(uuid4(), tenant_id, envelopes)

        assert last_pos is not None
        assert await target_store.get_stream_version(stream) == 2
        target_ids = [env.event.event_id async for env in target_store.read_stream(stream)]
        assert target_ids == [mirrored.event_id, absent.event_id]

    @pytest.mark.asyncio
    async def test_conflicting_append_rechecks_and_confirms_duplicate(self) -> None:
        """A mirror landing between the version read and the append raises
        OptimisticLockError (not DuplicateEventError); the copier re-reads
        the version and the retry confirms the duplicate. No mapping is
        recorded for the event this run never appended."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(side_effect=[0, 1])
        target_store.append = AsyncMock(
            side_effect=[
                OptimisticLockError(aggregate_id, 0, 1),
                DuplicateEventError("the mirror landed this very event"),
            ]
        )
        position_mapper = AsyncMock()

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            position_mapper=position_mapper,
            enable_tracing=False,
        )

        envelope = EventEnvelope(
            event=event,
            stream_id=stream,
            stream_version=1,
            position=_pos("source", 1),
            stored_at=datetime.now(UTC),
        )

        last_pos = await copier._write_batch(uuid4(), tenant_id, [envelope])

        assert last_pos is None
        assert target_store.append.call_count == 2
        # The retry used the re-read version.
        retry_expected = target_store.append.call_args.args[2]
        assert retry_expected == ExpectedVersion.exact(1)
        position_mapper.record_mapping.assert_not_called()

    @pytest.mark.asyncio
    async def test_sustained_conflict_fails_the_copy_honestly(self) -> None:
        """A stream that keeps moving under the copier past the retry bound
        fails the copy rather than spinning or skipping."""
        tenant_id = uuid4()
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="TestAggregate")
        event = TestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append = AsyncMock(side_effect=OptimisticLockError(aggregate_id, 0, 1))

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        envelope = EventEnvelope(
            event=event,
            stream_id=stream,
            stream_version=1,
            position=_pos("source", 1),
            stored_at=datetime.now(UTC),
        )

        with pytest.raises(BulkCopyError, match="kept moving under the copier"):
            await copier._write_batch(uuid4(), tenant_id, [envelope])


class TestBulkCopierCountTenantEvents:
    """Tests for BulkCopier._count_tenant_events() method."""

    @pytest.mark.asyncio
    async def test_count_tenant_events(self) -> None:
        """Test counting tenant events."""
        tenant_id = uuid4()
        source_store = AsyncMock()

        async def event_generator(from_position, options):
            for _i in range(15):
                yield MagicMock()

        source_store.read_all = event_generator

        copier = BulkCopier(
            source_store=source_store,
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        count = await copier._count_tenant_events(tenant_id)
        assert count == 15

    @pytest.mark.asyncio
    async def test_count_tenant_events_empty(self) -> None:
        """Test counting with no events."""
        tenant_id = uuid4()
        source_store = AsyncMock()

        async def event_generator(from_position, options):
            return
            yield

        source_store.read_all = event_generator

        copier = BulkCopier(
            source_store=source_store,
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        count = await copier._count_tenant_events(tenant_id)
        assert count == 0


class TestBulkCopierStreamTenantEvents:
    """Tests for BulkCopier._stream_tenant_events() method."""

    @pytest.mark.asyncio
    async def test_stream_tenant_events_with_from_position(self) -> None:
        """Test streaming events with from_position."""
        tenant_id = uuid4()
        source_store = AsyncMock()
        captured_from_position = "unset"
        captured_options = None

        async def event_generator(from_position, options):
            nonlocal captured_from_position, captured_options
            captured_from_position = from_position
            captured_options = options
            yield MagicMock()

        source_store.read_all = event_generator

        copier = BulkCopier(
            source_store=source_store,
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        from_position = _pos("source", 100)
        async for _ in copier._stream_tenant_events(tenant_id, from_position):
            pass

        assert captured_from_position == from_position
        assert captured_options is not None
        assert captured_options.tenant_id == tenant_id


class TestBulkCopierWaitIfPaused:
    """Tests for BulkCopier._wait_if_paused() method."""

    @pytest.mark.asyncio
    async def test_wait_if_paused_returns_immediately_when_not_paused(self) -> None:
        """Test _wait_if_paused returns immediately when not paused."""
        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        # Should return immediately
        start = asyncio.get_event_loop().time()
        await copier._wait_if_paused()
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_wait_if_paused_waits_when_paused(self) -> None:
        """Test _wait_if_paused waits when paused."""
        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        copier.pause()

        async def resume_after_delay():
            await asyncio.sleep(0.1)
            copier.resume()

        # Start resume task
        asyncio.create_task(resume_after_delay())

        # Should wait until resumed
        start = asyncio.get_event_loop().time()
        await copier._wait_if_paused()
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 0.05  # Allow some tolerance
