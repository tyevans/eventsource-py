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

from eventsource.events.base import DomainEvent
from eventsource.migration.bulk_copier import (
    BulkCopier,
    BulkCopyProgress,
    BulkCopyResult,
    RateLimiter,
)
from eventsource.migration.exceptions import BulkCopyError
from eventsource.migration.models import Migration, MigrationConfig, MigrationPhase
from eventsource.stores.interface import AppendResult, StoredEvent


# Test event class for testing
class TestEvent(DomainEvent):
    """Test event for unit tests."""

    event_type: str = "TestEvent"
    aggregate_type: str = "TestAggregate"
    value: str = "test"


class TestBulkCopyProgress:
    """Tests for BulkCopyProgress dataclass."""

    def test_progress_percent_with_events(self) -> None:
        """Test progress percentage calculation with events."""
        progress = BulkCopyProgress(
            migration_id=uuid4(),
            events_copied=500,
            events_total=1000,
            last_source_position=500,
            last_target_position=250,
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
            last_source_position=0,
            last_target_position=0,
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
            last_source_position=1500,
            last_target_position=750,
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
            last_source_position=100,
            last_target_position=50,
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
            last_source_position=1000,
            last_target_position=500,
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
            last_source_position=500,
            last_target_position=250,
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

    def _create_stored_events(
        self,
        count: int,
        tenant_id: UUID,
        aggregate_id: UUID | None = None,
    ) -> list[StoredEvent]:
        """Create a list of stored events for testing."""
        events = []
        agg_id = aggregate_id or uuid4()
        for i in range(count):
            event = TestEvent(
                aggregate_id=agg_id,
                tenant_id=tenant_id,
                value=f"test_{i}",
            )
            stored = StoredEvent(
                event=event,
                stream_id=f"{agg_id}:TestAggregate",
                stream_position=i + 1,
                global_position=i + 1,
                stored_at=datetime.now(UTC),
            )
            events.append(stored)
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

        async def empty_generator(options):
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
        events = self._create_stored_events(5, migration.tenant_id)

        async def event_generator(options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append_events = AsyncMock(return_value=AppendResult.successful(5, 5))
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
        events = self._create_stored_events(25, migration.tenant_id)

        async def event_generator(options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append_events = AsyncMock(
            side_effect=[
                AppendResult.successful(10, 10),
                AppendResult.successful(20, 20),
                AppendResult.successful(25, 25),
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
        events = self._create_stored_events(5, migration.tenant_id)

        async def event_generator(options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append_events = AsyncMock(return_value=AppendResult.successful(5, 5))
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
        events = self._create_stored_events(100, migration.tenant_id)
        event_index = 0

        async def event_generator(options):
            nonlocal event_index
            for event in events:
                event_index += 1
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append_events = AsyncMock(return_value=AppendResult.successful(10, 10))
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
        events = self._create_stored_events(25, tenant_id)
        # Simulate events starting from position 10
        for i, event in enumerate(events):
            object.__setattr__(event, "global_position", i + 11)

        async def event_generator(options):
            # Should only return events after from_position
            start_pos = options.from_position if options else 0
            for event in events:
                if event.global_position > start_pos:
                    yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=10)
        target_store.append_events = AsyncMock(return_value=AppendResult.successful(25, 35))
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
            last_source_position=10,  # Checkpoint
            last_target_position=5,
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
        events = self._create_stored_events(5, migration.tenant_id)

        async def event_generator(options):
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append_events = AsyncMock(side_effect=Exception("Connection failed"))
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
        events = self._create_stored_events(15, tenant_id)
        call_count = 0

        async def event_generator(options):
            nonlocal call_count
            call_count += 1
            for event in events:
                yield event

        source_store.read_all = event_generator
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append_events = AsyncMock(return_value=AppendResult.successful(15, 15))
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
        """Test write batch groups events by aggregate."""
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append_events = AsyncMock(
            side_effect=[
                AppendResult.successful(2, 2),
                AppendResult.successful(3, 5),
            ]
        )

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        tenant_id = uuid4()
        agg1_id = uuid4()
        agg2_id = uuid4()

        # Create events for two different aggregates
        events = []
        for i in range(2):
            event = TestEvent(aggregate_id=agg1_id, tenant_id=tenant_id)
            events.append(
                StoredEvent(
                    event=event,
                    stream_id=f"{agg1_id}:TestAggregate",
                    stream_position=i + 1,
                    global_position=i + 1,
                    stored_at=datetime.now(UTC),
                )
            )
        for i in range(3):
            event = TestEvent(aggregate_id=agg2_id, tenant_id=tenant_id)
            events.append(
                StoredEvent(
                    event=event,
                    stream_id=f"{agg2_id}:TestAggregate",
                    stream_position=i + 1,
                    global_position=i + 3,
                    stored_at=datetime.now(UTC),
                )
            )

        migration_id = uuid4()
        last_pos = await copier._write_batch(migration_id, tenant_id, events)

        # Should have called append_events twice (once per aggregate)
        assert target_store.append_events.call_count == 2
        assert last_pos == 5

    @pytest.mark.asyncio
    async def test_write_batch_with_position_mapper(self) -> None:
        """Test write batch records position mappings."""
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(return_value=0)
        target_store.append_events = AsyncMock(return_value=AppendResult.successful(3, 3))

        position_mapper = AsyncMock()
        position_mapper.record_mapping = AsyncMock()

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            position_mapper=position_mapper,
            enable_tracing=False,
        )

        tenant_id = uuid4()
        agg_id = uuid4()
        migration_id = uuid4()

        events = []
        for i in range(3):
            event = TestEvent(aggregate_id=agg_id, tenant_id=tenant_id)
            events.append(
                StoredEvent(
                    event=event,
                    stream_id=f"{agg_id}:TestAggregate",
                    stream_position=i + 1,
                    global_position=i + 1,
                    stored_at=datetime.now(UTC),
                )
            )

        await copier._write_batch(migration_id, tenant_id, events)

        # Should have recorded 3 position mappings
        assert position_mapper.record_mapping.call_count == 3

    @pytest.mark.asyncio
    async def test_write_batch_handles_missing_stream_version(self) -> None:
        """Test write batch handles exception when getting stream version."""
        target_store = AsyncMock()
        target_store.get_stream_version = AsyncMock(side_effect=Exception("Stream not found"))
        target_store.append_events = AsyncMock(return_value=AppendResult.successful(1, 1))

        copier = BulkCopier(
            source_store=MagicMock(),
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        tenant_id = uuid4()
        agg_id = uuid4()
        event = TestEvent(aggregate_id=agg_id, tenant_id=tenant_id)
        stored_event = StoredEvent(
            event=event,
            stream_id=f"{agg_id}:TestAggregate",
            stream_position=1,
            global_position=1,
            stored_at=datetime.now(UTC),
        )

        # Should not raise, should use version 0
        last_pos = await copier._write_batch(uuid4(), tenant_id, [stored_event])
        assert last_pos == 1

        # Should have called append with version 0
        call_args = target_store.append_events.call_args
        assert call_args[0][3] == 0  # expected_version


class TestBulkCopierCountTenantEvents:
    """Tests for BulkCopier._count_tenant_events() method."""

    @pytest.mark.asyncio
    async def test_count_tenant_events(self) -> None:
        """Test counting tenant events."""
        tenant_id = uuid4()
        source_store = AsyncMock()

        async def event_generator(options):
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

        async def event_generator(options):
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
        captured_options = None

        async def event_generator(options):
            nonlocal captured_options
            captured_options = options
            yield MagicMock()

        source_store.read_all = event_generator

        copier = BulkCopier(
            source_store=source_store,
            target_store=MagicMock(),
            migration_repo=MagicMock(),
            enable_tracing=False,
        )

        async for _ in copier._stream_tenant_events(tenant_id, from_position=100):
            pass

        assert captured_options is not None
        assert captured_options.from_position == 100
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
