"""Unit tests for BulkCopier lifecycle, models, rate limiter, and pause/resume."""

import asyncio
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.bulk_copier import (
    BulkCopier,
    BulkCopyProgress,
    BulkCopyResult,
    RateLimiter,
)
from tests.unit.application.migration.test_bulk_copier._fixtures import _pos


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
        start = asyncio.get_event_loop().time()
        await limiter.wait(100)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_rate_limiter_waits_when_exceeding_capacity(self) -> None:
        """Test rate limiter waits when exceeding capacity."""
        limiter = RateLimiter(max_rate=100)
        await limiter.wait(100)
        start = asyncio.get_event_loop().time()
        await limiter.wait(10)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 0.05

    @pytest.mark.asyncio
    async def test_rate_limiter_with_zero_rate(self) -> None:
        """Test rate limiter with zero rate does not wait."""
        limiter = RateLimiter(max_rate=0)
        start = asyncio.get_event_loop().time()
        await limiter.wait(1000)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.1

    @pytest.mark.asyncio
    async def test_rate_limiter_refills_tokens_over_time(self) -> None:
        """Test rate limiter refills tokens over time."""
        limiter = RateLimiter(max_rate=1000)
        await limiter.wait(1000)
        await asyncio.sleep(0.5)
        start = asyncio.get_event_loop().time()
        await limiter.wait(400)
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed < 0.1


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

        async def resume_after_delay() -> None:
            await asyncio.sleep(0.1)
            copier.resume()

        asyncio.create_task(resume_after_delay())

        start = asyncio.get_event_loop().time()
        await copier._wait_if_paused()
        elapsed = asyncio.get_event_loop().time() - start
        assert elapsed >= 0.05
