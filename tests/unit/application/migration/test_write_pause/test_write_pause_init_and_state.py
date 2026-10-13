"""Unit tests for WritePauseManager initialization, state, and basic operations."""

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.application.migration.write_pause import (
    PauseMetrics,
    PauseState,
    WritePausedError,
    WritePauseManager,
)

# =============================================================================
# Test WritePausedError
# =============================================================================


class TestWritePausedError:
    """Tests for WritePausedError exception."""

    def test_error_attributes(self) -> None:
        """Test error stores tenant_id and timeout."""
        tenant_id = uuid4()
        error = WritePausedError(tenant_id, 5.0)

        assert error.tenant_id == tenant_id
        assert error.timeout == 5.0
        assert error.waited_ms is None

    def test_error_with_waited_ms(self) -> None:
        """Test error stores waited_ms when provided."""
        tenant_id = uuid4()
        error = WritePausedError(tenant_id, 5.0, waited_ms=4999.5)

        assert error.tenant_id == tenant_id
        assert error.timeout == 5.0
        assert error.waited_ms == 4999.5

    def test_error_message_basic(self) -> None:
        """Test error message formatting without waited_ms."""
        tenant_id = uuid4()
        error = WritePausedError(tenant_id, 5.0)

        assert str(tenant_id) in str(error)
        assert "5.0s" in str(error)
        assert "waited" not in str(error)

    def test_error_message_with_waited(self) -> None:
        """Test error message includes waited_ms when provided."""
        tenant_id = uuid4()
        error = WritePausedError(tenant_id, 5.0, waited_ms=4999.5)

        assert str(tenant_id) in str(error)
        assert "waited 4999.50ms" in str(error)


# =============================================================================
# Test PauseState
# =============================================================================


class TestPauseState:
    """Tests for PauseState dataclass."""

    def test_default_initialization(self) -> None:
        """Test PauseState initializes with correct defaults."""
        state = PauseState()

        assert isinstance(state.event, asyncio.Event)
        assert state.event.is_set() is False
        assert state.waiting_count == 0
        assert state.started_at > 0
        assert state.started_at_utc.tzinfo == UTC


# =============================================================================
# Test PauseMetrics
# =============================================================================


class TestPauseMetrics:
    """Tests for PauseMetrics dataclass."""

    def test_metrics_attributes(self) -> None:
        """Test metrics stores all attributes correctly."""
        tenant_id = uuid4()
        started = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        ended = datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC)

        metrics = PauseMetrics(
            tenant_id=tenant_id,
            duration_ms=100.5,
            started_at=started,
            ended_at=ended,
            max_waiters=5,
            total_waiters=10,
        )

        assert metrics.tenant_id == tenant_id
        assert metrics.duration_ms == 100.5
        assert metrics.started_at == started
        assert metrics.ended_at == ended
        assert metrics.max_waiters == 5
        assert metrics.total_waiters == 10

    def test_duration_seconds_property(self) -> None:
        """Test duration_seconds calculation."""
        metrics = PauseMetrics(
            tenant_id=uuid4(),
            duration_ms=1500.0,
            started_at=datetime.now(UTC),
            ended_at=datetime.now(UTC),
        )

        assert metrics.duration_seconds == 1.5

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        tenant_id = uuid4()
        started = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        ended = datetime(2024, 1, 1, 12, 0, 1, tzinfo=UTC)

        metrics = PauseMetrics(
            tenant_id=tenant_id,
            duration_ms=100.5,
            started_at=started,
            ended_at=ended,
            max_waiters=5,
            total_waiters=10,
        )

        d = metrics.to_dict()

        assert d["tenant_id"] == str(tenant_id)
        assert d["duration_ms"] == 100.5
        assert d["duration_seconds"] == 0.1005
        assert d["max_waiters"] == 5
        assert d["total_waiters"] == 10
        assert "started_at" in d
        assert "ended_at" in d


# =============================================================================
# Test WritePauseManager Initialization
# =============================================================================


class TestWritePauseManagerInit:
    """Tests for WritePauseManager initialization."""

    def test_default_initialization(self) -> None:
        """Test manager initializes with correct defaults."""
        manager = WritePauseManager()

        assert manager.default_timeout == 5.0
        assert manager._max_history_size == 100
        assert len(manager._paused_tenants) == 0
        assert len(manager._metrics_history) == 0

    def test_custom_initialization(self) -> None:
        """Test manager with custom parameters."""
        manager = WritePauseManager(
            default_timeout=10.0,
            max_history_size=50,
        )

        assert manager.default_timeout == 10.0
        assert manager._max_history_size == 50


# =============================================================================
# Test Basic Pause/Resume
# =============================================================================


class TestBasicPauseResume:
    """Tests for basic pause and resume functionality."""

    @pytest.mark.asyncio
    async def test_pause_writes(self) -> None:
        """Test pausing writes for a tenant."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        result = await manager.pause_writes(tenant_id)

        assert result is True
        assert manager.is_paused(tenant_id) is True

    @pytest.mark.asyncio
    async def test_resume_writes(self) -> None:
        """Test resuming writes for a tenant."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)
        metrics = await manager.resume_writes(tenant_id)

        assert metrics is not None
        assert metrics.tenant_id == tenant_id
        assert metrics.duration_ms >= 0
        assert manager.is_paused(tenant_id) is False

    @pytest.mark.asyncio
    async def test_resume_returns_metrics(self) -> None:
        """Test resume returns proper metrics."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)
        # Add a small delay to have measurable duration
        await asyncio.sleep(0.01)
        metrics = await manager.resume_writes(tenant_id)

        assert metrics is not None
        assert metrics.duration_ms >= 10.0  # At least 10ms
        assert metrics.started_at is not None
        assert metrics.ended_at is not None
        assert metrics.ended_at > metrics.started_at

    @pytest.mark.asyncio
    async def test_is_paused_when_not_paused(self) -> None:
        """Test is_paused returns False for non-paused tenant."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        assert manager.is_paused(tenant_id) is False


# =============================================================================
# Test Idempotent Operations
# =============================================================================


class TestIdempotentOperations:
    """Tests for idempotent pause/resume behavior."""

    @pytest.mark.asyncio
    async def test_double_pause_is_idempotent(self) -> None:
        """Test pausing twice returns False on second call."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        result1 = await manager.pause_writes(tenant_id)
        result2 = await manager.pause_writes(tenant_id)

        assert result1 is True
        assert result2 is False
        assert manager.is_paused(tenant_id) is True

    @pytest.mark.asyncio
    async def test_resume_without_pause_returns_none(self) -> None:
        """Test resuming a non-paused tenant returns None."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        result = await manager.resume_writes(tenant_id)

        assert result is None

    @pytest.mark.asyncio
    async def test_double_resume_second_returns_none(self) -> None:
        """Test resuming twice returns None on second call."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)
        result1 = await manager.resume_writes(tenant_id)
        result2 = await manager.resume_writes(tenant_id)

        assert result1 is not None
        assert result2 is None
