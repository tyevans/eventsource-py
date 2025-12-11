"""
Unit tests for WritePauseManager.

Tests cover:
- Basic pause/resume functionality
- Timeout handling
- Multiple waiters coordination
- Idempotent operations (double pause, resume without pause)
- Concurrent operations safety
- Metrics tracking and history
- Edge cases
"""

import asyncio
import contextlib
from datetime import UTC
from uuid import uuid4

import pytest

from eventsource.migration.write_pause import (
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
        from datetime import datetime

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
        from datetime import datetime

        metrics = PauseMetrics(
            tenant_id=uuid4(),
            duration_ms=1500.0,
            started_at=datetime.now(UTC),
            ended_at=datetime.now(UTC),
        )

        assert metrics.duration_seconds == 1.5

    def test_to_dict(self) -> None:
        """Test serialization to dictionary."""
        from datetime import datetime

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


# =============================================================================
# Test Wait If Paused
# =============================================================================


class TestWaitIfPaused:
    """Tests for wait_if_paused functionality."""

    @pytest.mark.asyncio
    async def test_wait_returns_immediately_when_not_paused(self) -> None:
        """Test wait returns immediately for non-paused tenant."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        # Should complete instantly
        result = await asyncio.wait_for(
            manager.wait_if_paused(tenant_id),
            timeout=0.1,
        )

        assert result == 0.0

    @pytest.mark.asyncio
    async def test_wait_returns_immediately_for_none_tenant(self) -> None:
        """Test wait returns immediately for None tenant_id."""
        manager = WritePauseManager()

        result = await asyncio.wait_for(
            manager.wait_if_paused(None),
            timeout=0.1,
        )

        assert result == 0.0

    @pytest.mark.asyncio
    async def test_wait_blocks_until_resume(self) -> None:
        """Test wait blocks until resume is called."""
        manager = WritePauseManager()
        tenant_id = uuid4()
        wait_completed = False

        async def waiter():
            nonlocal wait_completed
            await manager.wait_if_paused(tenant_id)
            wait_completed = True

        await manager.pause_writes(tenant_id)

        # Start waiter
        waiter_task = asyncio.create_task(waiter())

        # Give time for waiter to start
        await asyncio.sleep(0.01)
        assert wait_completed is False

        # Resume should unblock
        await manager.resume_writes(tenant_id)

        await asyncio.wait_for(waiter_task, timeout=1.0)
        assert wait_completed is True

    @pytest.mark.asyncio
    async def test_wait_returns_time_waited(self) -> None:
        """Test wait returns actual time waited in ms."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)

        async def delayed_resume():
            await asyncio.sleep(0.05)  # 50ms
            await manager.resume_writes(tenant_id)

        asyncio.create_task(delayed_resume())

        waited_ms = await manager.wait_if_paused(tenant_id)

        assert waited_ms >= 40.0  # At least 40ms (allowing for timing variance)

    @pytest.mark.asyncio
    async def test_wait_raises_on_timeout(self) -> None:
        """Test wait raises WritePausedError on timeout."""
        manager = WritePauseManager(default_timeout=0.05)  # 50ms
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)

        with pytest.raises(WritePausedError) as exc_info:
            await manager.wait_if_paused(tenant_id)

        assert exc_info.value.tenant_id == tenant_id
        assert exc_info.value.timeout == 0.05
        assert exc_info.value.waited_ms is not None
        assert exc_info.value.waited_ms >= 40.0

    @pytest.mark.asyncio
    async def test_wait_uses_custom_timeout(self) -> None:
        """Test wait uses per-call timeout override."""
        manager = WritePauseManager(default_timeout=10.0)  # Long default
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)

        with pytest.raises(WritePausedError) as exc_info:
            await manager.wait_if_paused(tenant_id, timeout=0.05)

        assert exc_info.value.timeout == 0.05


# =============================================================================
# Test Multiple Waiters
# =============================================================================


class TestMultipleWaiters:
    """Tests for multiple concurrent waiters."""

    @pytest.mark.asyncio
    async def test_multiple_waiters_all_unblocked(self) -> None:
        """Test multiple waiters are all unblocked on resume."""
        manager = WritePauseManager()
        tenant_id = uuid4()
        completed_count = 0

        async def waiter():
            nonlocal completed_count
            await manager.wait_if_paused(tenant_id)
            completed_count += 1

        await manager.pause_writes(tenant_id)

        # Start multiple waiters
        tasks = [asyncio.create_task(waiter()) for _ in range(5)]

        # Give waiters time to start
        await asyncio.sleep(0.02)
        assert completed_count == 0

        # Resume should unblock all
        await manager.resume_writes(tenant_id)

        await asyncio.gather(*tasks)
        assert completed_count == 5

    @pytest.mark.asyncio
    async def test_waiter_count_tracking(self) -> None:
        """Test waiter count is tracked correctly."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)

        async def waiter():
            await manager.wait_if_paused(tenant_id, timeout=5.0)

        # Start multiple waiters
        tasks = [asyncio.create_task(waiter()) for _ in range(3)]

        # Give waiters time to register
        await asyncio.sleep(0.02)

        # Check pause state
        state = await manager.get_pause_state(tenant_id)
        assert state is not None
        assert state["waiting_count"] == 3

        # Resume and check metrics
        metrics = await manager.resume_writes(tenant_id)

        await asyncio.gather(*tasks)

        assert metrics is not None
        assert metrics.max_waiters == 3
        assert metrics.total_waiters == 3


# =============================================================================
# Test Metrics History
# =============================================================================


class TestMetricsHistory:
    """Tests for metrics history tracking."""

    @pytest.mark.asyncio
    async def test_metrics_added_to_history(self) -> None:
        """Test metrics are added to history on resume."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)
        await manager.resume_writes(tenant_id)

        history = manager.get_metrics_history()
        assert len(history) == 1
        assert history[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_metrics_history_limit(self) -> None:
        """Test metrics history respects max size."""
        manager = WritePauseManager(max_history_size=3)

        for _i in range(5):
            tenant_id = uuid4()
            await manager.pause_writes(tenant_id)
            await manager.resume_writes(tenant_id)

        history = manager.get_metrics_history()
        assert len(history) == 3  # Only last 3 kept

    @pytest.mark.asyncio
    async def test_metrics_history_returns_copy(self) -> None:
        """Test get_metrics_history returns a copy."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)
        await manager.resume_writes(tenant_id)

        history1 = manager.get_metrics_history()
        history2 = manager.get_metrics_history()

        assert history1 is not history2
        assert history1[0] is history2[0]  # Same metric objects


# =============================================================================
# Test Get All Paused
# =============================================================================


class TestGetAllPaused:
    """Tests for get_all_paused functionality."""

    @pytest.mark.asyncio
    async def test_get_all_paused_empty(self) -> None:
        """Test returns empty list when no tenants paused."""
        manager = WritePauseManager()

        paused = await manager.get_all_paused()

        assert paused == []

    @pytest.mark.asyncio
    async def test_get_all_paused_returns_all(self) -> None:
        """Test returns all paused tenant IDs."""
        manager = WritePauseManager()
        tenant_ids = [uuid4() for _ in range(3)]

        for tid in tenant_ids:
            await manager.pause_writes(tid)

        paused = await manager.get_all_paused()

        assert len(paused) == 3
        assert set(paused) == set(tenant_ids)


# =============================================================================
# Test Get Pause State
# =============================================================================


class TestGetPauseState:
    """Tests for get_pause_state functionality."""

    @pytest.mark.asyncio
    async def test_get_pause_state_when_not_paused(self) -> None:
        """Test returns None when tenant not paused."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        state = await manager.get_pause_state(tenant_id)

        assert state is None

    @pytest.mark.asyncio
    async def test_get_pause_state_returns_info(self) -> None:
        """Test returns detailed state info."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)
        await asyncio.sleep(0.01)

        state = await manager.get_pause_state(tenant_id)

        assert state is not None
        assert state["tenant_id"] == str(tenant_id)
        assert state["duration_ms"] >= 10.0
        assert state["waiting_count"] == 0
        assert "started_at" in state


# =============================================================================
# Test Force Resume All
# =============================================================================


class TestForceResumeAll:
    """Tests for force_resume_all functionality."""

    @pytest.mark.asyncio
    async def test_force_resume_all_empty(self) -> None:
        """Test force resume with no paused tenants."""
        manager = WritePauseManager()

        metrics_list = await manager.force_resume_all()

        assert metrics_list == []

    @pytest.mark.asyncio
    async def test_force_resume_all_resumes_all(self) -> None:
        """Test force resume all paused tenants."""
        manager = WritePauseManager()
        tenant_ids = [uuid4() for _ in range(3)]

        for tid in tenant_ids:
            await manager.pause_writes(tid)

        metrics_list = await manager.force_resume_all()

        assert len(metrics_list) == 3
        assert all(m.tenant_id in tenant_ids for m in metrics_list)

        # Verify all are resumed
        for tid in tenant_ids:
            assert manager.is_paused(tid) is False


# =============================================================================
# Test Wait For No Waiters
# =============================================================================


class TestWaitForNoWaiters:
    """Tests for wait_for_no_waiters functionality."""

    @pytest.mark.asyncio
    async def test_wait_for_no_waiters_immediate_if_not_paused(self) -> None:
        """Test returns True immediately if tenant not paused."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        result = await manager.wait_for_no_waiters(tenant_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_wait_for_no_waiters_immediate_if_no_waiters(self) -> None:
        """Test returns True immediately if no waiters."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)

        result = await manager.wait_for_no_waiters(tenant_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_wait_for_no_waiters_waits_for_waiters(self) -> None:
        """Test waits for waiters to complete."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)

        async def waiter():
            with contextlib.suppress(WritePausedError):
                await manager.wait_if_paused(tenant_id, timeout=1.0)

        # Start a waiter
        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)

        # Now resume - waiter should complete
        await manager.resume_writes(tenant_id)
        await waiter_task

        # Re-pause
        await manager.pause_writes(tenant_id)

        # Should return True since no waiters
        result = await manager.wait_for_no_waiters(tenant_id, timeout=0.1)
        assert result is True


# =============================================================================
# Test Concurrent Operations Safety
# =============================================================================


class TestConcurrentOperationsSafety:
    """Tests for thread safety of concurrent operations."""

    @pytest.mark.asyncio
    async def test_concurrent_pause_resume_safe(self) -> None:
        """Test concurrent pause/resume is safe."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        async def pause_resume():
            await manager.pause_writes(tenant_id)
            await asyncio.sleep(0.001)
            await manager.resume_writes(tenant_id)

        # Run many concurrent pause/resume cycles
        tasks = [asyncio.create_task(pause_resume()) for _ in range(10)]
        await asyncio.gather(*tasks)

        # Should end unparsed
        assert manager.is_paused(tenant_id) is False

    @pytest.mark.asyncio
    async def test_concurrent_waits_and_resume(self) -> None:
        """Test concurrent waits with resume."""
        manager = WritePauseManager()
        tenant_id = uuid4()
        completed = 0
        errors = 0

        async def waiter():
            nonlocal completed, errors
            try:
                await manager.wait_if_paused(tenant_id, timeout=1.0)
                completed += 1
            except WritePausedError:
                errors += 1

        await manager.pause_writes(tenant_id)

        # Start many waiters
        tasks = [asyncio.create_task(waiter()) for _ in range(20)]

        # Give time to start
        await asyncio.sleep(0.02)

        # Resume
        await manager.resume_writes(tenant_id)

        await asyncio.gather(*tasks)

        # All should complete without error
        assert completed == 20
        assert errors == 0

    @pytest.mark.asyncio
    async def test_different_tenants_independent(self) -> None:
        """Test different tenants are independent."""
        manager = WritePauseManager()
        tenant1 = uuid4()
        tenant2 = uuid4()

        await manager.pause_writes(tenant1)

        # tenant2 should not be paused
        assert manager.is_paused(tenant1) is True
        assert manager.is_paused(tenant2) is False

        # Wait on tenant2 should return immediately
        result = await manager.wait_if_paused(tenant2)
        assert result == 0.0

        # Wait on tenant1 should block
        with pytest.raises(WritePausedError):
            await manager.wait_if_paused(tenant1, timeout=0.01)


# =============================================================================
# Test Integration with TenantStoreRouter Pattern
# =============================================================================


class TestRouterIntegration:
    """Tests simulating integration with TenantStoreRouter."""

    @pytest.mark.asyncio
    async def test_cutover_sequence_simulation(self) -> None:
        """Simulate a cutover sequence."""
        manager = WritePauseManager(default_timeout=5.0)
        tenant_id = uuid4()
        writes_no_wait = 0
        writes_after_wait = 0

        async def simulate_writer():
            nonlocal writes_no_wait, writes_after_wait
            # Simulate checking pause before write
            waited = await manager.wait_if_paused(tenant_id, timeout=2.0)
            if waited == 0:
                # Did not wait - tenant was not paused
                writes_no_wait += 1
            else:
                # Had to wait for pause to be lifted
                writes_after_wait += 1

        # Phase 1: Normal writes (not paused) - should complete immediately
        await simulate_writer()
        assert writes_no_wait == 1
        assert writes_after_wait == 0

        # Phase 2: Pause for cutover
        await manager.pause_writes(tenant_id)

        # Start writers that will wait
        writer_tasks = [asyncio.create_task(simulate_writer()) for _ in range(3)]

        # Simulate cutover work
        await asyncio.sleep(0.05)

        # Phase 3: Resume after cutover
        metrics = await manager.resume_writes(tenant_id)

        # Wait for writers to complete
        await asyncio.gather(*writer_tasks)

        # Verify
        assert metrics is not None
        assert metrics.duration_ms >= 50.0
        # 1 writer did not wait (before pause), 3 writers had to wait
        assert writes_no_wait == 1
        assert writes_after_wait == 3  # These waited for pause to lift


# =============================================================================
# Test Edge Cases
# =============================================================================


class TestEdgeCases:
    """Tests for edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_pause_resume_rapid_sequence(self) -> None:
        """Test rapid pause/resume sequence."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        for _ in range(100):
            await manager.pause_writes(tenant_id)
            await manager.resume_writes(tenant_id)

        assert manager.is_paused(tenant_id) is False
        assert len(manager.get_metrics_history()) == 100

    @pytest.mark.asyncio
    async def test_many_tenants(self) -> None:
        """Test handling many different tenants."""
        manager = WritePauseManager()
        tenant_ids = [uuid4() for _ in range(50)]

        # Pause all
        for tid in tenant_ids:
            await manager.pause_writes(tid)

        paused = await manager.get_all_paused()
        assert len(paused) == 50

        # Resume all
        for tid in tenant_ids:
            await manager.resume_writes(tid)

        paused = await manager.get_all_paused()
        assert len(paused) == 0

    @pytest.mark.asyncio
    async def test_waiter_count_decrements_on_timeout(self) -> None:
        """Test waiter count decrements even on timeout."""
        manager = WritePauseManager()
        tenant_id = uuid4()

        await manager.pause_writes(tenant_id)

        # Start a waiter that will timeout
        with pytest.raises(WritePausedError):
            await manager.wait_if_paused(tenant_id, timeout=0.02)

        # Waiter count should be back to 0
        state = await manager.get_pause_state(tenant_id)
        assert state is not None
        assert state["waiting_count"] == 0
