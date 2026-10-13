"""Unit tests for WritePauseManager operations, metrics history, and edge cases."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.migration.write_pause import (
    WritePausedError,
    WritePauseManager,
)

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
