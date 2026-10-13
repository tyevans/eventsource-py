"""Unit tests for WritePauseManager waiting functionality."""

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource.application.migration.write_pause import (
    WritePausedError,
    WritePauseManager,
)

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
