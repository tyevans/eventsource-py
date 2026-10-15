"""Unit tests for WritePauseManager waiting functionality."""

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource.application.migration.write_pause import WritePausedError, WritePauseManager


class TestWaitIfPaused:
    @pytest.mark.asyncio
    async def test_wait_returns_immediately_when_not_paused(self) -> None:
        manager = WritePauseManager()
        result = await asyncio.wait_for(manager.wait_if_paused(uuid4()), timeout=0.1)
        assert result == 0.0

    @pytest.mark.asyncio
    async def test_wait_returns_immediately_for_none_tenant(self) -> None:
        manager = WritePauseManager()
        result = await asyncio.wait_for(manager.wait_if_paused(None), timeout=0.1)
        assert result == 0.0

    @pytest.mark.asyncio
    async def test_wait_blocks_until_resume(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        wait_completed = False

        async def waiter():
            nonlocal wait_completed
            await manager.wait_if_paused(tenant_id)
            wait_completed = True

        await manager.pause_writes(tenant_id)
        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)
        assert wait_completed is False
        await manager.resume_writes(tenant_id)
        await asyncio.wait_for(waiter_task, timeout=1.0)
        assert wait_completed is True

    @pytest.mark.asyncio
    async def test_wait_returns_time_waited(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)

        async def delayed_resume():
            await asyncio.sleep(0.05)
            await manager.resume_writes(tenant_id)

        asyncio.create_task(delayed_resume())
        waited_ms = await manager.wait_if_paused(tenant_id)
        assert waited_ms >= 40.0

    @pytest.mark.asyncio
    async def test_wait_raises_on_timeout(self) -> None:
        manager = WritePauseManager(default_timeout=0.05)
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
        manager = WritePauseManager(default_timeout=10.0)
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)
        with pytest.raises(WritePausedError) as exc_info:
            await manager.wait_if_paused(tenant_id, timeout=0.05)
        assert exc_info.value.timeout == 0.05


class TestMultipleWaiters:
    @pytest.mark.asyncio
    async def test_multiple_waiters_all_unblocked(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        completed_count = 0

        async def waiter():
            nonlocal completed_count
            await manager.wait_if_paused(tenant_id)
            completed_count += 1

        await manager.pause_writes(tenant_id)
        tasks = [asyncio.create_task(waiter()) for _ in range(5)]
        await asyncio.sleep(0.02)
        assert completed_count == 0
        await manager.resume_writes(tenant_id)
        await asyncio.gather(*tasks)
        assert completed_count == 5

    @pytest.mark.asyncio
    async def test_waiter_count_tracking(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)

        async def waiter():
            await manager.wait_if_paused(tenant_id, timeout=5.0)

        tasks = [asyncio.create_task(waiter()) for _ in range(3)]
        await asyncio.sleep(0.02)
        state = await manager.get_pause_state(tenant_id)
        assert state is not None
        assert state["waiting_count"] == 3
        metrics = await manager.resume_writes(tenant_id)
        await asyncio.gather(*tasks)
        assert metrics is not None
        assert metrics.max_waiters == 3
        assert metrics.total_waiters == 3


class TestWaitForNoWaiters:
    @pytest.mark.asyncio
    async def test_wait_for_no_waiters_immediate_if_not_paused(self) -> None:
        manager = WritePauseManager()
        assert await manager.wait_for_no_waiters(uuid4()) is True

    @pytest.mark.asyncio
    async def test_wait_for_no_waiters_immediate_if_no_waiters(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)
        assert await manager.wait_for_no_waiters(tenant_id) is True

    @pytest.mark.asyncio
    async def test_wait_for_no_waiters_waits_for_waiters(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)

        async def waiter():
            with contextlib.suppress(WritePausedError):
                await manager.wait_if_paused(tenant_id, timeout=1.0)

        waiter_task = asyncio.create_task(waiter())
        await asyncio.sleep(0.01)
        await manager.resume_writes(tenant_id)
        await waiter_task
        await manager.pause_writes(tenant_id)
        assert await manager.wait_for_no_waiters(tenant_id, timeout=0.1) is True
