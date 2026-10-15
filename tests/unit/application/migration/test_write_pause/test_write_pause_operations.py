"""Unit tests for WritePauseManager operations, metrics history, and edge cases."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.migration.write_pause import WritePausedError, WritePauseManager


class TestMetricsHistory:
    @pytest.mark.asyncio
    async def test_metrics_added_to_history(self) -> None:
        manager, tenant_id = WritePauseManager(), uuid4()
        await manager.pause_writes(tenant_id)
        await manager.resume_writes(tenant_id)
        history = manager.get_metrics_history()
        assert len(history) == 1 and history[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_metrics_history_limit(self) -> None:
        manager = WritePauseManager(max_history_size=3)
        for _i in range(5):
            tenant_id = uuid4()
            await manager.pause_writes(tenant_id)
            await manager.resume_writes(tenant_id)
        assert len(manager.get_metrics_history()) == 3

    @pytest.mark.asyncio
    async def test_metrics_history_returns_copy(self) -> None:
        manager, tenant_id = WritePauseManager(), uuid4()
        await manager.pause_writes(tenant_id)
        await manager.resume_writes(tenant_id)
        history1, history2 = manager.get_metrics_history(), manager.get_metrics_history()
        assert history1 is not history2 and history1[0] is history2[0]


class TestGetAllPaused:
    @pytest.mark.asyncio
    async def test_get_all_paused_empty(self) -> None:
        assert await WritePauseManager().get_all_paused() == []

    @pytest.mark.asyncio
    async def test_get_all_paused_returns_all(self) -> None:
        manager, tenant_ids = WritePauseManager(), [uuid4() for _ in range(3)]
        for tid in tenant_ids:
            await manager.pause_writes(tid)
        paused = await manager.get_all_paused()
        assert len(paused) == 3 and set(paused) == set(tenant_ids)


class TestGetPauseState:
    @pytest.mark.asyncio
    async def test_get_pause_state_when_not_paused(self) -> None:
        assert await WritePauseManager().get_pause_state(uuid4()) is None

    @pytest.mark.asyncio
    async def test_get_pause_state_returns_info(self) -> None:
        manager, tenant_id = WritePauseManager(), uuid4()
        await manager.pause_writes(tenant_id)
        await asyncio.sleep(0.01)
        state = await manager.get_pause_state(tenant_id)
        assert state is not None and state["tenant_id"] == str(tenant_id)
        assert state["duration_ms"] >= 10.0 and state["waiting_count"] == 0
        assert "started_at" in state


class TestForceResumeAll:
    @pytest.mark.asyncio
    async def test_force_resume_all_empty(self) -> None:
        assert await WritePauseManager().force_resume_all() == []

    @pytest.mark.asyncio
    async def test_force_resume_all_resumes_all(self) -> None:
        manager, tenant_ids = WritePauseManager(), [uuid4() for _ in range(3)]
        for tid in tenant_ids:
            await manager.pause_writes(tid)
        metrics_list = await manager.force_resume_all()
        assert len(metrics_list) == 3 and all(m.tenant_id in tenant_ids for m in metrics_list)
        assert all(manager.is_paused(tid) is False for tid in tenant_ids)


class TestConcurrentOperationsSafety:
    @pytest.mark.asyncio
    async def test_concurrent_pause_resume_safe(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()

        async def pause_resume():
            await manager.pause_writes(tenant_id)
            await asyncio.sleep(0.001)
            await manager.resume_writes(tenant_id)

        await asyncio.gather(*(asyncio.create_task(pause_resume()) for _ in range(10)))
        assert manager.is_paused(tenant_id) is False

    @pytest.mark.asyncio
    async def test_concurrent_waits_and_resume(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        completed, errors = 0, 0

        async def waiter():
            nonlocal completed, errors
            try:
                await manager.wait_if_paused(tenant_id, timeout=1.0)
                completed += 1
            except WritePausedError:
                errors += 1

        await manager.pause_writes(tenant_id)
        tasks = [asyncio.create_task(waiter()) for _ in range(20)]
        await asyncio.sleep(0.02)
        await manager.resume_writes(tenant_id)
        await asyncio.gather(*tasks)
        assert completed == 20 and errors == 0

    @pytest.mark.asyncio
    async def test_different_tenants_independent(self) -> None:
        manager = WritePauseManager()
        tenant1, tenant2 = uuid4(), uuid4()
        await manager.pause_writes(tenant1)
        assert manager.is_paused(tenant1) is True
        assert manager.is_paused(tenant2) is False
        assert await manager.wait_if_paused(tenant2) == 0.0
        with pytest.raises(WritePausedError):
            await manager.wait_if_paused(tenant1, timeout=0.01)


class TestRouterIntegration:
    @pytest.mark.asyncio
    async def test_cutover_sequence_simulation(self) -> None:
        manager = WritePauseManager(default_timeout=5.0)
        tenant_id = uuid4()
        writes_no_wait, writes_after_wait = 0, 0

        async def simulate_writer():
            nonlocal writes_no_wait, writes_after_wait
            if await manager.wait_if_paused(tenant_id, timeout=2.0) == 0:
                writes_no_wait += 1
            else:
                writes_after_wait += 1

        await simulate_writer()
        assert writes_no_wait == 1 and writes_after_wait == 0

        await manager.pause_writes(tenant_id)
        writer_tasks = [asyncio.create_task(simulate_writer()) for _ in range(3)]
        await asyncio.sleep(0.05)
        metrics = await manager.resume_writes(tenant_id)
        await asyncio.gather(*writer_tasks)

        assert metrics is not None and metrics.duration_ms >= 50.0
        assert writes_no_wait == 1 and writes_after_wait == 3


class TestEdgeCases:
    @pytest.mark.asyncio
    async def test_pause_resume_rapid_sequence(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        for _ in range(100):
            await manager.pause_writes(tenant_id)
            await manager.resume_writes(tenant_id)
        assert manager.is_paused(tenant_id) is False
        assert len(manager.get_metrics_history()) == 100

    @pytest.mark.asyncio
    async def test_many_tenants(self) -> None:
        manager = WritePauseManager()
        tenant_ids = [uuid4() for _ in range(50)]
        for tid in tenant_ids:
            await manager.pause_writes(tid)
        assert len(await manager.get_all_paused()) == 50
        for tid in tenant_ids:
            await manager.resume_writes(tid)
        assert len(await manager.get_all_paused()) == 0

    @pytest.mark.asyncio
    async def test_waiter_count_decrements_on_timeout(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)
        with pytest.raises(WritePausedError):
            await manager.wait_if_paused(tenant_id, timeout=0.02)
        state = await manager.get_pause_state(tenant_id)
        assert state is not None and state["waiting_count"] == 0
