"""Unit tests for WritePauseManager operations, metrics history, and edge cases."""

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.migration.write_pause import (
    WritePausedError,
    WritePauseManager,
)


class TestMetricsHistory:
    @pytest.mark.asyncio
    async def test_metrics_added_to_history(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)
        await manager.resume_writes(tenant_id)
        history = manager.get_metrics_history()
        assert len(history) == 1
        assert history[0].tenant_id == tenant_id

    @pytest.mark.asyncio
    async def test_metrics_history_limit(self) -> None:
        manager = WritePauseManager(max_history_size=3)
        for _i in range(5):
            tenant_id = uuid4()
            await manager.pause_writes(tenant_id)
            await manager.resume_writes(tenant_id)
        history = manager.get_metrics_history()
        assert len(history) == 3

    @pytest.mark.asyncio
    async def test_metrics_history_returns_copy(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)
        await manager.resume_writes(tenant_id)
        history1 = manager.get_metrics_history()
        history2 = manager.get_metrics_history()
        assert history1 is not history2
        assert history1[0] is history2[0]


class TestGetAllPaused:
    @pytest.mark.asyncio
    async def test_get_all_paused_empty(self) -> None:
        manager = WritePauseManager()
        paused = await manager.get_all_paused()
        assert paused == []

    @pytest.mark.asyncio
    async def test_get_all_paused_returns_all(self) -> None:
        manager = WritePauseManager()
        tenant_ids = [uuid4() for _ in range(3)]
        for tid in tenant_ids:
            await manager.pause_writes(tid)
        paused = await manager.get_all_paused()
        assert len(paused) == 3
        assert set(paused) == set(tenant_ids)


class TestGetPauseState:
    @pytest.mark.asyncio
    async def test_get_pause_state_when_not_paused(self) -> None:
        manager = WritePauseManager()
        state = await manager.get_pause_state(uuid4())
        assert state is None

    @pytest.mark.asyncio
    async def test_get_pause_state_returns_info(self) -> None:
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


class TestForceResumeAll:
    @pytest.mark.asyncio
    async def test_force_resume_all_empty(self) -> None:
        manager = WritePauseManager()
        metrics_list = await manager.force_resume_all()
        assert metrics_list == []

    @pytest.mark.asyncio
    async def test_force_resume_all_resumes_all(self) -> None:
        manager = WritePauseManager()
        tenant_ids = [uuid4() for _ in range(3)]
        for tid in tenant_ids:
            await manager.pause_writes(tid)
        metrics_list = await manager.force_resume_all()
        assert len(metrics_list) == 3
        assert all(m.tenant_id in tenant_ids for m in metrics_list)
        for tid in tenant_ids:
            assert manager.is_paused(tid) is False


class TestConcurrentOperationsSafety:
    @pytest.mark.asyncio
    async def test_concurrent_pause_resume_safe(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()

        async def pause_resume():
            await manager.pause_writes(tenant_id)
            await asyncio.sleep(0.001)
            await manager.resume_writes(tenant_id)

        tasks = [asyncio.create_task(pause_resume()) for _ in range(10)]
        await asyncio.gather(*tasks)
        assert manager.is_paused(tenant_id) is False

    @pytest.mark.asyncio
    async def test_concurrent_waits_and_resume(self) -> None:
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
        tasks = [asyncio.create_task(waiter()) for _ in range(20)]
        await asyncio.sleep(0.02)
        await manager.resume_writes(tenant_id)
        await asyncio.gather(*tasks)
        assert completed == 20
        assert errors == 0

    @pytest.mark.asyncio
    async def test_different_tenants_independent(self) -> None:
        manager = WritePauseManager()
        tenant1, tenant2 = uuid4(), uuid4()
        await manager.pause_writes(tenant1)
        assert manager.is_paused(tenant1) is True
        assert manager.is_paused(tenant2) is False
        result = await manager.wait_if_paused(tenant2)
        assert result == 0.0
        with pytest.raises(WritePausedError):
            await manager.wait_if_paused(tenant1, timeout=0.01)


class TestRouterIntegration:
    @pytest.mark.asyncio
    async def test_cutover_sequence_simulation(self) -> None:
        manager = WritePauseManager(default_timeout=5.0)
        tenant_id = uuid4()
        writes_no_wait = 0
        writes_after_wait = 0

        async def simulate_writer():
            nonlocal writes_no_wait, writes_after_wait
            waited = await manager.wait_if_paused(tenant_id, timeout=2.0)
            if waited == 0:
                writes_no_wait += 1
            else:
                writes_after_wait += 1

        await simulate_writer()
        assert writes_no_wait == 1
        assert writes_after_wait == 0

        await manager.pause_writes(tenant_id)
        writer_tasks = [asyncio.create_task(simulate_writer()) for _ in range(3)]
        await asyncio.sleep(0.05)
        metrics = await manager.resume_writes(tenant_id)
        await asyncio.gather(*writer_tasks)

        assert metrics is not None
        assert metrics.duration_ms >= 50.0
        assert writes_no_wait == 1
        assert writes_after_wait == 3


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
        paused = await manager.get_all_paused()
        assert len(paused) == 50
        for tid in tenant_ids:
            await manager.resume_writes(tid)
        paused = await manager.get_all_paused()
        assert len(paused) == 0

    @pytest.mark.asyncio
    async def test_waiter_count_decrements_on_timeout(self) -> None:
        manager = WritePauseManager()
        tenant_id = uuid4()
        await manager.pause_writes(tenant_id)
        with pytest.raises(WritePausedError):
            await manager.wait_if_paused(tenant_id, timeout=0.02)
        state = await manager.get_pause_state(tenant_id)
        assert state is not None
        assert state["waiting_count"] == 0
