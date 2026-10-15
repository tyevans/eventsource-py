"""Unit tests for pre-shutdown hook functionality and imports."""

import asyncio

import pytest

from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
)


async def _noop_hook() -> None:
    pass


class TestPreShutdownHooks:
    """Tests for pre-shutdown hook functionality."""

    def test_register_hook(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_pre_shutdown(_noop_hook, timeout=5.0)
        assert len(coordinator._pre_shutdown_hooks) == 1

    def test_register_hook_default_timeout(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_pre_shutdown(_noop_hook)
        assert len(coordinator._pre_shutdown_hooks) == 1
        assert coordinator._pre_shutdown_hooks[0][1] == 5.0

    def test_register_hook_validates_async(self):
        coordinator = ShutdownCoordinator()

        def sync_hook():
            pass

        with pytest.raises(TypeError, match="must be async function"):
            coordinator.on_pre_shutdown(sync_hook)  # type: ignore[arg-type]

    def test_register_hook_validates_timeout_zero(self):
        coordinator = ShutdownCoordinator()
        with pytest.raises(ValueError, match="must be positive"):
            coordinator.on_pre_shutdown(_noop_hook, timeout=0)

    def test_register_hook_validates_timeout_negative(self):
        coordinator = ShutdownCoordinator()
        with pytest.raises(ValueError, match="must be positive"):
            coordinator.on_pre_shutdown(_noop_hook, timeout=-1)

    def test_register_multiple_hooks(self):
        coordinator = ShutdownCoordinator()

        async def hook1():
            pass

        async def hook2():
            pass

        async def hook3():
            pass

        coordinator.on_pre_shutdown(hook1, timeout=5.0)
        coordinator.on_pre_shutdown(hook2, timeout=10.0)
        coordinator.on_pre_shutdown(hook3, timeout=2.0)

        assert len(coordinator._pre_shutdown_hooks) == 3
        assert coordinator._pre_shutdown_hooks[0] == (hook1, 5.0)
        assert coordinator._pre_shutdown_hooks[1] == (hook2, 10.0)
        assert coordinator._pre_shutdown_hooks[2] == (hook3, 2.0)

    @pytest.mark.asyncio
    async def test_hooks_execute_in_order(self):
        coordinator = ShutdownCoordinator()
        execution_order = []

        async def hook1():
            execution_order.append(1)

        async def hook2():
            execution_order.append(2)

        async def hook3():
            execution_order.append(3)

        coordinator.on_pre_shutdown(hook1)
        coordinator.on_pre_shutdown(hook2)
        coordinator.on_pre_shutdown(hook3)

        await coordinator._execute_pre_shutdown_hooks()
        assert execution_order == [1, 2, 3]

    @pytest.mark.asyncio
    async def test_hook_timeout(self):
        coordinator = ShutdownCoordinator()
        completed = False

        async def slow_hook():
            nonlocal completed
            await asyncio.sleep(10.0)
            completed = True

        coordinator.on_pre_shutdown(slow_hook, timeout=0.1)

        start = asyncio.get_event_loop().time()
        await coordinator._execute_pre_shutdown_hooks()
        duration = asyncio.get_event_loop().time() - start

        assert not completed
        assert duration < 0.5

    @pytest.mark.asyncio
    async def test_hook_exception_continues_execution(self):
        coordinator = ShutdownCoordinator()
        hook3_executed = False

        async def failing_hook():
            raise RuntimeError("Hook failed")

        async def success_hook():
            nonlocal hook3_executed
            hook3_executed = True

        coordinator.on_pre_shutdown(failing_hook)
        coordinator.on_pre_shutdown(success_hook)

        await coordinator._execute_pre_shutdown_hooks()
        assert hook3_executed

    @pytest.mark.asyncio
    async def test_hooks_execute_before_stopping_phase(self):
        coordinator = ShutdownCoordinator()
        phase_at_hook_execution = None

        async def capture_phase():
            nonlocal phase_at_hook_execution
            phase_at_hook_execution = coordinator.phase

        coordinator.on_pre_shutdown(capture_phase)
        await coordinator.shutdown(stop_func=_noop_hook)
        assert phase_at_hook_execution == ShutdownPhase.RUNNING

    def test_reset_clears_hooks(self):
        coordinator = ShutdownCoordinator()
        coordinator.on_pre_shutdown(_noop_hook)
        assert len(coordinator._pre_shutdown_hooks) == 1

        coordinator.reset()
        assert len(coordinator._pre_shutdown_hooks) == 0

    @pytest.mark.asyncio
    async def test_no_hooks_execution_is_safe(self):
        coordinator = ShutdownCoordinator()
        await coordinator._execute_pre_shutdown_hooks()

    @pytest.mark.asyncio
    async def test_hooks_run_during_shutdown_sequence(self):
        coordinator = ShutdownCoordinator()
        hook_executed = False

        async def pre_hook():
            nonlocal hook_executed
            hook_executed = True

        coordinator.on_pre_shutdown(pre_hook)
        await coordinator.shutdown(stop_func=_noop_hook)
        assert hook_executed

    @pytest.mark.asyncio
    async def test_hook_failure_does_not_prevent_shutdown(self):
        coordinator = ShutdownCoordinator()
        stop_called = False

        async def failing_hook():
            raise RuntimeError("Hook exploded")

        async def stop_func():
            nonlocal stop_called
            stop_called = True

        coordinator.on_pre_shutdown(failing_hook)
        result = await coordinator.shutdown(stop_func=stop_func)

        assert stop_called
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_hook_timeout_does_not_prevent_shutdown(self):
        coordinator = ShutdownCoordinator()
        stop_called = False

        async def slow_hook():
            await asyncio.sleep(10.0)

        async def stop_func():
            nonlocal stop_called
            stop_called = True

        coordinator.on_pre_shutdown(slow_hook, timeout=0.1)
        result = await coordinator.shutdown(stop_func=stop_func)

        assert stop_called
        assert result.phase == ShutdownPhase.STOPPED

    @pytest.mark.asyncio
    async def test_multiple_hooks_with_different_timeouts(self):
        coordinator = ShutdownCoordinator()
        results = []

        async def fast_hook():
            results.append("fast")

        async def slow_hook():
            await asyncio.sleep(10.0)
            results.append("slow")

        async def medium_hook():
            await asyncio.sleep(0.05)
            results.append("medium")

        coordinator.on_pre_shutdown(fast_hook, timeout=1.0)
        coordinator.on_pre_shutdown(slow_hook, timeout=0.1)
        coordinator.on_pre_shutdown(medium_hook, timeout=1.0)

        await coordinator._execute_pre_shutdown_hooks()

        assert "fast" in results
        assert "slow" not in results
        assert "medium" in results

    @pytest.mark.asyncio
    async def test_hooks_execute_before_drain_and_checkpoint(self):
        coordinator = ShutdownCoordinator()
        hook_time = drain_time = checkpoint_time = None

        async def pre_hook():
            nonlocal hook_time
            hook_time = asyncio.get_event_loop().time()

        async def drain_func():
            nonlocal drain_time
            drain_time = asyncio.get_event_loop().time()
            return 0

        async def checkpoint_func():
            nonlocal checkpoint_time
            checkpoint_time = asyncio.get_event_loop().time()
            return 0

        coordinator.on_pre_shutdown(pre_hook)
        await coordinator.shutdown(
            stop_func=_noop_hook,
            drain_func=drain_func,
            checkpoint_func=checkpoint_func,
        )

        assert hook_time is not None
        assert drain_time is not None
        assert checkpoint_time is not None
        assert hook_time < drain_time < checkpoint_time


class TestPreShutdownHookImports:
    """Tests for pre-shutdown hook type imports."""

    def test_import_pre_shutdown_hook_type(self):
        from eventsource.application.subscriptions.shutdown import PreShutdownHook

        assert PreShutdownHook is not None

    def test_pre_shutdown_hook_in_all(self):
        from eventsource.application.subscriptions.shutdown import __all__

        assert "PreShutdownHook" in __all__
