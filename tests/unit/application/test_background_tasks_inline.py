"""BackgroundTaskManager.submit_or_run_inline: tracked or inline, with an on_done hook."""

from __future__ import annotations

import asyncio
import logging

import pytest

from eventsource.application.background_tasks import BackgroundTaskManager


async def _noop() -> None:
    return None


async def test_below_capacity_returns_the_tracked_task() -> None:
    manager = BackgroundTaskManager(max_pending=2)
    task = await manager.submit_or_run_inline(_noop())
    assert isinstance(task, asyncio.Task)
    assert manager.pending_count == 1
    await manager.await_all()
    assert manager.pending_count == 0


async def test_at_capacity_runs_inline_and_returns_none() -> None:
    manager = BackgroundTaskManager(max_pending=1)
    gate = asyncio.Event()

    async def wait_for_gate() -> None:
        await gate.wait()

    first = await manager.submit_or_run_inline(wait_for_gate())
    ran: list[str] = []

    async def record() -> None:
        ran.append("inline")

    second = await manager.submit_or_run_inline(record())
    assert first is not None
    assert second is None
    assert ran == ["inline"]
    assert manager.pending_count == 1
    gate.set()
    await manager.await_all()


async def test_on_done_is_called_with_the_finished_task() -> None:
    manager = BackgroundTaskManager(max_pending=4)
    seen: list[asyncio.Task[object]] = []
    task = await manager.submit_or_run_inline(_noop(), on_done=seen.append)
    await manager.await_all()
    assert seen == [task]


async def test_unbounded_manager_never_runs_inline() -> None:
    manager = BackgroundTaskManager(max_pending=None)
    tasks = [await manager.submit_or_run_inline(_noop()) for _ in range(5)]
    assert all(t is not None for t in tasks)
    await manager.await_all()


async def test_failure_with_falsy_exception_is_still_logged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FalsyError(RuntimeError):
        def __bool__(self) -> bool:
            return False

    async def boom() -> None:
        raise FalsyError("distinctive falsy failure")

    manager = BackgroundTaskManager(max_pending=4)
    with caplog.at_level(logging.ERROR, logger="eventsource.application.background_tasks"):
        await manager.submit_or_run_inline(boom())
        await manager.await_all()

    messages = [r.message for r in caplog.records if r.levelno == logging.ERROR]
    assert any("Background task failed: distinctive falsy failure" in m for m in messages)
