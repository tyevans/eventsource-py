"""Unit tests for BackgroundTaskManager."""

import asyncio
import contextlib

from eventsource.application.background_tasks import BackgroundTaskManager


async def test_submit_runs_the_coroutine_and_tracks_the_task() -> None:
    manager = BackgroundTaskManager()
    ran = asyncio.Event()

    async def work() -> None:
        ran.set()

    manager.submit(work())
    assert manager.pending_count == 1

    await manager.await_all()

    assert ran.is_set()
    assert manager.pending_count == 0


async def test_pending_count_and_has_pending_reflect_in_flight_tasks() -> None:
    manager = BackgroundTaskManager()
    started = asyncio.Event()
    release = asyncio.Event()

    async def work() -> None:
        started.set()
        await release.wait()

    manager.submit(work())
    await started.wait()

    assert manager.pending_count == 1
    assert manager.has_pending is True

    release.set()
    await manager.await_all()

    assert manager.pending_count == 0
    assert manager.has_pending is False


async def test_await_all_with_no_timeout_waits_for_everything() -> None:
    manager = BackgroundTaskManager()
    done_events = [asyncio.Event() for _ in range(3)]

    async def work(ev: asyncio.Event) -> None:
        await asyncio.sleep(0)
        ev.set()

    for ev in done_events:
        manager.submit(work(ev))

    count = await manager.await_all()

    assert count == 3
    assert all(ev.is_set() for ev in done_events)
    assert manager.pending_count == 0


async def test_await_all_returns_zero_when_nothing_pending() -> None:
    manager = BackgroundTaskManager()

    assert await manager.await_all() == 0
    assert await manager.await_all(timeout=1.0) == 0


async def test_await_all_with_timeout_cancels_stragglers() -> None:
    manager = BackgroundTaskManager()

    async def slow() -> None:
        await asyncio.sleep(30)

    task = manager.submit(slow())

    count = await manager.await_all(timeout=0.05)

    assert count == 1
    assert task.cancelled() or task.done()
    assert manager.pending_count == 0


async def test_cancel_all_cancels_pending_tasks_and_returns_the_count() -> None:
    manager = BackgroundTaskManager()

    async def slow() -> None:
        await asyncio.sleep(30)

    task = manager.submit(slow())

    count = manager.cancel_all()
    assert count == 1

    # Give the event loop a tick to run the cancellation and done-callback.
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert task.cancelled()


async def test_failed_task_is_logged_under_this_modules_logger(
    caplog,  # type: ignore[no-untyped-def]
) -> None:
    manager = BackgroundTaskManager()

    async def boom() -> None:
        raise ValueError("distinctive background task manager failure")

    with caplog.at_level("ERROR", logger="eventsource.application.background_tasks"):
        manager.submit(boom())
        await manager.await_all()

    own_records = [
        r for r in caplog.records if r.name == "eventsource.application.background_tasks"
    ]
    # Logged both by the task's done-callback and by await_all's own
    # exception collection -- this mirrors the pre-refactor behaviour of
    # the original standalone BackgroundTaskManager.
    assert len(own_records) == 2
    assert all(
        "Background task failed: distinctive background task manager failure" in r.message
        for r in own_records
    )
    assert all(r.exc_info is not None for r in own_records)


async def test_submit_with_on_done_delegates_instead_of_default_logging() -> None:
    manager = BackgroundTaskManager()
    seen: list[asyncio.Task[None]] = []

    async def boom() -> None:
        raise ValueError("handled elsewhere")

    manager.submit(boom(), on_done=seen.append)
    await manager.await_all()

    assert len(seen) == 1
    assert seen[0].exception() is not None
