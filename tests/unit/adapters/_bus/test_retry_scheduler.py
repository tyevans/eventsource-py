"""Unit tests for RetryScheduler."""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock

import pytest

from eventsource.adapters._bus.retry_scheduler import RetryScheduler


@pytest.mark.asyncio
async def test_schedule_executes_after_delay() -> None:
    scheduler = RetryScheduler()
    executed = False

    async def action() -> None:
        nonlocal executed
        executed = True

    task = scheduler.schedule(0.01, action, name="test-task")
    assert scheduler.active_count == 1
    assert task in scheduler.tasks

    await task
    assert executed is True
    assert scheduler.active_count == 0


@pytest.mark.asyncio
async def test_schedule_zero_delay_runs_immediately() -> None:
    scheduler = RetryScheduler()
    executed = False

    async def action() -> None:
        nonlocal executed
        executed = True

    task = scheduler.schedule(0.0, action)
    await task
    assert executed is True
    assert scheduler.active_count == 0


@pytest.mark.asyncio
async def test_drain_awaits_all_tasks() -> None:
    scheduler = RetryScheduler()
    results: list[int] = []

    async def make_action(val: int) -> None:
        await asyncio.sleep(0.02)
        results.append(val)

    scheduler.schedule(0.01, lambda: make_action(1))
    scheduler.schedule(0.02, lambda: make_action(2))
    assert scheduler.active_count == 2

    await scheduler.drain(timeout=1.0)
    assert results == [1, 2]
    assert scheduler.active_count == 0


@pytest.mark.asyncio
async def test_drain_empty_scheduler_returns_immediately() -> None:
    scheduler = RetryScheduler()
    await scheduler.drain()
    assert scheduler.active_count == 0


@pytest.mark.asyncio
async def test_drain_with_timeout_logs_warning_on_timeout() -> None:
    mock_logger = MagicMock()
    scheduler = RetryScheduler(custom_logger=mock_logger)

    async def slow_action() -> None:
        await asyncio.sleep(0.5)

    scheduler.schedule(0.1, slow_action)
    await scheduler.drain(timeout=0.01)
    mock_logger.warning.assert_called_once()
    scheduler.cancel_all()


@pytest.mark.asyncio
async def test_cancel_all_cancels_pending_tasks() -> None:
    scheduler = RetryScheduler()
    ran = False

    async def slow_action() -> None:
        nonlocal ran
        await asyncio.sleep(1.0)
        ran = True

    task = scheduler.schedule(0.1, slow_action)
    scheduler.cancel_all()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert ran is False


@pytest.mark.asyncio
async def test_action_exception_is_logged_and_handled() -> None:
    mock_logger = MagicMock()
    scheduler = RetryScheduler(custom_logger=mock_logger)

    async def failing_action() -> None:
        raise RuntimeError("boom")

    task = scheduler.schedule(0.0, failing_action, name="exploding-task")
    await task
    mock_logger.error.assert_called_once()
    assert scheduler.active_count == 0
