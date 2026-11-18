"""Pending-task limit for RetryScheduler."""

from __future__ import annotations

import asyncio

import pytest

from eventsource.adapters._bus.retry_scheduler import RetryScheduler


async def _never() -> None:
    await asyncio.sleep(3600)


def test_default_limit_is_a_positive_bound() -> None:
    assert RetryScheduler().is_full is False


@pytest.mark.asyncio
async def test_is_full_once_pending_tasks_reach_max_pending() -> None:
    scheduler = RetryScheduler(max_pending=2)

    scheduler.schedule(3600, _never)
    assert scheduler.is_full is False

    scheduler.schedule(3600, _never)
    assert scheduler.is_full is True

    scheduler.cancel_all()
    await scheduler.drain(timeout=1.0)
    assert scheduler.is_full is False


@pytest.mark.asyncio
async def test_is_full_clears_when_a_pending_task_completes() -> None:
    scheduler = RetryScheduler(max_pending=1)

    scheduler.schedule(0.01, _never_fast)
    assert scheduler.is_full is True

    await scheduler.drain(timeout=1.0)
    assert scheduler.is_full is False


async def _never_fast() -> None:
    return None


@pytest.mark.parametrize("bad", [0, -1])
def test_rejects_non_positive_max_pending(bad: int) -> None:
    with pytest.raises(ValueError, match="max_pending"):
        RetryScheduler(max_pending=bad)
