"""Background task manager for fire-and-forget asyncio work."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import Any

logger = logging.getLogger(__name__)


class BackgroundTaskManager:
    """Manager for fire-and-forget background asyncio tasks with done-callback cleanup."""

    def __init__(self, max_pending: int | None = None) -> None:
        """Initialize the task manager with an optional max_pending capacity ceiling."""
        self.tasks: set[asyncio.Task[Any]] = set()
        self.max_pending = max_pending

    @property
    def at_capacity(self) -> bool:
        """Whether the in-flight set has reached `max_pending` (False when None)."""
        return self.max_pending is not None and len(self.tasks) >= self.max_pending

    def submit(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        on_done: Callable[[asyncio.Task[Any]], None] | None = None,
    ) -> asyncio.Task[Any]:
        """Submit a coroutine as a tracked background task."""
        task = asyncio.create_task(coro)
        self.tasks.add(task)
        task.add_done_callback(lambda t: self._on_task_done(t, on_done))
        return task

    def _on_task_done(
        self,
        task: asyncio.Task[Any],
        on_done: Callable[[asyncio.Task[Any]], None] | None,
    ) -> None:
        """Discard a finished task and either delegate or log its failure."""
        self.tasks.discard(task)
        if on_done is not None:
            on_done(task)
            return
        if not task.cancelled():
            exc = task.exception()
            if exc:
                logger.error("Background task failed: %s", exc, exc_info=exc)

    @property
    def pending_count(self) -> int:
        """Get the number of pending (not yet completed) tasks."""
        return len(self.tasks)

    @property
    def has_pending(self) -> bool:
        """Check if there are any pending tasks."""
        return self.pending_count > 0

    async def await_all(self, timeout: float | None = None) -> int:
        """Wait for all pending tasks to complete, cancelling any that exceed timeout."""
        pending = {task for task in self.tasks if not task.done()}
        if not pending:
            return 0

        count = len(pending)

        if timeout is not None:
            _done, remaining = await asyncio.wait(
                pending,
                timeout=timeout,
                return_when=asyncio.ALL_COMPLETED,
            )

            if remaining:
                logger.warning(
                    "Background task manager: %d task(s) did not complete within timeout",
                    len(remaining),
                    extra={"remaining_tasks": len(remaining), "timeout": timeout},
                )
                for task in remaining:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        else:
            results = await asyncio.gather(*pending, return_exceptions=True)
            for result in results:
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    logger.error("Background task failed: %s", result, exc_info=result)

        return count

    def cancel_all(self) -> int:
        """Cancel all pending tasks and return the number cancelled."""
        pending = [task for task in self.tasks if not task.done()]
        for task in pending:
            task.cancel()
        return len(pending)

    def __repr__(self) -> str:
        return f"BackgroundTaskManager(pending={self.pending_count})"


__all__ = ["BackgroundTaskManager"]
