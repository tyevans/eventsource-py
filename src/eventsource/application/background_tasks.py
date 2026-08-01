"""
Background task manager for fire-and-forget asyncio work.

This is an internal helper, not part of the public API. It exists because
two independent pieces of the library needed the same bookkeeping for
fire-and-forget ``asyncio.Task`` objects:

- ``eventsource.application.aggregates.snapshotting.BackgroundScheduler`` uses
  it to track in-flight background snapshot creation and to support
  ``await_pending()`` in tests.
- ``eventsource.adapters._bus.base.BaseEventBus`` uses it to track in-flight publish
  work and to support draining on shutdown.

Both call sites previously hand-rolled nearly identical task tracking; this
module is the shared implementation they now delegate to.

Example:
    >>> from eventsource.application.background_tasks import BackgroundTaskManager
    >>>
    >>> manager = BackgroundTaskManager()
    >>> manager.submit(create_snapshot_coro())
    >>> # Later...
    >>> count = await manager.await_all()
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable, Coroutine
from typing import Any

logger = logging.getLogger(__name__)


class BackgroundTaskManager:
    """
    Manager for background asyncio tasks.

    Provides a clean interface for managing fire-and-forget operations
    that shouldn't block the main execution path, such as:
    - Background snapshot creation
    - Async event publishing
    - Deferred cleanup operations

    Features:
    - Task tracking with automatic cleanup via done-callback discard
      (no unbounded growth between calls)
    - Graceful shutdown with timeout
    - Pending task count monitoring
    - Error logging for failed tasks

    Example:
        >>> manager = BackgroundTaskManager()
        >>>
        >>> # Submit background work
        >>> task = manager.submit(async_operation())
        >>>
        >>> # Check pending tasks
        >>> print(f"Pending: {manager.pending_count}")
        >>>
        >>> # Wait for all tasks (e.g., during shutdown or tests)
        >>> completed = await manager.await_all(timeout=30.0)
        >>> print(f"Completed {completed} tasks")
    """

    def __init__(self) -> None:
        """Initialize the task manager with an empty task set."""
        self.tasks: set[asyncio.Task[Any]] = set()

    def submit(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        on_done: Callable[[asyncio.Task[Any]], None] | None = None,
    ) -> asyncio.Task[Any]:
        """
        Submit a coroutine as a background task.

        The task is tracked and automatically discarded from tracking as
        soon as it completes (via a done-callback), so pending tasks never
        accumulate between calls.

        Args:
            coro: The coroutine to run in the background.
            on_done: Optional callback invoked with the finished task
                instead of this manager's default error logging. Callers
                that need failures logged under their own module's logger
                (e.g. to keep existing log output stable) can pass one.

        Returns:
            The created asyncio.Task
        """
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
                logger.error(
                    "Background task failed: %s",
                    exc,
                    exc_info=exc,
                )

    @property
    def pending_count(self) -> int:
        """
        Get the number of pending (not yet completed) tasks.

        Returns:
            Number of tasks still running
        """
        return len(self.tasks)

    @property
    def has_pending(self) -> bool:
        """
        Check if there are any pending tasks.

        Returns:
            True if any tasks are still running
        """
        return self.pending_count > 0

    async def await_all(self, timeout: float | None = None) -> int:
        """
        Wait for all pending tasks to complete.

        Args:
            timeout: Maximum time to wait in seconds.
                    If None, waits indefinitely.

        Returns:
            Number of tasks that were awaited

        Note:
            Tasks that don't complete within the timeout are cancelled.
        """
        pending = {task for task in self.tasks if not task.done()}
        if not pending:
            return 0

        count = len(pending)

        if timeout is not None:
            # Wait with timeout
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
                # Cancel remaining tasks
                for task in remaining:
                    task.cancel()
                    with contextlib.suppress(asyncio.CancelledError):
                        await task
        else:
            # Wait indefinitely, collecting exceptions
            results = await asyncio.gather(
                *pending,
                return_exceptions=True,
            )

            # Log any exceptions
            for result in results:
                if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                    logger.error(
                        "Background task failed: %s",
                        result,
                        exc_info=result,
                    )

        return count

    def cancel_all(self) -> int:
        """
        Cancel all pending tasks.

        Returns:
            Number of tasks that were cancelled
        """
        pending = [task for task in self.tasks if not task.done()]
        count = len(pending)

        for task in pending:
            task.cancel()

        return count

    def __repr__(self) -> str:
        return f"BackgroundTaskManager(pending={self.pending_count})"


__all__ = [
    "BackgroundTaskManager",
]
