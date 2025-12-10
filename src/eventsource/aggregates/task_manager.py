"""
Background task manager for async operations.

This module provides the BackgroundTaskManager class for managing
fire-and-forget asyncio tasks with proper lifecycle tracking.

This extraction addresses the Single Responsibility Principle by
separating background task management from repository logic.

Example:
    >>> from eventsource.aggregates.task_manager import BackgroundTaskManager
    >>>
    >>> manager = BackgroundTaskManager()
    >>> manager.submit(create_snapshot_coro())
    >>> # Later...
    >>> count = await manager.await_all()
"""

import asyncio
import contextlib
import logging
from collections.abc import Coroutine
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
    - Task tracking with automatic cleanup
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
        """Initialize the task manager with empty task list."""
        self._tasks: list[asyncio.Task[Any]] = []

    def submit(self, coro: Coroutine[Any, Any, Any]) -> asyncio.Task[Any]:
        """
        Submit a coroutine as a background task.

        The task is tracked and can be awaited later. Completed tasks
        are automatically cleaned up on subsequent operations.

        Args:
            coro: The coroutine to run in the background

        Returns:
            The created asyncio.Task
        """
        task = asyncio.create_task(coro)
        self._tasks.append(task)

        # Add done callback for logging
        task.add_done_callback(self._on_task_done)

        # Cleanup completed tasks to prevent memory growth
        self._cleanup_completed()

        return task

    def _on_task_done(self, task: asyncio.Task[Any]) -> None:
        """
        Callback when a task completes.

        Logs any exceptions that occurred in the task.
        """
        if not task.cancelled():
            exc = task.exception()
            if exc:
                logger.error(
                    "Background task failed: %s",
                    exc,
                    exc_info=exc,
                )

    def _cleanup_completed(self) -> None:
        """Remove completed tasks from the tracking list."""
        self._tasks = [task for task in self._tasks if not task.done()]

    @property
    def pending_count(self) -> int:
        """
        Get the number of pending (not yet completed) tasks.

        Returns:
            Number of tasks still running
        """
        self._cleanup_completed()
        return len(self._tasks)

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
        if not self._tasks:
            return 0

        pending = [task for task in self._tasks if not task.done()]
        if not pending:
            self._tasks.clear()
            return 0

        count = len(pending)

        if timeout is not None:
            # Wait with timeout
            done, remaining = await asyncio.wait(
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

        self._tasks.clear()
        return count

    def cancel_all(self) -> int:
        """
        Cancel all pending tasks.

        Returns:
            Number of tasks that were cancelled
        """
        pending = [task for task in self._tasks if not task.done()]
        count = len(pending)

        for task in pending:
            task.cancel()

        self._tasks.clear()
        return count

    def __repr__(self) -> str:
        return f"BackgroundTaskManager(pending={self.pending_count})"


__all__ = [
    "BackgroundTaskManager",
]
