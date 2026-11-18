"""Non-blocking retry scheduler for event bus consumer adapters.

Provides asynchronous timer-based retry scheduling that does not block
the consumer loop, allowing concurrent messages across partitions and queues
to continue processing while failed messages wait out their backoff period.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

from eventsource.observability.error_summary import error_summary

logger = logging.getLogger(__name__)


class RetryScheduler:
    """Schedules retry operations using non-blocking async timers.

    Instead of sleeping synchronously within the consumer loop, retries
    are scheduled as background tasks with async sleep timers. Tasks are
    tracked to enable graceful draining and shutdown.

    The number of pending tasks is bounded by ``max_pending``. Callers check
    :attr:`is_full` before scheduling and route work elsewhere (for example,
    a dead letter queue) when it is set. ``schedule`` itself does not refuse.
    """

    def __init__(
        self, *, custom_logger: logging.Logger | None = None, max_pending: int = 1000
    ) -> None:
        if max_pending < 1:
            raise ValueError("max_pending must be at least 1")
        self._logger = custom_logger or logger
        self._max_pending = max_pending
        self._tasks: set[asyncio.Task[None]] = set()

    @property
    def active_count(self) -> int:
        """Return the number of currently pending retry tasks."""
        return len(self._tasks)

    @property
    def is_full(self) -> bool:
        """Return True when the pending task count has reached ``max_pending``."""
        return len(self._tasks) >= self._max_pending

    @property
    def tasks(self) -> frozenset[asyncio.Task[None]]:
        """Return a snapshot of active retry tasks."""
        return frozenset(self._tasks)

    def schedule(
        self,
        delay: float,
        action: Callable[[], Awaitable[None]],
        *,
        name: str | None = None,
    ) -> asyncio.Task[None]:
        """Schedule an async action to run after a non-blocking delay.

        Args:
            delay: Delay in seconds before executing action. If <= 0,
                   executes immediately in a task.
            action: Async callable to execute after the delay.
            name: Optional name for the background task.

        Returns:
            The created asyncio.Task.
        """

        async def _runner() -> None:
            try:
                if delay > 0:
                    await asyncio.sleep(delay)
                await action()
            except asyncio.CancelledError:
                self._logger.debug("Retry task cancelled: %s", name)
                raise
            except Exception as e:
                self._logger.error(
                    "Error executing retry action in %s: %s",
                    name or "unnamed-task",
                    error_summary(e),
                )

        task = asyncio.create_task(_runner(), name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def drain(self, timeout: float | None = None) -> None:
        """Wait for all pending retry tasks to complete.

        Args:
            timeout: Maximum seconds to wait. If None, waits indefinitely.
        """
        if not self._tasks:
            return

        tasks_to_wait = set(self._tasks)
        try:
            if timeout is not None:
                _, pending = await asyncio.wait(tasks_to_wait, timeout=timeout)
                if pending:
                    self._logger.warning(
                        "RetryScheduler drain timed out with %d tasks remaining",
                        len(pending),
                    )
            else:
                await asyncio.gather(*tasks_to_wait, return_exceptions=True)
        except Exception as e:
            self._logger.warning("Error draining retry tasks: %s", e)

    def cancel_all(self) -> None:
        """Cancel all pending retry tasks."""
        for task in self._tasks:
            if not task.done():
                task.cancel()


__all__ = ["RetryScheduler"]
