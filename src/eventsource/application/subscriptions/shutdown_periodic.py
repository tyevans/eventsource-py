"""
Periodic checkpoint background runner for graceful shutdown.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
- TASK-0008 (Decompose Monolithic Modules SubscriptionManager and Shutdown)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


class PeriodicCheckpointManager:
    """
    Manages periodic checkpoint saves during the draining phase of shutdown.
    """

    def __init__(self, checkpoint_interval: float = 0.0) -> None:
        self.checkpoint_interval = checkpoint_interval
        self._periodic_checkpoint_task: asyncio.Task[None] | None = None
        self._periodic_checkpoints_saved = 0

    @property
    def periodic_checkpoints_saved(self) -> int:
        """Total checkpoints saved by the periodic loop."""
        return self._periodic_checkpoints_saved

    async def _periodic_checkpoint_loop(
        self,
        checkpoint_func: Callable[[], Awaitable[int]],
        is_draining: Callable[[], bool],
    ) -> None:
        """
        Background task for periodic checkpoint saves during drain phase.
        """
        logger.info(
            "Starting periodic checkpoint loop during drain",
            extra={"interval_seconds": self.checkpoint_interval},
        )

        while is_draining():
            try:
                await asyncio.sleep(self.checkpoint_interval)

                if not is_draining():
                    break

                saved = await checkpoint_func()
                self._periodic_checkpoints_saved += saved

                if saved > 0:
                    logger.debug(
                        "Periodic checkpoint save completed during drain",
                        extra={
                            "checkpoints_saved": saved,
                            "total_periodic_checkpoints": self._periodic_checkpoints_saved,
                        },
                    )
            except asyncio.CancelledError:
                logger.debug("Periodic checkpoint loop cancelled")
                break
            except Exception as e:
                logger.warning(
                    "Error in periodic checkpoint loop",
                    extra={"error": str(e)},
                )
                await asyncio.sleep(0.5)

        logger.debug(
            "Periodic checkpoint loop ended",
            extra={"total_periodic_checkpoints": self._periodic_checkpoints_saved},
        )

    def start(
        self,
        checkpoint_func: Callable[[], Awaitable[int]],
        is_draining: Callable[[], bool],
    ) -> None:
        """
        Start the periodic checkpoint background task.
        """
        if self.checkpoint_interval <= 0:
            logger.debug("Periodic checkpoints disabled (interval <= 0)")
            return

        self._periodic_checkpoints_saved = 0
        self._periodic_checkpoint_task = asyncio.create_task(
            self._periodic_checkpoint_loop(checkpoint_func, is_draining),
            name="periodic-checkpoint-loop",
        )

    async def stop(self) -> None:
        """
        Stop the periodic checkpoint background task.
        """
        if self._periodic_checkpoint_task is None:
            return

        self._periodic_checkpoint_task.cancel()
        try:
            await self._periodic_checkpoint_task
        except asyncio.CancelledError:
            pass
        finally:
            self._periodic_checkpoint_task = None

    def reset(self) -> None:
        """Reset periodic checkpoints counter and task reference."""
        self._periodic_checkpoint_task = None
        self._periodic_checkpoints_saved = 0
