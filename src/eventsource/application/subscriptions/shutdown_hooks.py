"""
Hook management for pre-shutdown and post-shutdown lifecycle events.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
- TASK-0008 (Decompose Monolithic Modules SubscriptionManager and Shutdown)
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.application.subscriptions.shutdown_models import ShutdownResult

logger = logging.getLogger(__name__)

# Type aliases for hooks
PreShutdownHook = Callable[[], Awaitable[None]]
PostShutdownHook = Callable[["ShutdownResult"], Awaitable[None]]


class ShutdownHookManager:
    """
    Manages pre-shutdown and post-shutdown asynchronous hooks.
    """

    def __init__(self) -> None:
        self._pre_shutdown_hooks: list[tuple[PreShutdownHook, float]] = []
        self._post_shutdown_hooks: list[PostShutdownHook] = []

    def on_pre_shutdown(
        self,
        callback: PreShutdownHook,
        timeout: float = 5.0,
    ) -> None:
        """
        Register callback to execute before shutdown begins.

        Args:
            callback: Async function to call before shutdown.
            timeout: Maximum seconds to wait for this hook (default 5.0).
        """
        if not asyncio.iscoroutinefunction(callback):
            raise TypeError(f"Pre-shutdown callback must be async function, got {type(callback)}")

        if timeout <= 0:
            raise ValueError(f"Timeout must be positive, got {timeout}")

        self._pre_shutdown_hooks.append((callback, timeout))

        logger.debug(
            "Registered pre-shutdown hook",
            extra={
                "callback": getattr(callback, "__name__", str(callback)),
                "timeout": timeout,
                "total_hooks": len(self._pre_shutdown_hooks),
            },
        )

    async def execute_pre_shutdown_hooks(self) -> None:
        """
        Execute all pre-shutdown hooks in registration order.
        """
        if not self._pre_shutdown_hooks:
            return

        logger.info(
            "Executing pre-shutdown hooks",
            extra={"hook_count": len(self._pre_shutdown_hooks)},
        )

        for callback, timeout in self._pre_shutdown_hooks:
            hook_name = getattr(callback, "__name__", str(callback))
            try:
                logger.debug(
                    "Executing pre-shutdown hook",
                    extra={"hook": hook_name, "timeout": timeout},
                )
                await asyncio.wait_for(callback(), timeout=timeout)
                logger.debug("Pre-shutdown hook completed", extra={"hook": hook_name})
            except TimeoutError:
                logger.warning(
                    "Pre-shutdown hook timed out",
                    extra={"hook": hook_name, "timeout": timeout},
                )
            except Exception as e:
                logger.error(
                    "Pre-shutdown hook failed",
                    extra={"hook": hook_name, "error": str(e)},
                    exc_info=True,
                )

        logger.info("Pre-shutdown hooks completed")

    def on_post_shutdown(
        self,
        callback: PostShutdownHook,
    ) -> None:
        """
        Register callback to execute after shutdown completes.

        Args:
            callback: Async function accepting ShutdownResult.
        """
        if not asyncio.iscoroutinefunction(callback):
            raise TypeError(f"Post-shutdown callback must be async function, got {type(callback)}")

        self._post_shutdown_hooks.append(callback)

        logger.debug(
            "Registered post-shutdown hook",
            extra={
                "callback": getattr(callback, "__name__", str(callback)),
                "total_hooks": len(self._post_shutdown_hooks),
            },
        )

    async def execute_post_shutdown_hooks(self, result: ShutdownResult) -> None:
        """
        Execute all post-shutdown hooks with the shutdown result.
        """
        if not self._post_shutdown_hooks:
            return

        logger.info(
            "Executing post-shutdown hooks",
            extra={
                "hook_count": len(self._post_shutdown_hooks),
                "shutdown_forced": result.forced,
            },
        )

        for callback in self._post_shutdown_hooks:
            hook_name = getattr(callback, "__name__", str(callback))
            try:
                logger.debug("Executing post-shutdown hook", extra={"hook": hook_name})
                # Fixed timeout for post-shutdown hooks
                await asyncio.wait_for(callback(result), timeout=5.0)
                logger.debug("Post-shutdown hook completed", extra={"hook": hook_name})
            except TimeoutError:
                logger.warning(
                    "Post-shutdown hook timed out",
                    extra={"hook": hook_name, "timeout": 5.0},
                )
            except Exception as e:
                logger.error(
                    "Post-shutdown hook failed",
                    extra={"hook": hook_name, "error": str(e)},
                    exc_info=True,
                )

        logger.info("Post-shutdown hooks completed")

    def clear(self) -> None:
        """Clear all registered hooks."""
        self._pre_shutdown_hooks.clear()
        self._post_shutdown_hooks.clear()
