"""
Signal management for graceful shutdown.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
- TASK-0008 (Decompose Monolithic Modules SubscriptionManager and Shutdown)
"""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.shutdown_models import (
    ShutdownPhase,
    ShutdownReason,
)

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class ShutdownSignalManager:
    """
    Manages OS signal registration, signal callbacks, and programmatic triggers.
    """

    def __init__(self, timeout: float = 30.0) -> None:
        self.timeout = timeout
        self._shutdown_event = asyncio.Event()
        self._shutdown_requested = False
        self._shutdown_reason: ShutdownReason | None = None
        self._phase = ShutdownPhase.RUNNING
        self._signal_handlers_registered = False
        self._on_shutdown_callbacks: list[Callable[[], Awaitable[None]]] = []

    @property
    def shutdown_event(self) -> asyncio.Event:
        return self._shutdown_event

    @property
    def shutdown_requested(self) -> bool:
        return self._shutdown_requested

    @shutdown_requested.setter
    def shutdown_requested(self, value: bool) -> None:
        self._shutdown_requested = value

    @property
    def shutdown_reason(self) -> ShutdownReason | None:
        return self._shutdown_reason

    @shutdown_reason.setter
    def shutdown_reason(self, value: ShutdownReason | None) -> None:
        self._shutdown_reason = value

    @property
    def phase(self) -> ShutdownPhase:
        return self._phase

    @phase.setter
    def phase(self, value: ShutdownPhase) -> None:
        self._phase = value

    @property
    def callbacks(self) -> list[Callable[[], Awaitable[None]]]:
        return self._on_shutdown_callbacks

    def register_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Register signal handlers for graceful shutdown (SIGTERM and SIGINT)."""
        if self._signal_handlers_registered:
            logger.warning("Signal handlers already registered")
            return

        loop = loop or asyncio.get_running_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(
                    sig,
                    lambda s=sig: asyncio.create_task(self._handle_signal(s)),  # type: ignore[misc]
                )
                logger.debug("Registered signal handler", extra={"signal": sig.name})
            except NotImplementedError:
                logger.warning(
                    "Signal handling not fully supported on this platform",
                    extra={"signal": sig.name},
                )

        self._signal_handlers_registered = True
        logger.info("Shutdown signal handlers registered")

    def unregister_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        """Unregister signal handlers."""
        if not self._signal_handlers_registered:
            return

        loop = loop or asyncio.get_running_loop()

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.remove_signal_handler(sig)
                logger.debug("Removed signal handler", extra={"signal": sig.name})
            except (NotImplementedError, ValueError):
                pass

        self._signal_handlers_registered = False
        logger.info("Shutdown signal handlers unregistered")

    async def _handle_signal(self, sig: signal.Signals) -> None:
        """Handle received shutdown signal."""
        if self._shutdown_requested:
            logger.warning(
                "Received second shutdown signal, forcing shutdown",
                extra={
                    "signal": sig.name,
                    "current_phase": self._phase.value,
                },
            )
            self._phase = ShutdownPhase.FORCED
            self._shutdown_reason = ShutdownReason.DOUBLE_SIGNAL
            self._shutdown_event.set()
            return

        if sig == signal.SIGTERM:
            self._shutdown_reason = ShutdownReason.SIGNAL_SIGTERM
        elif sig == signal.SIGINT:
            self._shutdown_reason = ShutdownReason.SIGNAL_SIGINT

        logger.info(
            "Received shutdown signal, initiating graceful shutdown",
            extra={
                "signal": sig.name,
                "timeout": self.timeout,
                "reason": self._shutdown_reason.value if self._shutdown_reason else None,
            },
        )
        self._shutdown_requested = True
        self._shutdown_event.set()

        for callback in self._on_shutdown_callbacks:
            try:
                await callback()
            except Exception as e:
                logger.error(
                    "Shutdown callback error",
                    extra={"error": str(e)},
                    exc_info=True,
                )

    def on_shutdown(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Register a callback to be invoked on shutdown signal."""
        self._on_shutdown_callbacks.append(callback)

    def remove_callback(self, callback: Callable[[], Awaitable[None]]) -> bool:
        """Remove a registered shutdown callback."""
        try:
            self._on_shutdown_callbacks.remove(callback)
            return True
        except ValueError:
            return False

    async def wait_for_shutdown(self) -> None:
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()

    def request_shutdown(self, reason: ShutdownReason = ShutdownReason.PROGRAMMATIC) -> None:
        """Programmatically request shutdown."""
        if not self._shutdown_requested:
            self._shutdown_reason = reason
            logger.info("Programmatic shutdown requested", extra={"reason": reason.value})
            self._shutdown_requested = True
            self._shutdown_event.set()

    def reset(self) -> None:
        """Reset signal manager state."""
        self._shutdown_event.clear()
        self._shutdown_requested = False
        self._shutdown_reason = None
        self._phase = ShutdownPhase.RUNNING


__all__ = [
    "ShutdownSignalManager",
]
