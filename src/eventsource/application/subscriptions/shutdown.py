"""Graceful shutdown coordinator for subscriptions (ADR-0002, ADR-0007, TASK-0008)."""

from __future__ import annotations

import asyncio
import logging
import signal
from collections.abc import Awaitable, Callable
from datetime import datetime

from eventsource.application.subscriptions.shutdown_deadlines import ShutdownDeadlineManager
from eventsource.application.subscriptions.shutdown_hooks import (
    PostShutdownHook,
    PreShutdownHook,
    ShutdownHookManager,
)
from eventsource.application.subscriptions.shutdown_metrics import (
    OTEL_METRICS_AVAILABLE,
    ShutdownMetricsSnapshot,
    get_in_flight_at_shutdown,
    record_drain_duration,
    record_events_drained,
    record_in_flight_at_shutdown,
    record_shutdown_completed,
    record_shutdown_initiated,
    reset_shutdown_metrics,
)
from eventsource.application.subscriptions.shutdown_models import (
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
)
from eventsource.application.subscriptions.shutdown_periodic import PeriodicCheckpointManager
from eventsource.application.subscriptions.shutdown_runner import (
    create_shutdown_result,
    execute_shutdown_sequence,
)
from eventsource.application.subscriptions.shutdown_signals import ShutdownSignalManager

logger = logging.getLogger(__name__)


class ShutdownCoordinator:
    """
    Coordinates Kubernetes-friendly graceful shutdown of subscriptions:
    traps SIGTERM/SIGINT, stops accepting new events, drains in-flight events,
    saves final checkpoints, and closes connections cleanly.
    """

    def __init__(
        self,
        timeout: float = 30.0,
        drain_timeout: float = 10.0,
        checkpoint_timeout: float = 5.0,
        checkpoint_interval: float = 5.0,
    ) -> None:
        self.timeout = timeout
        self.drain_timeout = drain_timeout
        self.checkpoint_timeout = checkpoint_timeout
        self.checkpoint_interval = checkpoint_interval

        self._signal_manager = ShutdownSignalManager(timeout=timeout)
        self._hook_manager = ShutdownHookManager()
        self._deadline_manager = ShutdownDeadlineManager(timeout=timeout)
        self._periodic_manager = PeriodicCheckpointManager(checkpoint_interval=checkpoint_interval)

        self._last_metrics_snapshot: ShutdownMetricsSnapshot | None = None
        self._in_flight_at_start: int = 0
        self._drain_start_time: float = 0.0
        self._drain_duration_seconds: float = 0.0

    @property
    def _phase(self) -> ShutdownPhase:
        return self._signal_manager.phase

    @_phase.setter
    def _phase(self, value: ShutdownPhase) -> None:
        self._signal_manager.phase = value

    @property
    def _shutdown_event(self) -> asyncio.Event:
        return self._signal_manager.shutdown_event

    @property
    def _shutdown_requested(self) -> bool:
        return self._signal_manager.shutdown_requested

    @_shutdown_requested.setter
    def _shutdown_requested(self, value: bool) -> None:
        self._signal_manager.shutdown_requested = value

    @property
    def _shutdown_reason(self) -> ShutdownReason | None:
        return self._signal_manager.shutdown_reason

    @_shutdown_reason.setter
    def _shutdown_reason(self, value: ShutdownReason | None) -> None:
        self._signal_manager.shutdown_reason = value

    @property
    def _signal_handlers_registered(self) -> bool:
        return self._signal_manager._signal_handlers_registered

    @_signal_handlers_registered.setter
    def _signal_handlers_registered(self, value: bool) -> None:
        self._signal_manager._signal_handlers_registered = value

    @property
    def _on_shutdown_callbacks(self) -> list[Callable[[], Awaitable[None]]]:
        return self._signal_manager.callbacks

    @property
    def _pre_shutdown_hooks(self) -> list[tuple[PreShutdownHook, float]]:
        return self._hook_manager._pre_shutdown_hooks

    @property
    def _post_shutdown_hooks(self) -> list[PostShutdownHook]:
        return self._hook_manager._post_shutdown_hooks

    @property
    def _periodic_checkpoint_task(self) -> asyncio.Task[None] | None:
        return self._periodic_manager._periodic_checkpoint_task

    @_periodic_checkpoint_task.setter
    def _periodic_checkpoint_task(self, task: asyncio.Task[None] | None) -> None:
        self._periodic_manager._periodic_checkpoint_task = task

    @property
    def _periodic_checkpoints_saved(self) -> int:
        return self._periodic_manager.periodic_checkpoints_saved

    @_periodic_checkpoints_saved.setter
    def _periodic_checkpoints_saved(self, count: int) -> None:
        self._periodic_manager._periodic_checkpoints_saved = count

    @property
    def _shutdown_deadline(self) -> datetime | None:
        return self._deadline_manager.deadline

    @_shutdown_deadline.setter
    def _shutdown_deadline(self, deadline: datetime | None) -> None:
        self._deadline_manager._shutdown_deadline = deadline

    def register_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._signal_manager.register_signals(loop)

    def unregister_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._signal_manager.unregister_signals(loop)

    async def _handle_signal(self, sig: signal.Signals) -> None:
        await self._signal_manager._handle_signal(sig)

    def on_shutdown(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._signal_manager.on_shutdown(callback)

    def remove_callback(self, callback: Callable[[], Awaitable[None]]) -> bool:
        return self._signal_manager.remove_callback(callback)

    def on_pre_shutdown(self, callback: PreShutdownHook, timeout: float = 5.0) -> None:
        self._hook_manager.on_pre_shutdown(callback, timeout)

    async def _execute_pre_shutdown_hooks(self) -> None:
        await self._hook_manager.execute_pre_shutdown_hooks()

    def on_post_shutdown(self, callback: PostShutdownHook) -> None:
        self._hook_manager.on_post_shutdown(callback)

    async def _execute_post_shutdown_hooks(self, result: ShutdownResult) -> None:
        await self._hook_manager.execute_post_shutdown_hooks(result)

    async def wait_for_shutdown(self) -> None:
        await self._signal_manager.wait_for_shutdown()

    def request_shutdown(self, reason: ShutdownReason = ShutdownReason.PROGRAMMATIC) -> None:
        self._signal_manager.request_shutdown(reason)

    def _start_periodic_checkpoints(self, checkpoint_func: Callable[[], Awaitable[int]]) -> None:
        self._periodic_manager.start(checkpoint_func, lambda: self._phase == ShutdownPhase.DRAINING)

    async def _stop_periodic_checkpoints(self) -> None:
        await self._periodic_manager.stop()

    def set_shutdown_deadline(self, deadline: datetime) -> None:
        self._deadline_manager.set_shutdown_deadline(deadline)

    def get_remaining_shutdown_time(self) -> float:
        return self._deadline_manager.get_remaining_shutdown_time()

    @property
    def deadline(self) -> datetime | None:
        return self._deadline_manager.deadline

    def _get_adjusted_timeouts(self) -> tuple[float, float, float]:
        return self._deadline_manager.get_adjusted_timeouts(
            self.drain_timeout, self.checkpoint_timeout
        )

    def set_in_flight_count(self, count: int) -> None:
        self._in_flight_at_start = count
        try:
            record_in_flight_at_shutdown(count)
        except Exception as e:
            logger.debug("Failed to record in-flight count metric", extra={"error": str(e)})

    @property
    def is_shutting_down(self) -> bool:
        return self._signal_manager.shutdown_requested

    @property
    def phase(self) -> ShutdownPhase:
        return self._phase

    @property
    def is_forced(self) -> bool:
        return self._phase == ShutdownPhase.FORCED

    @property
    def last_metrics_snapshot(self) -> ShutdownMetricsSnapshot | None:
        return self._last_metrics_snapshot

    def _create_result(
        self,
        start_time: datetime,
        subscriptions_stopped: int,
        events_drained: int,
        checkpoints_saved: int,
        forced: bool,
        error_msg: str | None,
    ) -> ShutdownResult:
        return create_shutdown_result(
            self,
            start_time,
            subscriptions_stopped,
            events_drained,
            checkpoints_saved,
            forced,
            error_msg,
        )

    async def shutdown(
        self,
        stop_func: Callable[[], Awaitable[None]],
        drain_func: Callable[[], Awaitable[int]] | None = None,
        checkpoint_func: Callable[[], Awaitable[int]] | None = None,
        close_func: Callable[[], Awaitable[None]] | None = None,
    ) -> ShutdownResult:
        """Execute graceful shutdown sequence."""
        return await execute_shutdown_sequence(
            self, stop_func, drain_func, checkpoint_func, close_func
        )

    def reset(self) -> None:
        """Reset the coordinator for reuse."""
        self._signal_manager.reset()
        self._signal_manager.callbacks.clear()
        self._hook_manager.clear()
        self._periodic_manager.reset()
        self._deadline_manager.reset()
        self._last_metrics_snapshot = None
        self._in_flight_at_start = 0
        self._drain_start_time = 0.0
        self._drain_duration_seconds = 0.0
        logger.debug("Shutdown coordinator reset")


__all__ = [
    "ShutdownPhase",
    "ShutdownReason",
    "ShutdownResult",
    "ShutdownMetricsSnapshot",
    "ShutdownCoordinator",
    "PreShutdownHook",
    "PostShutdownHook",
    "OTEL_METRICS_AVAILABLE",
    "record_shutdown_initiated",
    "record_shutdown_completed",
    "record_drain_duration",
    "record_events_drained",
    "record_in_flight_at_shutdown",
    "get_in_flight_at_shutdown",
    "reset_shutdown_metrics",
]
