"""Graceful shutdown coordinator for subscriptions (ADR-0002, ADR-0007, TASK-0008)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

from eventsource.application.subscriptions.shutdown_deadlines import ShutdownDeadlineManager
from eventsource.application.subscriptions.shutdown_hooks import (  # fmt: skip
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
from eventsource.application.subscriptions.shutdown_models import (  # fmt: skip
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
)
from eventsource.application.subscriptions.shutdown_periodic import PeriodicCheckpointManager
from eventsource.application.subscriptions.shutdown_runner import (  # fmt: skip
    create_shutdown_result,
    execute_shutdown_sequence,
)
from eventsource.application.subscriptions.shutdown_signals import ShutdownSignalManager

logger = logging.getLogger(__name__)


def _delegate(m: str, a: str) -> Any:
    return property(lambda s: getattr(getattr(s, m), a), lambda s, v: setattr(getattr(s, m), a, v))


class ShutdownCoordinator:
    """Coordinates Kubernetes-friendly graceful shutdown of subscriptions."""

    def __init__(
        self, timeout: float = 30.0, drain_timeout: float = 10.0,
        checkpoint_timeout: float = 5.0, checkpoint_interval: float = 5.0,
    ) -> None:  # fmt: skip
        self.timeout, self.drain_timeout = timeout, drain_timeout
        self.checkpoint_timeout, self.checkpoint_interval = checkpoint_timeout, checkpoint_interval
        self._signal_manager = ShutdownSignalManager(timeout=timeout)
        self._hook_manager = ShutdownHookManager()
        self._deadline_manager = ShutdownDeadlineManager(timeout=timeout)
        self._periodic_manager = PeriodicCheckpointManager(checkpoint_interval=checkpoint_interval)
        self._last_metrics_snapshot: ShutdownMetricsSnapshot | None = None
        self._in_flight_at_start, self._drain_start_time, self._drain_duration_seconds = 0, 0.0, 0.0

    _phase = _delegate("_signal_manager", "phase")
    _shutdown_event = _delegate("_signal_manager", "shutdown_event")
    _shutdown_requested = _delegate("_signal_manager", "shutdown_requested")
    _shutdown_reason = _delegate("_signal_manager", "shutdown_reason")
    _signal_handlers_registered = _delegate("_signal_manager", "_signal_handlers_registered")
    _on_shutdown_callbacks = _delegate("_signal_manager", "callbacks")
    _pre_shutdown_hooks = _delegate("_hook_manager", "_pre_shutdown_hooks")
    _post_shutdown_hooks = _delegate("_hook_manager", "_post_shutdown_hooks")
    _periodic_checkpoint_task = _delegate("_periodic_manager", "_periodic_checkpoint_task")
    _periodic_checkpoints_saved = _delegate("_periodic_manager", "_periodic_checkpoints_saved")
    _shutdown_deadline = _delegate("_deadline_manager", "_shutdown_deadline")
    _handle_signal = _delegate("_signal_manager", "_handle_signal")
    _execute_pre_shutdown_hooks = _delegate("_hook_manager", "execute_pre_shutdown_hooks")
    _execute_post_shutdown_hooks = _delegate("_hook_manager", "execute_post_shutdown_hooks")
    _stop_periodic_checkpoints = _delegate("_periodic_manager", "stop")
    is_shutting_down: bool = _delegate("_signal_manager", "shutdown_requested")
    phase: ShutdownPhase = _delegate("_signal_manager", "phase")
    deadline: datetime | None = _delegate("_deadline_manager", "deadline")
    last_metrics_snapshot: Any = property(lambda self: self._last_metrics_snapshot)
    is_forced: Any = property(lambda self: bool(self._phase == ShutdownPhase.FORCED))

    def register_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._signal_manager.register_signals(loop)

    def unregister_signals(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self._signal_manager.unregister_signals(loop)

    def on_shutdown(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._signal_manager.on_shutdown(callback)

    def remove_callback(self, callback: Callable[[], Awaitable[None]]) -> bool:
        return self._signal_manager.remove_callback(callback)

    def on_pre_shutdown(self, callback: PreShutdownHook, timeout: float = 5.0) -> None:
        self._hook_manager.on_pre_shutdown(callback, timeout)

    def on_post_shutdown(self, callback: PostShutdownHook) -> None:
        self._hook_manager.on_post_shutdown(callback)

    async def wait_for_shutdown(self) -> None:
        await self._signal_manager.wait_for_shutdown()

    def request_shutdown(self, reason: ShutdownReason = ShutdownReason.PROGRAMMATIC) -> None:
        self._signal_manager.request_shutdown(reason)

    def _start_periodic_checkpoints(self, checkpoint_func: Callable[[], Awaitable[int]]) -> None:
        self._periodic_manager.start(checkpoint_func, lambda: self._phase == ShutdownPhase.DRAINING)

    def set_shutdown_deadline(self, deadline: datetime) -> None:
        self._deadline_manager.set_shutdown_deadline(deadline)

    def get_remaining_shutdown_time(self) -> float:
        return self._deadline_manager.get_remaining_shutdown_time()

    def _get_adjusted_timeouts(self) -> tuple[float, float, float]:
        return self._deadline_manager.get_adjusted_timeouts(self.drain_timeout, self.checkpoint_timeout)  # fmt: skip

    def set_in_flight_count(self, count: int) -> None:
        self._in_flight_at_start = count
        try:
            record_in_flight_at_shutdown(count)
        except Exception as e:
            logger.debug("Failed to record in-flight count metric", extra={"error": str(e)})

    def _create_result(self, start_time: datetime, *args: Any) -> ShutdownResult:
        return create_shutdown_result(self, start_time, *args)

    async def shutdown(
        self, stop_func: Callable[[], Awaitable[None]], drain_func: Callable[[], Awaitable[int]] | None = None,
        checkpoint_func: Callable[[], Awaitable[int]] | None = None, close_func: Callable[[], Awaitable[None]] | None = None,
    ) -> ShutdownResult:  # fmt: skip
        return await execute_shutdown_sequence(self, stop_func, drain_func, checkpoint_func, close_func)  # fmt: skip

    def reset(self) -> None:
        for mgr in (self._signal_manager, self._periodic_manager, self._deadline_manager):
            mgr.reset()
        self._signal_manager.callbacks.clear()
        self._hook_manager.clear()
        self._last_metrics_snapshot, self._in_flight_at_start = None, 0
        self._drain_start_time = self._drain_duration_seconds = 0.0
        logger.debug("Shutdown coordinator reset")


__all__ = [
    "OTEL_METRICS_AVAILABLE", "PostShutdownHook", "PreShutdownHook", "ShutdownCoordinator",
    "ShutdownMetricsSnapshot", "ShutdownPhase", "ShutdownReason", "ShutdownResult",
    "get_in_flight_at_shutdown", "record_drain_duration", "record_events_drained",
    "record_in_flight_at_shutdown", "record_shutdown_completed", "record_shutdown_initiated",
    "reset_shutdown_metrics",
]  # fmt: skip
