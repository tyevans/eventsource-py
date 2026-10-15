"""Execution runner for graceful shutdown sequence."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.shutdown_metrics import (
    ShutdownMetricsSnapshot,
    record_drain_duration,
    record_events_drained,
    record_shutdown_completed,
    record_shutdown_initiated,
)
from eventsource.application.subscriptions.shutdown_models import (
    ShutdownPhase,
    ShutdownReason,
    ShutdownResult,
)

if TYPE_CHECKING:
    from eventsource.application.subscriptions.shutdown import ShutdownCoordinator

logger = logging.getLogger(__name__)


def create_shutdown_result(
    coordinator: ShutdownCoordinator,
    start_time: datetime,
    subscriptions_stopped: int,
    events_drained: int,
    checkpoints_saved: int,
    forced: bool,
    error_msg: str | None,
) -> ShutdownResult:
    """Create a ShutdownResult and update metrics snapshot on coordinator."""
    duration = (datetime.now(UTC) - start_time).total_seconds()
    outcome = "forced" if (forced and error_msg and "double signal" in error_msg.lower()) else ("timeout" if forced else "clean")  # fmt: skip
    reason = ShutdownReason.TIMEOUT if (forced and coordinator._shutdown_reason != ShutdownReason.DOUBLE_SIGNAL) else coordinator._shutdown_reason  # fmt: skip
    in_flight = coordinator._in_flight_at_start
    not_drained = max(0, in_flight - events_drained)
    try:
        record_shutdown_completed(outcome, duration)
    except Exception as e:
        logger.debug("Failed to record shutdown completed metric", extra={"error": str(e)})
    coordinator._last_metrics_snapshot = ShutdownMetricsSnapshot(duration, coordinator._drain_duration_seconds, events_drained, checkpoints_saved, in_flight, outcome)  # fmt: skip
    logger.info(
        "Shutdown complete",
        extra={
            "phase": coordinator._phase.value,
            "duration_seconds": duration,
            "subscriptions_stopped": subscriptions_stopped,
            "events_drained": events_drained,
            "checkpoints_saved": checkpoints_saved,
            "forced": forced,
            "error": error_msg,
            "outcome": outcome,
            "reason": reason.value if reason else None,
            "in_flight_at_start": in_flight,
            "events_not_drained": not_drained,
        },
    )
    return ShutdownResult(coordinator._phase, duration, subscriptions_stopped, events_drained, checkpoints_saved, forced, error_msg, reason, in_flight, not_drained)  # fmt: skip


async def execute_shutdown_sequence(
    coordinator: ShutdownCoordinator,
    stop_func: Callable[[], Awaitable[None]],
    drain_func: Callable[[], Awaitable[int]] | None = None,
    checkpoint_func: Callable[[], Awaitable[int]] | None = None,
    close_func: Callable[[], Awaitable[None]] | None = None,
) -> ShutdownResult:
    """Execute graceful shutdown sequence across coordinator phases."""
    start_time, drained, saved, stopped, forced, error_msg = datetime.now(UTC), 0, 0, 0, False, None
    coordinator._drain_duration_seconds = coordinator._drain_start_time = 0.0
    stop_timeout, drain_timeout, checkpoint_timeout = coordinator._get_adjusted_timeouts()
    if coordinator._shutdown_deadline is not None:
        logger.info("Using deadline-adjusted timeouts", extra={"stop_timeout": stop_timeout, "drain_timeout": drain_timeout, "checkpoint_timeout": checkpoint_timeout, "deadline": coordinator._shutdown_deadline.isoformat()})  # fmt: skip
    try:
        record_shutdown_initiated()
    except Exception as e:
        logger.debug("Failed to record shutdown initiated metric", extra={"error": str(e)})

    try:
        await coordinator._execute_pre_shutdown_hooks()
        coordinator._phase = ShutdownPhase.STOPPING
        logger.info("Shutdown phase: stopping", extra={"phase": coordinator._phase.value, "timeout": stop_timeout})  # fmt: skip
        try:
            await asyncio.wait_for(stop_func(), timeout=stop_timeout)
            stopped = 1
        except TimeoutError:
            logger.warning("Stop phase timed out", extra={"timeout": stop_timeout})
            forced = True
        if coordinator._phase == ShutdownPhase.FORCED:
            return coordinator._create_result(start_time, stopped, drained, saved, True, "Forced by double signal")  # fmt: skip

        if drain_func and not forced and drain_timeout > 0:
            coordinator._phase = ShutdownPhase.DRAINING
            drain_start = coordinator._drain_start_time = time.perf_counter()
            logger.info("Shutdown phase: draining in-flight events", extra={"phase": coordinator._phase.value, "timeout": drain_timeout, "checkpoint_interval": coordinator.checkpoint_interval})  # fmt: skip
            if checkpoint_func and coordinator.checkpoint_interval > 0:
                coordinator._start_periodic_checkpoints(checkpoint_func)
            try:
                drained = await asyncio.wait_for(drain_func(), timeout=drain_timeout)
                logger.info("Events drained successfully", extra={"events_drained": drained, "periodic_checkpoints_saved": coordinator._periodic_checkpoints_saved})  # fmt: skip
            except TimeoutError:
                logger.warning("Drain phase timed out", extra={"timeout": drain_timeout})
                forced = True
            finally:
                await coordinator._stop_periodic_checkpoints()
                coordinator._drain_duration_seconds = time.perf_counter() - drain_start
                try:
                    record_drain_duration(coordinator._drain_duration_seconds)
                    record_events_drained(drained)
                except Exception as e:
                    logger.debug("Failed to record drain metrics", extra={"error": str(e)})
        elif drain_func and drain_timeout == 0:
            logger.info("Skipping drain phase due to time constraint")
        if coordinator._phase == ShutdownPhase.FORCED:
            return coordinator._create_result(start_time, stopped, drained, saved, True, "Forced by double signal")  # fmt: skip

        if checkpoint_func and checkpoint_timeout > 0:
            coordinator._phase = ShutdownPhase.CHECKPOINTING
            logger.info("Shutdown phase: saving checkpoints", extra={"phase": coordinator._phase.value, "timeout": checkpoint_timeout})  # fmt: skip
            try:
                saved = await asyncio.wait_for(checkpoint_func(), timeout=checkpoint_timeout)
                logger.info("Checkpoints saved successfully", extra={"checkpoints_saved": saved})
            except TimeoutError:
                logger.warning("Checkpoint save timed out", extra={"timeout": checkpoint_timeout})
                forced = True

        if close_func:
            try:
                await asyncio.wait_for(close_func(), timeout=5.0)
                logger.debug("Connections closed")
            except TimeoutError:
                logger.warning("Connection close timed out")
            except Exception as e:
                logger.error("Error closing connections", extra={"error": str(e)})
        coordinator._phase = ShutdownPhase.FORCED if forced else ShutdownPhase.STOPPED
    except Exception as e:
        logger.error("Shutdown error", extra={"error": str(e)}, exc_info=True)
        coordinator._phase, forced, error_msg = ShutdownPhase.FORCED, True, str(e)

    result = coordinator._create_result(start_time, stopped, drained, saved, forced, error_msg)
    try:
        await coordinator._execute_post_shutdown_hooks(result)
    except Exception as e:
        logger.error("Error executing post-shutdown hooks", extra={"error": str(e)}, exc_info=True)
    return result
