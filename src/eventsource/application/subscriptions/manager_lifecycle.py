"""Lifecycle and shutdown coordination mixin for SubscriptionManager (ADR-0002, ADR-0007, TASK-0008)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Self

from .shutdown import ShutdownCoordinator, ShutdownResult
from .subscription import render_position

logger = logging.getLogger(__name__)
type _Names = list[str] | None


class ManagerLifecycleMixin:
    _shutdown_coordinator: ShutdownCoordinator
    _health_provider: Any = None
    _running = _started_at = _last_shutdown_result = _health_provider
    _registry = _lifecycle = checkpoint_repo = _tracer = _health_provider

    def _select_subscriptions(self, names: _Names) -> list[Any]:
        return [s for s in self._registry.get_all() if names is None or s.name in names]

    async def start(self, subscription_names: _Names = None, concurrent: bool = True) -> Any:
        if self._running:
            logger.warning("Subscription manager already running")
            return {}
        self._running, self._started_at = True, datetime.now(UTC)
        self._health_provider.set_started(self._started_at)
        subs = self._select_subscriptions(subscription_names)
        ex = {"subscription_count": len(subs), "concurrent": concurrent}
        logger.info("Starting subscription manager", extra=ex)
        return await self._lifecycle.start_all(subs, concurrent)

    async def stop(self, timeout: float = 30.0, subscription_names: _Names = None) -> None:
        with self._tracer.span("eventsource.subscription_manager.stop"):
            if not self._running and subscription_names is None:
                return
            if subscription_names is None:
                self._running, _ = False, self._health_provider.clear_started()
            subs = self._select_subscriptions(subscription_names)
            ex = {"subscription_count": len(subs), "timeout": timeout}
            logger.info("Stopping subscription manager", extra=ex)
            await self._lifecycle.stop_all(subs, timeout)
            logger.info("Subscription manager stopped")

    def register_signals(self) -> None:
        self._shutdown_coordinator.on_shutdown(self._on_shutdown_signal)
        self._shutdown_coordinator.register_signals()

    def unregister_signals(self) -> None:
        self._shutdown_coordinator.unregister_signals()

    async def _on_shutdown_signal(self) -> None:
        logger.info("Shutdown signal received by SubscriptionManager")

    async def run_until_shutdown(self, shutdown_timeout: float | None = None) -> ShutdownResult:
        sc = self._shutdown_coordinator
        sc.timeout = sc.timeout if shutdown_timeout is None else shutdown_timeout
        extra = {"subscription_count": len(self._registry), "shutdown_timeout": sc.timeout}
        logger.info("Starting manager in daemon mode", extra=extra)
        self.register_signals()
        try:
            await self.start()
            extra_run = {"subscriptions": self._registry.get_names()}
            logger.info("Manager running, waiting for shutdown signal", extra=extra_run)
            await sc.wait_for_shutdown()
            a, b = self._stop_accepting_events, self._drain_in_flight_events
            self._last_shutdown_result = r = await sc.shutdown(a, b, self._save_final_checkpoints)
            return r
        except Exception as e:
            logger.error("Error during run_until_shutdown", extra={"error": str(e)}, exc_info=True)
            await self.stop()
            raise
        finally:
            self.unregister_signals()

    async def _stop_accepting_events(self) -> None:
        ex = {"subscription_count": len(self._lifecycle.coordinators)}
        logger.info("Stopping event acceptance", extra=ex)
        await self.stop()

    async def _drain_in_flight_events(self) -> int:
        total, dt, tasks = 0, self._shutdown_coordinator.drain_timeout, []
        for n, coord in self._lifecycle.coordinators.items():
            if (fc := coord.flow_controller) is None:
                logger.debug("No flow controller for subscription", extra={"subscription": n})
            elif (inf := fc.in_flight) > 0:
                total, ex = total + inf, {"subscription": n, "in_flight_count": inf}
                logger.info("Draining in-flight events for subscription", extra=ex)
                tasks.append((n, asyncio.create_task(fc.wait_for_drain(dt), name=f"drain-{n}")))
        ex_all = {"total_in_flight": total, "subscriptions_with_events": len(tasks)}
        logger.info("Draining in-flight events", extra=ex_all)
        if not tasks:
            return total
        try:
            results = await asyncio.gather(*(t for _, t in tasks), return_exceptions=True)
        except Exception as e:
            logger.error("Error during drain phase", extra={"error": str(e)}, exc_info=True)
            return total
        rem = 0
        for (n, _), res in zip(tasks, results, strict=True):
            if isinstance(res, BaseException):
                logger.error("Drain task failed", extra={"subscription": n, "error": str(res)})
            elif res > 0:
                rem, ex = rem + res, {"subscription": n, "remaining_events": res}
                logger.warning("Subscription did not fully drain", extra=ex)
            else:
                logger.debug("Subscription drained successfully", extra={"subscription": n})
        if rem > 0:
            ex_rem = {"total_remaining": rem, "drain_timeout": dt}
            logger.warning("Some events did not drain before timeout", extra=ex_rem)
        else:
            logger.info("All events drained successfully", extra={"total_drained": total})
        return total

    async def _save_final_checkpoints(self) -> int:
        saved = 0
        for name, s in self._registry.items():
            ev_id, ev_type, pos = s.last_event_id, s.last_event_type, s.last_processed_position
            if ev_id is None or ev_type is None or pos is None:
                why = "no position to checkpoint" if ev_id and ev_type else "no event info"
                logger.debug(f"Skipping checkpoint save - {why}", extra={"subscription": name})
                continue
            try:
                await self.checkpoint_repo.save_position(name, pos, ev_id, ev_type)
                saved, ex = saved + 1, {"subscription": name, "position": render_position(pos)}
                logger.debug("Checkpoint saved", extra=ex)
            except Exception as e:
                logger.error(
                    "Failed to save checkpoint", extra={"subscription": name, "error": f"{e}"}
                )
        logger.info("Final checkpoints saved", extra={"checkpoints_saved": saved})
        return saved

    def request_shutdown(self) -> None:
        self._shutdown_coordinator.request_shutdown()

    def shutdown_coordinator(self) -> ShutdownCoordinator:
        return self._shutdown_coordinator

    is_running: Any = property(lambda self: self._running)
    is_shutting_down: Any = property(lambda self: self._shutdown_coordinator.is_shutting_down)
    shutdown_phase: Any = property(lambda self: self._shutdown_coordinator.phase)
    last_shutdown_result: Any = property(lambda self: self._last_shutdown_result)

    async def __aenter__(self: Self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
