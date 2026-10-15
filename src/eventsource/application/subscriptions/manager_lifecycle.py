"""Lifecycle and shutdown coordination mixin for SubscriptionManager (ADR-0002, ADR-0007, TASK-0008)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any, Self

from eventsource.observability import Tracer
from eventsource.ports.checkpoints import SubscriptionPositions

from .health_provider import HealthCheckProvider
from .lifecycle import SubscriptionLifecycleManager
from .registry import SubscriptionRegistry
from .shutdown import ShutdownCoordinator, ShutdownPhase, ShutdownResult
from .subscription import render_position

logger = logging.getLogger(__name__)


class ManagerLifecycleMixin:
    _running: bool
    _started_at: datetime | None
    _health_provider: HealthCheckProvider
    _registry: SubscriptionRegistry
    _lifecycle: SubscriptionLifecycleManager
    _shutdown_coordinator: ShutdownCoordinator
    _last_shutdown_result: ShutdownResult | None
    checkpoint_repo: SubscriptionPositions
    _tracer: Tracer

    def _select_subscriptions(self, names: list[str] | None) -> list[Any]:
        all_subs = self._registry.get_all()
        return all_subs if names is None else [s for s in all_subs if s.name in names]

    async def start(
        self, subscription_names: list[str] | None = None, concurrent: bool = True
    ) -> dict[str, Exception | None]:
        if self._running:
            logger.warning("Subscription manager already running")
            return {}
        self._running, self._started_at = True, datetime.now(UTC)
        self._health_provider.set_started(self._started_at)
        subs = self._select_subscriptions(subscription_names)
        extra = {"subscription_count": len(subs), "concurrent": concurrent}
        logger.info("Starting subscription manager", extra=extra)
        return await self._lifecycle.start_all(subs, concurrent)

    async def stop(
        self, timeout: float = 30.0, subscription_names: list[str] | None = None
    ) -> None:
        with self._tracer.span("eventsource.subscription_manager.stop"):
            if not self._running and subscription_names is None:
                return
            if subscription_names is None:
                self._running = False
                self._health_provider.clear_started()
            subs = self._select_subscriptions(subscription_names)
            extra = {"subscription_count": len(subs), "timeout": timeout}
            logger.info("Stopping subscription manager", extra=extra)
            await self._lifecycle.stop_all(subs, timeout)
            logger.info("Subscription manager stopped")

    @property
    def is_running(self) -> bool:
        return self._running

    def register_signals(self) -> None:
        self._shutdown_coordinator.on_shutdown(self._on_shutdown_signal)
        self._shutdown_coordinator.register_signals()

    def unregister_signals(self) -> None:
        self._shutdown_coordinator.unregister_signals()

    async def _on_shutdown_signal(self) -> None:
        logger.info("Shutdown signal received by SubscriptionManager")

    async def run_until_shutdown(self, shutdown_timeout: float | None = None) -> ShutdownResult:
        sc = self._shutdown_coordinator
        if shutdown_timeout is not None:
            sc.timeout = shutdown_timeout
        extra = {"subscription_count": len(self._registry), "shutdown_timeout": sc.timeout}
        logger.info("Starting manager in daemon mode", extra=extra)
        self.register_signals()
        try:
            await self.start()
            extra_run = {"subscriptions": self._registry.get_names()}
            logger.info("Manager running, waiting for shutdown signal", extra=extra_run)
            await sc.wait_for_shutdown()
            return await self._execute_graceful_shutdown()
        except Exception as e:
            logger.error("Error during run_until_shutdown", extra={"error": str(e)}, exc_info=True)
            await self.stop()
            raise
        finally:
            self.unregister_signals()

    async def _execute_graceful_shutdown(self) -> ShutdownResult:
        self._last_shutdown_result = await self._shutdown_coordinator.shutdown(
            self._stop_accepting_events, self._drain_in_flight_events, self._save_final_checkpoints
        )
        return self._last_shutdown_result

    async def _stop_accepting_events(self) -> None:
        extra = {"subscription_count": len(self._lifecycle.coordinators)}
        logger.info("Stopping event acceptance", extra=extra)
        await self.stop()

    async def _drain_in_flight_events(self) -> int:
        total_in_flight, dt = 0, self._shutdown_coordinator.drain_timeout
        tasks: list[tuple[str, asyncio.Task[int]]] = []
        for n, coordinator in self._lifecycle.coordinators.items():
            if (fc := coordinator.flow_controller) is None:
                logger.debug("No flow controller for subscription", extra={"subscription": n})
            elif fc.in_flight > 0:
                total_in_flight += fc.in_flight
                extra = {"subscription": n, "in_flight_count": fc.in_flight}
                logger.info("Draining in-flight events for subscription", extra=extra)
                tasks.append((n, asyncio.create_task(fc.wait_for_drain(dt), name=f"drain-{n}")))
        extra = {"total_in_flight": total_in_flight, "subscriptions_with_events": len(tasks)}
        logger.info("Draining in-flight events", extra=extra)
        if not tasks:
            return total_in_flight
        try:
            results = await asyncio.gather(*(t for _, t in tasks), return_exceptions=True)
        except Exception as e:
            logger.error("Error during drain phase", extra={"error": str(e)}, exc_info=True)
            return total_in_flight
        rem = 0
        for (n, _), res in zip(tasks, results, strict=True):
            s = {"subscription": n}
            if isinstance(res, BaseException):
                logger.error("Drain task failed", extra={**s, "error": str(res)})
            elif res > 0:
                rem += res
                w = {**s, "remaining_events": res}
                logger.warning("Subscription did not fully drain", extra=w)
            else:
                logger.debug("Subscription drained successfully", extra=s)
        if rem > 0:
            extra_t = {"total_remaining": rem, "drain_timeout": dt}
            logger.warning("Some events did not drain before timeout", extra=extra_t)
        else:
            logger.info("All events drained successfully", extra={"total_drained": total_in_flight})
        return total_in_flight

    async def _save_final_checkpoints(self) -> int:
        saved_count = 0
        for name, s in self._registry.items():
            ext = {"subscription": name}
            ev_id, ev_type, pos = s.last_event_id, s.last_event_type, s.last_processed_position
            if ev_id is None or ev_type is None or pos is None:
                why = "no position to checkpoint" if ev_id and ev_type else "no event info"
                logger.debug(f"Skipping checkpoint save - {why}", extra=ext)
                continue
            try:
                await self.checkpoint_repo.save_position(name, pos, ev_id, ev_type)
                saved_count += 1
                logger.debug("Checkpoint saved", extra={**ext, "position": render_position(pos)})
            except Exception as e:
                logger.error("Failed to save checkpoint", extra={**ext, "error": str(e)})
        logger.info("Final checkpoints saved", extra={"checkpoints_saved": saved_count})
        return saved_count

    def request_shutdown(self) -> None:
        self._shutdown_coordinator.request_shutdown()

    @property
    def is_shutting_down(self) -> bool:
        return self._shutdown_coordinator.is_shutting_down

    @property
    def shutdown_phase(self) -> ShutdownPhase:
        return self._shutdown_coordinator.phase

    @property
    def last_shutdown_result(self) -> ShutdownResult | None:
        return self._last_shutdown_result

    def shutdown_coordinator(self) -> ShutdownCoordinator:
        return self._shutdown_coordinator

    async def __aenter__(self: Self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.stop()
