"""Lifecycle and shutdown coordination mixin for SubscriptionManager (ADR-0002, ADR-0007, TASK-0008)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Self

from eventsource.application.subscriptions.health_provider import HealthCheckProvider
from eventsource.application.subscriptions.lifecycle import SubscriptionLifecycleManager
from eventsource.application.subscriptions.registry import SubscriptionRegistry
from eventsource.application.subscriptions.shutdown import (
    ShutdownCoordinator,
    ShutdownPhase,
    ShutdownResult,
)
from eventsource.application.subscriptions.subscription import render_position
from eventsource.observability import Tracer

if TYPE_CHECKING:
    from eventsource.ports.checkpoints import SubscriptionPositions

logger = logging.getLogger(__name__)


class ManagerLifecycleMixin:
    """Mixin providing start, stop, signal handling, and graceful shutdown for SubscriptionManager."""

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
        """Start registered subscriptions concurrently."""
        if self._running:
            logger.warning("Subscription manager already running")
            return {}
        self._running = True
        self._started_at = datetime.now(UTC)
        self._health_provider.set_started(self._started_at)
        subs = self._select_subscriptions(subscription_names)
        logger.info(
            "Starting subscription manager",
            extra={"subscription_count": len(subs), "concurrent": concurrent},
        )
        return await self._lifecycle.start_all(subs, concurrent)

    async def stop(
        self, timeout: float = 30.0, subscription_names: list[str] | None = None
    ) -> None:
        """Stop subscriptions gracefully."""
        with self._tracer.span("eventsource.subscription_manager.stop"):
            if not self._running and subscription_names is None:
                return
            if subscription_names is None:
                self._running = False
                self._health_provider.clear_started()
            subs = self._select_subscriptions(subscription_names)
            logger.info(
                "Stopping subscription manager",
                extra={"subscription_count": len(subs), "timeout": timeout},
            )
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
        """Run the manager until a shutdown signal is received."""
        if shutdown_timeout is not None:
            self._shutdown_coordinator.timeout = shutdown_timeout
        logger.info(
            "Starting manager in daemon mode",
            extra={
                "subscription_count": len(self._registry),
                "shutdown_timeout": self._shutdown_coordinator.timeout,
            },
        )
        self.register_signals()
        try:
            await self.start()
            logger.info(
                "Manager running, waiting for shutdown signal",
                extra={"subscriptions": self._registry.get_names()},
            )
            await self._shutdown_coordinator.wait_for_shutdown()
            self._last_shutdown_result = await self._execute_graceful_shutdown()
            return self._last_shutdown_result
        except Exception as e:
            logger.error("Error during run_until_shutdown", extra={"error": str(e)}, exc_info=True)
            await self.stop()
            raise
        finally:
            self.unregister_signals()

    async def _execute_graceful_shutdown(self) -> ShutdownResult:
        return await self._shutdown_coordinator.shutdown(
            stop_func=self._stop_accepting_events,
            drain_func=self._drain_in_flight_events,
            checkpoint_func=self._save_final_checkpoints,
        )

    async def _stop_accepting_events(self) -> None:
        logger.info(
            "Stopping event acceptance",
            extra={"subscription_count": len(self._lifecycle.coordinators)},
        )
        await self.stop()

    async def _drain_in_flight_events(self) -> int:
        total_in_flight = 0
        drain_tasks: list[tuple[str, asyncio.Task[int]]] = []
        for name, coordinator in self._lifecycle.coordinators.items():
            if (fc := coordinator.flow_controller) is None:
                logger.debug("No flow controller for subscription", extra={"subscription": name})
                continue
            total_in_flight += fc.in_flight
            if fc.in_flight > 0:
                logger.info(
                    "Draining in-flight events for subscription",
                    extra={"subscription": name, "in_flight_count": fc.in_flight},
                )
                task = asyncio.create_task(
                    fc.wait_for_drain(self._shutdown_coordinator.drain_timeout),
                    name=f"drain-{name}",
                )
                drain_tasks.append((name, task))

        logger.info(
            "Draining in-flight events",
            extra={
                "total_in_flight": total_in_flight,
                "subscriptions_with_events": len(drain_tasks),
            },
        )
        if not drain_tasks:
            return total_in_flight

        try:
            results = await asyncio.gather(*(t for _, t in drain_tasks), return_exceptions=True)
            total_remaining = 0
            for (name, _), result in zip(drain_tasks, results, strict=True):
                if isinstance(result, BaseException):
                    logger.error(
                        "Drain task failed", extra={"subscription": name, "error": str(result)}
                    )
                elif result > 0:
                    total_remaining += result
                    logger.warning(
                        "Subscription did not fully drain",
                        extra={"subscription": name, "remaining_events": result},
                    )
                else:
                    logger.debug("Subscription drained successfully", extra={"subscription": name})

            if total_remaining > 0:
                logger.warning(
                    "Some events did not drain before timeout",
                    extra={
                        "total_remaining": total_remaining,
                        "drain_timeout": self._shutdown_coordinator.drain_timeout,
                    },
                )
            else:
                logger.info(
                    "All events drained successfully", extra={"total_drained": total_in_flight}
                )
        except Exception as e:
            logger.error("Error during drain phase", extra={"error": str(e)}, exc_info=True)
        return total_in_flight

    async def _save_final_checkpoints(self) -> int:
        saved_count = 0
        for name, sub in self._registry.items():
            if sub.last_event_id is None or sub.last_event_type is None:
                logger.debug(
                    "Skipping checkpoint save - no event info", extra={"subscription": name}
                )
                continue
            if (position := sub.last_processed_position) is None:
                logger.debug(
                    "Skipping checkpoint save - no position to checkpoint",
                    extra={"subscription": name},
                )
                continue
            try:
                await self.checkpoint_repo.save_position(
                    name, position, sub.last_event_id, sub.last_event_type
                )
                saved_count += 1
                logger.debug(
                    "Checkpoint saved",
                    extra={"subscription": name, "position": render_position(position)},
                )
            except Exception as e:
                logger.error(
                    "Failed to save checkpoint", extra={"subscription": name, "error": str(e)}
                )
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

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any
    ) -> None:
        await self.stop()
