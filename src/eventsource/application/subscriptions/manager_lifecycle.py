"""
Lifecycle and shutdown coordination mixin for SubscriptionManager.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- ADR-0002 (Single-Responsibility Modules and File Limits)
- TASK-0008 (Decompose Monolithic Modules SubscriptionManager and Shutdown)
"""

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
    """
    Mixin providing start, stop, signal handling, and graceful shutdown for SubscriptionManager.
    """

    _running: bool
    _started_at: datetime | None
    _health_provider: HealthCheckProvider
    _registry: SubscriptionRegistry
    _lifecycle: SubscriptionLifecycleManager
    _shutdown_coordinator: ShutdownCoordinator
    _last_shutdown_result: ShutdownResult | None
    checkpoint_repo: SubscriptionPositions
    _tracer: Tracer

    async def start(
        self,
        subscription_names: list[str] | None = None,
        concurrent: bool = True,
    ) -> dict[str, Exception | None]:
        """Start registered subscriptions concurrently."""
        if self._running:
            logger.warning("Subscription manager already running")
            return {}

        self._running = True
        self._started_at = datetime.now(UTC)
        self._health_provider.set_started(self._started_at)

        if subscription_names is None:
            subscriptions_to_start = self._registry.get_all()
        else:
            subscriptions_to_start = [
                sub for sub in self._registry.get_all() if sub.name in subscription_names
            ]

        logger.info(
            "Starting subscription manager",
            extra={
                "subscription_count": len(subscriptions_to_start),
                "concurrent": concurrent,
            },
        )

        return await self._lifecycle.start_all(subscriptions_to_start, concurrent)

    async def stop(
        self,
        timeout: float = 30.0,
        subscription_names: list[str] | None = None,
    ) -> None:
        """Stop subscriptions gracefully."""
        with self._tracer.span("eventsource.subscription_manager.stop"):
            if not self._running and subscription_names is None:
                return

            if subscription_names is None:
                subscriptions_to_stop = self._registry.get_all()
                self._running = False
                self._health_provider.clear_started()
            else:
                subscriptions_to_stop = [
                    sub for sub in self._registry.get_all() if sub.name in subscription_names
                ]

            logger.info(
                "Stopping subscription manager",
                extra={
                    "subscription_count": len(subscriptions_to_stop),
                    "timeout": timeout,
                },
            )

            await self._lifecycle.stop_all(subscriptions_to_stop, timeout)
            logger.info("Subscription manager stopped")

    @property
    def is_running(self) -> bool:
        """Check if the manager is running."""
        return self._running

    def register_signals(self) -> None:
        """Register signal handlers for graceful shutdown."""
        self._shutdown_coordinator.on_shutdown(self._on_shutdown_signal)
        self._shutdown_coordinator.register_signals()

    def unregister_signals(self) -> None:
        """Unregister signal handlers."""
        self._shutdown_coordinator.unregister_signals()

    async def _on_shutdown_signal(self) -> None:
        """Internal callback invoked when a shutdown signal is trapped."""
        logger.info("Shutdown signal received by SubscriptionManager")

    async def run_until_shutdown(
        self,
        shutdown_timeout: float | None = None,
    ) -> ShutdownResult:
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

            result = await self._execute_graceful_shutdown()
            self._last_shutdown_result = result
            return result

        except Exception as e:
            logger.error(
                "Error during run_until_shutdown",
                extra={"error": str(e)},
                exc_info=True,
            )
            await self.stop()
            raise
        finally:
            self.unregister_signals()

    async def _execute_graceful_shutdown(self) -> ShutdownResult:
        """Execute graceful shutdown sequence."""
        return await self._shutdown_coordinator.shutdown(
            stop_func=self._stop_accepting_events,
            drain_func=self._drain_in_flight_events,
            checkpoint_func=self._save_final_checkpoints,
        )

    async def _stop_accepting_events(self) -> None:
        """Stop accepting new events by stopping all coordinators."""
        logger.info(
            "Stopping event acceptance",
            extra={"subscription_count": len(self._lifecycle.coordinators)},
        )
        await self.stop()

    async def _drain_in_flight_events(self) -> int:
        """Drain in-flight events from all subscriptions."""
        total_in_flight = 0
        drain_tasks: list[tuple[str, asyncio.Task[int]]] = []

        for name, coordinator in self._lifecycle.coordinators.items():
            flow_controller = coordinator.flow_controller
            if flow_controller is None:
                logger.debug(
                    "No flow controller for subscription",
                    extra={"subscription": name},
                )
                continue

            in_flight = flow_controller.in_flight
            total_in_flight += in_flight

            if in_flight > 0:
                logger.info(
                    "Draining in-flight events for subscription",
                    extra={
                        "subscription": name,
                        "in_flight_count": in_flight,
                    },
                )
                drain_task = asyncio.create_task(
                    flow_controller.wait_for_drain(self._shutdown_coordinator.drain_timeout),
                    name=f"drain-{name}",
                )
                drain_tasks.append((name, drain_task))

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
            results = await asyncio.gather(
                *(task for _, task in drain_tasks),
                return_exceptions=True,
            )

            total_remaining = 0
            for (name, _), result in zip(drain_tasks, results, strict=True):
                if isinstance(result, BaseException):
                    logger.error(
                        "Drain task failed",
                        extra={
                            "subscription": name,
                            "error": str(result),
                        },
                    )
                elif result > 0:
                    total_remaining += result
                    logger.warning(
                        "Subscription did not fully drain",
                        extra={
                            "subscription": name,
                            "remaining_events": result,
                        },
                    )
                else:
                    logger.debug(
                        "Subscription drained successfully",
                        extra={"subscription": name},
                    )

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
                    "All events drained successfully",
                    extra={"total_drained": total_in_flight},
                )

        except Exception as e:
            logger.error(
                "Error during drain phase",
                extra={"error": str(e)},
                exc_info=True,
            )

        return total_in_flight

    async def _save_final_checkpoints(self) -> int:
        """Save final checkpoints for all subscriptions."""
        saved_count = 0
        for name, subscription in self._registry.items():
            if subscription.last_event_id is None or subscription.last_event_type is None:
                logger.debug(
                    "Skipping checkpoint save - no event info",
                    extra={"subscription": name},
                )
                continue

            position = subscription.last_processed_position
            if position is None:
                logger.debug(
                    "Skipping checkpoint save - no position to checkpoint",
                    extra={"subscription": name},
                )
                continue

            try:
                await self.checkpoint_repo.save_position(
                    subscription_id=name,
                    position=position,
                    event_id=subscription.last_event_id,
                    event_type=subscription.last_event_type,
                )
                saved_count += 1
                logger.debug(
                    "Checkpoint saved",
                    extra={
                        "subscription": name,
                        "position": render_position(position),
                    },
                )
            except Exception as e:
                logger.error(
                    "Failed to save checkpoint",
                    extra={
                        "subscription": name,
                        "error": str(e),
                    },
                )

        logger.info(
            "Final checkpoints saved",
            extra={"checkpoints_saved": saved_count},
        )
        return saved_count

    def request_shutdown(self) -> None:
        """Programmatically request shutdown."""
        self._shutdown_coordinator.request_shutdown()

    @property
    def is_shutting_down(self) -> bool:
        """Check if shutdown has been requested."""
        return self._shutdown_coordinator.is_shutting_down

    @property
    def shutdown_phase(self) -> ShutdownPhase:
        """Get the current shutdown phase."""
        return self._shutdown_coordinator.phase

    @property
    def last_shutdown_result(self) -> ShutdownResult | None:
        """Get the result of the last shutdown operation."""
        return self._last_shutdown_result

    def shutdown_coordinator(self) -> ShutdownCoordinator:
        """Get the underlying ShutdownCoordinator instance."""
        return self._shutdown_coordinator

    async def __aenter__(self: Self) -> Self:
        """Async context manager entry."""
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Async context manager exit."""
        await self.stop()
