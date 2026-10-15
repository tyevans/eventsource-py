"""Pause/resume controller for subscription management."""

import logging
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.subscription import (
    PauseReason,
    Subscription,
    render_position,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_SUBSCRIPTION_NAME

if TYPE_CHECKING:
    from eventsource.application.subscriptions.lifecycle import SubscriptionLifecycleManager
    from eventsource.application.subscriptions.registry import SubscriptionRegistry

logger = logging.getLogger(__name__)


class PauseResumeController:
    """Controls individual and batch pause/resume operations for subscriptions."""

    def __init__(
        self,
        registry: "SubscriptionRegistry",
        lifecycle: "SubscriptionLifecycleManager",
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the pause/resume controller."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._registry = registry
        self._lifecycle = lifecycle

    async def pause(self, name: str, reason: PauseReason | None = None) -> bool:
        """Pause a specific subscription by name, returning True if paused."""
        with self._tracer.span("eventsource.pause_resume.pause", {ATTR_SUBSCRIPTION_NAME: name}):
            subscription = self._registry.get(name)
            if subscription is None:
                logger.warning("Cannot pause subscription: not found", extra={"subscription": name})
                return False

            if not subscription.is_running:
                logger.warning(
                    "Cannot pause subscription: not running",
                    extra={"subscription": name, "state": subscription.state.value},
                )
                return False

            try:
                pause_reason = reason or PauseReason.MANUAL
                await subscription.pause(reason=pause_reason)
                logger.info(
                    "Subscription paused",
                    extra={
                        "subscription": name,
                        "reason": pause_reason.value,
                        "position": render_position(subscription.last_processed_position),
                    },
                )
                return True
            except Exception as e:
                logger.error(
                    "Failed to pause subscription",
                    extra={"subscription": name, "error": str(e)},
                    exc_info=True,
                )
                return False

    async def resume(self, name: str) -> bool:
        """Resume a paused subscription by name and process any buffered events."""
        with self._tracer.span("eventsource.pause_resume.resume", {ATTR_SUBSCRIPTION_NAME: name}):
            subscription = self._registry.get(name)
            if subscription is None:
                logger.warning(
                    "Cannot resume subscription: not found", extra={"subscription": name}
                )
                return False

            if not subscription.is_paused:
                logger.warning(
                    "Cannot resume subscription: not paused",
                    extra={"subscription": name, "state": subscription.state.value},
                )
                return False

            try:
                await subscription.resume()
                coordinator = self._lifecycle.get_coordinator(name)
                runner = coordinator.live_runner if coordinator else None
                if runner and runner.pause_buffer_size > 0:
                    processed = await runner.process_pause_buffer()
                    logger.info(
                        "Processed pause buffer after resume",
                        extra={"subscription": name, "events_processed": processed},
                    )

                logger.info(
                    "Subscription resumed",
                    extra={
                        "subscription": name,
                        "position": render_position(subscription.last_processed_position),
                        "state": subscription.state.value,
                    },
                )
                return True
            except Exception as e:
                logger.error(
                    "Failed to resume subscription",
                    extra={"subscription": name, "error": str(e)},
                    exc_info=True,
                )
                coordinator = self._lifecycle.get_coordinator(name)
                if coordinator and coordinator.live_runner:
                    await coordinator.live_runner.clear_buffer()
                else:
                    await subscription.reconcile_lag(0)
                return False

    async def pause_all(self, reason: PauseReason | None = None) -> dict[str, bool]:
        """Pause all running subscriptions and return map of name to pause result."""
        pause_reason = reason or PauseReason.MANUAL
        results = {
            name: (await self.pause(name, reason=pause_reason) if sub.is_running else False)
            for name, sub in self._registry.items()
        }
        paused_count = sum(1 for r in results.values() if r)
        logger.info(
            "Paused all subscriptions",
            extra={
                "reason": pause_reason.value,
                "paused_count": paused_count,
                "total_count": len(self._registry),
            },
        )
        return results

    async def resume_all(self) -> dict[str, bool]:
        """Resume all paused subscriptions and return map of name to resume result."""
        results = {
            name: (await self.resume(name) if sub.is_paused else False)
            for name, sub in self._registry.items()
        }
        resumed_count = sum(1 for r in results.values() if r)
        logger.info(
            "Resumed all subscriptions",
            extra={"resumed_count": resumed_count, "total_count": len(self._registry)},
        )
        return results

    def get_paused(self) -> list[Subscription]:
        """Get all paused subscriptions."""
        return [sub for sub in self._registry if sub.is_paused]

    def get_paused_names(self) -> list[str]:
        """Get names of all paused subscriptions."""
        return [name for name, sub in self._registry.items() if sub.is_paused]


__all__ = ["PauseResumeController"]
