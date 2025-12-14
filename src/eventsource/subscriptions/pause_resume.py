"""
Pause/resume controller for subscription management.

The PauseResumeController follows the Single Responsibility Principle by
handling only pause and resume operations:
- Pausing individual subscriptions
- Resuming individual subscriptions
- Batch pause/resume operations

Example:
    >>> controller = PauseResumeController(
    ...     registry=registry,
    ...     lifecycle=lifecycle,
    ... )
    >>> await controller.pause("OrderProjection")
    >>> await controller.resume("OrderProjection")
"""

import logging
from typing import TYPE_CHECKING

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_SUBSCRIPTION_NAME
from eventsource.subscriptions.subscription import PauseReason, Subscription

if TYPE_CHECKING:
    from eventsource.subscriptions.lifecycle import SubscriptionLifecycleManager
    from eventsource.subscriptions.registry import SubscriptionRegistry

logger = logging.getLogger(__name__)


class PauseResumeController:
    """
    Controls pause and resume operations for subscriptions.

    Handles:
    - Pausing individual subscriptions with reason tracking
    - Resuming paused subscriptions
    - Processing pause buffers on resume
    - Batch pause/resume operations

    Example:
        >>> controller = PauseResumeController(
        ...     registry=registry,
        ...     lifecycle=lifecycle,
        ... )
        >>> await controller.pause("OrderProjection")
    """

    def __init__(
        self,
        registry: "SubscriptionRegistry",
        lifecycle: "SubscriptionLifecycleManager",
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the pause/resume controller.

        Args:
            registry: Subscription registry
            lifecycle: Subscription lifecycle manager (for coordinator access)
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing.
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self._registry = registry
        self._lifecycle = lifecycle

    async def pause(
        self,
        name: str,
        reason: PauseReason | None = None,
    ) -> bool:
        """
        Pause a specific subscription by name.

        The subscription will stop processing events but maintain its position.
        Events received during pause will be buffered for processing on resume.

        Args:
            name: The subscription name to pause
            reason: Optional reason for pausing (defaults to PauseReason.MANUAL)

        Returns:
            True if subscription was paused, False if not found or not pausable
        """
        with self._tracer.span(
            "eventsource.pause_resume.pause",
            {ATTR_SUBSCRIPTION_NAME: name},
        ):
            subscription = self._registry.get(name)
            if subscription is None:
                logger.warning(
                    "Cannot pause subscription: not found",
                    extra={"subscription": name},
                )
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
                        "position": subscription.last_processed_position,
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
        """
        Resume a paused subscription by name.

        Continues processing from the last position. Events that were
        buffered during pause will be processed first.

        Args:
            name: The subscription name to resume

        Returns:
            True if subscription was resumed, False if not found or not paused
        """
        with self._tracer.span(
            "eventsource.pause_resume.resume",
            {ATTR_SUBSCRIPTION_NAME: name},
        ):
            subscription = self._registry.get(name)
            if subscription is None:
                logger.warning(
                    "Cannot resume subscription: not found",
                    extra={"subscription": name},
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

                # Process any buffered events from the live runner
                coordinator = self._lifecycle.get_coordinator(name)
                if coordinator and coordinator.live_runner:
                    pause_buffer_size = coordinator.live_runner.pause_buffer_size
                    if pause_buffer_size > 0:
                        processed = await coordinator.live_runner.process_pause_buffer()
                        logger.info(
                            "Processed pause buffer after resume",
                            extra={
                                "subscription": name,
                                "events_processed": processed,
                            },
                        )

                logger.info(
                    "Subscription resumed",
                    extra={
                        "subscription": name,
                        "position": subscription.last_processed_position,
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
                return False

    async def pause_all(
        self,
        reason: PauseReason | None = None,
    ) -> dict[str, bool]:
        """
        Pause all running subscriptions.

        Pauses all subscriptions that are currently in a running state
        (CATCHING_UP or LIVE). Subscriptions in other states are skipped.

        Args:
            reason: Optional reason for pausing (defaults to PauseReason.MANUAL)

        Returns:
            Dictionary mapping subscription names to pause result (True/False)
        """
        pause_reason = reason or PauseReason.MANUAL
        results: dict[str, bool] = {}

        for name, subscription in self._registry.items():
            if subscription.is_running:
                results[name] = await self.pause(name, reason=pause_reason)
            else:
                results[name] = False

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
        """
        Resume all paused subscriptions.

        Resumes all subscriptions that are currently in PAUSED state.
        Subscriptions in other states are skipped.

        Returns:
            Dictionary mapping subscription names to resume result (True/False)
        """
        results: dict[str, bool] = {}

        for name, subscription in self._registry.items():
            if subscription.is_paused:
                results[name] = await self.resume(name)
            else:
                results[name] = False

        resumed_count = sum(1 for r in results.values() if r)
        logger.info(
            "Resumed all subscriptions",
            extra={
                "resumed_count": resumed_count,
                "total_count": len(self._registry),
            },
        )

        return results

    def get_paused(self) -> list[Subscription]:
        """
        Get all paused subscriptions.

        Returns:
            List of subscriptions currently in PAUSED state
        """
        return [sub for sub in self._registry if sub.is_paused]

    def get_paused_names(self) -> list[str]:
        """
        Get names of all paused subscriptions.

        Returns:
            List of subscription names currently in PAUSED state
        """
        return [name for name, sub in self._registry.items() if sub.is_paused]


__all__ = ["PauseResumeController"]
