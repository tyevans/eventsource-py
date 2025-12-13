"""
Subscription lifecycle management for coordinating start/stop operations.

The SubscriptionLifecycleManager follows the Single Responsibility Principle
by handling only subscription lifecycle operations:
- Starting individual and multiple subscriptions
- Stopping individual and multiple subscriptions
- Coordinating transition from catch-up to live events

Example:
    >>> lifecycle = SubscriptionLifecycleManager(
    ...     event_store=event_store,
    ...     event_bus=event_bus,
    ...     checkpoint_repo=checkpoint_repo,
    ... )
    >>> results = await lifecycle.start_all(subscriptions)
    >>> await lifecycle.stop_all()
"""

import asyncio
import logging
from typing import TYPE_CHECKING

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_EVENTS_PROCESSED,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
)
from eventsource.subscriptions.exceptions import SubscriptionError
from eventsource.subscriptions.subscription import Subscription, SubscriptionState
from eventsource.subscriptions.transition import StartFromResolver, TransitionCoordinator

if TYPE_CHECKING:
    from eventsource.bus.interface import EventBus
    from eventsource.repositories.checkpoint import CheckpointRepository
    from eventsource.stores.interface import EventStore

logger = logging.getLogger(__name__)


class SubscriptionLifecycleManager:
    """
    Manages subscription lifecycle operations.

    Handles:
    - Starting subscriptions (resolving positions, catch-up, transition to live)
    - Stopping subscriptions gracefully
    - Managing transition coordinators

    Example:
        >>> lifecycle = SubscriptionLifecycleManager(
        ...     event_store=event_store,
        ...     event_bus=event_bus,
        ...     checkpoint_repo=checkpoint_repo,
        ... )
        >>> results = await lifecycle.start_all(subscriptions)
    """

    def __init__(
        self,
        event_store: "EventStore",
        event_bus: "EventBus",
        checkpoint_repo: "CheckpointRepository",
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the lifecycle manager.

        Args:
            event_store: Event store for historical events
            event_bus: Event bus for live events
            checkpoint_repo: Checkpoint repository for position tracking
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing.
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self.event_store = event_store
        self.event_bus = event_bus
        self.checkpoint_repo = checkpoint_repo

        self._coordinators: dict[str, TransitionCoordinator] = {}
        self._start_resolver = StartFromResolver(event_store, checkpoint_repo)

    async def start_subscription(self, subscription: Subscription) -> None:
        """
        Start a single subscription.

        Resolves the starting position, performs catch-up from event store,
        and transitions to live events from event bus.

        Args:
            subscription: The subscription to start

        Raises:
            SubscriptionError: If the subscription fails to start
        """
        name = subscription.name

        with self._tracer.span(
            "eventsource.lifecycle.start_subscription",
            {ATTR_SUBSCRIPTION_NAME: name},
        ) as span:
            try:
                # Resolve starting position
                start_position = await self._start_resolver.resolve(subscription)
                subscription.last_processed_position = start_position

                if span:
                    span.set_attribute(ATTR_POSITION, start_position)

                logger.info(
                    "Starting subscription",
                    extra={
                        "subscription": name,
                        "start_position": start_position,
                    },
                )

                # Create and execute transition coordinator
                coordinator = TransitionCoordinator(
                    event_store=self.event_store,
                    event_bus=self.event_bus,
                    checkpoint_repo=self.checkpoint_repo,
                    subscription=subscription,
                )
                self._coordinators[name] = coordinator

                # Execute transition (catch-up to live)
                result = await coordinator.execute()

                if not result.success:
                    await subscription.set_error(
                        result.error or SubscriptionError("Transition failed")
                    )
                    raise SubscriptionError(
                        f"Subscription {name} failed to transition: {result.error}"
                    )

                if span:
                    span.set_attribute(ATTR_EVENTS_PROCESSED, result.catchup_events_processed)

                logger.info(
                    "Subscription started successfully",
                    extra={
                        "subscription": name,
                        "catchup_events": result.catchup_events_processed,
                        "buffer_events": result.buffer_events_processed,
                        "final_position": result.final_position,
                    },
                )

            except Exception as e:
                logger.error(
                    "Failed to start subscription",
                    extra={
                        "subscription": name,
                        "error": str(e),
                    },
                    exc_info=True,
                )
                await subscription.set_error(e)
                raise

    async def start_subscription_isolated(
        self,
        subscription: Subscription,
    ) -> Exception | None:
        """
        Start a single subscription with isolation.

        This method catches and returns exceptions instead of raising them,
        ensuring that one subscription failure does not affect others.

        Args:
            subscription: The subscription to start

        Returns:
            None if successful, Exception if failed
        """
        try:
            await self.start_subscription(subscription)
            return None
        except Exception as e:
            return e

    async def start_all(
        self,
        subscriptions: list[Subscription],
        concurrent: bool = True,
    ) -> dict[str, Exception | None]:
        """
        Start multiple subscriptions.

        Args:
            subscriptions: List of subscriptions to start
            concurrent: If True (default), start concurrently; if False, sequentially

        Returns:
            Dictionary mapping subscription names to None (success) or Exception (failure)
        """
        logger.info(
            "Starting subscriptions",
            extra={
                "subscription_count": len(subscriptions),
                "concurrent": concurrent,
            },
        )

        results: dict[str, Exception | None] = {}

        if concurrent and len(subscriptions) > 1:
            tasks = []
            for subscription in subscriptions:
                task = asyncio.create_task(
                    self.start_subscription_isolated(subscription),
                    name=f"start_{subscription.name}",
                )
                tasks.append((subscription.name, task))

            for name, task in tasks:
                try:
                    result = await task
                    results[name] = result
                except Exception as e:
                    results[name] = e
                    logger.error(
                        "Unexpected error starting subscription",
                        extra={"subscription": name, "error": str(e)},
                        exc_info=True,
                    )
        else:
            for subscription in subscriptions:
                result = await self.start_subscription_isolated(subscription)
                results[subscription.name] = result

        # Log summary
        succeeded = sum(1 for r in results.values() if r is None)
        failed = sum(1 for r in results.values() if r is not None)

        if failed > 0:
            logger.warning(
                "Some subscriptions failed to start",
                extra={
                    "succeeded": succeeded,
                    "failed": failed,
                    "failures": {k: str(v) for k, v in results.items() if v is not None},
                },
            )
        else:
            logger.info(
                "All subscriptions started successfully",
                extra={"count": succeeded},
            )

        return results

    async def stop_subscription(
        self,
        name: str,
        timeout: float = 30.0,
    ) -> None:
        """
        Stop a single subscription.

        Args:
            name: Subscription name
            timeout: Shutdown timeout in seconds
        """
        with self._tracer.span(
            "eventsource.lifecycle.stop_subscription",
            {ATTR_SUBSCRIPTION_NAME: name},
        ):
            if name not in self._coordinators:
                return

            coordinator = self._coordinators[name]
            try:
                await asyncio.wait_for(coordinator.stop(), timeout=timeout)
                logger.info(
                    "Subscription stopped",
                    extra={"subscription": name},
                )
            except TimeoutError:
                logger.warning(
                    "Subscription stop timed out",
                    extra={
                        "subscription": name,
                        "timeout": timeout,
                    },
                )

    async def stop_all(
        self,
        subscriptions: list[Subscription],
        timeout: float = 30.0,
    ) -> None:
        """
        Stop multiple subscriptions gracefully.

        Args:
            subscriptions: List of subscriptions to stop
            timeout: Maximum seconds to wait for graceful shutdown per subscription
        """
        logger.info(
            "Stopping subscriptions",
            extra={
                "subscription_count": len(subscriptions),
                "timeout": timeout,
            },
        )

        stop_tasks = []
        for subscription in subscriptions:
            name = subscription.name
            if name in self._coordinators:
                stop_tasks.append(self.stop_subscription(name, timeout))

        if stop_tasks:
            await asyncio.gather(*stop_tasks, return_exceptions=True)

        # Update subscription states
        for subscription in subscriptions:
            if not subscription.is_terminal:
                await subscription.transition_to(SubscriptionState.STOPPED)

        logger.info("Subscriptions stopped")

    def get_coordinator(self, name: str) -> TransitionCoordinator | None:
        """
        Get the coordinator for a subscription.

        Args:
            name: Subscription name

        Returns:
            TransitionCoordinator or None if not found
        """
        return self._coordinators.get(name)

    def remove_coordinator(self, name: str) -> TransitionCoordinator | None:
        """
        Remove and return a coordinator.

        Args:
            name: Subscription name

        Returns:
            The removed TransitionCoordinator or None
        """
        return self._coordinators.pop(name, None)

    @property
    def coordinator_names(self) -> list[str]:
        """Get names of all active coordinators."""
        return list(self._coordinators.keys())

    @property
    def coordinators(self) -> dict[str, TransitionCoordinator]:
        """Get all coordinators."""
        return self._coordinators


__all__ = ["SubscriptionLifecycleManager"]
