"""Subscription registry for managing subscription registration and lookup."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import ItemsView, Iterator
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.subscription import (
    Subscription,
    SubscriptionState,
    SubscriptionStatus,
)
from eventsource.ports.exceptions import SubscriptionAlreadyExistsError

if TYPE_CHECKING:
    from eventsource.ports.handlers import EventSubscriber

logger = logging.getLogger(__name__)


class SubscriptionRegistry:
    """
    Registry for managing subscription registration and lookup.

    Example:
        >>> registry = SubscriptionRegistry()
        >>> subscription = await registry.register(
        ...     my_projection,
        ...     config=SubscriptionConfig(batch_size=500),
        ... )
    """

    def __init__(self) -> None:
        """Initialize the subscription registry."""
        self._subscriptions: dict[str, Subscription] = {}
        self._lock = asyncio.Lock()

    async def register(
        self,
        subscriber: EventSubscriber,
        config: SubscriptionConfig | None = None,
        name: str | None = None,
    ) -> Subscription:
        """
        Register a subscriber for event delivery.

        Args:
            subscriber: The event subscriber (e.g., projection) to register
            config: Optional configuration for this subscription
            name: Optional custom name (defaults to subscriber's class name)

        Raises:
            SubscriptionAlreadyExistsError: If a subscription with this name exists
        """
        subscription_name = name or subscriber.__class__.__name__
        config = config or SubscriptionConfig()

        async with self._lock:
            if subscription_name in self._subscriptions:
                raise SubscriptionAlreadyExistsError(subscription_name)

            subscription = Subscription(
                name=subscription_name,
                config=config,
                subscriber=subscriber,
            )

            self._subscriptions[subscription_name] = subscription

            logger.info(
                "Subscription registered",
                extra={
                    "subscription": subscription_name,
                    "start_from": str(config.start_from),
                    "batch_size": config.batch_size,
                },
            )

            return subscription

    async def unregister(self, name: str) -> Subscription | None:
        """Unregister a subscription by name and return it, or None if not found."""
        async with self._lock:
            subscription = self._subscriptions.pop(name, None)
            if subscription:
                if not subscription.is_terminal:
                    await subscription.transition_to(SubscriptionState.STOPPED)

                logger.info(
                    "Subscription unregistered",
                    extra={"subscription": name},
                )

            return subscription

    def get(self, name: str) -> Subscription | None:
        """Get a subscription by name, or None if not found."""
        return self._subscriptions.get(name)

    def get_all(self) -> list[Subscription]:
        """Get all registered subscriptions."""
        return list(self._subscriptions.values())

    def get_names(self) -> list[str]:
        """Get all registered subscription names."""
        return list(self._subscriptions.keys())

    def get_statuses(self) -> dict[str, SubscriptionStatus]:
        """Get status snapshots of all subscriptions."""
        return {
            name: subscription.get_status() for name, subscription in self._subscriptions.items()
        }

    def contains(self, target: str | Subscription | EventSubscriber) -> bool:
        """Check if a subscription name, Subscription, or EventSubscriber is registered."""
        if isinstance(target, str):
            return target in self._subscriptions
        if isinstance(target, Subscription):
            return self._subscriptions.get(target.name) is target
        return any(sub.subscriber is target for sub in self._subscriptions.values())

    def __contains__(self, target: str | Subscription | EventSubscriber) -> bool:
        """Support ``target in registry`` for names, Subscriptions, or EventSubscribers."""
        return self.contains(target)

    def __len__(self) -> int:
        """Get the number of registered subscriptions."""
        return len(self._subscriptions)

    def __bool__(self) -> bool:
        """Empty registries remain truthy."""
        return True

    def __iter__(self) -> Iterator[Subscription]:
        """Iterate over registered subscriptions."""
        return iter(self._subscriptions.values())

    def items(self) -> ItemsView[str, Subscription]:
        """Iterate over (name, subscription) pairs."""
        return self._subscriptions.items()

    @property
    def lock(self) -> asyncio.Lock:
        """Get the internal lock for external synchronization."""
        return self._lock


__all__ = ["SubscriptionRegistry"]
