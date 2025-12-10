"""
Subscription registry for managing subscription registration and lookup.

The SubscriptionRegistry follows the Single Responsibility Principle by
handling only subscription CRUD operations:
- Registering new subscriptions
- Unregistering subscriptions
- Looking up subscriptions by name
- Iterating over subscriptions

Example:
    >>> registry = SubscriptionRegistry()
    >>> subscription = await registry.register(my_projection)
    >>> sub = registry.get("MyProjection")
    >>> await registry.unregister("MyProjection")
"""

import asyncio
import logging
from collections.abc import ItemsView, Iterator
from typing import TYPE_CHECKING

from eventsource.subscriptions.config import SubscriptionConfig
from eventsource.subscriptions.exceptions import SubscriptionAlreadyExistsError
from eventsource.subscriptions.subscription import (
    Subscription,
    SubscriptionState,
    SubscriptionStatus,
)

if TYPE_CHECKING:
    from eventsource.protocols import EventSubscriber

logger = logging.getLogger(__name__)


class SubscriptionRegistry:
    """
    Registry for managing subscription registration and lookup.

    Handles:
    - Thread-safe subscription registration
    - Subscription unregistration with cleanup
    - Subscription lookup by name
    - Iteration over registered subscriptions

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
        subscriber: "EventSubscriber",
        config: SubscriptionConfig | None = None,
        name: str | None = None,
    ) -> Subscription:
        """
        Register a subscriber for event delivery.

        Creates a subscription that will receive events from both the
        event store (historical) and event bus (live).

        Args:
            subscriber: The event subscriber (e.g., projection) to register
            config: Optional configuration for this subscription
            name: Optional custom name for the subscription. If not provided,
                  uses the subscriber's class name.

        Returns:
            The created Subscription object for monitoring

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
        """
        Unregister a subscription by name.

        Args:
            name: The subscription name to remove

        Returns:
            The removed Subscription, or None if not found
        """
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
        """
        Get a subscription by name.

        Args:
            name: The subscription name

        Returns:
            The Subscription, or None if not found
        """
        return self._subscriptions.get(name)

    def get_all(self) -> list[Subscription]:
        """Get all registered subscriptions."""
        return list(self._subscriptions.values())

    def get_names(self) -> list[str]:
        """Get all registered subscription names."""
        return list(self._subscriptions.keys())

    def get_statuses(self) -> dict[str, SubscriptionStatus]:
        """
        Get status snapshots of all subscriptions.

        Returns:
            Dictionary of subscription name to SubscriptionStatus
        """
        return {
            name: subscription.get_status() for name, subscription in self._subscriptions.items()
        }

    def contains(self, name: str) -> bool:
        """Check if a subscription with the given name exists."""
        return name in self._subscriptions

    def __len__(self) -> int:
        """Get the number of registered subscriptions."""
        return len(self._subscriptions)

    def __iter__(self) -> "Iterator[Subscription]":
        """Iterate over registered subscriptions."""
        return iter(self._subscriptions.values())

    def items(self) -> "ItemsView[str, Subscription]":
        """Iterate over (name, subscription) pairs."""
        return self._subscriptions.items()

    @property
    def lock(self) -> asyncio.Lock:
        """Get the internal lock for external synchronization."""
        return self._lock


__all__ = ["SubscriptionRegistry"]
