"""
Start position resolver for subscriptions.

Governed by:
- ADR-0007 (Domain-Driven Design and Bounded Contexts)
- TASK-0006 (Reconcile Dropped Live Events on Transition)
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.application.subscriptions.subscription import Subscription
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


class StartFromResolver:
    """
    Resolves the start position based on configuration.

    Handles different start_from values:
    - "beginning": Start from the start of the feed (None)
    - "end": Start from the current feed position (live-only)
    - "checkpoint": Resume from last checkpoint
    - Position: Start from an explicit position

    Example:
        >>> resolver = StartFromResolver(event_store, checkpoint_repo)
        >>> position = await resolver.resolve(subscription)
        >>> print(f"Starting from position {position}")
    """

    def __init__(
        self,
        event_store: GlobalEventFeed,
        checkpoint_repo: SubscriptionPositions,
    ) -> None:
        """
        Initialize the start position resolver.

        Args:
            event_store: Event store for getting max position
            checkpoint_repo: Checkpoint repository for reading checkpoints
        """
        self.event_store = event_store
        self.checkpoint_repo = checkpoint_repo

    async def resolve(
        self,
        subscription: Subscription,
    ) -> Position | None:
        """
        Resolve the starting position for a subscription.

        Interprets the subscription's start_from configuration and
        returns the appropriate starting position.

        Args:
            subscription: The subscription to resolve position for

        Returns:
            Starting position, or None to read from the start of the feed
            (which is also the result when "checkpoint" finds none)

        Raises:
            ValueError: If start_from has an unknown value
        """
        start_from = subscription.config.start_from

        if isinstance(start_from, Position):
            # Explicit position
            return start_from

        if start_from == "beginning":
            return None

        if start_from == "end":
            return await self.event_store.current_position()

        if start_from == "checkpoint":
            position = await self.checkpoint_repo.get_position(subscription.name)
            if position is not None:
                return position
            # No checkpoint found, start from beginning
            logger.info(
                "No checkpoint found, starting from beginning",
                extra={"subscription": subscription.name},
            )
            return None

        raise ValueError(f"Unknown start_from value: {start_from}")


__all__ = [
    "StartFromResolver",
]
