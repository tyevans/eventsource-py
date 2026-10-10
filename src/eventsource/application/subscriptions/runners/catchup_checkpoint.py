"""Checkpoint and event store reading operations for CatchUpRunner."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.retry import (
    TRANSIENT_EXCEPTIONS,
    RetryableOperation,
)
from eventsource.application.subscriptions.subscription import (
    Subscription,
    render_position,
)
from eventsource.ports.envelopes import EventEnvelope, FeedReadOptions
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


class CatchUpCheckpointMixin:
    """Mixin providing event reading and checkpoint persistence for catch-up runner."""

    event_store: GlobalEventFeed
    checkpoint_repo: SubscriptionPositions
    subscription: Subscription
    config: SubscriptionConfig
    _retry: RetryableOperation
    _last_checkpoint_time: float

    async def _read_batch_with_retry(
        self,
        from_position: Position | None,
        limit: int,
    ) -> list[EventEnvelope]:
        """
        Read a batch of events from the global feed with retry.

        Args:
            from_position: Position to read from, None for the feed start
            limit: Maximum events to read

        Returns:
            List of event envelopes

        Raises:
            RetryError: If all retries are exhausted
        """
        options = FeedReadOptions(
            tenant_id=self.config.tenant_id,
            limit=limit,
        )

        async def read_batch() -> list[EventEnvelope]:
            envelopes = []
            async for envelope in self.event_store.read_all(from_position, options):
                envelopes.append(envelope)
            return envelopes

        return await self._retry.execute(
            operation=read_batch,
            name="read_batch",
            retryable_exceptions=TRANSIENT_EXCEPTIONS,
        )

    async def _save_checkpoint(self, envelope: EventEnvelope) -> None:
        """
        Save checkpoint for the processed event (no retry).

        An envelope with no position is not checkpointable: there is no
        token to persist and inventing one is not an option.

        Args:
            envelope: The envelope to checkpoint
        """
        if envelope.position is None:
            return

        await self.checkpoint_repo.save_position(
            subscription_id=self.subscription.name,
            position=envelope.position,
            event_id=envelope.event.event_id,
            event_type=envelope.event.event_type,
        )

        # Update time for periodic checkpointing
        self._last_checkpoint_time = time.monotonic()

        logger.debug(
            "Checkpoint saved",
            extra={
                "subscription": self.subscription.name,
                "position": render_position(envelope.position),
            },
        )

    async def _save_checkpoint_with_retry(self, envelope: EventEnvelope) -> None:
        """
        Save checkpoint for the processed event with retry.

        An envelope with no position is not checkpointable and is skipped.

        Args:
            envelope: The envelope to checkpoint

        Raises:
            RetryError: If all retries are exhausted
        """
        position = envelope.position
        if position is None:
            return

        async def save_checkpoint() -> None:
            await self.checkpoint_repo.save_position(
                subscription_id=self.subscription.name,
                position=position,
                event_id=envelope.event.event_id,
                event_type=envelope.event.event_type,
            )

        await self._retry.execute(
            operation=save_checkpoint,
            name="save_checkpoint",
            retryable_exceptions=TRANSIENT_EXCEPTIONS,
        )

        # Update time for periodic checkpointing
        self._last_checkpoint_time = time.monotonic()

        logger.debug(
            "Checkpoint saved",
            extra={
                "subscription": self.subscription.name,
                "position": render_position(position),
            },
        )

    async def _maybe_save_periodic_checkpoint(self, envelope: EventEnvelope) -> None:
        """
        Save checkpoint if enough time has passed (for PERIODIC strategy).

        Args:
            envelope: The envelope to potentially checkpoint
        """
        current_time = time.monotonic()
        elapsed = current_time - self._last_checkpoint_time

        if elapsed >= self.config.checkpoint_interval_seconds:
            await self._save_checkpoint_with_retry(envelope)


__all__ = ["CatchUpCheckpointMixin"]
