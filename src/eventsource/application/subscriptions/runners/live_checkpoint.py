"""Checkpoint and progress persistence operations for LiveRunner."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.config import CheckpointStrategy
from eventsource.application.subscriptions.retry import (
    TRANSIENT_EXCEPTIONS,
    RetryableOperation,
)
from eventsource.application.subscriptions.subscription import (
    Subscription,
    render_position,
)
from eventsource.ports.envelopes import EventEnvelope
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.domain.event import DomainEvent
    from eventsource.ports.checkpoints import SubscriptionPositions

logger = logging.getLogger(__name__)


class LiveCheckpointMixin:
    """Mixin providing checkpoint and progress tracking for live runner."""

    if TYPE_CHECKING:
        checkpoint_repo: SubscriptionPositions
        subscription: Subscription
        config: SubscriptionConfig
        _retry: RetryableOperation | None
        _last_checkpoint_time: float

    async def _record_filtered(self, envelope: EventEnvelope) -> None:
        """Record progress for an envelope the filter rejected."""
        if envelope.position is not None:
            await self.subscription.record_event_processed(
                position=envelope.position,
                event_id=envelope.event.event_id,
                event_type=envelope.event.event_type,
            )
        else:
            await self.subscription.record_events_unseen(1)

    async def _maybe_checkpoint_in_batch(self, position: Position, event: DomainEvent) -> None:
        """
        Per-event checkpointing inside a grouped delivery.

        `EVERY_BATCH` is deliberately absent here -- unlike `_maybe_checkpoint`,
        which treats it as `EVERY_EVENT` because the per-event path has no batch
        boundary to attach to. `_deliver_page` checkpoints once after the page.
        """
        if self.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT:
            await self._save_checkpoint_with_retry(position, event)
        elif self.config.checkpoint_strategy == CheckpointStrategy.PERIODIC:
            await self._maybe_save_periodic_checkpoint(position, event)

    async def _maybe_checkpoint(self, position: Position, event: DomainEvent) -> None:
        """
        Handle checkpointing based on configured strategy.

        Args:
            position: Global-feed position of the event
            event: The event that was processed
        """
        if self.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT:
            await self._save_checkpoint_with_retry(position, event)
        elif self.config.checkpoint_strategy == CheckpointStrategy.PERIODIC:
            await self._maybe_save_periodic_checkpoint(position, event)
        # Note: EVERY_BATCH doesn't apply to live processing since events
        # arrive one at a time. We treat it like EVERY_EVENT for live mode.
        elif self.config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH:
            await self._save_checkpoint_with_retry(position, event)

    async def _save_checkpoint(self, position: Position, event: DomainEvent) -> None:
        """
        Save checkpoint for the processed event (no retry).

        Args:
            position: Global-feed position of the event
            event: The event that was processed
        """
        await self.checkpoint_repo.save_position(
            subscription_id=self.subscription.name,
            position=position,
            event_id=event.event_id,
            event_type=event.event_type,
        )

        self._last_checkpoint_time = time.monotonic()

        logger.debug(
            "Checkpoint saved",
            extra={
                "subscription": self.subscription.name,
                "position": render_position(position),
            },
        )

    async def _save_checkpoint_with_retry(
        self,
        position: Position,
        event: DomainEvent,
    ) -> None:
        """
        Save checkpoint for the processed event with retry.

        Args:
            position: Global-feed position of the event
            event: The event that was processed

        Raises:
            RetryError: If all retries are exhausted
        """

        async def save_checkpoint() -> None:
            await self.checkpoint_repo.save_position(
                subscription_id=self.subscription.name,
                position=position,
                event_id=event.event_id,
                event_type=event.event_type,
            )

        assert self._retry is not None
        await self._retry.execute(
            operation=save_checkpoint,
            name="save_checkpoint",
            retryable_exceptions=TRANSIENT_EXCEPTIONS,
        )

        self._last_checkpoint_time = time.monotonic()

        logger.debug(
            "Checkpoint saved",
            extra={
                "subscription": self.subscription.name,
                "position": render_position(position),
            },
        )

    async def _maybe_save_periodic_checkpoint(
        self,
        position: Position,
        event: DomainEvent,
    ) -> None:
        """
        Save checkpoint if enough time has passed (for PERIODIC strategy).

        Args:
            position: Global-feed position of the event
            event: The event to potentially checkpoint
        """
        current_time = time.monotonic()
        elapsed = current_time - self._last_checkpoint_time

        if elapsed >= self.config.checkpoint_interval_seconds:
            await self._save_checkpoint_with_retry(position, event)


__all__ = ["LiveCheckpointMixin"]
