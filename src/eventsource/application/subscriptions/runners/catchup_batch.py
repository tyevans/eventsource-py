"""Batch processing and dispatch loops for CatchUpRunner."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.config import CheckpointStrategy
from eventsource.application.subscriptions.runners.catchup_batch_modes import CatchUpBatchModesMixin
from eventsource.application.subscriptions.runners.catchup_result import _BatchOutcome
from eventsource.application.subscriptions.subscription import Subscription, render_position
from eventsource.ports.envelopes import EventEnvelope
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    import asyncio

    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.application.subscriptions.filtering import EventFilter
    from eventsource.application.subscriptions.flow_control import FlowController

logger = logging.getLogger(__name__)


class CatchUpBatchMixin(CatchUpBatchModesMixin):
    """Mixin providing single and grouped batch processing for catch-up runner."""

    if TYPE_CHECKING:
        _batch_capable: bool
        subscription: Subscription
        config: SubscriptionConfig
        _stop_event: asyncio.Event
        _reached_target: bool
        _filter: EventFilter
        _flow_controller: FlowController

        @property
        def _stop_requested(self) -> bool: ...
        async def _read_batch_with_retry(
            self, from_position: Position | None, limit: int
        ) -> list[EventEnvelope]: ...
        async def _deliver_event(self, envelope: EventEnvelope) -> bool: ...
        async def _deliver_batch(self, envelopes: list[EventEnvelope]) -> bool: ...
        async def _save_checkpoint_with_retry(self, envelope: EventEnvelope) -> None: ...
        async def _maybe_save_periodic_checkpoint(self, envelope: EventEnvelope) -> None: ...

    async def _process_batch(self, target_position: Position) -> _BatchOutcome:
        if self._batch_capable:
            return await self._process_batch_grouped(target_position)
        return await self._process_batch_single(target_position)

    async def _check_envelope_ready(
        self, envelope: EventEnvelope, target_position: Position
    ) -> bool:
        if self._stop_requested:
            return False
        if envelope.position is None or envelope.position > target_position:
            self._reached_target = True
            return False
        await self.subscription.wait_if_paused(self._stop_event)
        return not self._stop_requested

    async def _finalize_batch(
        self, log_msg: str, in_batch: int, filtered: int, last_envelope: EventEnvelope | None
    ) -> None:
        if (
            (in_batch > 0 or filtered > 0)
            and last_envelope is not None
            and self.config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH
        ):
            await self._save_checkpoint_with_retry(last_envelope)
        pos = render_position(self.subscription.last_processed_position)
        extra = {"subscription": self.subscription.name, "batch_size": in_batch, "events_filtered": filtered, "position": pos}  # fmt: skip
        logger.debug(log_msg, extra=extra)

    async def _record_and_checkpoint(
        self, envelope: EventEnvelope, passes_filter: bool, *, succeeded: bool = True
    ) -> None:
        ev = envelope.event
        if succeeded:
            await self.subscription.record_event_processed(
                position=envelope.position, event_id=ev.event_id, event_type=ev.event_type
            )
        else:
            await self.subscription.advance_past_failed_event(
                position=envelope.position, event_id=ev.event_id, event_type=ev.event_type
            )
        if not passes_filter:
            return
        if self.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT:
            await self._save_checkpoint_with_retry(envelope)
        elif self.config.checkpoint_strategy == CheckpointStrategy.PERIODIC:
            await self._maybe_save_periodic_checkpoint(envelope)


__all__ = ["CatchUpBatchMixin"]
