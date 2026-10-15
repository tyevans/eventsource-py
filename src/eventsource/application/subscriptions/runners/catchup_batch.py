"""Batch processing and dispatch loops for CatchUpRunner."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.config import CheckpointStrategy
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


class CatchUpBatchMixin:
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
        async def _deliver_event(self, envelope: EventEnvelope) -> None: ...
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

    async def _process_batch_single(self, target_position: Position) -> _BatchOutcome:
        current_position = self.subscription.last_processed_position
        envelopes = await self._read_batch_with_retry(current_position, self.config.batch_size)
        if not envelopes:
            self._reached_target = True
            return _BatchOutcome(envelopes_read=0, events_delivered=0)
        await self.subscription.record_events_seen(len(envelopes))

        events_in_batch = events_filtered = delivered_this_batch = 0
        last_envelope: EventEnvelope | None = None
        try:
            for envelope in envelopes:
                if not await self._check_envelope_ready(envelope, target_position):
                    break
                passes = self._filter.matches(envelope.event)
                if passes:
                    async with await self._flow_controller.acquire():
                        await self._deliver_event(envelope)
                        await self._record_and_checkpoint(envelope, True)
                    events_in_batch += 1
                else:
                    await self._record_and_checkpoint(envelope, False)
                    events_filtered += 1
                last_envelope = envelope
                delivered_this_batch += 1
        finally:
            if (undelivered := len(envelopes) - delivered_this_batch) > 0:
                await self.subscription.record_events_unseen(undelivered)

        await self._finalize_batch(
            "Batch processed", events_in_batch, events_filtered, last_envelope
        )
        return _BatchOutcome(envelopes_read=len(envelopes), events_delivered=events_in_batch)

    async def _process_batch_grouped(self, target_position: Position) -> _BatchOutcome:
        current_position = self.subscription.last_processed_position
        envelopes = await self._read_batch_with_retry(current_position, self.config.batch_size)
        if not envelopes:
            self._reached_target = True
            return _BatchOutcome(envelopes_read=0, events_delivered=0)
        await self.subscription.record_events_seen(len(envelopes))

        included: list[tuple[EventEnvelope, bool]] = []
        for envelope in envelopes:
            if not await self._check_envelope_ready(envelope, target_position):
                break
            included.append((envelope, self._filter.matches(envelope.event)))

        deliverable = [envelope for envelope, passes in included if passes]
        events_in_batch = events_filtered = delivered_this_batch = 0
        last_envelope: EventEnvelope | None = None
        try:
            batch_succeeded = True
            if deliverable:
                async with await self._flow_controller.acquire():
                    batch_succeeded = await self._deliver_batch(deliverable)
            for envelope, passes in included:
                if passes and not batch_succeeded:
                    async with await self._flow_controller.acquire():
                        await self._deliver_event(envelope)
                        await self._record_and_checkpoint(envelope, passes)
                else:
                    await self._record_and_checkpoint(envelope, passes)
                delivered_this_batch += 1
                last_envelope = envelope
                if passes:
                    events_in_batch += 1
                else:
                    events_filtered += 1
        finally:
            if (undelivered := len(envelopes) - delivered_this_batch) > 0:
                await self.subscription.record_events_unseen(undelivered)

        await self._finalize_batch(
            "Batch processed via handle_batch", events_in_batch, events_filtered, last_envelope
        )
        return _BatchOutcome(envelopes_read=len(envelopes), events_delivered=events_in_batch)

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
        extra = {
            "subscription": self.subscription.name,
            "batch_size": in_batch,
            "events_filtered": filtered,
            "position": pos,
        }
        logger.debug(log_msg, extra=extra)

    async def _record_and_checkpoint(self, envelope: EventEnvelope, passes_filter: bool) -> None:
        ev = envelope.event
        await self.subscription.record_event_processed(
            position=envelope.position, event_id=ev.event_id, event_type=ev.event_type
        )
        if not passes_filter:
            return
        if self.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT:
            await self._save_checkpoint_with_retry(envelope)
        elif self.config.checkpoint_strategy == CheckpointStrategy.PERIODIC:
            await self._maybe_save_periodic_checkpoint(envelope)


__all__ = ["CatchUpBatchMixin"]
