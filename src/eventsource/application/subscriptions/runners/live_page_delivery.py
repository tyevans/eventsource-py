"""Grouped page delivery (handle_batch) for LiveRunner."""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, cast

from eventsource.application.subscriptions.config import CheckpointStrategy
from eventsource.application.subscriptions.runners.live_checkpoint_errors import checkpoint_errors
from eventsource.application.subscriptions.subscriber import settle_handler_result
from eventsource.observability.error_summary import error_summary
from eventsource.ports.subscribers import BatchSubscriber

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.application.subscriptions.flow_control import FlowController
    from eventsource.application.subscriptions.metrics import SubscriptionMetrics
    from eventsource.application.subscriptions.runners.live_models import LiveRunnerStats
    from eventsource.application.subscriptions.subscription import Subscription
    from eventsource.domain.event import DomainEvent
    from eventsource.ports.envelopes import EventEnvelope
    from eventsource.ports.positions import Position

logger = logging.getLogger(__name__)


class LivePageDeliveryMixin:
    """Mixin delivering one scanned page to a batch subscriber, with per-event fallback."""

    if TYPE_CHECKING:
        subscription: Subscription
        config: SubscriptionConfig
        _metrics: SubscriptionMetrics | None
        _flow_controller: FlowController | None
        _stats: LiveRunnerStats

        async def _call_guarded(
            self, op: Callable[[], Awaitable[object]], op_name: str
        ) -> object: ...
        async def _process_live_event(self, envelope: EventEnvelope) -> None: ...
        async def _record_filtered(self, envelope: EventEnvelope) -> None: ...
        async def _maybe_checkpoint_in_batch(self, pos: Position, event: DomainEvent) -> None: ...
        async def _save_checkpoint_with_retry(self, pos: Position, event: DomainEvent) -> None: ...

    async def _deliver_page(self, included: list[tuple[EventEnvelope, bool]]) -> None:
        """Deliver one scanned page as a batch, then record every envelope."""
        deliverable = [envelope.event for envelope, passes in included if passes]
        batch_succeeded = True
        if deliverable:
            subscriber = cast(BatchSubscriber, self.subscription.subscriber)
            assert self._flow_controller is not None
            start_time = time.perf_counter()
            async with await self._flow_controller.acquire():
                try:
                    op = lambda: settle_handler_result(subscriber.handle_batch(deliverable))  # noqa: E731
                    await self._call_guarded(op, "handle_batch")
                except Exception as e:
                    batch_succeeded = False
                    ex = {"subscription": self.subscription.name, "batch_size": len(deliverable), "error": error_summary(e)}  # fmt: skip
                    logger.warning("Live batch handler failed, falling back to per-event delivery", extra=ex)  # fmt: skip
                else:
                    if self._metrics:
                        per_ev_ms = (time.perf_counter() - start_time) * 1000 / len(deliverable)
                        for ev in deliverable:
                            self._metrics.record_event_processed(ev.event_type, per_ev_ms)
        if not batch_succeeded:
            for envelope, _passes in included:
                await self._process_live_event(envelope)
            return
        last_env: EventEnvelope | None = None
        for envelope, passes in included:
            if not passes:
                self._stats.events_skipped_filtered += 1
                await self._record_filtered(envelope)
                continue
            self._stats.events_processed += 1
            last_env = envelope
            if self._metrics:
                self._metrics.record_lag(self.subscription.lag)
            pos, ev = envelope.position or self.subscription.last_processed_position, envelope.event
            await self.subscription.record_event_processed(pos, ev.event_id, ev.event_type)
            if envelope.position is not None:
                async with checkpoint_errors(self.subscription.name, str(ev.event_id)):
                    await self._maybe_checkpoint_in_batch(envelope.position, ev)
        if last_env and last_env.position and self.config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH:  # fmt: skip
            async with checkpoint_errors(self.subscription.name, str(last_env.event.event_id)):
                await self._save_checkpoint_with_retry(last_env.position, last_env.event)


__all__ = ["LivePageDeliveryMixin"]
