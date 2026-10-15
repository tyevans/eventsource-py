"""Event and batch delivery operations for LiveRunner."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

from eventsource.application.subscriptions.config import CheckpointStrategy
from eventsource.application.subscriptions.subscriber import settle_handler_result
from eventsource.application.subscriptions.subscription import Subscription, render_position
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
)
from eventsource.ports.envelopes import EventEnvelope
from eventsource.ports.positions import Position
from eventsource.ports.subscribers import BatchSubscriber

if TYPE_CHECKING:
    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.application.subscriptions.filtering import EventFilter
    from eventsource.application.subscriptions.flow_control import FlowController
    from eventsource.application.subscriptions.metrics import SubscriptionMetrics
    from eventsource.application.subscriptions.retry import CircuitBreaker
    from eventsource.application.subscriptions.runners.live_models import LiveRunnerStats
    from eventsource.domain.event import DomainEvent

logger = logging.getLogger(__name__)


class LiveDeliveryMixin:
    """Mixin providing subscriber event delivery and filtering for live runner."""

    if TYPE_CHECKING:
        subscription: Subscription
        config: SubscriptionConfig
        _tracer: Tracer
        _metrics: SubscriptionMetrics | None
        _flow_controller: FlowController | None
        _filter: EventFilter | None
        _stats: LiveRunnerStats
        _handler_circuit_breaker: CircuitBreaker | None
        _batch_only: bool

        async def _record_filtered(self, envelope: EventEnvelope) -> None: ...
        async def _maybe_checkpoint_in_batch(self, pos: Position, event: DomainEvent) -> None: ...
        async def _maybe_checkpoint(self, position: Position, event: DomainEvent) -> None: ...
        async def _save_checkpoint_with_retry(self, pos: Position, event: DomainEvent) -> None: ...

    def _passes_filter(self, envelope: EventEnvelope) -> bool:
        """Whether the envelope's event passes the configured event filter."""
        return self._filter is None or self._filter.matches(envelope.event)

    async def _call_guarded[T](
        self, operation: Callable[[], Awaitable[T]], operation_name: str
    ) -> T:
        """Call `operation` under `processing_timeout` and optional circuit breaker."""

        async def bounded() -> T:
            async with asyncio.timeout(self.config.processing_timeout):
                return await operation()

        if self._handler_circuit_breaker is not None:
            return await self._handler_circuit_breaker.execute(bounded, operation_name)
        return await bounded()

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
                    extra = {
                        "subscription": self.subscription.name,
                        "batch_size": len(deliverable),
                        "error": str(e),
                    }
                    msg = "Live batch handler failed, falling back to per-event delivery"
                    logger.warning(msg, extra=extra)
                else:
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        per_event_ms = duration_ms / len(deliverable)
                        for ev in deliverable:
                            self._metrics.record_event_processed(ev.event_type, per_event_ms)

        if not batch_succeeded:
            for envelope, _passes in included:
                await self._process_live_event(envelope)
            return

        last_envelope: EventEnvelope | None = None
        for envelope, passes in included:
            if not passes:
                self._stats.events_skipped_filtered += 1
                await self._record_filtered(envelope)
                continue
            self._stats.events_processed += 1
            last_envelope = envelope
            if self._metrics:
                self._metrics.record_lag(self.subscription.lag)
            pos = envelope.position or self.subscription.last_processed_position
            ev = envelope.event
            await self.subscription.record_event_processed(pos, ev.event_id, ev.event_type)
            if envelope.position is not None:
                await self._maybe_checkpoint_in_batch(envelope.position, ev)

        if (
            last_envelope is not None
            and last_envelope.position is not None
            and self.config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH
        ):
            await self._save_checkpoint_with_retry(last_envelope.position, last_envelope.event)

    async def _process_live_event(self, envelope: EventEnvelope) -> None:
        """Process one envelope read from the global feed, applying filters."""
        event, position = envelope.event, envelope.position
        span_attrs = {
            ATTR_SUBSCRIPTION_NAME: self.subscription.name,
            ATTR_EVENT_ID: str(event.event_id),
            ATTR_EVENT_TYPE: event.event_type,
            ATTR_POSITION: render_position(position),
        }
        with self._tracer.span("eventsource.live_runner.process_event", span_attrs):
            if self._filter and not self._filter.matches(event):
                self._stats.events_skipped_filtered += 1
                extra = {
                    "subscription": self.subscription.name,
                    "event_id": str(event.event_id),
                    "event_type": event.event_type,
                }
                logger.debug("Event filtered out", extra=extra)
                if position is not None:
                    await self.subscription.record_event_processed(
                        position, event.event_id, event.event_type
                    )
                else:
                    await self.subscription.record_events_unseen(1)
                return

            assert self._flow_controller is not None
            async with await self._flow_controller.acquire():
                start_time = time.perf_counter()
                try:
                    sub = self.subscription.subscriber
                    if self._batch_only:
                        batch_sub = cast(BatchSubscriber, sub)
                        op = lambda: settle_handler_result(batch_sub.handle_batch([event]))  # noqa: E731
                        await self._call_guarded(op, "handle_batch")
                    else:
                        op = lambda: settle_handler_result(sub.handle(event))  # noqa: E731
                        await self._call_guarded(op, "handle_event")
                    self._stats.events_processed += 1
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        self._metrics.record_event_processed(event.event_type, duration_ms)
                        self._metrics.record_lag(self.subscription.lag)

                    pos = position or self.subscription.last_processed_position
                    await self.subscription.record_event_processed(
                        pos, event.event_id, event.event_type
                    )
                    if position is not None:
                        await self._maybe_checkpoint(position, event)
                except Exception as e:
                    self._stats.events_failed += 1
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        self._metrics.record_event_failed(
                            event.event_type, type(e).__name__, duration_ms
                        )
                    await self.subscription.record_event_failed(e)
                    if not self.config.continue_on_error:
                        raise
                    await self.subscription.record_events_unseen(1)
                    extra = {
                        "subscription": self.subscription.name,
                        "event_id": str(event.event_id),
                        "error": str(e),
                    }
                    logger.warning("Live event processing failed, continuing", extra=extra)


__all__ = ["LiveDeliveryMixin"]
