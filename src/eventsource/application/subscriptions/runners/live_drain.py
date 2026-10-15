"""Feed draining and event bus notification handling for LiveRunner."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.runners.live_models import _LiveEventHandler
from eventsource.application.subscriptions.subscription import Subscription, SubscriptionState
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_BUFFER_SIZE,
    ATTR_EVENTS_PROCESSED,
    ATTR_SUBSCRIPTION_NAME,
)
from eventsource.ports.envelopes import EventEnvelope, FeedReadOptions
from eventsource.ports.subscribers import get_subscribed_event_types

if TYPE_CHECKING:
    import asyncio

    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.application.subscriptions.metrics import SubscriptionMetrics
    from eventsource.application.subscriptions.runners.live_models import LiveRunnerStats
    from eventsource.domain.event import DomainEvent
    from eventsource.ports.bus import SubscribableEventBus
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


class LiveDrainMixin:
    """Mixin providing feed draining and bus subscription for live runner."""

    if TYPE_CHECKING:
        event_bus: SubscribableEventBus
        event_feed: GlobalEventFeed
        subscription: Subscription
        config: SubscriptionConfig
        _tracer: Tracer
        _metrics: SubscriptionMetrics | None
        _stats: LiveRunnerStats
        _stop_event: asyncio.Event
        _subscribed: bool
        _buffer_enabled: bool
        _buffered_wakes: int
        _paused_wakes: int
        _events_buffered_during_pause: int
        _batch_capable: bool
        _handlers: dict[type[DomainEvent], _LiveEventHandler]

        @property
        def _stop_requested(self) -> bool: ...

        def _passes_filter(self, envelope: EventEnvelope) -> bool: ...
        async def _deliver_page(self, included: list[tuple[EventEnvelope, bool]]) -> None: ...
        async def _process_live_event(self, envelope: EventEnvelope) -> None: ...

    def _subscribe_to_bus(self) -> None:
        event_types = get_subscribed_event_types(self.subscription.subscriber)
        for event_type in event_types:
            self._handlers[event_type] = handler = self._create_event_handler()
            self.event_bus.subscribe(event_type, handler)
        self._subscribed = True
        names = [et.__name__ for et in event_types]
        logger.debug(
            "Subscribed to event types",
            extra={"subscription": self.subscription.name, "event_types": names},
        )

    def _create_event_handler(self) -> _LiveEventHandler:
        return _LiveEventHandler(self)

    async def _handle_live_event(self, event: DomainEvent) -> None:
        del event
        sub = self.subscription.name
        if self._buffer_enabled:
            self._buffered_wakes += 1
            logger.debug(
                "Wake-up buffered", extra={"subscription": sub, "buffer_size": self._buffered_wakes}
            )
        elif self.subscription.is_paused:
            self._paused_wakes, self._events_buffered_during_pause = (
                self._paused_wakes + 1,
                self._events_buffered_during_pause + 1,
            )
            logger.debug(
                "Wake-up buffered during pause",
                extra={"subscription": sub, "pause_buffer_size": self._paused_wakes},
            )
        else:
            await self._drain_feed()

    async def _drain_feed(self) -> int:
        options = FeedReadOptions(tenant_id=self.config.tenant_id, limit=self.config.batch_size)
        if self._batch_capable:
            return await self._drain_feed_grouped(options)
        processed = 0
        while not self._stop_requested:
            pos = self.subscription.last_processed_position
            envelopes_in_batch, stopped = 0, False
            async for envelope in self.event_feed.read_all(from_position=pos, options=options):
                if not self._stop_requested:
                    await self.subscription.wait_if_paused(self._stop_event)
                if self._stop_requested:
                    stopped = True
                    break
                self._stats.events_received += 1
                await self.subscription.record_events_seen(1)
                await self._process_live_event(envelope)
                processed, envelopes_in_batch = processed + 1, envelopes_in_batch + 1
            if stopped or envelopes_in_batch < self.config.batch_size:
                break
        return processed

    async def _drain_feed_grouped(self, options: FeedReadOptions) -> int:
        processed = 0
        while not self._stop_requested:
            pos = self.subscription.last_processed_position
            page = [e async for e in self.event_feed.read_all(from_position=pos, options=options)]
            if not page:
                break
            included: list[tuple[EventEnvelope, bool]] = []
            for envelope in page:
                if not self._stop_requested:
                    await self.subscription.wait_if_paused(self._stop_event)
                if self._stop_requested:
                    break
                self._stats.events_received += 1
                await self.subscription.record_events_seen(1)
                included.append((envelope, self._passes_filter(envelope)))
            await self._deliver_page(included)
            processed += len(included)
            if len(included) < len(page) or len(page) < self.config.batch_size:
                break
        return processed

    async def process_buffer(self) -> int:
        sub = self.subscription.name
        attrs = {ATTR_SUBSCRIPTION_NAME: sub, ATTR_BUFFER_SIZE: self._buffered_wakes}
        with self._tracer.span("eventsource.live_runner.process_buffer", attrs) as span:
            self._buffered_wakes = 0
            processed = await self._drain_feed()
            if span:
                span.set_attribute(ATTR_EVENTS_PROCESSED, processed)
            logger.info(
                "Buffer processed", extra={"subscription": sub, "events_processed": processed}
            )
            return processed

    async def disable_buffer(self) -> None:
        self._buffer_enabled = False
        await self.subscription.transition_to(SubscriptionState.LIVE)
        if self._metrics:
            self._metrics.record_state("live")
        logger.info(
            "Buffer disabled, now processing live", extra={"subscription": self.subscription.name}
        )

    async def process_pause_buffer(self) -> int:
        sub = self.subscription.name
        attrs = {ATTR_SUBSCRIPTION_NAME: sub, ATTR_BUFFER_SIZE: self._paused_wakes}
        with self._tracer.span("eventsource.live_runner.process_pause_buffer", attrs) as span:
            logger.info(
                "Processing pause buffer",
                extra={"subscription": sub, "pause_buffer_size": self._paused_wakes},
            )
            self._paused_wakes = 0
            processed = await self._drain_feed()
            if span:
                span.set_attribute(ATTR_EVENTS_PROCESSED, processed)
            logger.info(
                "Pause buffer processed", extra={"subscription": sub, "events_processed": processed}
            )
            self._events_buffered_during_pause = 0
            return processed

    async def clear_buffer(self) -> int:
        dropped = self._buffered_wakes + self._paused_wakes
        self._buffered_wakes = self._paused_wakes = self._events_buffered_during_pause = 0
        self._buffer_enabled = False
        await self.subscription.reconcile_lag(0)
        return dropped


__all__ = ["LiveDrainMixin"]
