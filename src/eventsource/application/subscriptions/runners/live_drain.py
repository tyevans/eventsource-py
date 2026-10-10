"""Feed draining and event bus notification handling for LiveRunner."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.runners.live_models import _LiveEventHandler
from eventsource.application.subscriptions.subscription import (
    Subscription,
    SubscriptionState,
)
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
        _running: bool
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
        """Subscribe to the event bus with our internal handler."""
        event_types = get_subscribed_event_types(self.subscription.subscriber)

        for event_type in event_types:
            handler = self._create_event_handler()
            self._handlers[event_type] = handler
            self.event_bus.subscribe(event_type, handler)

        self._subscribed = True

        logger.debug(
            "Subscribed to event types",
            extra={
                "subscription": self.subscription.name,
                "event_types": [et.__name__ for et in event_types],
            },
        )

    def _create_event_handler(self) -> _LiveEventHandler:
        """Create a handler wrapper that routes events to our processing method."""
        return _LiveEventHandler(self)

    async def _handle_live_event(self, event: DomainEvent) -> None:
        """
        Handle a wake-up notification from the event bus.

        `event` is never delivered directly -- it carries no position and
        the bus is not the ordered source of truth. It only signals that new
        work may exist. When not buffering or paused, this drains the global
        feed forward from the subscription's last checkpoint via
        `_drain_feed()`.
        """
        del event

        if self._buffer_enabled:
            self._buffered_wakes += 1
            logger.debug(
                "Wake-up buffered",
                extra={
                    "subscription": self.subscription.name,
                    "buffer_size": self._buffered_wakes,
                },
            )
        elif self.subscription.is_paused:
            self._paused_wakes += 1
            self._events_buffered_during_pause += 1
            logger.debug(
                "Wake-up buffered during pause",
                extra={
                    "subscription": self.subscription.name,
                    "pause_buffer_size": self._paused_wakes,
                },
            )
        else:
            await self._drain_feed()

    async def _drain_feed(self) -> int:
        """
        Read and process every envelope on the global feed past our checkpoint.

        Returns:
            Number of envelopes processed (including filtered ones)
        """
        processed = 0
        options = FeedReadOptions(tenant_id=self.config.tenant_id, limit=self.config.batch_size)

        if self._batch_capable:
            return await self._drain_feed_grouped(options)

        while not self._stop_requested:
            from_position = self.subscription.last_processed_position
            envelopes_in_batch = 0
            stopped = False

            async for envelope in self.event_feed.read_all(
                from_position=from_position, options=options
            ):
                if self._stop_requested:
                    stopped = True
                    break

                await self.subscription.wait_if_paused(self._stop_event)
                if self._stop_requested:
                    stopped = True
                    break

                self._stats.events_received += 1
                await self.subscription.record_events_seen(1)
                await self._process_live_event(envelope)
                processed += 1
                envelopes_in_batch += 1

            if stopped:
                break

            if envelopes_in_batch < self.config.batch_size:
                break

        return processed

    async def _drain_feed_grouped(self, options: FeedReadOptions) -> int:
        """
        Drain the feed delivering each read's envelopes through `handle_batch()`.

        Returns:
            Number of envelopes processed (including filtered ones)
        """
        processed = 0

        while not self._stop_requested:
            from_position = self.subscription.last_processed_position
            page = [
                envelope
                async for envelope in self.event_feed.read_all(
                    from_position=from_position, options=options
                )
            ]
            if not page:
                break

            included: list[tuple[EventEnvelope, bool]] = []
            for envelope in page:
                if self._stop_requested:
                    break
                await self.subscription.wait_if_paused(self._stop_event)
                if self._stop_requested:
                    break

                self._stats.events_received += 1
                await self.subscription.record_events_seen(1)
                included.append((envelope, self._passes_filter(envelope)))

            await self._deliver_page(included)
            processed += len(included)

            if len(included) < len(page):
                break
            if len(page) < self.config.batch_size:
                break

        return processed

    async def process_buffer(self) -> int:
        """
        Drain the global feed to deliver whatever arrived during buffering.

        Returns:
            Number of envelopes processed from the feed
        """
        with self._tracer.span(
            "eventsource.live_runner.process_buffer",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_BUFFER_SIZE: self._buffered_wakes,
            },
        ) as span:
            self._buffered_wakes = 0

            processed = await self._drain_feed()

            if span:
                span.set_attribute(ATTR_EVENTS_PROCESSED, processed)

            logger.info(
                "Buffer processed",
                extra={
                    "subscription": self.subscription.name,
                    "events_processed": processed,
                },
            )

            return processed

    async def disable_buffer(self) -> None:
        """Disable buffering and switch to direct processing."""
        self._buffer_enabled = False
        await self.subscription.transition_to(SubscriptionState.LIVE)
        if self._metrics:
            self._metrics.record_state("live")

        logger.info(
            "Buffer disabled, now processing live",
            extra={"subscription": self.subscription.name},
        )

    async def process_pause_buffer(self) -> int:
        """
        Drain the global feed to deliver whatever arrived during pause.

        Returns:
            Number of envelopes processed from the feed
        """
        with self._tracer.span(
            "eventsource.live_runner.process_pause_buffer",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_BUFFER_SIZE: self._paused_wakes,
            },
        ) as span:
            logger.info(
                "Processing pause buffer",
                extra={
                    "subscription": self.subscription.name,
                    "pause_buffer_size": self._paused_wakes,
                },
            )

            self._paused_wakes = 0

            processed = await self._drain_feed()

            if span:
                span.set_attribute(ATTR_EVENTS_PROCESSED, processed)

            logger.info(
                "Pause buffer processed",
                extra={
                    "subscription": self.subscription.name,
                    "events_processed": processed,
                },
            )

            self._events_buffered_during_pause = 0

            return processed

    async def clear_buffer(self) -> int:
        """
        Clear buffered wakes and pause buffer, reconciling subscription lag.

        Returns:
            Total count of dropped buffered wakes that were cleared.
        """
        dropped = self._buffered_wakes + self._paused_wakes
        self._buffered_wakes = 0
        self._paused_wakes = 0
        self._events_buffered_during_pause = 0
        self._buffer_enabled = False
        await self.subscription.reconcile_lag(0)
        return dropped


__all__ = ["LiveDrainMixin"]
