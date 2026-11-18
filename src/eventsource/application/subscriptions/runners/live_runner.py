"""LiveRunner implementation for real-time event processing."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.error_handling import SubscriptionErrorHandler
from eventsource.application.subscriptions.filtering import EventFilter
from eventsource.application.subscriptions.flow_control import FlowController
from eventsource.application.subscriptions.metrics import SubscriptionMetrics
from eventsource.application.subscriptions.retry import CircuitBreaker, RetryableOperation
from eventsource.application.subscriptions.runners.live_checkpoint import LiveCheckpointMixin
from eventsource.application.subscriptions.runners.live_delivery import LiveDeliveryMixin
from eventsource.application.subscriptions.runners.live_drain import LiveDrainMixin
from eventsource.application.subscriptions.runners.live_models import (  # fmt: skip
    LiveRunnerStats,
    _LiveEventHandler,
)
from eventsource.application.subscriptions.runners.live_runner_properties import (
    LiveRunnerPropertiesMixin,
)
from eventsource.application.subscriptions.subscription import Subscription, SubscriptionState
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import ATTR_SUBSCRIPTION_NAME
from eventsource.ports.subscribers import supports_batch_handling

if TYPE_CHECKING:
    from eventsource.ports.bus import SubscribableEventBus
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


@dataclass
class LiveRunner(LiveRunnerPropertiesMixin, LiveDrainMixin, LiveDeliveryMixin, LiveCheckpointMixin):
    """Wakes on bus notifications and delivers events read from the global feed."""

    event_bus: SubscribableEventBus
    checkpoint_repo: SubscriptionPositions
    event_feed: GlobalEventFeed
    subscription: Subscription
    tracer: Tracer | None = None
    enable_metrics: bool = True
    enable_tracing: bool = True
    error_handler: SubscriptionErrorHandler | None = None

    def __post_init__(self) -> None:
        # Standalone runners get a private handler (DLQ-less, no callbacks) so failures still classify.
        self._error_handler = self.error_handler or SubscriptionErrorHandler(self.subscription.name)
        self._running = self._subscribed = self._buffer_enabled = False
        self._buffered_wakes = self._paused_wakes = self._events_buffered_during_pause = 0
        self._last_checkpoint_time, self._stop_event, self._stats = 0.0, asyncio.Event(), LiveRunnerStats()  # fmt: skip
        self._handlers: dict[type[DomainEvent], _LiveEventHandler] = {}
        self._tracer = self.tracer or create_tracer(__name__, self.enable_tracing)
        self._enable_tracing, self.config = self._tracer.enabled, self.subscription.config
        self._flow_controller, subscriber = FlowController(), self.subscription.subscriber
        self._has_handle, self._batch_capable = callable(getattr(subscriber, "handle", None)), supports_batch_handling(subscriber)  # fmt: skip
        self._batch_only = not self._has_handle and self._batch_capable
        if not self._has_handle and not self._batch_capable:
            raise TypeError(f"Subscriber {type(subscriber).__name__!r} for subscription {self.subscription.name!r} implements neither handle() nor handle_batch(); it satisfies no subscriber Protocol and has no way to receive events.")  # fmt: skip
        self._filter = EventFilter.from_config_and_subscriber(self.config, subscriber)
        self._infra_circuit_breaker = self._handler_circuit_breaker = None
        if self.config.circuit_breaker_enabled:
            cb = self.config.get_circuit_breaker_config()
            self._infra_circuit_breaker, self._handler_circuit_breaker = CircuitBreaker(cb), CircuitBreaker(cb)  # fmt: skip
        self._retry = RetryableOperation(config=self.config.get_retry_config(), circuit_breaker=self._infra_circuit_breaker)  # fmt: skip
        self._metrics = SubscriptionMetrics(self.subscription.name, self.enable_metrics)

    async def start(self, buffer_events: bool = False) -> None:
        with self._tracer.span("eventsource.live_runner.start", {ATTR_SUBSCRIPTION_NAME: self.subscription.name}):  # fmt: skip
            if self._running:
                return
            self._running, self._buffer_enabled = True, buffer_events
            self._stop_event.clear()
            self._last_checkpoint_time = time.monotonic()
            self._subscribe_to_bus()
            if not buffer_events:
                await self.subscription.transition_to(SubscriptionState.LIVE)
                if self._metrics:
                    self._metrics.record_state("live")
            log_extra: dict[str, object] = {"subscription": self.subscription.name, "buffer_enabled": buffer_events}  # fmt: skip
            if self.config.tenant_id:
                log_extra["tenant_id"] = str(self.config.tenant_id)
            logger.info("Live runner started", extra=log_extra)

    async def stop(self) -> None:
        with self._tracer.span("eventsource.live_runner.stop", {ATTR_SUBSCRIPTION_NAME: self.subscription.name}):  # fmt: skip
            if not self._running:
                return
            self._running, self._subscribed = False, False
            self._stop_event.set()
            for event_type, handler in self._handlers.items():
                self.event_bus.unsubscribe(event_type, handler)
            self._handlers.clear()
            await self.clear_buffer()
            s = self._stats
            stats = {"received": s.events_received, "processed": s.events_processed, "skipped_filtered": s.events_skipped_filtered, "failed": s.events_failed}  # fmt: skip
            logger.info("Live runner stopped", extra={"subscription": self.subscription.name, "stats": stats})  # fmt: skip


__all__ = ["LiveRunner"]
