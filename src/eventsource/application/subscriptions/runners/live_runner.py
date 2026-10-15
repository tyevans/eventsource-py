"""LiveRunner implementation for real-time event processing."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.filtering import EventFilter, FilterStats
from eventsource.application.subscriptions.flow_control import FlowController, FlowControlStats
from eventsource.application.subscriptions.metrics import SubscriptionMetrics
from eventsource.application.subscriptions.retry import CircuitBreaker, RetryableOperation
from eventsource.application.subscriptions.runners.live_checkpoint import LiveCheckpointMixin
from eventsource.application.subscriptions.runners.live_delivery import LiveDeliveryMixin
from eventsource.application.subscriptions.runners.live_drain import LiveDrainMixin
from eventsource.application.subscriptions.runners.live_models import (
    LiveRunnerStats,
    _LiveEventHandler,
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
class LiveRunner(LiveDrainMixin, LiveDeliveryMixin, LiveCheckpointMixin):
    """Wakes on bus notifications and delivers events read from the global feed."""

    event_bus: SubscribableEventBus
    checkpoint_repo: SubscriptionPositions
    event_feed: GlobalEventFeed
    subscription: Subscription
    tracer: Tracer | None = None
    enable_metrics: bool = True
    enable_tracing: bool = True

    _running: bool = field(default=False, init=False, repr=False)
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)
    _subscribed: bool = field(default=False, init=False, repr=False)
    _buffered_wakes: int = field(default=0, init=False, repr=False)
    _buffer_enabled: bool = field(default=False, init=False, repr=False)
    _paused_wakes: int = field(default=0, init=False, repr=False)
    _events_buffered_during_pause: int = field(default=0, init=False, repr=False)
    _stats: LiveRunnerStats = field(default_factory=LiveRunnerStats, init=False, repr=False)
    _last_checkpoint_time: float = field(default=0.0, init=False, repr=False)
    _flow_controller: FlowController | None = field(default=None, init=False, repr=False)
    _filter: EventFilter | None = field(default=None, init=False, repr=False)
    _infra_circuit_breaker: CircuitBreaker | None = field(default=None, init=False, repr=False)
    _handler_circuit_breaker: CircuitBreaker | None = field(default=None, init=False, repr=False)
    _retry: RetryableOperation | None = field(default=None, init=False, repr=False)
    _metrics: SubscriptionMetrics | None = field(default=None, init=False, repr=False)
    _handlers: dict[type[DomainEvent], _LiveEventHandler] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        self._tracer = self.tracer or create_tracer(__name__, self.enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self.config = self.subscription.config
        self._flow_controller = FlowController()
        subscriber = self.subscription.subscriber
        self._has_handle = callable(getattr(subscriber, "handle", None))
        self._batch_capable = supports_batch_handling(subscriber)
        self._batch_only = not self._has_handle and self._batch_capable
        if not self._has_handle and not self._batch_capable:
            msg = (
                f"Subscriber {type(subscriber).__name__!r} for subscription "
                f"{self.subscription.name!r} implements neither handle() nor "
                "handle_batch(); it satisfies no subscriber Protocol and has "
                "no way to receive events."
            )
            raise TypeError(msg)
        self._filter = EventFilter.from_config_and_subscriber(self.config, subscriber)
        if self.config.circuit_breaker_enabled:
            cb_cfg = self.config.get_circuit_breaker_config()
            self._infra_circuit_breaker = CircuitBreaker(cb_cfg)
            self._handler_circuit_breaker = CircuitBreaker(cb_cfg)
        self._retry = RetryableOperation(
            config=self.config.get_retry_config(), circuit_breaker=self._infra_circuit_breaker
        )
        self._metrics = SubscriptionMetrics(self.subscription.name, self.enable_metrics)

    async def start(self, buffer_events: bool = False) -> None:
        span_attrs = {ATTR_SUBSCRIPTION_NAME: self.subscription.name}
        with self._tracer.span("eventsource.live_runner.start", span_attrs):
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
            log_extra: dict[str, object] = {
                "subscription": self.subscription.name,
                "buffer_enabled": buffer_events,
            }
            if self.config.tenant_id:
                log_extra["tenant_id"] = str(self.config.tenant_id)
            logger.info("Live runner started", extra=log_extra)

    async def stop(self) -> None:
        span_attrs = {ATTR_SUBSCRIPTION_NAME: self.subscription.name}
        with self._tracer.span("eventsource.live_runner.stop", span_attrs):
            if not self._running:
                return
            self._running, self._subscribed = False, False
            self._stop_event.set()
            for event_type, handler in self._handlers.items():
                self.event_bus.unsubscribe(event_type, handler)
            self._handlers.clear()
            await self.clear_buffer()
            stats = {
                "received": self._stats.events_received,
                "processed": self._stats.events_processed,
                "skipped_filtered": self._stats.events_skipped_filtered,
                "failed": self._stats.events_failed,
            }
            logger.info(
                "Live runner stopped",
                extra={"subscription": self.subscription.name, "stats": stats},
            )

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def _stop_requested(self) -> bool:
        return self._stop_event.is_set()

    @property
    def buffer_size(self) -> int:
        return self._buffered_wakes

    @property
    def pause_buffer_size(self) -> int:
        return self._paused_wakes

    @property
    def events_buffered_during_pause(self) -> int:
        return self._events_buffered_during_pause

    @property
    def stats(self) -> LiveRunnerStats:
        return self._stats

    @property
    def flow_controller(self) -> FlowController:
        assert self._flow_controller is not None
        return self._flow_controller

    @property
    def flow_control_stats(self) -> FlowControlStats:
        return self.flow_controller.stats

    @property
    def handler_circuit_breaker(self) -> CircuitBreaker | None:
        return self._handler_circuit_breaker

    @property
    def infra_circuit_breaker(self) -> CircuitBreaker | None:
        return self._infra_circuit_breaker

    @property
    def retry_operation(self) -> RetryableOperation | None:
        return self._retry

    @property
    def event_filter(self) -> EventFilter:
        assert self._filter is not None
        return self._filter

    @property
    def filter_stats(self) -> FilterStats:
        return self.event_filter.stats

    @property
    def metrics(self) -> SubscriptionMetrics:
        assert self._metrics is not None
        return self._metrics


__all__ = ["LiveRunner"]
