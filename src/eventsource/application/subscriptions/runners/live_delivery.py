"""Event and batch delivery operations for LiveRunner."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

from eventsource.application.subscriptions.config import SubscriptionConfig
from eventsource.application.subscriptions.runners.live_checkpoint_errors import checkpoint_errors
from eventsource.application.subscriptions.runners.live_page_delivery import LivePageDeliveryMixin
from eventsource.application.subscriptions.subscriber import settle_handler_result
from eventsource.application.subscriptions.subscription import Subscription, render_position
from eventsource.observability import Tracer

from eventsource.observability.attributes import ATTR_EVENT_ID, ATTR_EVENT_TYPE, ATTR_POSITION, ATTR_SUBSCRIPTION_NAME  # fmt: skip  # isort: skip
from eventsource.observability.error_summary import error_summary
from eventsource.ports.envelopes import EventEnvelope
from eventsource.ports.positions import Position
from eventsource.ports.subscribers import BatchSubscriber

if TYPE_CHECKING:
    from eventsource.application.subscriptions.error_handling import SubscriptionErrorHandler
    from eventsource.application.subscriptions.filtering import EventFilter
    from eventsource.application.subscriptions.flow_control import FlowController
    from eventsource.application.subscriptions.metrics import SubscriptionMetrics
    from eventsource.application.subscriptions.retry import CircuitBreaker
    from eventsource.application.subscriptions.runners.live_models import LiveRunnerStats
    from eventsource.domain.event import DomainEvent

logger = logging.getLogger(__name__)


class LiveDeliveryMixin(LivePageDeliveryMixin):
    """Mixin providing subscriber event delivery and filtering for live runner."""

    if TYPE_CHECKING:
        subscription: Subscription
        config: SubscriptionConfig
        _error_handler: SubscriptionErrorHandler
        _tracer: Tracer
        _metrics: SubscriptionMetrics | None
        _flow_controller: FlowController | None
        _filter: EventFilter | None
        _stats: LiveRunnerStats
        _handler_circuit_breaker: CircuitBreaker | None
        _batch_only: bool

        async def _maybe_checkpoint(self, position: Position, event: DomainEvent) -> None: ...

    def _passes_filter(self, envelope: EventEnvelope) -> bool:
        """Whether the envelope's event passes the configured event filter."""
        return self._filter is None or self._filter.matches(envelope.event)

    async def _call_guarded[T](self, op: Callable[[], Awaitable[T]], op_name: str) -> T:
        """Call `op` under `processing_timeout` and optional circuit breaker."""

        async def bounded() -> T:
            async with asyncio.timeout(self.config.processing_timeout):
                return await op()

        if self._handler_circuit_breaker is not None:
            return await self._handler_circuit_breaker.execute(bounded, op_name)
        return await bounded()

    async def _process_live_event(self, envelope: EventEnvelope) -> None:
        """Process one envelope read from the global feed, applying filters."""
        event, position = envelope.event, envelope.position
        sub_name, eid, etype = self.subscription.name, event.event_id, event.event_type
        span_attrs = {ATTR_SUBSCRIPTION_NAME: sub_name, ATTR_EVENT_ID: str(eid), ATTR_EVENT_TYPE: etype, ATTR_POSITION: render_position(position)}  # fmt: skip
        with self._tracer.span("eventsource.live_runner.process_event", span_attrs):
            if self._filter and not self._filter.matches(event):
                self._stats.events_skipped_filtered += 1
                logger.debug("Event filtered out", extra={"subscription": sub_name, "event_id": str(eid), "event_type": etype})  # fmt: skip
                if position is not None:
                    await self.subscription.record_event_processed(position, eid, etype)
                else:
                    await self.subscription.record_events_unseen(1)
                return
            assert self._flow_controller is not None
            async with await self._flow_controller.acquire():
                start_time = time.perf_counter()
                # Only the handler call is inside the try. A checkpoint failure is a
                # checkpoint error, not an event failure (see live_checkpoint_errors).
                try:
                    sub = self.subscription.subscriber
                    if self._batch_only:
                        await self._call_guarded(lambda: settle_handler_result(cast(BatchSubscriber, sub).handle_batch([event])), "handle_batch")  # fmt: skip
                    else:
                        await self._call_guarded(lambda: settle_handler_result(sub.handle(event)), "handle_event")  # fmt: skip
                except Exception as e:
                    self._stats.events_failed += 1
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        self._metrics.record_event_failed(etype, type(e).__name__, duration_ms)
                    await self.subscription.record_event_failed(e)
                    await self._error_handler.handle_error(e, envelope=envelope, event=event)
                    if not self.config.continue_on_error:
                        raise
                    if position is not None:
                        await self.subscription.advance_past_failed_event(position, eid, etype)
                        async with checkpoint_errors(sub_name, str(eid)):
                            await self._maybe_checkpoint(position, event)
                    else:
                        await self.subscription.record_events_unseen(1)
                    logger.warning("Live event processing failed, continuing", extra={"subscription": sub_name, "event_id": str(eid), "error": error_summary(e)})  # fmt: skip
                else:
                    self._stats.events_processed += 1
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        self._metrics.record_event_processed(etype, duration_ms)
                        self._metrics.record_lag(self.subscription.lag)
                    await self.subscription.record_event_processed(position or self.subscription.last_processed_position, eid, etype)  # fmt: skip
                    if position is not None:
                        async with checkpoint_errors(sub_name, str(eid)):
                            await self._maybe_checkpoint(position, event)


__all__ = ["LiveDeliveryMixin"]
