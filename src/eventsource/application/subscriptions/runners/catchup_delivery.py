"""Subscriber event and batch delivery operations for CatchUpRunner."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

from eventsource.application.subscriptions.subscriber import settle_handler_result
from eventsource.application.subscriptions.subscription import (
    Subscription,
    render_position,
)
from eventsource.observability import Tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
)
from eventsource.ports.envelopes import EventEnvelope
from eventsource.ports.subscribers import BatchSubscriber

if TYPE_CHECKING:
    from eventsource.application.subscriptions.config import SubscriptionConfig
    from eventsource.application.subscriptions.metrics import SubscriptionMetrics
    from eventsource.application.subscriptions.retry import CircuitBreaker

logger = logging.getLogger(__name__)


class CatchUpDeliveryMixin:
    """Mixin providing subscriber event delivery and timeout guarding for catch-up runner."""

    subscription: Subscription
    config: SubscriptionConfig
    _tracer: Tracer
    _metrics: SubscriptionMetrics
    _handler_circuit_breaker: CircuitBreaker | None

    async def _call_guarded[T](
        self,
        operation: Callable[[], Awaitable[T]],
        operation_name: str,
    ) -> T:
        """
        Call `operation` under `processing_timeout`, through the handler
        circuit breaker if one is configured.

        `config.processing_timeout` bounds **one handler call**: a
        `handle_batch()` of 500 events gets the same budget as a single
        `handle()`, because it is one call. Exceeding it raises `TimeoutError`,
        which is an ordinary handler failure from here on -- `continue_on_error`
        decides whether the subscription proceeds, and the event takes the same
        DLQ path any raising handler would.

        The timeout is applied **inside** the breaker rather than around it, so
        a run of hanging handlers opens the circuit exactly as a run of raising
        ones does.

        Args:
            operation: Zero-argument async callable to run
            operation_name: Name for logging/tracing

        Returns:
            The operation's result

        Raises:
            CircuitBreakerOpenError: If the breaker is open
            TimeoutError: If the call exceeds `config.processing_timeout`
            Exception: Whatever `operation` itself raises
        """

        async def bounded() -> T:
            async with asyncio.timeout(self.config.processing_timeout):
                return await operation()

        if self._handler_circuit_breaker is not None:
            return await self._handler_circuit_breaker.execute(bounded, operation_name)
        return await bounded()

    async def _deliver_batch(self, envelopes: list[EventEnvelope]) -> bool:
        """
        Deliver a batch of envelopes through `subscriber.handle_batch()`.

        Returns:
            True if the batch call succeeded. False if it raised, in which
            case the caller falls back to per-event delivery -- per
            `BatchSubscriber.handle_batch`'s documented contract
            (`ports/subscribers.py`).
        """
        events = [envelope.event for envelope in envelopes]
        with self._tracer.span(
            "eventsource.catchup_runner.deliver_batch",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_BATCH_SIZE: len(events),
            },
        ):
            start_time = time.perf_counter()
            try:
                subscriber = cast(BatchSubscriber, self.subscription.subscriber)
                await self._call_guarded(
                    lambda: settle_handler_result(subscriber.handle_batch(events)),
                    "handle_batch",
                )
            except Exception as e:
                logger.warning(
                    "handle_batch failed, falling back to single-event delivery",
                    extra={
                        "subscription": self.subscription.name,
                        "batch_size": len(events),
                        "error": str(e),
                    },
                )
                return False

            duration_ms = (time.perf_counter() - start_time) * 1000
            per_event_duration_ms = duration_ms / len(events) if events else 0.0
            for envelope in envelopes:
                self._metrics.record_event_processed(
                    event_type=envelope.event.event_type,
                    duration_ms=per_event_duration_ms,
                )
            self._metrics.record_lag(self.subscription.lag)
            return True

    async def _deliver_event(self, envelope: EventEnvelope) -> None:
        """
        Deliver an event to the subscriber.

        Args:
            envelope: The envelope to deliver

        Raises:
            Exception: If continue_on_error is False and handler fails
        """
        event = envelope.event
        with self._tracer.span(
            "eventsource.catchup_runner.deliver_event",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_EVENT_TYPE: event.event_type,
                ATTR_POSITION: render_position(envelope.position),
            },
        ):
            start_time = time.perf_counter()
            try:
                subscriber = self.subscription.subscriber
                await self._call_guarded(
                    lambda: settle_handler_result(subscriber.handle(event)), "handle_event"
                )
                # Record success metrics
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.record_event_processed(
                    event_type=event.event_type,
                    duration_ms=duration_ms,
                )
                # Update lag after each event
                self._metrics.record_lag(self.subscription.lag)
            except Exception as e:
                # Record failure metrics
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.record_event_failed(
                    event_type=event.event_type,
                    error_type=type(e).__name__,
                    duration_ms=duration_ms,
                )
                await self.subscription.record_event_failed(e)

                if not self.config.continue_on_error:
                    raise

                logger.warning(
                    "Event processing failed, continuing",
                    extra={
                        "subscription": self.subscription.name,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "position": render_position(envelope.position),
                        "error": str(e),
                    },
                )


__all__ = ["CatchUpDeliveryMixin"]
