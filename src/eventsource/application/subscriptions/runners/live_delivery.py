"""Event and batch delivery operations for LiveRunner."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, cast

from eventsource.application.subscriptions.config import CheckpointStrategy
from eventsource.application.subscriptions.subscriber import settle_handler_result
from eventsource.application.subscriptions.subscription import (
    Subscription,
    render_position,
)
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

        async def _maybe_checkpoint_in_batch(
            self, position: Position, event: DomainEvent
        ) -> None: ...

        async def _maybe_checkpoint(self, position: Position, event: DomainEvent) -> None: ...

        async def _save_checkpoint_with_retry(
            self, position: Position, event: DomainEvent
        ) -> None: ...

        async def _maybe_save_periodic_checkpoint(
            self, position: Position, event: DomainEvent
        ) -> None: ...

    def _passes_filter(self, envelope: EventEnvelope) -> bool:
        """Whether the envelope's event passes the configured event filter."""
        return self._filter is None or self._filter.matches(envelope.event)

    async def _call_guarded[T](
        self,
        operation: Callable[[], Awaitable[T]],
        operation_name: str,
    ) -> T:
        """
        Call `operation` under `processing_timeout`, through the handler
        circuit breaker if one is configured.

        `config.processing_timeout` bounds one handler call. Exceeding it
        raises `TimeoutError`, an ordinary handler failure from here on:
        `continue_on_error` decides whether the subscription proceeds.

        The timeout is applied inside the breaker rather than around it, so
        a run of hanging handlers opens the circuit exactly as a run of raising
        ones does.
        """

        async def bounded() -> T:
            async with asyncio.timeout(self.config.processing_timeout):
                return await operation()

        if self._handler_circuit_breaker is not None:
            return await self._handler_circuit_breaker.execute(bounded, operation_name)
        return await bounded()

    async def _deliver_page(self, included: list[tuple[EventEnvelope, bool]]) -> None:
        """
        Deliver one scanned page as a batch, then record every envelope.

        Args:
            included: `(envelope, passes_filter)` pairs, in feed order
        """
        deliverable = [envelope.event for envelope, passes in included if passes]

        batch_succeeded = True
        if deliverable:
            subscriber = cast(BatchSubscriber, self.subscription.subscriber)
            assert self._flow_controller is not None
            start_time = time.perf_counter()
            async with await self._flow_controller.acquire():
                try:
                    await self._call_guarded(
                        lambda: settle_handler_result(subscriber.handle_batch(deliverable)),
                        "handle_batch",
                    )
                except Exception as e:
                    batch_succeeded = False
                    logger.warning(
                        "Live batch handler failed, falling back to per-event delivery",
                        extra={
                            "subscription": self.subscription.name,
                            "batch_size": len(deliverable),
                            "error": str(e),
                        },
                    )
                else:
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        for event in deliverable:
                            self._metrics.record_event_processed(
                                event_type=event.event_type,
                                duration_ms=duration_ms / len(deliverable),
                            )

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
            await self.subscription.record_event_processed(
                position=envelope.position
                if envelope.position is not None
                else self.subscription.last_processed_position,
                event_id=envelope.event.event_id,
                event_type=envelope.event.event_type,
            )
            if envelope.position is not None:
                await self._maybe_checkpoint_in_batch(envelope.position, envelope.event)

        if (
            last_envelope is not None
            and last_envelope.position is not None
            and self.config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH
        ):
            await self._save_checkpoint_with_retry(last_envelope.position, last_envelope.event)

    async def _process_live_event(self, envelope: EventEnvelope) -> None:
        """
        Process one envelope read from the global feed, applying filters.

        Args:
            envelope: The feed envelope to process
        """
        event = envelope.event
        position = envelope.position

        with self._tracer.span(
            "eventsource.live_runner.process_event",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_EVENT_TYPE: event.event_type,
                ATTR_POSITION: render_position(position),
            },
        ):
            if self._filter and not self._filter.matches(event):
                self._stats.events_skipped_filtered += 1
                logger.debug(
                    "Event filtered out",
                    extra={
                        "subscription": self.subscription.name,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                    },
                )
                if position is not None:
                    await self.subscription.record_event_processed(
                        position=position,
                        event_id=event.event_id,
                        event_type=event.event_type,
                    )
                else:
                    await self.subscription.record_events_unseen(1)
                return

            assert self._flow_controller is not None
            async with await self._flow_controller.acquire():
                start_time = time.perf_counter()
                try:
                    subscriber = self.subscription.subscriber
                    if self._batch_only:
                        batch_subscriber = cast(BatchSubscriber, subscriber)
                        await self._call_guarded(
                            lambda: settle_handler_result(batch_subscriber.handle_batch([event])),
                            "handle_batch",
                        )
                    else:
                        await self._call_guarded(
                            lambda: settle_handler_result(subscriber.handle(event)),
                            "handle_event",
                        )
                    self._stats.events_processed += 1

                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        self._metrics.record_event_processed(
                            event_type=event.event_type,
                            duration_ms=duration_ms,
                        )
                        self._metrics.record_lag(self.subscription.lag)

                    if position is not None:
                        await self.subscription.record_event_processed(
                            position=position,
                            event_id=event.event_id,
                            event_type=event.event_type,
                        )
                        await self._maybe_checkpoint(position, event)
                    else:
                        await self.subscription.record_event_processed(
                            position=self.subscription.last_processed_position,
                            event_id=event.event_id,
                            event_type=event.event_type,
                        )

                except Exception as e:
                    self._stats.events_failed += 1

                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        self._metrics.record_event_failed(
                            event_type=event.event_type,
                            error_type=type(e).__name__,
                            duration_ms=duration_ms,
                        )

                    await self.subscription.record_event_failed(e)

                    if not self.config.continue_on_error:
                        raise

                    await self.subscription.record_events_unseen(1)

                    logger.warning(
                        "Live event processing failed, continuing",
                        extra={
                            "subscription": self.subscription.name,
                            "event_id": str(event.event_id),
                            "error": str(e),
                        },
                    )


__all__ = ["LiveDeliveryMixin"]
