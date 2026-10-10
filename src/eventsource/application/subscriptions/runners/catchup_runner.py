"""Catch-up runner implementation for reading historical events from the event store."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.filtering import EventFilter, FilterStats
from eventsource.application.subscriptions.flow_control import FlowController, FlowControlStats
from eventsource.application.subscriptions.metrics import SubscriptionMetrics
from eventsource.application.subscriptions.retry import (
    CircuitBreaker,
    RetryableOperation,
)
from eventsource.application.subscriptions.runners.catchup_batch import CatchUpBatchMixin
from eventsource.application.subscriptions.runners.catchup_checkpoint import (
    CatchUpCheckpointMixin,
)
from eventsource.application.subscriptions.runners.catchup_delivery import (
    CatchUpDeliveryMixin,
)
from eventsource.application.subscriptions.runners.catchup_result import CatchUpResult
from eventsource.application.subscriptions.subscription import (
    Subscription,
    SubscriptionState,
    render_position,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_EVENTS_PROCESSED,
    ATTR_FROM_POSITION,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
    ATTR_TO_POSITION,
)
from eventsource.ports.positions import Position
from eventsource.ports.subscribers import supports_batch_handling

if TYPE_CHECKING:
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


class CatchUpRunner(CatchUpBatchMixin, CatchUpDeliveryMixin, CatchUpCheckpointMixin):
    """
    Reads historical events from the event store and delivers to subscriber.

    The CatchUpRunner handles:
    - Batch reading from EventStore.read_all()
    - Delivering events to the subscriber
    - Checkpointing according to configuration
    - Progress tracking and logging

    The runner processes events from the current subscription position up to
    a target position (typically obtained from GlobalEventFeed.current_position()).

    Example:
        >>> runner = CatchUpRunner(event_store, checkpoint_repo, subscription)
        >>> result = await runner.run_until_position(target_position=watermark)
        >>> if result.completed:
        ...     print(f"Processed {result.events_processed} events")
    """

    def __init__(
        self,
        event_store: GlobalEventFeed,
        checkpoint_repo: SubscriptionPositions,
        subscription: Subscription,
        event_filter: EventFilter | None = None,
        tracer: Tracer | None = None,
        enable_metrics: bool = True,
        enable_tracing: bool = True,
    ) -> None:
        """Initialize the catch-up runner."""
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self.event_store = event_store
        self.checkpoint_repo = checkpoint_repo
        self.subscription = subscription
        self.config = subscription.config

        self._running = False
        self._stop_event = asyncio.Event()
        self._last_checkpoint_time: float = 0.0
        self._reached_target = False

        if event_filter is not None:
            self._filter = event_filter
        else:
            self._filter = EventFilter.from_config_and_subscriber(
                self.config,
                subscription.subscriber,
            )

        self._batch_capable = supports_batch_handling(subscription.subscriber)
        self._flow_controller = FlowController()

        self._infra_circuit_breaker: CircuitBreaker | None = None
        self._handler_circuit_breaker: CircuitBreaker | None = None
        if self.config.circuit_breaker_enabled:
            self._infra_circuit_breaker = CircuitBreaker(self.config.get_circuit_breaker_config())
            self._handler_circuit_breaker = CircuitBreaker(self.config.get_circuit_breaker_config())

        self._retry = RetryableOperation(
            config=self.config.get_retry_config(),
            circuit_breaker=self._infra_circuit_breaker,
        )

        self._metrics = SubscriptionMetrics(
            subscription_name=subscription.name,
            enable_metrics=enable_metrics,
        )

    async def run_until_position(
        self,
        target_position: Position,
    ) -> CatchUpResult:
        """
        Run catch-up until reaching the target position.

        Reads events in batches from the current position until reaching the
        target position, a stop request, or an error. A batch that delivers
        nothing (every envelope filtered out) does not stop the loop -- only
        reaching the target position (or a stop request) does.

        Args:
            target_position: Position to catch up to

        Returns:
            CatchUpResult with processing statistics
        """
        start_position = self.subscription.last_processed_position

        with self._tracer.span(
            "eventsource.catchup_runner.run_until_position",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_FROM_POSITION: render_position(start_position),
                ATTR_TO_POSITION: render_position(target_position),
                ATTR_BATCH_SIZE: self.config.batch_size,
            },
        ) as span:
            self._running = True
            self._stop_event.clear()
            self._reached_target = False
            self._last_checkpoint_time = time.monotonic()
            total_processed = 0

            log_extra: dict[str, object] = {
                "subscription": self.subscription.name,
                "from_position": render_position(start_position),
                "to_position": render_position(target_position),
                "batch_size": self.config.batch_size,
                "checkpoint_strategy": self.config.checkpoint_strategy.value,
            }
            if self.config.tenant_id:
                log_extra["tenant_id"] = str(self.config.tenant_id)
            logger.info("Starting catch-up", extra=log_extra)

            try:
                await self.subscription.transition_to(SubscriptionState.CATCHING_UP)
                self._metrics.record_state("catching_up")
                self._metrics.record_lag(self.subscription.lag)

                while self._running and not self._stop_requested and not self._reached_target:
                    was_paused = await self.subscription.wait_if_paused(self._stop_event)
                    if was_paused:
                        if self._stop_requested or not self._running:
                            break
                        logger.debug(
                            "Catch-up resumed after pause",
                            extra={"subscription": self.subscription.name},
                        )

                    outcome = await self._process_batch(target_position)
                    total_processed += outcome.events_delivered

                completed = self._reached_target
                final_position = self.subscription.last_processed_position

                if span:
                    span.set_attribute(ATTR_EVENTS_PROCESSED, total_processed)
                    if (token := render_position(final_position)) is not None:
                        span.set_attribute(ATTR_POSITION, token)

                logger.info(
                    "Catch-up completed",
                    extra={
                        "subscription": self.subscription.name,
                        "events_processed": total_processed,
                        "final_position": render_position(final_position),
                        "completed": completed,
                    },
                )

                return CatchUpResult(
                    events_processed=total_processed,
                    final_position=final_position,
                    completed=completed,
                )

            except Exception as e:
                logger.error(
                    "Catch-up failed",
                    extra={
                        "subscription": self.subscription.name,
                        "error": str(e),
                        "position": render_position(self.subscription.last_processed_position),
                        "events_processed": total_processed,
                    },
                    exc_info=True,
                )
                return CatchUpResult(
                    events_processed=total_processed,
                    final_position=self.subscription.last_processed_position,
                    completed=False,
                    error=e,
                )
            finally:
                self._running = False

    async def stop(self) -> None:
        """Request the runner to stop gracefully."""
        self._stop_event.set()
        logger.info(
            "Catch-up stop requested",
            extra={"subscription": self.subscription.name},
        )

    @property
    def is_running(self) -> bool:
        """Check if the runner is currently processing."""
        return self._running

    @property
    def _stop_requested(self) -> bool:
        """Whether stop has been requested."""
        return self._stop_event.is_set()

    @property
    def stop_requested(self) -> bool:
        """Check if a stop has been requested."""
        return self._stop_event.is_set()

    @property
    def flow_controller(self) -> FlowController:
        """Get the flow controller for this runner."""
        return self._flow_controller

    @property
    def flow_control_stats(self) -> FlowControlStats:
        """Get flow control statistics."""
        return self._flow_controller.stats

    @property
    def handler_circuit_breaker(self) -> CircuitBreaker | None:
        """Get the circuit breaker guarding subscriber handler calls."""
        return self._handler_circuit_breaker

    @property
    def infra_circuit_breaker(self) -> CircuitBreaker | None:
        """Get the circuit breaker guarding infrastructure operations."""
        return self._infra_circuit_breaker

    @property
    def retry_operation(self) -> RetryableOperation:
        """Get the retryable operation handler for this runner."""
        return self._retry

    @property
    def event_filter(self) -> EventFilter:
        """Get the event filter for this runner."""
        return self._filter

    @property
    def filter_stats(self) -> FilterStats:
        """Get filter statistics."""
        return self._filter.stats

    @property
    def metrics(self) -> SubscriptionMetrics:
        """Get the metrics instance for this runner."""
        return self._metrics


__all__ = ["CatchUpRunner"]
