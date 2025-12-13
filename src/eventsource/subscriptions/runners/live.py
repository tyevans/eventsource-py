"""
Live runner for receiving real-time events from the event bus.

The LiveRunner subscribes to the event bus and delivers live events
to the subscriber, tracking position and handling duplicates during
the catch-up to live transition.

This module provides:
- LiveRunnerStats: Statistics for live event processing
- LiveRunner: Runner for real-time event processing
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from eventsource.events.base import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BUFFER_SIZE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_EVENTS_PROCESSED,
    ATTR_EVENTS_SKIPPED,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
)
from eventsource.subscriptions.config import CheckpointStrategy
from eventsource.subscriptions.filtering import EventFilter, FilterStats
from eventsource.subscriptions.flow_control import FlowController, FlowControlStats
from eventsource.subscriptions.metrics import SubscriptionMetrics
from eventsource.subscriptions.retry import (
    TRANSIENT_EXCEPTIONS,
    CircuitBreaker,
    RetryableOperation,
)
from eventsource.subscriptions.subscription import Subscription, SubscriptionState

if TYPE_CHECKING:
    from eventsource.bus.interface import EventBus
    from eventsource.repositories.checkpoint import CheckpointRepository

logger = logging.getLogger(__name__)


@dataclass
class LiveRunnerStats:
    """
    Statistics for live event processing.

    Attributes:
        events_received: Total events received from the event bus
        events_processed: Events successfully processed by subscriber
        events_skipped_duplicate: Events skipped due to duplicate detection
        events_skipped_filtered: Events skipped due to event type filtering
        events_failed: Events that failed during processing
    """

    events_received: int = 0
    events_processed: int = 0
    events_skipped_duplicate: int = 0
    events_skipped_filtered: int = 0
    events_failed: int = 0


@dataclass
class LiveRunner:
    """
    Receives real-time events from the event bus and delivers to subscriber.

    The LiveRunner handles:
    - Subscribing to the event bus
    - Delivering events to the subscriber
    - Tracking position and filtering duplicates
    - Checkpointing according to configuration

    During the catch-up to live transition, the LiveRunner may buffer
    events and filter duplicates based on position.

    Attributes:
        event_bus: Event bus to subscribe to
        checkpoint_repo: Checkpoint repository for persistence
        subscription: The subscription being processed

    Example:
        >>> runner = LiveRunner(event_bus, checkpoint_repo, subscription)
        >>> await runner.start()
        >>> # Events now being delivered to subscriber
        >>> await runner.stop()
    """

    event_bus: "EventBus"
    checkpoint_repo: "CheckpointRepository"
    subscription: Subscription
    tracer: Tracer | None = None
    enable_metrics: bool = True
    enable_tracing: bool = True

    # Internal state - not part of init
    _running: bool = field(default=False, init=False, repr=False)
    _subscribed: bool = field(default=False, init=False, repr=False)
    _buffer: asyncio.Queue[DomainEvent] = field(
        default_factory=asyncio.Queue, init=False, repr=False
    )
    _buffer_enabled: bool = field(default=False, init=False, repr=False)
    _pause_buffer: asyncio.Queue[DomainEvent] = field(
        default_factory=asyncio.Queue, init=False, repr=False
    )
    _events_buffered_during_pause: int = field(default=0, init=False, repr=False)
    _stats: LiveRunnerStats = field(default_factory=LiveRunnerStats, init=False, repr=False)
    _last_checkpoint_time: float = field(default=0.0, init=False, repr=False)
    _flow_controller: FlowController | None = field(default=None, init=False, repr=False)
    _filter: EventFilter | None = field(default=None, init=False, repr=False)
    _circuit_breaker: CircuitBreaker | None = field(default=None, init=False, repr=False)
    _retry: RetryableOperation | None = field(default=None, init=False, repr=False)
    _metrics: SubscriptionMetrics | None = field(default=None, init=False, repr=False)
    _handlers: dict[type[DomainEvent], "_LiveEventHandler"] = field(
        default_factory=dict, init=False, repr=False
    )

    def __post_init__(self) -> None:
        """Initialize config reference, flow controller, filter, retry mechanism, metrics and tracing."""
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = self.tracer or create_tracer(__name__, self.enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self.config = self.subscription.config
        self._flow_controller = FlowController(
            max_in_flight=self.config.max_in_flight,
            backpressure_threshold=self.config.backpressure_threshold,
        )

        # Event filtering - create from config/subscriber
        self._filter = EventFilter.from_config_and_subscriber(
            self.config,
            self.subscription.subscriber,
        )

        # Initialize retry mechanism with optional circuit breaker
        if self.config.circuit_breaker_enabled:
            self._circuit_breaker = CircuitBreaker(self.config.get_circuit_breaker_config())

        self._retry = RetryableOperation(
            config=self.config.get_retry_config(),
            circuit_breaker=self._circuit_breaker,
        )

        # Metrics instrumentation
        self._metrics = SubscriptionMetrics(
            subscription_name=self.subscription.name,
            enable_metrics=self.enable_metrics,
        )

    async def start(self, buffer_events: bool = False) -> None:
        """
        Start receiving live events.

        Args:
            buffer_events: If True, buffer events instead of processing immediately.
                          Used during catch-up to live transition.
        """
        with self._tracer.span(
            "eventsource.live_runner.start",
            {ATTR_SUBSCRIPTION_NAME: self.subscription.name},
        ):
            if self._running:
                return

            self._running = True
            self._buffer_enabled = buffer_events
            self._last_checkpoint_time = time.monotonic()

            # Subscribe to event bus for all event types the subscriber handles
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

    def _subscribe_to_bus(self) -> None:
        """Subscribe to the event bus with our internal handler."""
        # Get event types from subscriber
        event_types = self.subscription.subscriber.subscribed_to()

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

    def _create_event_handler(self) -> "_LiveEventHandler":
        """Create a handler wrapper that routes events to our processing method."""
        return _LiveEventHandler(self)

    async def _handle_live_event(self, event: DomainEvent) -> None:
        """
        Handle a live event from the event bus.

        Args:
            event: The event received from the bus
        """
        self._stats.events_received += 1

        if self._buffer_enabled:
            # During transition, buffer events for later processing
            await self._buffer.put(event)
            logger.debug(
                "Event buffered",
                extra={
                    "subscription": self.subscription.name,
                    "event_id": str(event.event_id),
                    "buffer_size": self._buffer.qsize(),
                },
            )
        elif self.subscription.is_paused:
            # During pause, buffer events for processing on resume
            await self._pause_buffer.put(event)
            self._events_buffered_during_pause += 1
            logger.debug(
                "Event buffered during pause",
                extra={
                    "subscription": self.subscription.name,
                    "event_id": str(event.event_id),
                    "pause_buffer_size": self._pause_buffer.qsize(),
                },
            )
        else:
            # Normal live processing
            await self._process_live_event(event)

    async def _process_live_event(
        self,
        event: DomainEvent,
        position: int | None = None,
    ) -> None:
        """
        Process a live event, checking for duplicates and applying filters.

        Args:
            event: The event to process
            position: Optional known position (from buffer processing)
        """
        # Try to get position for this event if not provided
        if position is None:
            position = self._get_event_position(event)

        with self._tracer.span(
            "eventsource.live_runner.process_event",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_EVENT_ID: str(event.event_id),
                ATTR_EVENT_TYPE: event.event_type,
                ATTR_POSITION: position if position is not None else -1,
            },
        ):
            # Check for duplicate (already processed during catch-up)
            if position is not None and position <= self.subscription.last_processed_position:
                self._stats.events_skipped_duplicate += 1
                logger.debug(
                    "Skipping duplicate event",
                    extra={
                        "subscription": self.subscription.name,
                        "event_id": str(event.event_id),
                        "position": position,
                        "last_processed": self.subscription.last_processed_position,
                    },
                )
                return

            # Apply event type filtering
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
                # Still update position to track progress
                if position is not None:
                    await self.subscription.record_event_processed(
                        position=position,
                        event_id=event.event_id,
                        event_type=event.event_type,
                    )
                return

            # Acquire flow control slot (may block if at capacity)
            assert self._flow_controller is not None
            async with await self._flow_controller.acquire():
                # Deliver to subscriber
                start_time = time.perf_counter()
                try:
                    await self.subscription.subscriber.handle(event)
                    self._stats.events_processed += 1

                    # Record success metrics
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    if self._metrics:
                        self._metrics.record_event_processed(
                            event_type=event.event_type,
                            duration_ms=duration_ms,
                        )
                        # Update lag metric
                        self._metrics.record_lag(self.subscription.lag)

                    # Update subscription position if we have it
                    if position is not None:
                        await self.subscription.record_event_processed(
                            position=position,
                            event_id=event.event_id,
                            event_type=event.event_type,
                        )

                        # Checkpoint based on strategy
                        await self._maybe_checkpoint(position, event)
                    else:
                        # Update processed count without position
                        await self.subscription.record_event_processed(
                            position=self.subscription.last_processed_position,
                            event_id=event.event_id,
                            event_type=event.event_type,
                        )

                except Exception as e:
                    self._stats.events_failed += 1

                    # Record failure metrics
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

                    logger.warning(
                        "Live event processing failed, continuing",
                        extra={
                            "subscription": self.subscription.name,
                            "event_id": str(event.event_id),
                            "error": str(e),
                        },
                    )

    def _get_event_position(self, event: DomainEvent) -> int | None:
        """
        Get the global position for a live event.

        Live events from the bus may include position metadata.
        If not, we cannot track position for this event.

        Args:
            event: The event to look up

        Returns:
            Global position, or None if not available
        """
        # Check if position is attached to event (some buses may include it)
        if hasattr(event, "_global_position"):
            pos: int = event._global_position
            return pos

        # For now, return None - position lookup will be enhanced later
        # This is acceptable for Phase 1 as we handle None gracefully
        return None

    async def _maybe_checkpoint(self, position: int, event: DomainEvent) -> None:
        """
        Handle checkpointing based on configured strategy.

        Args:
            position: Global position of the event
            event: The event that was processed
        """
        if self.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT:
            await self._save_checkpoint_with_retry(position, event)
        elif self.config.checkpoint_strategy == CheckpointStrategy.PERIODIC:
            await self._maybe_save_periodic_checkpoint(position, event)
        # Note: EVERY_BATCH doesn't apply to live processing since events
        # arrive one at a time. We treat it like EVERY_EVENT for live mode.
        elif self.config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH:
            await self._save_checkpoint_with_retry(position, event)

    async def _save_checkpoint(self, position: int, event: DomainEvent) -> None:
        """
        Save checkpoint for the processed event (no retry).

        Args:
            position: Global position of the event
            event: The event that was processed
        """
        await self.checkpoint_repo.save_position(
            subscription_id=self.subscription.name,
            position=position,
            event_id=event.event_id,
            event_type=event.event_type,
        )

        self._last_checkpoint_time = time.monotonic()

        logger.debug(
            "Checkpoint saved",
            extra={
                "subscription": self.subscription.name,
                "position": position,
            },
        )

    async def _save_checkpoint_with_retry(
        self,
        position: int,
        event: DomainEvent,
    ) -> None:
        """
        Save checkpoint for the processed event with retry.

        Args:
            position: Global position of the event
            event: The event that was processed

        Raises:
            RetryError: If all retries are exhausted
        """

        async def save_checkpoint() -> None:
            await self.checkpoint_repo.save_position(
                subscription_id=self.subscription.name,
                position=position,
                event_id=event.event_id,
                event_type=event.event_type,
            )

        assert self._retry is not None
        await self._retry.execute(
            operation=save_checkpoint,
            name="save_checkpoint",
            retryable_exceptions=TRANSIENT_EXCEPTIONS,
        )

        self._last_checkpoint_time = time.monotonic()

        logger.debug(
            "Checkpoint saved",
            extra={
                "subscription": self.subscription.name,
                "position": position,
            },
        )

    async def _maybe_save_periodic_checkpoint(
        self,
        position: int,
        event: DomainEvent,
    ) -> None:
        """
        Save checkpoint if enough time has passed (for PERIODIC strategy).

        Args:
            position: Global position of the event
            event: The event to potentially checkpoint
        """
        current_time = time.monotonic()
        elapsed = current_time - self._last_checkpoint_time

        if elapsed >= self.config.checkpoint_interval_seconds:
            await self._save_checkpoint_with_retry(position, event)

    async def process_buffer(self) -> int:
        """
        Process all buffered events.

        Called after catch-up completes to process events that arrived
        during the transition.

        Returns:
            Number of events processed from buffer
        """
        with self._tracer.span(
            "eventsource.live_runner.process_buffer",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_BUFFER_SIZE: self._buffer.qsize(),
            },
        ) as span:
            processed = 0

            while not self._buffer.empty():
                try:
                    event = self._buffer.get_nowait()
                    await self._process_live_event(event)
                    processed += 1
                except asyncio.QueueEmpty:
                    break

            if span:
                span.set_attribute(ATTR_EVENTS_PROCESSED, processed)
                span.set_attribute(ATTR_EVENTS_SKIPPED, self._stats.events_skipped_duplicate)

            logger.info(
                "Buffer processed",
                extra={
                    "subscription": self.subscription.name,
                    "events_processed": processed,
                    "events_skipped": self._stats.events_skipped_duplicate,
                },
            )

            return processed

    async def disable_buffer(self) -> None:
        """
        Disable buffering and switch to direct processing.

        Called after catch-up to live transition completes.
        """
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
        Process all events buffered during pause.

        Called after subscription resumes to process events that arrived
        while the subscription was paused.

        Returns:
            Number of events processed from pause buffer
        """
        with self._tracer.span(
            "eventsource.live_runner.process_pause_buffer",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_BUFFER_SIZE: self._pause_buffer.qsize(),
            },
        ) as span:
            processed = 0

            logger.info(
                "Processing pause buffer",
                extra={
                    "subscription": self.subscription.name,
                    "pause_buffer_size": self._pause_buffer.qsize(),
                },
            )

            while not self._pause_buffer.empty():
                try:
                    event = self._pause_buffer.get_nowait()
                    await self._process_live_event(event)
                    processed += 1
                except asyncio.QueueEmpty:
                    break

            if span:
                span.set_attribute(ATTR_EVENTS_PROCESSED, processed)

            logger.info(
                "Pause buffer processed",
                extra={
                    "subscription": self.subscription.name,
                    "events_processed": processed,
                    "events_skipped": self._stats.events_skipped_duplicate,
                },
            )

            # Reset pause buffer tracking
            self._events_buffered_during_pause = 0

            return processed

    async def stop(self) -> None:
        """
        Stop the live runner.

        Unsubscribes from the event bus and stops processing.
        """
        with self._tracer.span(
            "eventsource.live_runner.stop",
            {ATTR_SUBSCRIPTION_NAME: self.subscription.name},
        ):
            if not self._running:
                return

            self._running = False

            # Unsubscribe from bus using stored handler references
            if self._subscribed:
                for event_type, handler in self._handlers.items():
                    self.event_bus.unsubscribe(event_type, handler)
                self._handlers.clear()
                self._subscribed = False

            logger.info(
                "Live runner stopped",
                extra={
                    "subscription": self.subscription.name,
                    "stats": {
                        "received": self._stats.events_received,
                        "processed": self._stats.events_processed,
                        "skipped_duplicate": self._stats.events_skipped_duplicate,
                        "skipped_filtered": self._stats.events_skipped_filtered,
                        "failed": self._stats.events_failed,
                    },
                },
            )

    @property
    def is_running(self) -> bool:
        """Check if the runner is active."""
        return self._running

    @property
    def buffer_size(self) -> int:
        """Get current buffer size."""
        return self._buffer.qsize()

    @property
    def pause_buffer_size(self) -> int:
        """Get current pause buffer size (events queued during pause)."""
        return self._pause_buffer.qsize()

    @property
    def events_buffered_during_pause(self) -> int:
        """Get total count of events buffered during current/last pause."""
        return self._events_buffered_during_pause

    @property
    def stats(self) -> LiveRunnerStats:
        """Get processing statistics."""
        return self._stats

    @property
    def flow_controller(self) -> FlowController:
        """
        Get the flow controller for this runner.

        Returns:
            The FlowController instance
        """
        assert self._flow_controller is not None
        return self._flow_controller

    @property
    def flow_control_stats(self) -> FlowControlStats:
        """
        Get flow control statistics.

        Returns:
            FlowControlStats snapshot
        """
        assert self._flow_controller is not None
        return self._flow_controller.stats

    @property
    def is_backpressured(self) -> bool:
        """
        Check if runner is experiencing backpressure.

        Returns:
            True if at or above backpressure threshold
        """
        assert self._flow_controller is not None
        return self._flow_controller.is_backpressured

    @property
    def circuit_breaker(self) -> CircuitBreaker | None:
        """
        Get the circuit breaker for this runner.

        Returns:
            CircuitBreaker instance if enabled, None otherwise
        """
        return self._circuit_breaker

    @property
    def retry_operation(self) -> RetryableOperation | None:
        """
        Get the retryable operation handler for this runner.

        Returns:
            RetryableOperation instance
        """
        return self._retry

    @property
    def event_filter(self) -> EventFilter:
        """
        Get the event filter for this runner.

        Returns:
            EventFilter instance
        """
        assert self._filter is not None
        return self._filter

    @property
    def filter_stats(self) -> FilterStats:
        """
        Get filter statistics.

        Returns:
            FilterStats snapshot
        """
        assert self._filter is not None
        return self._filter.stats

    @property
    def metrics(self) -> SubscriptionMetrics:
        """
        Get the metrics instance for this runner.

        Returns:
            SubscriptionMetrics instance
        """
        assert self._metrics is not None
        return self._metrics


class _LiveEventHandler:
    """
    Internal handler wrapper for event bus subscription.

    This class wraps the LiveRunner to provide a handler interface
    compatible with the EventBus subscription mechanism.
    """

    def __init__(self, runner: LiveRunner) -> None:
        """
        Initialize the handler wrapper.

        Args:
            runner: The LiveRunner to route events to
        """
        self._runner = runner

    async def handle(self, event: DomainEvent) -> None:
        """
        Handle an event from the event bus.

        Routes the event to the LiveRunner for processing.

        Args:
            event: The event to handle
        """
        await self._runner._handle_live_event(event)


__all__ = [
    "LiveRunner",
    "LiveRunnerStats",
]
