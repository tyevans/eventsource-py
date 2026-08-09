"""
Live runner for receiving real-time events from the event bus.

The LiveRunner subscribes to the event bus and delivers live events
to the subscriber, tracking position and handling duplicates during
the catch-up to live transition.

This module provides:
- LiveRunnerStats: Statistics for live event processing
- LiveRunner: Runner for real-time event processing
"""

import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.config import CheckpointStrategy
from eventsource.application.subscriptions.filtering import EventFilter, FilterStats
from eventsource.application.subscriptions.flow_control import FlowController, FlowControlStats
from eventsource.application.subscriptions.metrics import SubscriptionMetrics
from eventsource.application.subscriptions.retry import (
    TRANSIENT_EXCEPTIONS,
    CircuitBreaker,
    RetryableOperation,
)
from eventsource.application.subscriptions.subscription import (
    Subscription,
    SubscriptionState,
    render_position,
)
from eventsource.domain.event import DomainEvent
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BUFFER_SIZE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_EVENTS_PROCESSED,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
)
from eventsource.ports.envelopes import EventEnvelope, FeedReadOptions
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.ports.bus import SubscribableEventBus
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


@dataclass
class LiveRunnerStats:
    """
    Statistics for live event processing.

    Attributes:
        events_received: Total envelopes read from the global feed while draining
        events_processed: Events successfully processed by subscriber
        events_skipped_filtered: Events skipped due to event type filtering
        events_failed: Events that failed during processing
    """

    events_received: int = 0
    events_processed: int = 0
    events_skipped_filtered: int = 0
    events_failed: int = 0


@dataclass
class LiveRunner:
    """
    Wakes on bus notifications and delivers events read from the global feed.

    The store owns ordering; the bus is a wake-up signal only. A `DomainEvent`
    arriving from the bus carries no position and is never delivered directly
    -- it only tells the runner that new work may exist. On each wake, the
    runner reads `event_feed.read_all(from_position=...)` forward from the
    subscription's last checkpoint and processes every envelope it gets back,
    which is where the position for checkpointing actually comes from
    (ADR 0047). Because the global feed is the single ordered source, no
    duplicate suppression is needed between catch-up and live -- a position
    read once is never read again.

    The LiveRunner handles:
    - Subscribing to the event bus as a wake-up signal
    - Draining the global feed forward from the last checkpoint
    - Delivering feed envelopes to the subscriber
    - Checkpointing according to configuration

    During the catch-up to live transition, the LiveRunner buffers wake
    signals (not events) so nothing is drained until catch-up has caught the
    subscription's checkpoint up to the watermark.

    Attributes:
        event_bus: Event bus to subscribe to for wake-up notifications
        checkpoint_repo: Checkpoint repository for persistence
        event_feed: Global event feed to drain on each wake-up
        subscription: The subscription being processed

    Example:
        >>> runner = LiveRunner(event_bus, checkpoint_repo, event_feed, subscription)
        >>> await runner.start()
        >>> # Events now being delivered to subscriber
        >>> await runner.stop()
    """

    event_bus: "SubscribableEventBus"
    checkpoint_repo: "SubscriptionPositions"
    event_feed: "GlobalEventFeed"
    subscription: Subscription
    tracer: Tracer | None = None
    enable_metrics: bool = True
    enable_tracing: bool = True

    # Internal state - not part of init
    _running: bool = field(default=False, init=False, repr=False)
    # Mirrors `CatchUpRunner._stop_requested`: a distinct signal from
    # `_running` so `stop()` can interrupt a drain already in flight
    # (checked per envelope in `_drain_feed`) without depending on `start()`
    # having been called first -- `_running` alone stays False in that case
    # and would otherwise prevent the drain from doing anything.
    _stop_requested: bool = field(default=False, init=False, repr=False)
    _subscribed: bool = field(default=False, init=False, repr=False)
    # Counts of wake signals, not events. The feed is the source of truth for
    # what to process, so buffering only needs to remember *that* wakes
    # happened; the count is kept for `buffer_size`/lag observability.
    # These were `asyncio.Queue`s of `None`, which grew one interchangeable
    # sentinel per bus notification and were then discarded wholesale at
    # drain time -- unbounded retention of no information.
    _buffered_wakes: int = field(default=0, init=False, repr=False)
    _buffer_enabled: bool = field(default=False, init=False, repr=False)
    _paused_wakes: int = field(default=0, init=False, repr=False)
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
            self._stop_requested = False
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
        Handle a wake-up notification from the event bus.

        `event` is never delivered directly -- it carries no position and
        the bus is not the ordered source of truth. It only signals that new
        work may exist. When not buffering or paused, this drains the global
        feed forward from the subscription's last checkpoint via
        `_drain_feed()`, which is where events actually get delivered,
        checkpointed, and counted (`Subscription.lag`'s seen/delivered
        counters are recorded per envelope pulled from the feed, not per
        wake-up).

        Args:
            event: The (unused) event payload that triggered this wake-up
        """
        del event  # notification only -- see docstring

        if self._buffer_enabled:
            # During transition, remember that a wake happened; the buffer
            # is drained (from the feed, not from stored events) once
            # catch-up completes and disables buffering.
            self._buffered_wakes += 1
            logger.debug(
                "Wake-up buffered",
                extra={
                    "subscription": self.subscription.name,
                    "buffer_size": self._buffered_wakes,
                },
            )
        elif self.subscription.is_paused:
            # During pause, remember the wake for processing on resume.
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
            # Normal live processing: drain whatever is new on the feed.
            await self._drain_feed()

    async def _drain_feed(self) -> int:
        """
        Read and process every envelope on the global feed past our checkpoint.

        This is the single ordered source of live events: the subscription's
        `last_processed_position` is re-read before each bounded read is
        requested from `event_feed`, so a drain always starts exactly where
        the last one (or catch-up) left off. No duplicate suppression is
        needed -- a position is read from the feed at most once.

        Each read is bounded by `config.batch_size` (`FeedReadOptions.limit`,
        the same knob catch-up uses) rather than draining the feed in one
        unbounded call -- adapters materialize the full result set before
        yielding the first envelope, so an unbounded live read is a memory
        hazard on a busy feed. The loop re-reads bounded batches until one
        comes back short of the limit (the feed has nothing more right now).

        `_stop_requested` and `subscription.wait_if_paused()` are checked
        before every envelope, mirroring `CatchUpRunner._process_batch`, so
        `stop()` and `pause()` take effect mid-drain instead of only at the
        next wake-up.

        Returns:
            Number of envelopes processed (including filtered ones)
        """
        processed = 0
        options = FeedReadOptions(tenant_id=self.config.tenant_id, limit=self.config.batch_size)

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

                await self.subscription.wait_if_paused()
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

            # A batch shorter than the requested limit means the feed has
            # nothing more right now -- stop polling until the next wake-up.
            if envelopes_in_batch < self.config.batch_size:
                break

        return processed

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
                else:
                    # No position to record against, so nothing increments
                    # `_events_delivered` -- release the receipt instead of
                    # leaving it outstanding forever.
                    await self.subscription.record_events_unseen(1)
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

                    # Terminally disposed (possibly DLQ'd): the failure
                    # counters carry that signal, and lag must not report
                    # it as outstanding work forever.
                    await self.subscription.record_events_unseen(1)

                    logger.warning(
                        "Live event processing failed, continuing",
                        extra={
                            "subscription": self.subscription.name,
                            "event_id": str(event.event_id),
                            "error": str(e),
                        },
                    )

    async def _maybe_checkpoint(self, position: Position, event: DomainEvent) -> None:
        """
        Handle checkpointing based on configured strategy.

        Args:
            position: Global-feed position of the event
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

    async def _save_checkpoint(self, position: Position, event: DomainEvent) -> None:
        """
        Save checkpoint for the processed event (no retry).

        Args:
            position: Global-feed position of the event
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
                "position": render_position(position),
            },
        )

    async def _save_checkpoint_with_retry(
        self,
        position: Position,
        event: DomainEvent,
    ) -> None:
        """
        Save checkpoint for the processed event with retry.

        Args:
            position: Global-feed position of the event
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
                "position": render_position(position),
            },
        )

    async def _maybe_save_periodic_checkpoint(
        self,
        position: Position,
        event: DomainEvent,
    ) -> None:
        """
        Save checkpoint if enough time has passed (for PERIODIC strategy).

        Args:
            position: Global-feed position of the event
            event: The event to potentially checkpoint
        """
        current_time = time.monotonic()
        elapsed = current_time - self._last_checkpoint_time

        if elapsed >= self.config.checkpoint_interval_seconds:
            await self._save_checkpoint_with_retry(position, event)

    async def process_buffer(self) -> int:
        """
        Drain the global feed to deliver whatever arrived during buffering.

        Called after catch-up completes to process events that arrived
        during the transition. Buffered wake-up signals are discarded --
        the feed, not the queue, is the source of truth for what to
        process -- and a single drain picks up everything past the
        checkpoint regardless of how many wake-ups accumulated.

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
        Drain the global feed to deliver whatever arrived during pause.

        Called after subscription resumes to process events that arrived
        while the subscription was paused. As with `process_buffer`, the
        queued wake-up signals are discarded in favor of a single feed
        drain from the checkpoint.

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
            self._stop_requested = True

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
        return self._buffered_wakes

    @property
    def pause_buffer_size(self) -> int:
        """Get current pause buffer size (events queued during pause)."""
        return self._paused_wakes

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
