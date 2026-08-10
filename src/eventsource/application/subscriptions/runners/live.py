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
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from eventsource.application.subscriptions.config import CheckpointStrategy
from eventsource.application.subscriptions.filtering import EventFilter, FilterStats
from eventsource.application.subscriptions.flow_control import FlowController, FlowControlStats
from eventsource.application.subscriptions.metrics import SubscriptionMetrics
from eventsource.application.subscriptions.retry import (
    TRANSIENT_EXCEPTIONS,
    CircuitBreaker,
    RetryableOperation,
)
from eventsource.application.subscriptions.subscriber import settle_handler_result
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
from eventsource.ports.subscribers import (
    BatchSubscriber,
    get_subscribed_event_types,
    supports_batch_handling,
)

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
    # Mirrors `CatchUpRunner._stop_event`: a distinct signal from `_running`
    # so `stop()` can interrupt a drain already in flight (checked per
    # envelope in `_drain_feed`) without depending on `start()` having been
    # called first -- `_running` alone stays False in that case and would
    # otherwise prevent the drain from doing anything.
    #
    # An `asyncio.Event` rather than a bool because `wait_if_paused()` must be
    # able to wake on it: a drain parked waiting for a resume that may never
    # come has to be interruptible. `_stop_requested` below reads this event
    # rather than shadowing it, so there is one writable site for the fact.
    _stop_event: asyncio.Event = field(default_factory=asyncio.Event, init=False, repr=False)
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
    _infra_circuit_breaker: CircuitBreaker | None = field(default=None, init=False, repr=False)
    _handler_circuit_breaker: CircuitBreaker | None = field(default=None, init=False, repr=False)
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
        self._flow_controller = FlowController()

        # Which handler to call, decided once at construction -- a
        # subscriber's capabilities are a property of its class, not
        # something that changes mid-run (same rule as
        # `CatchUpRunner._batch_capable`).
        #
        # `BatchSubscriber` requires only `subscribed_to()` and
        # `handle_batch()`. This runner used to call `handle()`
        # unconditionally, so a subscriber implementing exactly that
        # published Protocol raised `AttributeError` per event once its
        # subscription went live -- recorded as a handler failure and,
        # under `continue_on_error`, potentially filling the DLQ with events
        # whose handler was never reached.
        #
        # A batch-capable subscriber is delivered to through `handle_batch()`
        # on both runners now (ADR 0063): `_drain_feed` groups the envelopes a
        # single bounded feed read already returned and dispatches them as one
        # unit. `_batch_only` is the narrower fact -- no `handle()` at all --
        # and is what the per-event path consults when it has to deliver one
        # envelope on its own (batch-handler fallback, or a non-batch-capable
        # subscriber, in which case it is False and `handle()` is called).
        subscriber = self.subscription.subscriber
        self._has_handle = callable(getattr(subscriber, "handle", None))
        self._batch_capable = supports_batch_handling(subscriber)
        self._batch_only = not self._has_handle and self._batch_capable
        if not self._has_handle and not self._batch_capable:
            raise TypeError(
                f"Subscriber {type(subscriber).__name__!r} for subscription "
                f"{self.subscription.name!r} implements neither handle() nor "
                "handle_batch(); it satisfies no subscriber Protocol and has "
                "no way to receive events."
            )

        # Event filtering - create from config/subscriber
        self._filter = EventFilter.from_config_and_subscriber(
            self.config,
            self.subscription.subscriber,
        )

        # Two independent circuit breakers, both built from the same
        # CircuitBreakerConfig -- one per failure domain. See
        # CatchUpRunner.__post_init__ for the full reasoning: a shared
        # breaker means a run of handler failures blocks checkpointing.
        if self.config.circuit_breaker_enabled:
            self._infra_circuit_breaker = CircuitBreaker(self.config.get_circuit_breaker_config())
            self._handler_circuit_breaker = CircuitBreaker(self.config.get_circuit_breaker_config())

        # Retry mechanism for infra ops (checkpoint-save) only. Handler
        # calls go through _call_guarded / _handler_circuit_breaker instead.
        self._retry = RetryableOperation(
            config=self.config.get_retry_config(),
            circuit_breaker=self._infra_circuit_breaker,
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
            self._stop_event.clear()
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

        When the subscriber is batch-capable, dispatch is grouped: the
        envelopes one bounded read already returned are handed to
        `handle_batch()` as a unit. See `_drain_page_grouped` for why that
        costs nothing in latency or stop/pause responsiveness (ADR 0063).

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

            # A batch shorter than the requested limit means the feed has
            # nothing more right now -- stop polling until the next wake-up.
            if envelopes_in_batch < self.config.batch_size:
                break

        return processed

    async def _drain_feed_grouped(self, options: FeedReadOptions) -> int:
        """
        Drain the feed delivering each read's envelopes through `handle_batch()`.

        **The batch is a page the feed already returned, never an accumulator.**
        Nothing is ever held back waiting for more events to arrive: the runner
        reads exactly what is available past the checkpoint (bounded by
        `config.batch_size`, the same read `_drain_feed` already issued) and
        dispatches that. A time-window accumulator was rejected -- it would buy
        throughput with the one thing the live path exists to provide (ADR 0063).
        Materializing the page adds no latency of its own either, because feed
        adapters build the whole result set before yielding its first envelope.

        Stop and pause are still checked **per envelope** during the scan, so a
        `stop()` or `pause()` that lands before dispatch takes effect at exactly
        the granularity it did before batching. What coarsens is only the window
        *inside one handler call* -- and that window is `processing_timeout`,
        which bounds one `handle_batch()` exactly as it bounds one `handle()`
        (`CatchUpRunner._call_guarded` documents the same budget for the same
        reason). Subscribers with no `handle_batch()` never reach this method.

        If `handle_batch()` raises, delivery falls back to the per-event path --
        `_process_live_event` per envelope, position recorded immediately after
        each -- exactly as `CatchUpRunner._process_batch_grouped` does, per
        `BatchSubscriber.handle_batch`'s documented contract.

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

            # Scan first -- same per-envelope stop/pause checks the
            # single-event path makes, just decoupled from dispatch since the
            # page is delivered as a whole.
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
                # Abandoned tail: stop or pause-interrupt landed mid-scan.
                break
            if len(page) < self.config.batch_size:
                break

        return processed

    def _passes_filter(self, envelope: EventEnvelope) -> bool:
        """Whether the envelope's event passes the configured event filter."""
        return self._filter is None or self._filter.matches(envelope.event)

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
            # Per-event delivery, with the ordinary per-event error handling
            # (`continue_on_error`, DLQ, circuit breaker) applying to each --
            # rather than inventing partial-batch semantics the runner has no
            # way to determine. Filtered envelopes are re-checked and skipped
            # by `_process_live_event` itself.
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

        # EVERY_BATCH now has a batch to attach to on the live path.
        if (
            last_envelope is not None
            and last_envelope.position is not None
            and self.config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH
        ):
            await self._save_checkpoint_with_retry(last_envelope.position, last_envelope.event)

    async def _record_filtered(self, envelope: EventEnvelope) -> None:
        """Record progress for an envelope the filter rejected."""
        if envelope.position is not None:
            await self.subscription.record_event_processed(
                position=envelope.position,
                event_id=envelope.event.event_id,
                event_type=envelope.event.event_type,
            )
        else:
            await self.subscription.record_events_unseen(1)

    async def _maybe_checkpoint_in_batch(self, position: Position, event: DomainEvent) -> None:
        """
        Per-event checkpointing inside a grouped delivery.

        `EVERY_BATCH` is deliberately absent here -- unlike `_maybe_checkpoint`,
        which treats it as `EVERY_EVENT` because the per-event path has no batch
        boundary to attach to. `_deliver_page` checkpoints once after the page.
        """
        if self.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT:
            await self._save_checkpoint_with_retry(position, event)
        elif self.config.checkpoint_strategy == CheckpointStrategy.PERIODIC:
            await self._maybe_save_periodic_checkpoint(position, event)

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
                    subscriber = self.subscription.subscriber
                    if self._batch_only:
                        # The subscriber's only handler. This path is reached
                        # for a single envelope -- the fallback after a
                        # `handle_batch()` that raised (ADR 0063).
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
        `continue_on_error` decides whether the subscription proceeds. Before
        this was enforced the field was read nowhere, so a hung live handler
        wedged the subscription silently and indefinitely.

        The timeout is applied **inside** the breaker rather than around it, so
        a run of hanging handlers opens the circuit exactly as a run of raising
        ones does. See `CatchUpRunner._call_guarded` -- both runners enforce
        the same budget at the same point, and the pair should stay in step.

        Guards the handler call (`handle()`) specifically --
        `self._handler_circuit_breaker`, never `self._infra_circuit_breaker`
        (which guards checkpoint-save via `self._retry` instead). The two
        are independent instances so a run of handler failures cannot block
        checkpointing, and a flaky checkpoint repo cannot silence a broken
        handler's signal.

        The call is gated but never retried here: `CircuitBreaker.execute()`
        calls `operation` at most once, raising `CircuitBreakerOpenError`
        instead of calling it at all when the breaker is OPEN. This is
        deliberately not routed through `self._retry`
        (`RetryableOperation`'s exponential-backoff loop) -- "nothing
        retries your handler" (docs/guides/subscriptions.md) stays true
        whether or not a circuit breaker is configured. Both success and
        failure feed the breaker's consecutive-failure count -- a single
        failure sandwiched between successes resets it to zero on the next
        success, so only a *run* of consecutive handler failures opens the
        circuit, not the occasional bad event (see the `handler_circuit_breaker`
        property docstring for why this means DLQ'd events need no special
        case).

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
            self._stop_event.set()

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
    def _stop_requested(self) -> bool:
        """Whether `stop()` has been called, derived from `_stop_event`.

        A read-only view, so the event stays the single writable site.
        """
        return self._stop_event.is_set()

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
    def handler_circuit_breaker(self) -> CircuitBreaker | None:
        """
        Get the circuit breaker guarding the subscriber's `handle()` calls.

        Every handler outcome -- success or failure -- feeds this breaker
        uniformly, regardless of `continue_on_error` or whether the event
        is later routed to the DLQ by the caller's own error handler.
        `CircuitBreaker.record_success` resets the consecutive-failure
        count to zero on the next success, so an isolated bad event never
        opens the circuit -- only a *run* of consecutive handler failures
        does. See `CatchUpRunner.handler_circuit_breaker` for the full
        reasoning.

        Independent from `infra_circuit_breaker`: a broken handler cannot
        block checkpointing, and a flaky checkpoint repo cannot mask a
        broken handler's signal.

        Returns:
            CircuitBreaker instance if `circuit_breaker_enabled`, None
            otherwise
        """
        return self._handler_circuit_breaker

    @property
    def infra_circuit_breaker(self) -> CircuitBreaker | None:
        """
        Get the circuit breaker guarding checkpoint-save via `self._retry`.

        Unchanged in behavior from before handler calls were gated:
        transient checkpoint-repo failures feed this breaker, not handler
        outcomes. Independent from `handler_circuit_breaker`.

        Returns:
            CircuitBreaker instance if `circuit_breaker_enabled`, None
            otherwise
        """
        return self._infra_circuit_breaker

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
