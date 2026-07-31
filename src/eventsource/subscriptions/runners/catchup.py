"""
Catch-up runner for reading historical events from the event store.

The CatchUpRunner reads events in batches from the event store,
delivers them to the subscriber, and persists checkpoints according
to the configured strategy.

This module provides:
- CatchUpResult: Result of a catch-up operation
- CatchUpRunner: Runner for historical event processing
"""

import logging
import time
from dataclasses import dataclass
from typing import TYPE_CHECKING

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BATCH_SIZE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_EVENTS_PROCESSED,
    ATTR_FROM_POSITION,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
    ATTR_TO_POSITION,
)
from eventsource.ports.envelopes import EventEnvelope, FeedReadOptions
from eventsource.ports.positions import Position
from eventsource.subscriptions.config import CheckpointStrategy
from eventsource.subscriptions.filtering import EventFilter, FilterStats
from eventsource.subscriptions.flow_control import FlowController, FlowControlStats
from eventsource.subscriptions.metrics import SubscriptionMetrics
from eventsource.subscriptions.retry import (
    TRANSIENT_EXCEPTIONS,
    CircuitBreaker,
    RetryableOperation,
)
from eventsource.subscriptions.subscription import (
    Subscription,
    SubscriptionState,
    render_position,
)

if TYPE_CHECKING:
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _BatchOutcome:
    """What one `_process_batch` call did.

    Two numbers, because they answer different questions and the public
    `CatchUpResult.events_processed` is defined by the second: how many
    envelopes the batch read (whether or not the filter passed them), and
    how many events reached the subscriber.
    """

    envelopes_read: int
    events_delivered: int


@dataclass
class CatchUpResult:
    """
    Result of a catch-up operation.

    Provides statistics and outcome information for a catch-up run.

    Attributes:
        events_processed: Number of events successfully processed
        final_position: Last processed global-feed position, None if nothing
            has been processed
        completed: True if caught up to target position
        error: Exception if catch-up failed, None otherwise
    """

    events_processed: int
    final_position: Position | None
    completed: bool
    error: Exception | None = None

    @property
    def success(self) -> bool:
        """Return True if catch-up completed without errors."""
        return self.completed and self.error is None


class CatchUpRunner:
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
        event_store: "GlobalEventFeed",
        checkpoint_repo: "SubscriptionPositions",
        subscription: Subscription,
        event_filter: EventFilter | None = None,
        tracer: Tracer | None = None,
        enable_metrics: bool = True,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the catch-up runner.

        Args:
            event_store: Event store to read from
            checkpoint_repo: Checkpoint repository for persistence
            subscription: The subscription being processed
            event_filter: Optional event filter. If not provided, creates one
                         from config and subscriber.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_metrics: Whether to enable OpenTelemetry metrics (default True)
            enable_tracing: Whether to enable OpenTelemetry tracing (default True).
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self.event_store = event_store
        self.checkpoint_repo = checkpoint_repo
        self.subscription = subscription
        self.config = subscription.config

        self._running = False
        self._stop_requested = False
        self._last_checkpoint_time: float = 0.0
        # Set once the feed is exhausted or an envelope past the target is
        # seen -- the loop's stop condition, since positions are opaque and
        # cannot be compared arithmetically against a remaining count.
        self._reached_target = False

        # Event filtering - create from config/subscriber if not provided
        if event_filter is not None:
            self._filter = event_filter
        else:
            self._filter = EventFilter.from_config_and_subscriber(
                self.config,
                subscription.subscriber,
            )

        # Flow control for backpressure
        self._flow_controller = FlowController(
            max_in_flight=self.config.max_in_flight,
            backpressure_threshold=self.config.backpressure_threshold,
        )

        # Retry mechanism with optional circuit breaker
        self._circuit_breaker: CircuitBreaker | None = None
        if self.config.circuit_breaker_enabled:
            self._circuit_breaker = CircuitBreaker(self.config.get_circuit_breaker_config())

        self._retry = RetryableOperation(
            config=self.config.get_retry_config(),
            circuit_breaker=self._circuit_breaker,
        )

        # Metrics instrumentation
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
            self._stop_requested = False
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
                # Transition to CATCHING_UP state
                await self.subscription.transition_to(SubscriptionState.CATCHING_UP)
                self._metrics.record_state("catching_up")

                self._metrics.record_lag(self.subscription.lag)

                # Process batches until we reach the target or are stopped.
                # Termination is `_reached_target` and the stop flags, not a
                # zero delivery: a batch can deliver nothing (every envelope
                # filtered) and still have advanced position with more feed
                # behind it. Progress is guaranteed regardless -- every
                # counted envelope, delivered or filtered, calls
                # `record_event_processed`, so the next read starts strictly
                # after it, and the position-None guard below converts the
                # only no-progress pathology into `_reached_target`.
                while self._running and not self._stop_requested and not self._reached_target:
                    # Check for pause and wait if paused
                    was_paused = await self.subscription.wait_if_paused()
                    if was_paused:
                        # After resume, check if we should stop
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

    async def _process_batch(self, target_position: Position) -> _BatchOutcome:
        """
        Process a single batch of events.

        Args:
            target_position: Position to stop at

        Returns:
            A `_BatchOutcome` with the number of envelopes read (whether or
            not the filter passed them) and the number of events delivered
            to the subscriber.

        Raises:
            RetryError: If event store read fails after all retries
        """
        current_position = self.subscription.last_processed_position

        # Read a full batch: overshoot is prevented by the per-envelope
        # comparison below, not by sizing the read, because the distance
        # to the target cannot be computed from opaque positions.
        envelopes = await self._read_batch_with_retry(current_position, self.config.batch_size)
        if not envelopes:
            self._reached_target = True
            return _BatchOutcome(envelopes_read=0, events_delivered=0)
        await self.subscription.record_events_seen(len(envelopes))

        events_in_batch = 0
        events_filtered = 0
        delivered_this_batch = 0
        last_envelope: EventEnvelope | None = None

        try:
            for envelope in envelopes:
                if self._stop_requested:
                    break

                # An envelope with no position cannot be ordered against
                # the target; stop rather than deliver it blind. In-tree
                # adapters always stamp one on feed reads, so this is a
                # guard, not a path.
                if envelope.position is None or envelope.position > target_position:
                    self._reached_target = True
                    break

                # Check for pause within batch processing
                await self.subscription.wait_if_paused()
                if self._stop_requested:
                    break

                # Apply event type filtering early before delivery
                if not self._filter.matches(envelope.event):
                    events_filtered += 1
                    # Still update position to track progress through the stream
                    await self.subscription.record_event_processed(
                        position=envelope.position,
                        event_id=envelope.event.event_id,
                        event_type=envelope.event.event_type,
                    )
                    delivered_this_batch += 1
                    last_envelope = envelope
                    continue

                # Acquire flow control slot (may block if at capacity)
                async with await self._flow_controller.acquire():
                    # Deliver event to subscriber
                    await self._deliver_event(envelope)

                    # Update subscription position
                    await self.subscription.record_event_processed(
                        position=envelope.position,
                        event_id=envelope.event.event_id,
                        event_type=envelope.event.event_type,
                    )

                last_envelope = envelope
                events_in_batch += 1
                delivered_this_batch += 1

                # Handle checkpoint strategies
                if self.config.checkpoint_strategy == CheckpointStrategy.EVERY_EVENT:
                    await self._save_checkpoint_with_retry(envelope)
                elif self.config.checkpoint_strategy == CheckpointStrategy.PERIODIC:
                    await self._maybe_save_periodic_checkpoint(envelope)
        finally:
            # On any exit (normal completion, stop request, or exception),
            # reconcile events read-but-not-delivered so the seen-counter
            # doesn't outlive this batch. Without this, a re-read of the
            # abandoned tail on resume would double-count it and inflate
            # lag permanently (it never decreases on its own).
            undelivered = len(envelopes) - delivered_this_batch
            if undelivered > 0:
                await self.subscription.record_events_unseen(undelivered)

        # Checkpoint after batch if configured
        if (
            (events_in_batch > 0 or events_filtered > 0)
            and last_envelope is not None
            and self.config.checkpoint_strategy == CheckpointStrategy.EVERY_BATCH
        ):
            await self._save_checkpoint_with_retry(last_envelope)

        logger.debug(
            "Batch processed",
            extra={
                "subscription": self.subscription.name,
                "batch_size": events_in_batch,
                "events_filtered": events_filtered,
                "position": render_position(self.subscription.last_processed_position),
            },
        )

        return _BatchOutcome(envelopes_read=len(envelopes), events_delivered=events_in_batch)

    async def _read_batch_with_retry(
        self,
        from_position: Position | None,
        limit: int,
    ) -> list[EventEnvelope]:
        """
        Read a batch of events from the global feed with retry.

        Args:
            from_position: Position to read from, None for the feed start
            limit: Maximum events to read

        Returns:
            List of event envelopes

        Raises:
            RetryError: If all retries are exhausted
        """
        options = FeedReadOptions(
            tenant_id=self.config.tenant_id,
            limit=limit,
        )

        async def read_batch() -> list[EventEnvelope]:
            envelopes = []
            async for envelope in self.event_store.read_all(from_position, options):
                envelopes.append(envelope)
            return envelopes

        return await self._retry.execute(
            operation=read_batch,
            name="read_batch",
            retryable_exceptions=TRANSIENT_EXCEPTIONS,
        )

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
                await self.subscription.subscriber.handle(event)
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

    async def _save_checkpoint(self, envelope: EventEnvelope) -> None:
        """
        Save checkpoint for the processed event (no retry).

        An envelope with no position is not checkpointable: there is no
        token to persist and inventing one is not an option.

        Args:
            envelope: The envelope to checkpoint
        """
        if envelope.position is None:
            return

        await self.checkpoint_repo.save_position(
            subscription_id=self.subscription.name,
            position=envelope.position,
            event_id=envelope.event.event_id,
            event_type=envelope.event.event_type,
        )

        # Update time for periodic checkpointing
        self._last_checkpoint_time = time.monotonic()

        logger.debug(
            "Checkpoint saved",
            extra={
                "subscription": self.subscription.name,
                "position": render_position(envelope.position),
            },
        )

    async def _save_checkpoint_with_retry(self, envelope: EventEnvelope) -> None:
        """
        Save checkpoint for the processed event with retry.

        An envelope with no position is not checkpointable and is skipped.

        Args:
            envelope: The envelope to checkpoint

        Raises:
            RetryError: If all retries are exhausted
        """
        position = envelope.position
        if position is None:
            return

        async def save_checkpoint() -> None:
            await self.checkpoint_repo.save_position(
                subscription_id=self.subscription.name,
                position=position,
                event_id=envelope.event.event_id,
                event_type=envelope.event.event_type,
            )

        await self._retry.execute(
            operation=save_checkpoint,
            name="save_checkpoint",
            retryable_exceptions=TRANSIENT_EXCEPTIONS,
        )

        # Update time for periodic checkpointing
        self._last_checkpoint_time = time.monotonic()

        logger.debug(
            "Checkpoint saved",
            extra={
                "subscription": self.subscription.name,
                "position": render_position(position),
            },
        )

    async def _maybe_save_periodic_checkpoint(self, envelope: EventEnvelope) -> None:
        """
        Save checkpoint if enough time has passed (for PERIODIC strategy).

        Args:
            envelope: The envelope to potentially checkpoint
        """
        current_time = time.monotonic()
        elapsed = current_time - self._last_checkpoint_time

        if elapsed >= self.config.checkpoint_interval_seconds:
            await self._save_checkpoint_with_retry(envelope)

    async def stop(self) -> None:
        """
        Request the runner to stop gracefully.

        The runner will finish the current event and stop at the next
        safe point. This is an async method to allow for any cleanup.
        """
        self._stop_requested = True
        logger.info(
            "Catch-up stop requested",
            extra={"subscription": self.subscription.name},
        )

    @property
    def is_running(self) -> bool:
        """
        Check if the runner is currently processing.

        Returns:
            True if the runner is active
        """
        return self._running

    @property
    def stop_requested(self) -> bool:
        """
        Check if a stop has been requested.

        Returns:
            True if stop() has been called
        """
        return self._stop_requested

    @property
    def flow_controller(self) -> FlowController:
        """
        Get the flow controller for this runner.

        Returns:
            The FlowController instance
        """
        return self._flow_controller

    @property
    def flow_control_stats(self) -> FlowControlStats:
        """
        Get flow control statistics.

        Returns:
            FlowControlStats snapshot
        """
        return self._flow_controller.stats

    @property
    def is_backpressured(self) -> bool:
        """
        Check if runner is experiencing backpressure.

        Returns:
            True if at or above backpressure threshold
        """
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
    def retry_operation(self) -> RetryableOperation:
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
        return self._filter

    @property
    def filter_stats(self) -> FilterStats:
        """
        Get filter statistics.

        Returns:
            FilterStats snapshot
        """
        return self._filter.stats

    @property
    def metrics(self) -> SubscriptionMetrics:
        """
        Get the metrics instance for this runner.

        Returns:
            SubscriptionMetrics instance
        """
        return self._metrics


__all__ = [
    "CatchUpResult",
    "CatchUpRunner",
]
