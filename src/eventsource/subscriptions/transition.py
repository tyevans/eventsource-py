"""
Transition coordinator for catch-up to live event transition.

Manages the critical transition from historical events (catch-up) to
real-time events (live) without losing or duplicating events.

This module provides:
- TransitionPhase: Enum of transition phases
- TransitionResult: Result of a transition operation
- TransitionCoordinator: Coordinates the catch-up to live transition
- StartFromResolver: Resolves subscription start position
"""

import logging
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BUFFER_SIZE,
    ATTR_EVENTS_PROCESSED,
    ATTR_EVENTS_SKIPPED,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
    ATTR_SUBSCRIPTION_PHASE,
    ATTR_WATERMARK,
)
from eventsource.subscriptions.exceptions import TransitionError
from eventsource.subscriptions.runners.catchup import CatchUpRunner
from eventsource.subscriptions.runners.live import LiveRunner
from eventsource.subscriptions.subscription import Subscription

if TYPE_CHECKING:
    from eventsource.bus.interface import EventBus
    from eventsource.repositories.checkpoint import CheckpointRepository
    from eventsource.stores.interface import EventStore
    from eventsource.subscriptions.flow_control import FlowController

logger = logging.getLogger(__name__)


class TransitionPhase(Enum):
    """
    Phases of the catch-up to live transition.

    The transition follows this sequence:
    1. NOT_STARTED: Initial state
    2. INITIAL_CATCHUP: Getting watermark and preparing
    3. LIVE_SUBSCRIBED: Live runner started in buffer mode
    4. FINAL_CATCHUP: Catching up to watermark position
    5. PROCESSING_BUFFER: Processing buffered live events
    6. LIVE: Now processing live events directly
    7. FAILED: Transition failed with error
    """

    NOT_STARTED = "not_started"
    INITIAL_CATCHUP = "initial_catchup"
    LIVE_SUBSCRIBED = "live_subscribed"
    FINAL_CATCHUP = "final_catchup"
    PROCESSING_BUFFER = "processing_buffer"
    LIVE = "live"
    FAILED = "failed"


@dataclass(frozen=True)
class TransitionResult:
    """
    Result of a transition operation.

    Provides statistics and outcome information for the catch-up
    to live transition.

    Attributes:
        success: True if transition completed successfully
        catchup_events_processed: Number of events processed during catch-up
        buffer_events_processed: Number of events processed from buffer
        buffer_events_skipped: Number of duplicate events skipped from buffer
        final_position: Last processed global position
        phase_reached: The phase reached when transition ended
        error: Exception if transition failed, None otherwise
    """

    success: bool
    catchup_events_processed: int
    buffer_events_processed: int
    buffer_events_skipped: int
    final_position: int
    phase_reached: TransitionPhase
    error: Exception | None = None


class TransitionCoordinator:
    """
    Coordinates the transition from catch-up to live event processing.

    Uses a watermark approach to ensure no events are lost:
    1. Get current max position (watermark)
    2. Subscribe to live events (buffering)
    3. Catch up to watermark
    4. Process buffered events (filtering duplicates)
    5. Switch to live mode

    This ensures gap-free event delivery during the transition from
    reading historical events to receiving real-time events.

    Timeline example:
        t0: Get watermark (max_position = 1000)
        t1: Subscribe to live events (buffering enabled)
        t2: Events 1001, 1002 published (go to buffer)
        t3: Catch up reads 1-1000 from store
        t4: Events 1003, 1004 published (go to buffer)
        t5: Catch up reads 1001-1004 from store (overlap with buffer)
        t6: Caught up to watermark (1000), switch to buffer processing
        t7: Process buffer: skip 1001-1004 (duplicates), continue with 1005+
        t8: Buffer empty, switch to live mode

    Attributes:
        event_store: Event store for catch-up and position lookup
        event_bus: Event bus for live subscription
        checkpoint_repo: Checkpoint repository for persistence
        subscription: The subscription being transitioned

    Example:
        >>> coordinator = TransitionCoordinator(
        ...     event_store, event_bus, checkpoint_repo, subscription
        ... )
        >>> result = await coordinator.execute()
        >>> if result.success:
        ...     print("Now processing live events")
        ...     live_runner = coordinator.live_runner
    """

    def __init__(
        self,
        event_store: "EventStore",
        event_bus: "EventBus",
        checkpoint_repo: "CheckpointRepository",
        subscription: Subscription,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        """
        Initialize the transition coordinator.

        Args:
            event_store: Event store to read from and get position
            event_bus: Event bus for live subscription
            checkpoint_repo: Checkpoint repository for persistence
            subscription: The subscription being transitioned
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: Whether to enable OpenTelemetry tracing (default True).
                          Ignored if tracer is explicitly provided.
        """
        # Composition-based tracing (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled

        self.event_store = event_store
        self.event_bus = event_bus
        self.checkpoint_repo = checkpoint_repo
        self.subscription = subscription

        self._phase = TransitionPhase.NOT_STARTED
        self._watermark: int = 0
        self._catchup_runner: CatchUpRunner | None = None
        self._live_runner: LiveRunner | None = None

    async def execute(self) -> TransitionResult:
        """
        Execute the catch-up to live transition.

        Performs the complete transition sequence:
        1. Get watermark (current max position)
        2. Start live runner in buffer mode
        3. Catch up to watermark
        4. Process buffered events with duplicate filtering
        5. Switch to live mode

        Returns:
            TransitionResult with transition statistics and outcome

        Note:
            After successful completion, access the live runner via
            the `live_runner` property for ongoing event processing.
        """
        with self._tracer.span(
            "eventsource.transition_coordinator.execute",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_POSITION: self.subscription.last_processed_position,
            },
        ) as span:
            catchup_processed = 0
            buffer_processed = 0
            buffer_skipped = 0

            try:
                # Phase 1: Initial setup and watermark
                self._phase = TransitionPhase.INITIAL_CATCHUP
                self._watermark = await self.event_store.get_global_position()

                if span:
                    span.set_attribute(ATTR_WATERMARK, self._watermark)
                    span.set_attribute(ATTR_SUBSCRIPTION_PHASE, self._phase.value)

                logger.info(
                    "Transition starting",
                    extra={
                        "subscription": self.subscription.name,
                        "watermark": self._watermark,
                        "current_position": self.subscription.last_processed_position,
                    },
                )

                # Check if already caught up (at or past watermark)
                if self.subscription.last_processed_position >= self._watermark:
                    logger.info(
                        "Already caught up, skipping to live",
                        extra={
                            "subscription": self.subscription.name,
                            "position": self.subscription.last_processed_position,
                            "watermark": self._watermark,
                        },
                    )
                    await self._start_live_directly()
                    if span:
                        span.set_attribute(ATTR_SUBSCRIPTION_PHASE, TransitionPhase.LIVE.value)
                    return TransitionResult(
                        success=True,
                        catchup_events_processed=0,
                        buffer_events_processed=0,
                        buffer_events_skipped=0,
                        final_position=self.subscription.last_processed_position,
                        phase_reached=TransitionPhase.LIVE,
                    )

                # Phase 2: Subscribe to live events (buffering)
                self._phase = TransitionPhase.LIVE_SUBSCRIBED
                if span:
                    span.set_attribute(ATTR_SUBSCRIPTION_PHASE, self._phase.value)

                self._live_runner = LiveRunner(
                    event_bus=self.event_bus,
                    checkpoint_repo=self.checkpoint_repo,
                    subscription=self.subscription,
                )
                await self._live_runner.start(buffer_events=True)

                logger.debug(
                    "Live subscription started (buffering)",
                    extra={"subscription": self.subscription.name},
                )

                # Phase 3: Catch up to watermark
                self._phase = TransitionPhase.FINAL_CATCHUP
                if span:
                    span.set_attribute(ATTR_SUBSCRIPTION_PHASE, self._phase.value)

                self._catchup_runner = CatchUpRunner(
                    event_store=self.event_store,
                    checkpoint_repo=self.checkpoint_repo,
                    subscription=self.subscription,
                )

                catchup_result = await self._catchup_runner.run_until_position(
                    target_position=self._watermark
                )
                catchup_processed = catchup_result.events_processed

                if catchup_result.error:
                    raise TransitionError(
                        f"Catch-up failed: {catchup_result.error}"
                    ) from catchup_result.error

                if span:
                    span.set_attribute(ATTR_EVENTS_PROCESSED, catchup_processed)
                    span.set_attribute(ATTR_BUFFER_SIZE, self._live_runner.buffer_size)

                logger.info(
                    "Catch-up to watermark complete",
                    extra={
                        "subscription": self.subscription.name,
                        "events_processed": catchup_processed,
                        "position": self.subscription.last_processed_position,
                        "buffer_size": self._live_runner.buffer_size,
                    },
                )

                # Phase 4: Process buffered events
                self._phase = TransitionPhase.PROCESSING_BUFFER
                if span:
                    span.set_attribute(ATTR_SUBSCRIPTION_PHASE, self._phase.value)

                buffer_processed = await self._live_runner.process_buffer()
                buffer_skipped = self._live_runner.stats.events_skipped_duplicate

                if span:
                    span.set_attribute(ATTR_EVENTS_SKIPPED, buffer_skipped)

                logger.info(
                    "Buffer processed",
                    extra={
                        "subscription": self.subscription.name,
                        "processed": buffer_processed,
                        "skipped": buffer_skipped,
                    },
                )

                # Phase 5: Switch to live mode
                self._phase = TransitionPhase.LIVE
                if span:
                    span.set_attribute(ATTR_SUBSCRIPTION_PHASE, self._phase.value)

                await self._live_runner.disable_buffer()

                logger.info(
                    "Transition complete, now live",
                    extra={
                        "subscription": self.subscription.name,
                        "final_position": self.subscription.last_processed_position,
                        "total_catchup": catchup_processed,
                        "total_buffer": buffer_processed,
                    },
                )

                return TransitionResult(
                    success=True,
                    catchup_events_processed=catchup_processed,
                    buffer_events_processed=buffer_processed,
                    buffer_events_skipped=buffer_skipped,
                    final_position=self.subscription.last_processed_position,
                    phase_reached=TransitionPhase.LIVE,
                )

            except Exception as e:
                self._phase = TransitionPhase.FAILED

                if span:
                    span.set_attribute(ATTR_SUBSCRIPTION_PHASE, self._phase.value)

                logger.error(
                    "Transition failed",
                    extra={
                        "subscription": self.subscription.name,
                        "phase": self._phase.value,
                        "error": str(e),
                    },
                    exc_info=True,
                )

                # Cleanup on failure
                await self._cleanup()

                return TransitionResult(
                    success=False,
                    catchup_events_processed=catchup_processed,
                    buffer_events_processed=buffer_processed,
                    buffer_events_skipped=buffer_skipped,
                    final_position=self.subscription.last_processed_position,
                    phase_reached=self._phase,
                    error=e,
                )

    async def _start_live_directly(self) -> None:
        """
        Start live mode without catch-up (already caught up).

        Used when the subscription is already at or past the watermark,
        meaning no catch-up is needed.
        """
        self._live_runner = LiveRunner(
            event_bus=self.event_bus,
            checkpoint_repo=self.checkpoint_repo,
            subscription=self.subscription,
        )
        await self._live_runner.start(buffer_events=False)
        self._phase = TransitionPhase.LIVE

    async def _cleanup(self) -> None:
        """
        Clean up resources after failure.

        Stops any running runners to release resources.
        """
        if self._catchup_runner and self._catchup_runner.is_running:
            await self._catchup_runner.stop()

        if self._live_runner and self._live_runner.is_running:
            await self._live_runner.stop()

    async def stop(self) -> None:
        """
        Stop the transition and all runners.

        Used for graceful shutdown during transition. Stops any
        active runners and releases resources.
        """
        with self._tracer.span(
            "eventsource.transition_coordinator.stop",
            {
                ATTR_SUBSCRIPTION_NAME: self.subscription.name,
                ATTR_SUBSCRIPTION_PHASE: self._phase.value,
            },
        ):
            logger.info(
                "Stopping transition",
                extra={
                    "subscription": self.subscription.name,
                    "phase": self._phase.value,
                },
            )

            await self._cleanup()

    @property
    def phase(self) -> TransitionPhase:
        """
        Get current transition phase.

        Returns:
            Current TransitionPhase enum value
        """
        return self._phase

    @property
    def watermark(self) -> int:
        """
        Get the watermark position.

        The watermark is the max global position captured at the start
        of the transition. The catch-up phase targets this position.

        Returns:
            Watermark position (0 if not yet captured)
        """
        return self._watermark

    @property
    def live_runner(self) -> LiveRunner | None:
        """
        Get the live runner (available after live subscription starts).

        The live runner is created during the transition and remains
        available after successful completion for ongoing event processing.

        Returns:
            LiveRunner instance, or None if not yet created
        """
        return self._live_runner

    @property
    def catchup_runner(self) -> CatchUpRunner | None:
        """
        Get the catch-up runner (available during catch-up phase).

        Returns:
            CatchUpRunner instance, or None if not yet created
        """
        return self._catchup_runner

    @property
    def flow_controller(self) -> "FlowController | None":
        """
        Get the FlowController for this subscription, if running.

        The FlowController is accessed through the live runner and is
        used for backpressure management and drain operations during shutdown.

        Returns:
            FlowController instance if live runner is active, None otherwise
        """
        if self._live_runner is not None:
            return self._live_runner.flow_controller
        return None


class StartFromResolver:
    """
    Resolves the start position based on configuration.

    Handles different start_from values:
    - "beginning": Start from position 0
    - "end": Start from current max position (live-only)
    - "checkpoint": Resume from last checkpoint
    - int: Start from explicit position

    Example:
        >>> resolver = StartFromResolver(event_store, checkpoint_repo)
        >>> position = await resolver.resolve(subscription)
        >>> print(f"Starting from position {position}")
    """

    def __init__(
        self,
        event_store: "EventStore",
        checkpoint_repo: "CheckpointRepository",
    ) -> None:
        """
        Initialize the start position resolver.

        Args:
            event_store: Event store for getting max position
            checkpoint_repo: Checkpoint repository for reading checkpoints
        """
        self.event_store = event_store
        self.checkpoint_repo = checkpoint_repo

    async def resolve(
        self,
        subscription: Subscription,
    ) -> int:
        """
        Resolve the starting position for a subscription.

        Interprets the subscription's start_from configuration and
        returns the appropriate starting position.

        Args:
            subscription: The subscription to resolve position for

        Returns:
            Starting position (0 for beginning, current max for end,
            checkpoint position for checkpoint, or explicit position)

        Raises:
            ValueError: If start_from has an unknown value
        """
        start_from = subscription.config.start_from

        if isinstance(start_from, int):
            # Explicit position
            return start_from

        if start_from == "beginning":
            return 0

        if start_from == "end":
            return await self.event_store.get_global_position()

        if start_from == "checkpoint":
            position = await self.checkpoint_repo.get_position(subscription.name)
            if position is not None:
                return position
            # No checkpoint found, start from beginning
            logger.info(
                "No checkpoint found, starting from beginning",
                extra={"subscription": subscription.name},
            )
            return 0

        raise ValueError(f"Unknown start_from value: {start_from}")


__all__ = [
    "TransitionPhase",
    "TransitionResult",
    "TransitionCoordinator",
    "StartFromResolver",
]
