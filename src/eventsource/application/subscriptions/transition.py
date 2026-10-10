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
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.resolver import StartFromResolver
from eventsource.application.subscriptions.runners.catchup import CatchUpRunner
from eventsource.application.subscriptions.runners.live import LiveRunner
from eventsource.application.subscriptions.subscription import Subscription, render_position
from eventsource.application.subscriptions.transition_models import (
    TransitionPhase,
    TransitionResult,
)
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_BUFFER_SIZE,
    ATTR_EVENTS_PROCESSED,
    ATTR_POSITION,
    ATTR_SUBSCRIPTION_NAME,
    ATTR_SUBSCRIPTION_PHASE,
    ATTR_WATERMARK,
)
from eventsource.ports.exceptions import TransitionError
from eventsource.ports.positions import Position

if TYPE_CHECKING:
    from eventsource.application.subscriptions.flow_control import FlowController
    from eventsource.application.subscriptions.retry import CircuitBreaker
    from eventsource.ports.bus import SubscribableEventBus
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


class TransitionCoordinator:
    """Coordinates the transition from catch-up to live event processing.

    Uses a watermark approach to ensure gap-free delivery:
    1. Get current max position (watermark)
    2. Start live runner in buffer mode
    3. Catch up to watermark
    4. Process buffered events from feed
    5. Switch to direct live mode
    """

    def __init__(
        self,
        event_store: "GlobalEventFeed",
        event_bus: "SubscribableEventBus",
        checkpoint_repo: "SubscriptionPositions",
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
        self._watermark: Position | None = None
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
                ATTR_POSITION: render_position(self.subscription.last_processed_position),
            },
        ) as span:
            catchup_processed = 0
            buffer_processed = 0

            try:
                # Phase 1: Initial setup and watermark
                self._phase = TransitionPhase.INITIAL_CATCHUP
                self._watermark = watermark = await self.event_store.current_position()

                if span:
                    if (token := render_position(self._watermark)) is not None:
                        span.set_attribute(ATTR_WATERMARK, token)
                    span.set_attribute(ATTR_SUBSCRIPTION_PHASE, self._phase.value)

                logger.info(
                    "Transition starting",
                    extra={
                        "subscription": self.subscription.name,
                        "watermark": render_position(self._watermark),
                        "current_position": render_position(
                            self.subscription.last_processed_position
                        ),
                    },
                )

                # Already caught up: an empty feed (watermark None) has
                # nothing to catch up to. Order matters -- a None position
                # means nothing has been processed, which is behind any
                # watermark, not ahead of it.
                current = self.subscription.last_processed_position
                if watermark is None or (current is not None and current >= watermark):
                    logger.info(
                        "Already caught up, skipping to live",
                        extra={
                            "subscription": self.subscription.name,
                            "position": render_position(current),
                            "watermark": render_position(self._watermark),
                        },
                    )
                    await self._start_live_directly()
                    if span:
                        span.set_attribute(ATTR_SUBSCRIPTION_PHASE, TransitionPhase.LIVE.value)
                    return TransitionResult(
                        success=True,
                        catchup_events_processed=0,
                        buffer_events_processed=0,
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
                    event_feed=self.event_store,
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
                    target_position=watermark
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
                        "position": render_position(self.subscription.last_processed_position),
                        "buffer_size": self._live_runner.buffer_size,
                    },
                )

                # Phase 4: Process buffered events
                self._phase = TransitionPhase.PROCESSING_BUFFER
                if span:
                    span.set_attribute(ATTR_SUBSCRIPTION_PHASE, self._phase.value)

                buffer_processed = await self._live_runner.process_buffer()

                if span:
                    span.set_attribute(ATTR_EVENTS_PROCESSED, buffer_processed)

                logger.info(
                    "Buffer processed",
                    extra={
                        "subscription": self.subscription.name,
                        "processed": buffer_processed,
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
                        "final_position": render_position(
                            self.subscription.last_processed_position
                        ),
                        "total_catchup": catchup_processed,
                        "total_buffer": buffer_processed,
                    },
                )

                return TransitionResult(
                    success=True,
                    catchup_events_processed=catchup_processed,
                    buffer_events_processed=buffer_processed,
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
            event_feed=self.event_store,
            subscription=self.subscription,
        )
        await self._live_runner.start(buffer_events=False)
        self._phase = TransitionPhase.LIVE

    async def _cleanup(self) -> None:
        """
        Clean up resources after failure.

        Stops any running runners to release resources, clears buffered
        events, and reconciles subscription lag to eliminate phantom telemetry.
        """
        if self._catchup_runner and self._catchup_runner.is_running:
            await self._catchup_runner.stop()

        if self._live_runner is not None:
            if self._live_runner.is_running:
                await self._live_runner.stop()
            else:
                await self._live_runner.clear_buffer()

        await self.subscription.reconcile_lag(0)

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
    def watermark(self) -> Position | None:
        """
        Get the watermark position.

        The watermark is the current global-feed position captured at the
        start of the transition. The catch-up phase targets this position.

        Returns:
            Watermark position, None if not yet captured or the feed is empty
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

        The FlowController is accessed through the live runner. It counts
        in-flight events so shutdown can wait for the drain; it does not
        limit concurrency, because delivery is sequential.

        Returns:
            FlowController instance if live runner is active, None otherwise
        """
        if self._live_runner is not None:
            return self._live_runner.flow_controller
        return None

    @property
    def handler_circuit_breaker(self) -> "CircuitBreaker | None":
        """
        Get the handler circuit breaker of whichever runner is currently
        active.

        Guards the subscriber's `handle()`/`handle_batch()` calls. During
        catch-up this is the catch-up runner's breaker; once the transition
        to live delivery completes, the live runner's breaker takes over.
        Distinct from `infra_circuit_breaker` -- the two guard unrelated
        failure domains and never share state (see
        `CatchUpRunner.handler_circuit_breaker`).

        Returns:
            The active runner's handler CircuitBreaker, or None if neither
            runner has been created yet, or if `circuit_breaker_enabled=False`
            left both runners without one.
        """
        if self._live_runner is not None:
            return self._live_runner.handler_circuit_breaker
        if self._catchup_runner is not None:
            return self._catchup_runner.handler_circuit_breaker
        return None

    @property
    def infra_circuit_breaker(self) -> "CircuitBreaker | None":
        """
        Get the infrastructure circuit breaker of whichever runner is
        currently active.

        Guards read-batch (catch-up only) and checkpoint-save. Distinct
        from `handler_circuit_breaker` -- a broken handler cannot open this
        one, and a flaky store cannot mask a broken handler.

        Returns:
            The active runner's infra CircuitBreaker, or None if neither
            runner has been created yet, or if `circuit_breaker_enabled=False`
            left both runners without one.
        """
        if self._live_runner is not None:
            return self._live_runner.infra_circuit_breaker
        if self._catchup_runner is not None:
            return self._catchup_runner.infra_circuit_breaker
        return None


__all__ = [
    "StartFromResolver",
    "TransitionCoordinator",
    "TransitionPhase",
    "TransitionResult",
]
