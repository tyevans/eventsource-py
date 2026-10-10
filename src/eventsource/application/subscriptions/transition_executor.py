"""Execution steps and lifecycle management for TransitionCoordinator."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.runners.catchup import CatchUpRunner
from eventsource.application.subscriptions.runners.live import LiveRunner
from eventsource.application.subscriptions.subscription import (
    Subscription,
    render_position,
)
from eventsource.application.subscriptions.transition_models import (
    TransitionPhase,
    TransitionResult,
)
from eventsource.observability import Tracer
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
    from eventsource.ports.bus import SubscribableEventBus
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


class TransitionExecutionMixin:
    """Mixin providing execution workflow and lifecycle control for TransitionCoordinator."""

    if TYPE_CHECKING:
        _tracer: Tracer
        event_store: GlobalEventFeed
        event_bus: SubscribableEventBus
        checkpoint_repo: SubscriptionPositions
        subscription: Subscription
        _phase: TransitionPhase
        _watermark: Position | None
        _catchup_runner: CatchUpRunner | None
        _live_runner: LiveRunner | None

    async def execute(self) -> TransitionResult:
        """
        Execute the catch-up to live transition.

        Returns:
            TransitionResult with transition statistics and outcome
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
        """Start live mode without catch-up (already caught up)."""
        self._live_runner = LiveRunner(
            event_bus=self.event_bus,
            checkpoint_repo=self.checkpoint_repo,
            event_feed=self.event_store,
            subscription=self.subscription,
        )
        await self._live_runner.start(buffer_events=False)
        self._phase = TransitionPhase.LIVE

    async def _cleanup(self) -> None:
        """Clean up resources after failure or stop."""
        if self._catchup_runner and self._catchup_runner.is_running:
            await self._catchup_runner.stop()

        if self._live_runner is not None:
            if self._live_runner.is_running:
                await self._live_runner.stop()
            else:
                await self._live_runner.clear_buffer()

        await self.subscription.reconcile_lag(0)

    async def stop(self) -> None:
        """Stop the transition and all runners."""
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


__all__ = ["TransitionExecutionMixin"]
