"""Execution steps and lifecycle management for TransitionCoordinator."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from eventsource.application.subscriptions.runners.catchup import CatchUpRunner
from eventsource.application.subscriptions.runners.live import LiveRunner
from eventsource.application.subscriptions.subscription import Subscription, render_position
from eventsource.application.subscriptions.transition_models import (  # fmt: skip
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

    def _set_phase(self, phase: TransitionPhase, span: Any) -> None:
        self._phase = phase
        if span:
            span.set_attribute(ATTR_SUBSCRIPTION_PHASE, phase.value)

    async def execute(self) -> TransitionResult:
        """Execute the catch-up to live transition."""
        sub_name = self.subscription.name
        span_attrs = {ATTR_SUBSCRIPTION_NAME: sub_name, ATTR_POSITION: render_position(self.subscription.last_processed_position)}  # fmt: skip
        with self._tracer.span("eventsource.transition_coordinator.execute", span_attrs) as span:
            catchup_processed = buffer_processed = 0
            try:
                self._set_phase(TransitionPhase.INITIAL_CATCHUP, span)
                self._watermark = watermark = await self.event_store.current_position()
                wm_tok = render_position(self._watermark)
                if span and wm_tok is not None:
                    span.set_attribute(ATTR_WATERMARK, wm_tok)
                current = self.subscription.last_processed_position
                cur_tok = render_position(current)
                ext_start = {"subscription": sub_name, "watermark": wm_tok, "current_position": cur_tok}  # fmt: skip
                logger.info("Transition starting", extra=ext_start)
                if watermark is None or (current is not None and current >= watermark):
                    ext = {"subscription": sub_name, "position": cur_tok, "watermark": wm_tok}
                    logger.info("Already caught up, skipping to live", extra=ext)
                    await self._start_live_directly()
                    self._set_phase(TransitionPhase.LIVE, span)
                    pos = self.subscription.last_processed_position
                    return TransitionResult(True, 0, 0, pos, TransitionPhase.LIVE)
                self._set_phase(TransitionPhase.LIVE_SUBSCRIBED, span)
                await self._start_live(buffer_events=True)
                logger.debug("Live subscription started (buffering)", extra={"subscription": sub_name})  # fmt: skip
                self._set_phase(TransitionPhase.FINAL_CATCHUP, span)
                self._catchup_runner = CatchUpRunner(
                    event_store=self.event_store,
                    checkpoint_repo=self.checkpoint_repo,
                    subscription=self.subscription,
                )
                catchup_result = await self._catchup_runner.run_until_position(watermark)
                catchup_processed = catchup_result.events_processed
                if catchup_result.error:
                    msg = f"Catch-up failed: {catchup_result.error}"
                    raise TransitionError(msg) from catchup_result.error
                assert self._live_runner is not None
                if span:
                    span.set_attribute(ATTR_EVENTS_PROCESSED, catchup_processed)
                    span.set_attribute(ATTR_BUFFER_SIZE, self._live_runner.buffer_size)
                ext_wm = {"subscription": sub_name, "events_processed": catchup_processed, "position": render_position(self.subscription.last_processed_position), "buffer_size": self._live_runner.buffer_size}  # fmt: skip
                logger.info("Catch-up to watermark complete", extra=ext_wm)
                self._set_phase(TransitionPhase.PROCESSING_BUFFER, span)
                buffer_processed = await self._live_runner.process_buffer()
                if span:
                    span.set_attribute(ATTR_EVENTS_PROCESSED, buffer_processed)
                logger.info("Buffer processed", extra={"subscription": sub_name, "processed": buffer_processed})  # fmt: skip
                self._set_phase(TransitionPhase.LIVE, span)
                await self._live_runner.disable_buffer()
                final_pos = self.subscription.last_processed_position
                ext_done = {"subscription": sub_name, "final_position": render_position(final_pos), "total_catchup": catchup_processed, "total_buffer": buffer_processed}  # fmt: skip
                logger.info("Transition complete, now live", extra=ext_done)
                return TransitionResult(True, catchup_processed, buffer_processed, final_pos, TransitionPhase.LIVE)  # fmt: skip
            except Exception as e:
                self._set_phase(TransitionPhase.FAILED, span)
                extra_err = {"subscription": sub_name, "phase": self._phase.value, "error": str(e)}
                logger.error("Transition failed", extra=extra_err, exc_info=True)
                await self._cleanup()
                final_pos = self.subscription.last_processed_position
                return TransitionResult(False, catchup_processed, buffer_processed, final_pos, self._phase, error=e)  # fmt: skip

    async def _start_live(self, *, buffer_events: bool) -> None:
        self._live_runner = LiveRunner(
            event_bus=self.event_bus,
            checkpoint_repo=self.checkpoint_repo,
            event_feed=self.event_store,
            subscription=self.subscription,
        )
        await self._live_runner.start(buffer_events=buffer_events)

    async def _start_live_directly(self) -> None:
        await self._start_live(buffer_events=False)
        self._phase = TransitionPhase.LIVE

    async def _cleanup(self) -> None:
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
        attrs = {ATTR_SUBSCRIPTION_NAME: self.subscription.name, ATTR_SUBSCRIPTION_PHASE: self._phase.value}  # fmt: skip
        with self._tracer.span("eventsource.transition_coordinator.stop", attrs):
            logger.info("Stopping transition", extra={"subscription": self.subscription.name, "phase": self._phase.value})  # fmt: skip
            await self._cleanup()


__all__ = ["TransitionExecutionMixin"]
