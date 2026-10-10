"""TransitionCoordinator coordinating the catch-up to live transition."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from eventsource.application.subscriptions.subscription import Subscription
from eventsource.application.subscriptions.transition_executor import TransitionExecutionMixin
from eventsource.application.subscriptions.transition_models import TransitionPhase
from eventsource.observability import Tracer, create_tracer

if TYPE_CHECKING:
    from eventsource.application.subscriptions.flow_control import FlowController
    from eventsource.application.subscriptions.retry import CircuitBreaker
    from eventsource.application.subscriptions.runners.catchup import CatchUpRunner
    from eventsource.application.subscriptions.runners.live import LiveRunner
    from eventsource.ports.bus import SubscribableEventBus
    from eventsource.ports.checkpoints import SubscriptionPositions
    from eventsource.ports.positions import Position
    from eventsource.ports.store import GlobalEventFeed

logger = logging.getLogger(__name__)


class TransitionCoordinator(TransitionExecutionMixin):
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
        event_store: GlobalEventFeed,
        event_bus: SubscribableEventBus,
        checkpoint_repo: SubscriptionPositions,
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
            tracer: Optional custom Tracer instance.
            enable_tracing: Whether to enable OpenTelemetry tracing (default True).
        """
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

    @property
    def phase(self) -> TransitionPhase:
        """Get current transition phase."""
        return self._phase

    @property
    def watermark(self) -> Position | None:
        """Get the watermark position."""
        return self._watermark

    @property
    def live_runner(self) -> LiveRunner | None:
        """Get the live runner (available after live subscription starts)."""
        return self._live_runner

    @property
    def catchup_runner(self) -> CatchUpRunner | None:
        """Get the catch-up runner (available during catch-up phase)."""
        return self._catchup_runner

    @property
    def flow_controller(self) -> FlowController | None:
        """Get the FlowController for this subscription, if running."""
        if self._live_runner is not None:
            return self._live_runner.flow_controller
        return None

    @property
    def handler_circuit_breaker(self) -> CircuitBreaker | None:
        """Get the handler circuit breaker of whichever runner is currently active."""
        if self._live_runner is not None:
            return self._live_runner.handler_circuit_breaker
        if self._catchup_runner is not None:
            return self._catchup_runner.handler_circuit_breaker
        return None

    @property
    def infra_circuit_breaker(self) -> CircuitBreaker | None:
        """Get the infrastructure circuit breaker of whichever runner is currently active."""
        if self._live_runner is not None:
            return self._live_runner.infra_circuit_breaker
        if self._catchup_runner is not None:
            return self._catchup_runner.infra_circuit_breaker
        return None


__all__ = ["TransitionCoordinator"]
