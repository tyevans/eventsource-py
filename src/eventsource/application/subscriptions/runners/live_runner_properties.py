"""Read-only accessors for LiveRunner state, buffers, and collaborator objects."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncio

    from eventsource.application.subscriptions.filtering import EventFilter, FilterStats
    from eventsource.application.subscriptions.flow_control import (
        FlowController,
        FlowControlStats,
    )
    from eventsource.application.subscriptions.metrics import SubscriptionMetrics
    from eventsource.application.subscriptions.retry import CircuitBreaker, RetryableOperation
    from eventsource.application.subscriptions.runners.live_models import LiveRunnerStats
    from eventsource.application.subscriptions.subscription import Subscription


class LiveRunnerPropertiesMixin:
    """Mixin exposing the live runner's run state, buffer counters, and collaborators."""

    if TYPE_CHECKING:
        subscription: Subscription
        _running: bool
        _buffered_wakes: int
        _paused_wakes: int
        _events_buffered_during_pause: int
        _stop_event: asyncio.Event
        _stats: LiveRunnerStats
        _flow_controller: FlowController | None
        _filter: EventFilter | None
        _infra_circuit_breaker: CircuitBreaker | None
        _handler_circuit_breaker: CircuitBreaker | None
        _retry: RetryableOperation | None
        _metrics: SubscriptionMetrics | None

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def _stop_requested(self) -> bool:
        return self._stop_event.is_set()

    @property
    def buffer_size(self) -> int:
        return self._buffered_wakes

    @property
    def pause_buffer_size(self) -> int:
        return self._paused_wakes

    @property
    def events_buffered_during_pause(self) -> int:
        return self._events_buffered_during_pause

    @property
    def stats(self) -> LiveRunnerStats:
        return self._stats

    @property
    def flow_controller(self) -> FlowController:
        assert self._flow_controller is not None
        return self._flow_controller

    @property
    def flow_control_stats(self) -> FlowControlStats:
        return self.flow_controller.stats

    @property
    def handler_circuit_breaker(self) -> CircuitBreaker | None:
        return self._handler_circuit_breaker

    @property
    def infra_circuit_breaker(self) -> CircuitBreaker | None:
        return self._infra_circuit_breaker

    @property
    def retry_operation(self) -> RetryableOperation | None:
        return self._retry

    @property
    def event_filter(self) -> EventFilter:
        assert self._filter is not None
        return self._filter

    @property
    def filter_stats(self) -> FilterStats:
        return self.event_filter.stats

    @property
    def metrics(self) -> SubscriptionMetrics:
        assert self._metrics is not None
        return self._metrics


__all__ = ["LiveRunnerPropertiesMixin"]
