"""Read-only accessors for CatchUpRunner state, filter, and circuit breakers."""

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


class CatchUpRunnerPropertiesMixin:
    """Mixin exposing the runner's run flag, stop flag, and collaborator objects."""

    if TYPE_CHECKING:
        _running: bool
        _stop_event: asyncio.Event
        _flow_controller: FlowController
        _filter: EventFilter
        _infra_circuit_breaker: CircuitBreaker | None
        _handler_circuit_breaker: CircuitBreaker | None
        _retry: RetryableOperation
        _metrics: SubscriptionMetrics

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def _stop_requested(self) -> bool:
        return self._stop_event.is_set()

    @property
    def stop_requested(self) -> bool:
        return self._stop_event.is_set()

    @property
    def flow_controller(self) -> FlowController:
        return self._flow_controller

    @property
    def flow_control_stats(self) -> FlowControlStats:
        return self._flow_controller.stats

    @property
    def handler_circuit_breaker(self) -> CircuitBreaker | None:
        return self._handler_circuit_breaker

    @property
    def infra_circuit_breaker(self) -> CircuitBreaker | None:
        return self._infra_circuit_breaker

    @property
    def retry_operation(self) -> RetryableOperation:
        return self._retry

    @property
    def event_filter(self) -> EventFilter:
        return self._filter

    @property
    def filter_stats(self) -> FilterStats:
        return self._filter.stats

    @property
    def metrics(self) -> SubscriptionMetrics:
        return self._metrics


__all__ = ["CatchUpRunnerPropertiesMixin"]
