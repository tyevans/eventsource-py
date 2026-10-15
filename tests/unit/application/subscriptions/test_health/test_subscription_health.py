"""Unit tests for SubscriptionHealthChecker."""

from unittest.mock import MagicMock

import pytest

from eventsource.application.subscriptions.health import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthIndicator,
    HealthStatus,
    SubscriptionHealthChecker,
)
from eventsource.application.subscriptions.retry import CircuitBreaker, CircuitState
from eventsource.application.subscriptions.subscription import Subscription, SubscriptionState


def _ind(result: HealthCheckResult, name: str) -> HealthIndicator | None:
    return next((i for i in result.indicators if i.name == name), None)


def _breaker(state: CircuitState, failures: int = 0) -> MagicMock:
    cb = MagicMock(spec=CircuitBreaker)
    cb.state = state
    cb.failure_count = failures
    return cb


class TestSubscriptionHealthChecker:
    @pytest.fixture
    def checker(self, subscription: Subscription) -> SubscriptionHealthChecker:
        return SubscriptionHealthChecker(subscription=subscription, config=HealthCheckConfig())

    def test_check_returns_result(self, checker: SubscriptionHealthChecker):
        result = checker.check()
        assert isinstance(result, HealthCheckResult)
        assert result.subscription_name == "TestSubscription"

    def test_check_starting_state(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        state_indicator = _ind(checker.check(), "state")
        assert state_indicator is not None
        assert state_indicator.status == HealthStatus.UNKNOWN

    @pytest.mark.asyncio
    async def test_check_live_state(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.LIVE)
        state_indicator = _ind(checker.check(), "state")
        assert state_indicator is not None
        assert state_indicator.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_error_state(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        subscription.last_error = RuntimeError("Test error")
        await subscription.transition_to(SubscriptionState.ERROR)
        state_indicator = _ind(checker.check(), "state")
        assert state_indicator is not None
        assert state_indicator.status == HealthStatus.UNHEALTHY

    def test_check_errors_indicator_healthy(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        errors_indicator = _ind(checker.check(), "errors")
        assert errors_indicator is not None
        assert errors_indicator.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_errors_indicator_degraded(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        for _ in range(15):
            await subscription.record_event_failed(ValueError("test"))
        errors_indicator = _ind(checker.check(), "errors")
        assert errors_indicator is not None
        assert errors_indicator.status == HealthStatus.DEGRADED

    @pytest.mark.asyncio
    async def test_check_errors_indicator_critical(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        for _ in range(150):
            await subscription.record_event_failed(ValueError("test"))
        errors_indicator = _ind(checker.check(), "errors")
        assert errors_indicator is not None
        assert errors_indicator.status == HealthStatus.CRITICAL

    def test_check_lag_indicator_healthy(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        lag_indicator = _ind(checker.check(), "lag")
        assert lag_indicator is not None
        assert lag_indicator.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_lag_indicator_degraded(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        await subscription.record_events_seen(2000)
        lag_indicator = _ind(checker.check(), "lag")
        assert lag_indicator is not None
        assert lag_indicator.status == HealthStatus.DEGRADED

    @pytest.mark.asyncio
    async def test_check_lag_indicator_critical(
        self, subscription: Subscription, checker: SubscriptionHealthChecker
    ):
        await subscription.record_events_seen(15000)
        lag_indicator = _ind(checker.check(), "lag")
        assert lag_indicator is not None
        assert lag_indicator.status == HealthStatus.CRITICAL

    def test_overall_status_worst_indicator(self, subscription: Subscription):
        config = HealthCheckConfig(max_errors_critical=1)
        checker = SubscriptionHealthChecker(subscription, config)
        result = checker.check()
        assert result.overall_status in (HealthStatus.UNKNOWN, HealthStatus.HEALTHY)

    def test_check_with_handler_circuit_breaker_closed(self, subscription: Subscription):
        cb = _breaker(CircuitState.CLOSED, 0)
        checker = SubscriptionHealthChecker(
            subscription=subscription, handler_circuit_breaker_provider=lambda: cb
        )
        cb_indicator = _ind(checker.check(), "handler_circuit_breaker")
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.HEALTHY

    def test_check_with_handler_circuit_breaker_open(self, subscription: Subscription):
        cb = _breaker(CircuitState.OPEN, 10)
        checker = SubscriptionHealthChecker(
            subscription=subscription, handler_circuit_breaker_provider=lambda: cb
        )
        cb_indicator = _ind(checker.check(), "handler_circuit_breaker")
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.UNHEALTHY

    def test_check_with_handler_circuit_breaker_half_open(self, subscription: Subscription):
        cb = _breaker(CircuitState.HALF_OPEN, 5)
        checker = SubscriptionHealthChecker(
            subscription=subscription, handler_circuit_breaker_provider=lambda: cb
        )
        cb_indicator = _ind(checker.check(), "handler_circuit_breaker")
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.DEGRADED

    def test_check_with_infra_circuit_breaker_open(self, subscription: Subscription):
        cb = _breaker(CircuitState.OPEN, 10)
        checker = SubscriptionHealthChecker(
            subscription=subscription, infra_circuit_breaker_provider=lambda: cb
        )
        result = checker.check()
        indicator_names = {i.name for i in result.indicators}
        assert "infra_circuit_breaker" in indicator_names
        assert "handler_circuit_breaker" not in indicator_names
        cb_indicator = _ind(result, "infra_circuit_breaker")
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.UNHEALTHY

    def test_check_reports_both_breakers_independently(self, subscription: Subscription):
        handler_breaker = _breaker(CircuitState.OPEN, 5)
        infra_breaker = _breaker(CircuitState.CLOSED, 0)
        checker = SubscriptionHealthChecker(
            subscription=subscription,
            handler_circuit_breaker_provider=lambda: handler_breaker,
            infra_circuit_breaker_provider=lambda: infra_breaker,
        )
        result = checker.check()
        by_name = {i.name: i for i in result.indicators}
        assert by_name["handler_circuit_breaker"].status == HealthStatus.UNHEALTHY
        assert by_name["infra_circuit_breaker"].status == HealthStatus.HEALTHY
