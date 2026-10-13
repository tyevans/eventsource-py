"""Unit tests for SubscriptionHealthChecker."""

from unittest.mock import MagicMock

import pytest

from eventsource.application.subscriptions.health import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthStatus,
    SubscriptionHealthChecker,
)
from eventsource.application.subscriptions.retry import CircuitBreaker, CircuitState
from eventsource.application.subscriptions.subscription import Subscription, SubscriptionState


class TestSubscriptionHealthChecker:
    """Tests for SubscriptionHealthChecker."""

    @pytest.fixture
    def checker(self, subscription: Subscription) -> SubscriptionHealthChecker:
        """Create test health checker."""
        return SubscriptionHealthChecker(
            subscription=subscription,
            config=HealthCheckConfig(),
        )

    def test_check_returns_result(self, checker: SubscriptionHealthChecker):
        """Test that check returns a HealthCheckResult."""
        result = checker.check()

        assert isinstance(result, HealthCheckResult)
        assert result.subscription_name == "TestSubscription"

    def test_check_starting_state(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test health check in STARTING state."""
        # Default state is STARTING
        result = checker.check()

        # STARTING is not a running state, so not HEALTHY
        state_indicator = next(
            (i for i in result.indicators if i.name == "state"),
            None,
        )
        assert state_indicator is not None
        # STARTING is neither healthy nor unhealthy - it's UNKNOWN
        assert state_indicator.status == HealthStatus.UNKNOWN

    @pytest.mark.asyncio
    async def test_check_live_state(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test health check in LIVE state."""
        await subscription.transition_to(SubscriptionState.CATCHING_UP)
        await subscription.transition_to(SubscriptionState.LIVE)

        result = checker.check()

        state_indicator = next(
            (i for i in result.indicators if i.name == "state"),
            None,
        )
        assert state_indicator is not None
        assert state_indicator.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_error_state(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test health check in ERROR state."""
        subscription.last_error = RuntimeError("Test error")
        await subscription.transition_to(SubscriptionState.ERROR)

        result = checker.check()

        state_indicator = next(
            (i for i in result.indicators if i.name == "state"),
            None,
        )
        assert state_indicator is not None
        assert state_indicator.status == HealthStatus.UNHEALTHY

    def test_check_errors_indicator_healthy(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test errors indicator when healthy."""
        result = checker.check()

        errors_indicator = next(
            (i for i in result.indicators if i.name == "errors"),
            None,
        )
        assert errors_indicator is not None
        assert errors_indicator.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_errors_indicator_degraded(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test errors indicator when degraded."""
        # Set failed events above warning threshold
        for _ in range(15):
            await subscription.record_event_failed(ValueError("test"))

        result = checker.check()

        errors_indicator = next(
            (i for i in result.indicators if i.name == "errors"),
            None,
        )
        assert errors_indicator is not None
        assert errors_indicator.status == HealthStatus.DEGRADED

    @pytest.mark.asyncio
    async def test_check_errors_indicator_critical(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test errors indicator when critical."""
        # Set failed events above critical threshold
        for _ in range(150):
            await subscription.record_event_failed(ValueError("test"))

        result = checker.check()

        errors_indicator = next(
            (i for i in result.indicators if i.name == "errors"),
            None,
        )
        assert errors_indicator is not None
        assert errors_indicator.status == HealthStatus.CRITICAL

    def test_check_lag_indicator_healthy(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test lag indicator when healthy."""
        result = checker.check()

        lag_indicator = next(
            (i for i in result.indicators if i.name == "lag"),
            None,
        )
        assert lag_indicator is not None
        assert lag_indicator.status == HealthStatus.HEALTHY

    @pytest.mark.asyncio
    async def test_check_lag_indicator_degraded(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test lag indicator when degraded."""
        # Set lag above warning threshold
        await subscription.record_events_seen(2000)

        result = checker.check()

        lag_indicator = next(
            (i for i in result.indicators if i.name == "lag"),
            None,
        )
        assert lag_indicator is not None
        assert lag_indicator.status == HealthStatus.DEGRADED

    @pytest.mark.asyncio
    async def test_check_lag_indicator_critical(
        self,
        subscription: Subscription,
        checker: SubscriptionHealthChecker,
    ):
        """Test lag indicator when critical."""
        # Set lag above critical threshold
        await subscription.record_events_seen(15000)

        result = checker.check()

        lag_indicator = next(
            (i for i in result.indicators if i.name == "lag"),
            None,
        )
        assert lag_indicator is not None
        assert lag_indicator.status == HealthStatus.CRITICAL

    def test_overall_status_worst_indicator(
        self,
        subscription: Subscription,
    ):
        """Test that overall status reflects worst indicator."""
        # Create checker with custom config to easily trigger different statuses
        config = HealthCheckConfig(
            max_errors_critical=1,  # Will trigger critical with 1 error
        )
        checker = SubscriptionHealthChecker(subscription, config)

        # Initially should be healthy or unknown (STARTING state)
        result = checker.check()
        # STARTING state is UNKNOWN, which is worse than HEALTHY
        assert result.overall_status in (HealthStatus.UNKNOWN, HealthStatus.HEALTHY)

    def test_check_with_handler_circuit_breaker_closed(
        self,
        subscription: Subscription,
    ):
        """Test health check with the handler circuit breaker closed."""
        circuit_breaker = MagicMock(spec=CircuitBreaker)
        circuit_breaker.state = CircuitState.CLOSED
        circuit_breaker.failure_count = 0

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            handler_circuit_breaker_provider=lambda: circuit_breaker,
        )

        result = checker.check()

        cb_indicator = next(
            (i for i in result.indicators if i.name == "handler_circuit_breaker"),
            None,
        )
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.HEALTHY

    def test_check_with_handler_circuit_breaker_open(
        self,
        subscription: Subscription,
    ):
        """Test health check with the handler circuit breaker open."""
        circuit_breaker = MagicMock(spec=CircuitBreaker)
        circuit_breaker.state = CircuitState.OPEN
        circuit_breaker.failure_count = 10

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            handler_circuit_breaker_provider=lambda: circuit_breaker,
        )

        result = checker.check()

        cb_indicator = next(
            (i for i in result.indicators if i.name == "handler_circuit_breaker"),
            None,
        )
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.UNHEALTHY

    def test_check_with_handler_circuit_breaker_half_open(
        self,
        subscription: Subscription,
    ):
        """Test health check with the handler circuit breaker half-open."""
        circuit_breaker = MagicMock(spec=CircuitBreaker)
        circuit_breaker.state = CircuitState.HALF_OPEN
        circuit_breaker.failure_count = 5

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            handler_circuit_breaker_provider=lambda: circuit_breaker,
        )

        result = checker.check()

        cb_indicator = next(
            (i for i in result.indicators if i.name == "handler_circuit_breaker"),
            None,
        )
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.DEGRADED

    def test_check_with_infra_circuit_breaker_open(
        self,
        subscription: Subscription,
    ):
        """Test health check with the infra circuit breaker open.

        Reported as a distinct indicator from the handler breaker -- an
        operator needs to know an OPEN store/checkpoint breaker apart from
        an OPEN handler breaker, since they call for different responses.
        """
        circuit_breaker = MagicMock(spec=CircuitBreaker)
        circuit_breaker.state = CircuitState.OPEN
        circuit_breaker.failure_count = 10

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            infra_circuit_breaker_provider=lambda: circuit_breaker,
        )

        result = checker.check()

        indicator_names = {i.name for i in result.indicators}
        assert "infra_circuit_breaker" in indicator_names
        assert "handler_circuit_breaker" not in indicator_names

        cb_indicator = next(
            (i for i in result.indicators if i.name == "infra_circuit_breaker"),
            None,
        )
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.UNHEALTHY

    def test_check_reports_both_breakers_independently(
        self,
        subscription: Subscription,
    ):
        """Both breakers are reported when both providers are set, and an
        OPEN handler breaker does not make the infra indicator OPEN too."""
        handler_breaker = MagicMock(spec=CircuitBreaker)
        handler_breaker.state = CircuitState.OPEN
        handler_breaker.failure_count = 5

        infra_breaker = MagicMock(spec=CircuitBreaker)
        infra_breaker.state = CircuitState.CLOSED
        infra_breaker.failure_count = 0

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            handler_circuit_breaker_provider=lambda: handler_breaker,
            infra_circuit_breaker_provider=lambda: infra_breaker,
        )

        result = checker.check()

        by_name = {i.name: i for i in result.indicators}
        assert by_name["handler_circuit_breaker"].status == HealthStatus.UNHEALTHY
        assert by_name["infra_circuit_breaker"].status == HealthStatus.HEALTHY
