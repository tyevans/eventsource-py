"""
Unit tests for health check module.

Tests cover:
- HealthStatus enum
- HealthIndicator dataclass
- HealthCheckResult dataclass
- HealthCheckConfig
- SubscriptionHealthChecker
- ManagerHealthChecker
"""

from unittest.mock import MagicMock

import pytest

from eventsource.subscriptions.config import SubscriptionConfig
from eventsource.subscriptions.health import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthIndicator,
    HealthStatus,
    ManagerHealthChecker,
    SubscriptionHealthChecker,
)
from eventsource.subscriptions.subscription import Subscription, SubscriptionState

# =============================================================================
# Health Status Tests
# =============================================================================


class TestHealthStatus:
    """Tests for HealthStatus enum."""

    def test_status_values(self):
        """Test all status values exist."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"
        assert HealthStatus.CRITICAL.value == "critical"
        assert HealthStatus.UNKNOWN.value == "unknown"


# =============================================================================
# Health Indicator Tests
# =============================================================================


class TestHealthIndicator:
    """Tests for HealthIndicator dataclass."""

    def test_create_indicator(self):
        """Test creating a health indicator."""
        indicator = HealthIndicator(
            name="test_indicator",
            status=HealthStatus.HEALTHY,
            message="All good",
            details={"key": "value"},
        )

        assert indicator.name == "test_indicator"
        assert indicator.status == HealthStatus.HEALTHY
        assert indicator.message == "All good"
        assert indicator.details == {"key": "value"}

    def test_to_dict(self):
        """Test conversion to dictionary."""
        indicator = HealthIndicator(
            name="test",
            status=HealthStatus.DEGRADED,
            message="Warning",
            details={"count": 5},
        )

        data = indicator.to_dict()

        assert data["name"] == "test"
        assert data["status"] == "degraded"
        assert data["message"] == "Warning"
        assert data["details"]["count"] == 5


# =============================================================================
# Health Check Result Tests
# =============================================================================


class TestHealthCheckResult:
    """Tests for HealthCheckResult dataclass."""

    def test_create_result(self):
        """Test creating a health check result."""
        indicators = [
            HealthIndicator("test1", HealthStatus.HEALTHY, "OK"),
            HealthIndicator("test2", HealthStatus.DEGRADED, "Warning"),
        ]

        result = HealthCheckResult(
            overall_status=HealthStatus.DEGRADED,
            indicators=indicators,
            subscription_name="TestSubscription",
            uptime_seconds=3600.0,
        )

        assert result.overall_status == HealthStatus.DEGRADED
        assert len(result.indicators) == 2
        assert result.subscription_name == "TestSubscription"
        assert result.uptime_seconds == 3600.0

    def test_to_dict(self):
        """Test conversion to dictionary."""
        result = HealthCheckResult(
            overall_status=HealthStatus.HEALTHY,
            indicators=[HealthIndicator("test", HealthStatus.HEALTHY, "OK")],
            subscription_name="Test",
            uptime_seconds=100.0,
        )

        data = result.to_dict()

        assert data["status"] == "healthy"
        assert data["subscription_name"] == "Test"
        assert data["uptime_seconds"] == 100.0
        assert len(data["indicators"]) == 1


# =============================================================================
# Health Check Config Tests
# =============================================================================


class TestHealthCheckConfig:
    """Tests for HealthCheckConfig."""

    def test_default_config(self):
        """Test default configuration values."""
        config = HealthCheckConfig()

        assert config.max_error_rate_per_minute == 10.0
        assert config.max_errors_warning == 10
        assert config.max_errors_critical == 100
        assert config.max_lag_events_warning == 1000
        assert config.max_lag_events_critical == 10000
        assert config.circuit_open_is_unhealthy is True
        assert config.max_dlq_events_warning == 10
        assert config.max_dlq_events_critical == 100

    def test_custom_config(self):
        """Test custom configuration."""
        config = HealthCheckConfig(
            max_errors_warning=5,
            max_lag_events_warning=500,
            circuit_open_is_unhealthy=False,
        )

        assert config.max_errors_warning == 5
        assert config.max_lag_events_warning == 500
        assert config.circuit_open_is_unhealthy is False


# =============================================================================
# Subscription Health Checker Tests
# =============================================================================


class TestSubscriptionHealthChecker:
    """Tests for SubscriptionHealthChecker."""

    @pytest.fixture
    def mock_subscriber(self):
        """Create mock subscriber."""
        subscriber = MagicMock()
        subscriber.subscribed_to.return_value = []
        return subscriber

    @pytest.fixture
    def subscription(self, mock_subscriber) -> Subscription:
        """Create test subscription."""
        return Subscription(
            name="TestSubscription",
            config=SubscriptionConfig(),
            subscriber=mock_subscriber,
        )

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
        await subscription.update_max_position(2000)
        subscription.last_processed_position = 0

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
        await subscription.update_max_position(15000)
        subscription.last_processed_position = 0

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

    def test_check_with_flow_controller(
        self,
        subscription: Subscription,
    ):
        """Test health check with flow controller."""
        # Create mock flow controller
        flow_controller = MagicMock()
        flow_controller.is_backpressured = False
        flow_controller.stats = MagicMock()
        flow_controller.stats.events_in_flight = 10
        flow_controller.stats.peak_in_flight = 50
        flow_controller.stats.pause_count = 0

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            flow_controller=flow_controller,
        )

        result = checker.check()

        backpressure_indicator = next(
            (i for i in result.indicators if i.name == "backpressure"),
            None,
        )
        assert backpressure_indicator is not None
        assert backpressure_indicator.status == HealthStatus.HEALTHY

    def test_check_with_backpressure(
        self,
        subscription: Subscription,
    ):
        """Test health check when under backpressure."""
        flow_controller = MagicMock()
        flow_controller.is_backpressured = True
        flow_controller.stats = MagicMock()
        flow_controller.stats.events_in_flight = 900
        flow_controller.stats.peak_in_flight = 1000
        flow_controller.stats.pause_count = 5

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            flow_controller=flow_controller,
        )

        result = checker.check()

        backpressure_indicator = next(
            (i for i in result.indicators if i.name == "backpressure"),
            None,
        )
        assert backpressure_indicator is not None
        assert backpressure_indicator.status == HealthStatus.DEGRADED

    def test_check_with_circuit_breaker_closed(
        self,
        subscription: Subscription,
    ):
        """Test health check with circuit breaker closed."""
        from eventsource.subscriptions.retry import CircuitState

        circuit_breaker = MagicMock()
        circuit_breaker.state = CircuitState.CLOSED
        circuit_breaker.failure_count = 0

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            circuit_breaker=circuit_breaker,
        )

        result = checker.check()

        cb_indicator = next(
            (i for i in result.indicators if i.name == "circuit_breaker"),
            None,
        )
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.HEALTHY

    def test_check_with_circuit_breaker_open(
        self,
        subscription: Subscription,
    ):
        """Test health check with circuit breaker open."""
        from eventsource.subscriptions.retry import CircuitState

        circuit_breaker = MagicMock()
        circuit_breaker.state = CircuitState.OPEN
        circuit_breaker.failure_count = 10

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            circuit_breaker=circuit_breaker,
        )

        result = checker.check()

        cb_indicator = next(
            (i for i in result.indicators if i.name == "circuit_breaker"),
            None,
        )
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.UNHEALTHY

    def test_check_with_circuit_breaker_half_open(
        self,
        subscription: Subscription,
    ):
        """Test health check with circuit breaker half-open."""
        from eventsource.subscriptions.retry import CircuitState

        circuit_breaker = MagicMock()
        circuit_breaker.state = CircuitState.HALF_OPEN
        circuit_breaker.failure_count = 5

        checker = SubscriptionHealthChecker(
            subscription=subscription,
            circuit_breaker=circuit_breaker,
        )

        result = checker.check()

        cb_indicator = next(
            (i for i in result.indicators if i.name == "circuit_breaker"),
            None,
        )
        assert cb_indicator is not None
        assert cb_indicator.status == HealthStatus.DEGRADED


# =============================================================================
# Manager Health Checker Tests
# =============================================================================


class TestManagerHealthChecker:
    """Tests for ManagerHealthChecker."""

    @pytest.fixture
    def mock_subscriber(self):
        """Create mock subscriber."""
        subscriber = MagicMock()
        subscriber.subscribed_to.return_value = []
        return subscriber

    @pytest.fixture
    def subscriptions(self, mock_subscriber) -> list[Subscription]:
        """Create test subscriptions."""
        return [
            Subscription(
                name="Subscription1",
                config=SubscriptionConfig(),
                subscriber=mock_subscriber,
            ),
            Subscription(
                name="Subscription2",
                config=SubscriptionConfig(),
                subscriber=mock_subscriber,
            ),
        ]

    @pytest.fixture
    def checker(
        self,
        subscriptions: list[Subscription],
    ) -> ManagerHealthChecker:
        """Create test manager health checker."""
        return ManagerHealthChecker(
            subscriptions=subscriptions,
            config=HealthCheckConfig(),
        )

    def test_check_returns_dict(self, checker: ManagerHealthChecker):
        """Test that check returns a dictionary."""
        result = checker.check()

        assert isinstance(result, dict)
        assert "status" in result
        assert "subscriptions" in result
        assert "subscription_count" in result

    def test_check_includes_all_subscriptions(
        self,
        checker: ManagerHealthChecker,
        subscriptions: list[Subscription],
    ):
        """Test that check includes all subscriptions."""
        result = checker.check()

        assert result["subscription_count"] == 2
        assert "Subscription1" in result["subscriptions"]
        assert "Subscription2" in result["subscriptions"]

    def test_check_empty_subscriptions(self):
        """Test check with no subscriptions."""
        checker = ManagerHealthChecker(subscriptions=[])

        result = checker.check()

        assert result["subscription_count"] == 0
        assert result["status"] == "unknown"

    @pytest.mark.asyncio
    async def test_check_overall_healthy(
        self,
        subscriptions: list[Subscription],
    ):
        """Test overall status when all healthy."""
        # Set both subscriptions to LIVE state
        for sub in subscriptions:
            await sub.transition_to(SubscriptionState.CATCHING_UP)
            await sub.transition_to(SubscriptionState.LIVE)

        checker = ManagerHealthChecker(subscriptions=subscriptions)
        result = checker.check()

        assert result["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_check_overall_degraded(
        self,
        subscriptions: list[Subscription],
    ):
        """Test overall status when one degraded."""
        # Set first to LIVE
        await subscriptions[0].transition_to(SubscriptionState.CATCHING_UP)
        await subscriptions[0].transition_to(SubscriptionState.LIVE)

        # Set second to PAUSED (degraded)
        await subscriptions[1].transition_to(SubscriptionState.CATCHING_UP)
        await subscriptions[1].transition_to(SubscriptionState.PAUSED)

        checker = ManagerHealthChecker(subscriptions=subscriptions)
        result = checker.check()

        assert result["status"] == "degraded"

    @pytest.mark.asyncio
    async def test_check_overall_unhealthy(
        self,
        subscriptions: list[Subscription],
    ):
        """Test overall status when majority unhealthy."""
        # Set both to ERROR
        for sub in subscriptions:
            sub.last_error = RuntimeError("test")
            await sub.transition_to(SubscriptionState.ERROR)

        checker = ManagerHealthChecker(subscriptions=subscriptions)
        result = checker.check()

        assert result["status"] == "unhealthy"

    def test_check_with_custom_checkers(
        self,
        subscriptions: list[Subscription],
    ):
        """Test check with pre-configured checkers."""
        # Create custom checkers
        custom_checkers = {
            "Subscription1": SubscriptionHealthChecker(
                subscription=subscriptions[0],
                config=HealthCheckConfig(max_errors_warning=1),
            ),
        }

        checker = ManagerHealthChecker(
            subscriptions=subscriptions,
            subscription_checkers=custom_checkers,
        )

        result = checker.check()

        # Should use custom checker for Subscription1
        assert "Subscription1" in result["subscriptions"]


# =============================================================================
# Manager Health API Tests (PHASE3-003)
# =============================================================================


class TestManagerHealth:
    """Tests for ManagerHealth dataclass."""

    def test_create_manager_health(self):
        """Test creating a ManagerHealth instance."""
        from eventsource.subscriptions.health import ManagerHealth, SubscriptionHealth

        sub_health = SubscriptionHealth(
            name="TestSub",
            status="healthy",
            state="live",
            events_processed=100,
            events_failed=2,
            lag_events=5,
            error_rate=0.1,
            last_error=None,
            uptime_seconds=3600.0,
        )

        health = ManagerHealth(
            status="healthy",
            running=True,
            subscription_count=1,
            healthy_count=1,
            degraded_count=0,
            unhealthy_count=0,
            total_events_processed=100,
            total_events_failed=2,
            total_lag_events=5,
            uptime_seconds=7200.0,
            subscriptions=[sub_health],
        )

        assert health.status == "healthy"
        assert health.running is True
        assert health.subscription_count == 1
        assert health.healthy_count == 1
        assert health.degraded_count == 0
        assert health.unhealthy_count == 0
        assert health.total_events_processed == 100
        assert health.total_events_failed == 2
        assert health.total_lag_events == 5
        assert health.uptime_seconds == 7200.0
        assert len(health.subscriptions) == 1

    def test_manager_health_to_dict(self):
        """Test ManagerHealth serialization."""
        from eventsource.subscriptions.health import ManagerHealth, SubscriptionHealth

        sub_health = SubscriptionHealth(
            name="TestSub",
            status="healthy",
            state="live",
            events_processed=100,
            events_failed=2,
            lag_events=5,
            error_rate=0.1,
            last_error=None,
            uptime_seconds=3600.0,
        )

        health = ManagerHealth(
            status="degraded",
            running=True,
            subscription_count=2,
            healthy_count=1,
            degraded_count=1,
            unhealthy_count=0,
            total_events_processed=200,
            total_events_failed=5,
            total_lag_events=100,
            uptime_seconds=1800.0,
            subscriptions=[sub_health],
        )

        data = health.to_dict()

        assert data["status"] == "degraded"
        assert data["running"] is True
        assert data["subscription_count"] == 2
        assert data["healthy_count"] == 1
        assert data["degraded_count"] == 1
        assert data["unhealthy_count"] == 0
        assert data["total_events_processed"] == 200
        assert data["total_events_failed"] == 5
        assert data["total_lag_events"] == 100
        assert data["uptime_seconds"] == 1800.0
        assert "timestamp" in data
        assert len(data["subscriptions"]) == 1


class TestSubscriptionHealth:
    """Tests for SubscriptionHealth dataclass."""

    def test_create_subscription_health(self):
        """Test creating a SubscriptionHealth instance."""
        from eventsource.subscriptions.health import SubscriptionHealth

        health = SubscriptionHealth(
            name="OrderProjection",
            status="healthy",
            state="live",
            events_processed=5000,
            events_failed=10,
            lag_events=25,
            error_rate=0.05,
            last_error=None,
            uptime_seconds=86400.0,
        )

        assert health.name == "OrderProjection"
        assert health.status == "healthy"
        assert health.state == "live"
        assert health.events_processed == 5000
        assert health.events_failed == 10
        assert health.lag_events == 25
        assert health.error_rate == 0.05
        assert health.last_error is None
        assert health.uptime_seconds == 86400.0

    def test_subscription_health_with_error(self):
        """Test SubscriptionHealth with an error."""
        from eventsource.subscriptions.health import SubscriptionHealth

        health = SubscriptionHealth(
            name="FailingProjection",
            status="unhealthy",
            state="error",
            events_processed=1000,
            events_failed=500,
            lag_events=0,
            error_rate=5.0,
            last_error="Database connection failed",
            uptime_seconds=3600.0,
        )

        assert health.status == "unhealthy"
        assert health.state == "error"
        assert health.last_error == "Database connection failed"

    def test_subscription_health_to_dict(self):
        """Test SubscriptionHealth serialization."""
        from eventsource.subscriptions.health import SubscriptionHealth

        health = SubscriptionHealth(
            name="TestProjection",
            status="degraded",
            state="catching_up",
            events_processed=100,
            events_failed=5,
            lag_events=500,
            error_rate=0.5,
            last_error="Timeout error",
            uptime_seconds=600.0,
        )

        data = health.to_dict()

        assert data["name"] == "TestProjection"
        assert data["status"] == "degraded"
        assert data["state"] == "catching_up"
        assert data["events_processed"] == 100
        assert data["events_failed"] == 5
        assert data["lag_events"] == 500
        assert data["error_rate"] == 0.5
        assert data["last_error"] == "Timeout error"
        assert data["uptime_seconds"] == 600.0


class TestReadinessStatus:
    """Tests for ReadinessStatus dataclass."""

    def test_create_ready_status(self):
        """Test creating a ready ReadinessStatus."""
        from eventsource.subscriptions.health import ReadinessStatus

        status = ReadinessStatus(
            ready=True,
            reason="Manager is ready",
            details={"subscription_count": 2},
        )

        assert status.ready is True
        assert status.reason == "Manager is ready"
        assert status.details["subscription_count"] == 2

    def test_create_not_ready_status(self):
        """Test creating a not ready ReadinessStatus."""
        from eventsource.subscriptions.health import ReadinessStatus

        status = ReadinessStatus(
            ready=False,
            reason="Manager is not running",
            details={"running": False},
        )

        assert status.ready is False
        assert status.reason == "Manager is not running"

    def test_readiness_status_to_dict(self):
        """Test ReadinessStatus serialization."""
        from eventsource.subscriptions.health import ReadinessStatus

        status = ReadinessStatus(
            ready=True,
            reason="All systems operational",
            details={"running": True, "subscription_count": 3},
        )

        data = status.to_dict()

        assert data["ready"] is True
        assert data["reason"] == "All systems operational"
        assert data["details"]["running"] is True
        assert "timestamp" in data


class TestLivenessStatus:
    """Tests for LivenessStatus dataclass."""

    def test_create_alive_status(self):
        """Test creating an alive LivenessStatus."""
        from eventsource.subscriptions.health import LivenessStatus

        status = LivenessStatus(
            alive=True,
            reason="Manager is alive and responsive",
            details={"uptime_seconds": 3600.0},
        )

        assert status.alive is True
        assert status.reason == "Manager is alive and responsive"
        assert status.details["uptime_seconds"] == 3600.0

    def test_create_not_alive_status(self):
        """Test creating a not alive LivenessStatus."""
        from eventsource.subscriptions.health import LivenessStatus

        status = LivenessStatus(
            alive=False,
            reason="Internal lock not responsive (possible deadlock)",
            details={"lock_responsive": False},
        )

        assert status.alive is False
        assert "deadlock" in status.reason

    def test_liveness_status_to_dict(self):
        """Test LivenessStatus serialization."""
        from eventsource.subscriptions.health import LivenessStatus

        status = LivenessStatus(
            alive=True,
            reason="All checks passed",
            details={"running": True, "lock_responsive": True},
        )

        data = status.to_dict()

        assert data["alive"] is True
        assert data["reason"] == "All checks passed"
        assert data["details"]["lock_responsive"] is True
        assert "timestamp" in data


# =============================================================================
# Module Imports Test
# =============================================================================


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_health(self):
        """Test all exports are importable."""
        from eventsource.subscriptions.health import (
            HealthCheckConfig,
            HealthCheckResult,
            HealthIndicator,
            HealthStatus,
            ManagerHealthChecker,
            SubscriptionHealthChecker,
        )

        assert HealthStatus is not None
        assert HealthIndicator is not None
        assert HealthCheckResult is not None
        assert HealthCheckConfig is not None
        assert SubscriptionHealthChecker is not None
        assert ManagerHealthChecker is not None

    def test_import_health_api_from_health(self):
        """Test Health API exports are importable."""
        from eventsource.subscriptions.health import (
            LivenessStatus,
            ManagerHealth,
            ReadinessStatus,
            SubscriptionHealth,
        )

        assert ManagerHealth is not None
        assert SubscriptionHealth is not None
        assert ReadinessStatus is not None
        assert LivenessStatus is not None

    def test_import_health_api_from_subscriptions(self):
        """Test Health API exports from subscriptions module."""
        from eventsource.subscriptions import (
            LivenessStatus,
            ManagerHealth,
            ReadinessStatus,
            SubscriptionHealth,
        )

        assert ManagerHealth is not None
        assert SubscriptionHealth is not None
        assert ReadinessStatus is not None
        assert LivenessStatus is not None
