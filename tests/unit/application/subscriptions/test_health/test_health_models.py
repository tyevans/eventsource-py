"""Unit tests for health check core models and module imports."""

from eventsource.application.subscriptions.health import (
    HealthCheckConfig,
    HealthCheckResult,
    HealthIndicator,
    HealthStatus,
)


class TestHealthStatus:
    """Tests for HealthStatus enum."""

    def test_status_values(self):
        """Test all status values exist."""
        assert HealthStatus.HEALTHY.value == "healthy"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.UNHEALTHY.value == "unhealthy"
        assert HealthStatus.CRITICAL.value == "critical"
        assert HealthStatus.UNKNOWN.value == "unknown"


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


class TestModuleImports:
    """Tests for module imports."""

    def test_import_from_health(self):
        """Test all exports are importable."""
        from eventsource.application.subscriptions.health import (
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
        from eventsource.application.subscriptions.health import (
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
        from eventsource.application.subscriptions import (
            LivenessStatus,
            ManagerHealth,
            ReadinessStatus,
            SubscriptionHealth,
        )

        assert ManagerHealth is not None
        assert SubscriptionHealth is not None
        assert ReadinessStatus is not None
        assert LivenessStatus is not None
