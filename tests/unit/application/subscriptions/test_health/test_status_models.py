"""Unit tests for subscription manager health status dataclasses (PHASE3-003)."""

from eventsource.application.subscriptions.health import (
    LivenessStatus,
    ManagerHealth,
    ReadinessStatus,
    SubscriptionHealth,
)


def _sample_sub_health() -> SubscriptionHealth:
    return SubscriptionHealth(
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


class TestManagerHealth:
    """Tests for ManagerHealth dataclass."""

    def test_create_manager_health(self):
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
            subscriptions=[_sample_sub_health()],
        )
        assert health.status == "healthy" and health.running is True
        assert (health.subscription_count, health.healthy_count) == (1, 1)
        assert (health.degraded_count, health.unhealthy_count) == (0, 0)
        assert (health.total_events_processed, health.total_events_failed) == (100, 2)
        assert health.total_lag_events == 5 and health.uptime_seconds == 7200.0
        assert len(health.subscriptions) == 1

    def test_manager_health_to_dict(self):
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
            subscriptions=[_sample_sub_health()],
        )
        data = health.to_dict()
        assert data["status"] == "degraded" and data["running"] is True
        assert (data["subscription_count"], data["healthy_count"]) == (2, 1)
        assert (data["degraded_count"], data["unhealthy_count"]) == (1, 0)
        assert (data["total_events_processed"], data["total_events_failed"]) == (200, 5)
        assert data["total_lag_events"] == 100 and data["uptime_seconds"] == 1800.0
        assert "timestamp" in data and len(data["subscriptions"]) == 1


class TestSubscriptionHealth:
    """Tests for SubscriptionHealth dataclass."""

    def test_create_subscription_health(self):
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
        assert health.name == "OrderProjection" and health.status == "healthy"
        assert health.state == "live" and health.events_processed == 5000
        assert health.events_failed == 10 and health.lag_events == 25
        assert health.error_rate == 0.05 and health.last_error is None
        assert health.uptime_seconds == 86400.0

    def test_subscription_health_with_error(self):
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
        assert health.status == "unhealthy" and health.state == "error"
        assert health.last_error == "Database connection failed"

    def test_subscription_health_to_dict(self):
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
        assert data["name"] == "TestProjection" and data["status"] == "degraded"
        assert data["state"] == "catching_up" and data["events_processed"] == 100
        assert data["events_failed"] == 5 and data["lag_events"] == 500
        assert data["error_rate"] == 0.5 and data["last_error"] == "Timeout error"
        assert data["uptime_seconds"] == 600.0


class TestReadinessStatus:
    """Tests for ReadinessStatus dataclass."""

    def test_create_ready_status(self):
        status = ReadinessStatus(
            ready=True, reason="Manager is ready", details={"subscription_count": 2}
        )
        assert status.ready is True and status.reason == "Manager is ready"
        assert status.details["subscription_count"] == 2

    def test_create_not_ready_status(self):
        status = ReadinessStatus(
            ready=False, reason="Manager is not running", details={"running": False}
        )
        assert status.ready is False and status.reason == "Manager is not running"

    def test_readiness_status_to_dict(self):
        status = ReadinessStatus(
            ready=True,
            reason="All systems operational",
            details={"running": True, "subscription_count": 3},
        )
        data = status.to_dict()
        assert data["ready"] is True and data["reason"] == "All systems operational"
        assert data["details"]["running"] is True and "timestamp" in data


class TestLivenessStatus:
    """Tests for LivenessStatus dataclass."""

    def test_create_alive_status(self):
        status = LivenessStatus(
            alive=True,
            reason="Manager is alive and responsive",
            details={"uptime_seconds": 3600.0},
        )
        assert status.alive is True and status.reason == "Manager is alive and responsive"
        assert status.details["uptime_seconds"] == 3600.0

    def test_create_not_alive_status(self):
        status = LivenessStatus(
            alive=False,
            reason="Internal lock not responsive (possible deadlock)",
            details={"lock_responsive": False},
        )
        assert status.alive is False and "deadlock" in status.reason

    def test_liveness_status_to_dict(self):
        status = LivenessStatus(
            alive=True,
            reason="All checks passed",
            details={"running": True, "lock_responsive": True},
        )
        data = status.to_dict()
        assert data["alive"] is True and data["reason"] == "All checks passed"
        assert data["details"]["lock_responsive"] is True and "timestamp" in data
