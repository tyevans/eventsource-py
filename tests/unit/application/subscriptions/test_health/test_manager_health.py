"""Unit tests for ManagerHealthChecker."""

import pytest

from eventsource.application.subscriptions.health import (
    HealthCheckConfig,
    ManagerHealthChecker,
    SubscriptionHealthChecker,
)
from eventsource.application.subscriptions.subscription import Subscription, SubscriptionState


class TestManagerHealthChecker:
    """Tests for ManagerHealthChecker."""

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
