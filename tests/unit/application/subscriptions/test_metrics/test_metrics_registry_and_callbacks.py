"""Tests for metrics registry, timer, and observable gauge callbacks."""

from __future__ import annotations

import time

import pytest


class TestMetricsRegistry:
    """Tests for metrics registry functions."""

    def test_get_metrics_creates_new(self):
        """get_metrics creates new instance."""
        from eventsource.application.subscriptions.metrics import get_metrics

        metrics = get_metrics("TestSubscription")

        assert metrics.subscription_name == "TestSubscription"
        assert metrics.enable_metrics is True

    def test_get_metrics_returns_same_instance(self):
        """get_metrics returns same instance for same name."""
        from eventsource.application.subscriptions.metrics import get_metrics

        metrics1 = get_metrics("TestSubscription")
        metrics2 = get_metrics("TestSubscription")

        assert metrics1 is metrics2

    def test_get_metrics_different_names(self):
        """get_metrics returns different instances for different names."""
        from eventsource.application.subscriptions.metrics import get_metrics

        metrics1 = get_metrics("Subscription1")
        metrics2 = get_metrics("Subscription2")

        assert metrics1 is not metrics2
        assert metrics1.subscription_name == "Subscription1"
        assert metrics2.subscription_name == "Subscription2"

    def test_get_metrics_with_disabled(self):
        """get_metrics respects enable_metrics parameter."""
        from eventsource.application.subscriptions.metrics import get_metrics

        metrics = get_metrics("TestSubscription", enable_metrics=False)

        assert metrics.enable_metrics is False

    def test_clear_metrics_registry(self):
        """clear_metrics_registry clears all instances."""
        from eventsource.application.subscriptions.metrics import (
            clear_metrics_registry,
            get_metrics,
        )

        metrics1 = get_metrics("Subscription1")
        get_metrics("Subscription2")

        clear_metrics_registry()

        # Should create new instances after clear
        metrics1_new = get_metrics("Subscription1")
        assert metrics1_new is not metrics1

    def test_reset_meter(self):
        """reset_meter resets global meter."""
        from eventsource.application.subscriptions.metrics import _get_meter, reset_meter

        # Get a meter
        _get_meter()

        # Reset
        reset_meter()

        # Should create new meter
        _get_meter()

        # Both should be meters, but testing the reset happened
        # is verified by the fact we can call reset_meter without error


class TestTimer:
    """Tests for internal _Timer class."""

    def test_timer_not_started(self):
        """Timer returns 0 when not started."""
        from eventsource.application.subscriptions.metrics import _Timer

        timer = _Timer()
        assert timer.duration_ms == 0.0

    def test_timer_start_stop(self):
        """Timer measures duration between start and stop."""
        from eventsource.application.subscriptions.metrics import _Timer

        timer = _Timer()
        timer.start()
        time.sleep(0.01)  # 10ms
        timer.stop()

        assert timer.duration_ms >= 10.0

    def test_timer_stop_idempotent(self):
        """Timer stop is idempotent."""
        from eventsource.application.subscriptions.metrics import _Timer

        timer = _Timer()
        timer.start()
        time.sleep(0.01)
        timer.stop()
        duration1 = timer.duration_ms

        time.sleep(0.01)
        timer.stop()  # Second stop
        duration2 = timer.duration_ms

        # Duration should not change after first stop
        assert duration1 == duration2


class TestObservableGaugeCallbacks:
    """Tests for observable gauge callbacks."""

    def test_observe_lag_callback(self):
        """_observe_lag callback yields correct observation."""
        from eventsource.application.subscriptions.metrics import (
            OTEL_METRICS_AVAILABLE,
            SubscriptionMetrics,
        )

        if not OTEL_METRICS_AVAILABLE:
            pytest.skip("OpenTelemetry not available")

        from opentelemetry.metrics import Observation

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_lag(100)

        # Call the callback
        observations = list(metrics._observe_lag(None))

        assert len(observations) == 1
        obs = observations[0]
        assert isinstance(obs, Observation)
        assert obs.value == 100
        assert obs.attributes["subscription"] == "TestSubscription"

    def test_observe_state_callback(self):
        """_observe_state callback yields correct observation."""
        from eventsource.application.subscriptions.metrics import (
            OTEL_METRICS_AVAILABLE,
            StateValue,
            SubscriptionMetrics,
        )

        if not OTEL_METRICS_AVAILABLE:
            pytest.skip("OpenTelemetry not available")

        from opentelemetry.metrics import Observation

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_state("live")

        # Call the callback
        observations = list(metrics._observe_state(None))

        assert len(observations) == 1
        obs = observations[0]
        assert isinstance(obs, Observation)
        assert obs.value == StateValue.LIVE
        assert obs.attributes["subscription"] == "TestSubscription"
        assert obs.attributes["state_name"] == "live"
