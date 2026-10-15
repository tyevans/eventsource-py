"""Tests for SubscriptionMetrics class with and without OpenTelemetry."""

from __future__ import annotations

import time

from eventsource.application.subscriptions.metrics import (
    OTEL_METRICS_AVAILABLE,
    NoOpCounter,
    NoOpHistogram,
    StateValue,
    SubscriptionMetrics,
)


class TestSubscriptionMetrics:
    """Tests for SubscriptionMetrics class."""

    def test_init_with_name(self):
        """Subscription metrics initializes with name."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        assert metrics.subscription_name == "TestSubscription"
        assert metrics.enable_metrics is True

    def test_init_with_metrics_disabled(self):
        """Subscription metrics initializes with metrics disabled."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription", enable_metrics=False)
        assert metrics.subscription_name == "TestSubscription"
        assert metrics.enable_metrics is False
        assert metrics.metrics_enabled is False

    def test_metrics_enabled_property(self):
        """metrics_enabled reflects actual state."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription", enable_metrics=True)
        assert metrics.metrics_enabled == OTEL_METRICS_AVAILABLE

    def test_record_event_processed(self):
        """record_event_processed updates internal counters."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_event_processed("OrderCreated", 15.5)
        metrics.record_event_processed("OrderUpdated", 10.0)
        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 2
        assert snapshot.total_processing_time_ms == 25.5

    def test_record_event_failed(self):
        """record_event_failed updates internal counters."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_event_failed("OrderCreated", "ValidationError")
        metrics.record_event_failed("OrderUpdated", "DatabaseError", duration_ms=5.0)
        snapshot = metrics.get_snapshot()
        assert snapshot.events_failed == 2
        assert snapshot.total_processing_time_ms == 5.0

    def test_record_lag(self):
        """record_lag updates lag value."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_lag(100)
        assert metrics.current_lag == 100
        assert metrics.get_snapshot().current_lag == 100
        metrics.record_lag(50)
        assert metrics.current_lag == 50

    def test_record_lag_negative_becomes_zero(self):
        """record_lag with negative value becomes zero."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_lag(-10)
        assert metrics.current_lag == 0

    def test_record_state(self):
        """record_state updates state value."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_state("live")
        assert metrics.current_state == "live"
        assert metrics.get_snapshot().current_state == StateValue.LIVE
        assert metrics.get_snapshot().current_state_name == "live"
        metrics.record_state("CATCHING_UP")
        assert metrics.current_state == "catching_up"
        assert metrics.get_snapshot().current_state == StateValue.CATCHING_UP

    def test_record_state_unknown(self):
        """record_state with unknown value maps to UNKNOWN."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_state("invalid_state")
        assert metrics.current_state == "invalid_state"
        assert metrics.get_snapshot().current_state == StateValue.UNKNOWN

    def test_time_processing_context_manager(self):
        """time_processing context manager measures duration."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        with metrics.time_processing() as timer:
            time.sleep(0.01)
        assert timer.duration_ms >= 10.0
        assert timer.duration_ms < 100.0

    def test_time_processing_duration_before_stop(self):
        """time_processing timer shows running duration."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        with metrics.time_processing() as timer:
            time.sleep(0.005)
            mid_duration = timer.duration_ms
            time.sleep(0.005)
        assert mid_duration < timer.duration_ms

    def test_get_snapshot(self):
        """get_snapshot returns accurate snapshot."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics.record_event_processed("Event1", 10.0)
        metrics.record_event_failed("Event2", "Error", duration_ms=5.0)
        metrics.record_lag(50)
        metrics.record_state("live")
        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 1
        assert snapshot.events_failed == 1
        assert snapshot.total_processing_time_ms == 15.0
        assert snapshot.current_lag == 50
        assert snapshot.current_state == StateValue.LIVE
        assert snapshot.current_state_name == "live"


class TestSubscriptionMetricsNoOTel:
    """Tests for SubscriptionMetrics when OTel is not available."""

    def test_metrics_disabled_uses_noop(self):
        """Metrics with enable_metrics=False uses no-op instruments."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription", enable_metrics=False)
        assert isinstance(metrics._events_processed_counter, NoOpCounter)
        assert isinstance(metrics._events_failed_counter, NoOpCounter)
        assert isinstance(metrics._processing_duration_histogram, NoOpHistogram)

    def test_noop_instruments_dont_raise(self):
        """No-op instruments don't raise when called."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription", enable_metrics=False)
        metrics.record_event_processed("Event1", 10.0)
        metrics.record_event_failed("Event2", "Error")
        metrics.record_lag(100)
        metrics.record_state("live")
        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 1
        assert snapshot.events_failed == 1


class TestMetricsWithMockedOTel:
    """Tests for metrics with mocked OpenTelemetry."""

    def test_record_event_processed_calls_counter(self, mock_counter, mock_histogram):
        """record_event_processed calls counter and histogram."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics._events_processed_counter = mock_counter
        metrics._processing_duration_histogram = mock_histogram
        metrics.record_event_processed("OrderCreated", 15.5, status="success")
        expected_attrs = {
            "subscription": "TestSubscription",
            "event.type": "OrderCreated",
            "status": "success",
        }
        mock_counter.add.assert_called_once_with(1, expected_attrs)
        mock_histogram.record.assert_called_once_with(15.5, expected_attrs)

    def test_record_event_failed_calls_counter(self, mock_counter):
        """record_event_failed calls counter."""
        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics._events_failed_counter = mock_counter
        metrics.record_event_failed("OrderCreated", "ValidationError")
        mock_counter.add.assert_called_once_with(
            1,
            {
                "subscription": "TestSubscription",
                "event.type": "OrderCreated",
                "error.type": "ValidationError",
            },
        )
