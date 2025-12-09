"""
Unit tests for subscription metrics module (PHASE3-004).

Tests for:
- SubscriptionMetrics class
- NoOp instruments when OTel unavailable
- Metric recording methods
- Metric snapshot functionality
- State and lag tracking
- Timer context manager
- Metrics registry
"""

from __future__ import annotations

import time
from unittest.mock import Mock

import pytest


class TestOTELMetricsAvailable:
    """Tests for OTEL_METRICS_AVAILABLE constant."""

    def test_otel_metrics_available_is_boolean(self):
        """OTEL_METRICS_AVAILABLE is a boolean."""
        from eventsource.subscriptions.metrics import OTEL_METRICS_AVAILABLE

        assert isinstance(OTEL_METRICS_AVAILABLE, bool)

    def test_otel_metrics_available_reflects_import(self):
        """OTEL_METRICS_AVAILABLE is True when opentelemetry is installed."""
        from eventsource.subscriptions.metrics import OTEL_METRICS_AVAILABLE

        # In test environment, OpenTelemetry should be available
        assert OTEL_METRICS_AVAILABLE is True


class TestStateValue:
    """Tests for StateValue enum."""

    def test_state_values(self):
        """State values are correctly defined."""
        from eventsource.subscriptions.metrics import StateValue

        assert StateValue.UNKNOWN == 0
        assert StateValue.STARTING == 1
        assert StateValue.CATCHING_UP == 2
        assert StateValue.LIVE == 3
        assert StateValue.PAUSED == 4
        assert StateValue.STOPPED == 5
        assert StateValue.ERROR == 6

    def test_state_mapping(self):
        """STATE_MAPPING maps strings to correct values."""
        from eventsource.subscriptions.metrics import STATE_MAPPING, StateValue

        assert STATE_MAPPING["starting"] == StateValue.STARTING
        assert STATE_MAPPING["catching_up"] == StateValue.CATCHING_UP
        assert STATE_MAPPING["live"] == StateValue.LIVE
        assert STATE_MAPPING["paused"] == StateValue.PAUSED
        assert STATE_MAPPING["stopped"] == StateValue.STOPPED
        assert STATE_MAPPING["error"] == StateValue.ERROR


class TestNoOpInstruments:
    """Tests for no-op metric instruments."""

    def test_noop_counter_add_does_nothing(self):
        """NoOpCounter.add does nothing."""
        from eventsource.subscriptions.metrics import NoOpCounter

        counter = NoOpCounter()
        # Should not raise
        counter.add(1)
        counter.add(10, {"key": "value"})

    def test_noop_histogram_record_does_nothing(self):
        """NoOpHistogram.record does nothing."""
        from eventsource.subscriptions.metrics import NoOpHistogram

        histogram = NoOpHistogram()
        # Should not raise
        histogram.record(1.5)
        histogram.record(10.0, {"key": "value"})

    def test_noop_gauge_init_does_nothing(self):
        """NoOpGauge init does nothing."""
        from eventsource.subscriptions.metrics import NoOpGauge

        gauge = NoOpGauge()
        # Should not raise - just verify we can create it
        assert gauge is not None


class TestMetricSnapshot:
    """Tests for MetricSnapshot dataclass."""

    def test_snapshot_default_values(self):
        """MetricSnapshot has sensible defaults."""
        from eventsource.subscriptions.metrics import MetricSnapshot, StateValue

        snapshot = MetricSnapshot()

        assert snapshot.events_processed == 0
        assert snapshot.events_failed == 0
        assert snapshot.total_processing_time_ms == 0.0
        assert snapshot.current_lag == 0
        assert snapshot.current_state == StateValue.UNKNOWN
        assert snapshot.current_state_name == "unknown"

    def test_snapshot_with_values(self):
        """MetricSnapshot stores provided values."""
        from eventsource.subscriptions.metrics import MetricSnapshot, StateValue

        snapshot = MetricSnapshot(
            events_processed=100,
            events_failed=5,
            total_processing_time_ms=1500.5,
            current_lag=10,
            current_state=StateValue.LIVE,
            current_state_name="live",
        )

        assert snapshot.events_processed == 100
        assert snapshot.events_failed == 5
        assert snapshot.total_processing_time_ms == 1500.5
        assert snapshot.current_lag == 10
        assert snapshot.current_state == StateValue.LIVE
        assert snapshot.current_state_name == "live"

    def test_snapshot_to_dict(self):
        """MetricSnapshot.to_dict returns correct dictionary."""
        from eventsource.subscriptions.metrics import MetricSnapshot, StateValue

        snapshot = MetricSnapshot(
            events_processed=100,
            events_failed=5,
            total_processing_time_ms=1500.5,
            current_lag=10,
            current_state=StateValue.LIVE,
            current_state_name="live",
        )

        result = snapshot.to_dict()

        assert result["events_processed"] == 100
        assert result["events_failed"] == 5
        assert result["total_processing_time_ms"] == 1500.5
        assert result["current_lag"] == 10
        assert result["current_state"] == StateValue.LIVE
        assert result["current_state_name"] == "live"


class TestSubscriptionMetrics:
    """Tests for SubscriptionMetrics class."""

    @pytest.fixture(autouse=True)
    def reset_meter(self):
        """Reset global meter state between tests."""
        from eventsource.subscriptions.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_init_with_name(self):
        """Subscription metrics initializes with name."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        assert metrics.subscription_name == "TestSubscription"
        assert metrics.enable_metrics is True

    def test_init_with_metrics_disabled(self):
        """Subscription metrics initializes with metrics disabled."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(
            subscription_name="TestSubscription",
            enable_metrics=False,
        )

        assert metrics.subscription_name == "TestSubscription"
        assert metrics.enable_metrics is False
        assert metrics.metrics_enabled is False

    def test_metrics_enabled_property(self):
        """metrics_enabled reflects actual state."""
        from eventsource.subscriptions.metrics import (
            OTEL_METRICS_AVAILABLE,
            SubscriptionMetrics,
        )

        metrics = SubscriptionMetrics(
            subscription_name="TestSubscription",
            enable_metrics=True,
        )

        # Should be True when OTel is available
        assert metrics.metrics_enabled == OTEL_METRICS_AVAILABLE

    def test_record_event_processed(self):
        """record_event_processed updates internal counters."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        metrics.record_event_processed("OrderCreated", 15.5)
        metrics.record_event_processed("OrderUpdated", 10.0)

        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 2
        assert snapshot.total_processing_time_ms == 25.5

    def test_record_event_failed(self):
        """record_event_failed updates internal counters."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        metrics.record_event_failed("OrderCreated", "ValidationError")
        metrics.record_event_failed("OrderUpdated", "DatabaseError", duration_ms=5.0)

        snapshot = metrics.get_snapshot()
        assert snapshot.events_failed == 2
        assert snapshot.total_processing_time_ms == 5.0

    def test_record_lag(self):
        """record_lag updates lag value."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        metrics.record_lag(100)
        assert metrics.current_lag == 100
        assert metrics.get_snapshot().current_lag == 100

        metrics.record_lag(50)
        assert metrics.current_lag == 50

    def test_record_lag_negative_becomes_zero(self):
        """record_lag with negative value becomes zero."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        metrics.record_lag(-10)
        assert metrics.current_lag == 0

    def test_record_state(self):
        """record_state updates state value."""
        from eventsource.subscriptions.metrics import StateValue, SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        metrics.record_state("live")
        assert metrics.current_state == "live"
        assert metrics.get_snapshot().current_state == StateValue.LIVE
        assert metrics.get_snapshot().current_state_name == "live"

        metrics.record_state("CATCHING_UP")  # Should work with uppercase
        assert metrics.current_state == "catching_up"
        assert metrics.get_snapshot().current_state == StateValue.CATCHING_UP

    def test_record_state_unknown(self):
        """record_state with unknown value maps to UNKNOWN."""
        from eventsource.subscriptions.metrics import StateValue, SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        metrics.record_state("invalid_state")
        assert metrics.current_state == "invalid_state"
        assert metrics.get_snapshot().current_state == StateValue.UNKNOWN

    def test_time_processing_context_manager(self):
        """time_processing context manager measures duration."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        with metrics.time_processing() as timer:
            time.sleep(0.01)  # 10ms

        # Should measure at least 10ms
        assert timer.duration_ms >= 10.0
        assert timer.duration_ms < 100.0  # Reasonable upper bound

    def test_time_processing_duration_before_stop(self):
        """time_processing timer shows running duration."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")

        with metrics.time_processing() as timer:
            time.sleep(0.005)
            mid_duration = timer.duration_ms
            time.sleep(0.005)

        # Mid-duration should be less than final
        assert mid_duration < timer.duration_ms

    def test_get_snapshot(self):
        """get_snapshot returns accurate snapshot."""
        from eventsource.subscriptions.metrics import StateValue, SubscriptionMetrics

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

    @pytest.fixture(autouse=True)
    def reset_meter(self):
        """Reset global meter state between tests."""
        from eventsource.subscriptions.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_metrics_disabled_uses_noop(self):
        """Metrics with enable_metrics=False uses no-op instruments."""
        from eventsource.subscriptions.metrics import (
            NoOpCounter,
            NoOpHistogram,
            SubscriptionMetrics,
        )

        metrics = SubscriptionMetrics(
            subscription_name="TestSubscription",
            enable_metrics=False,
        )

        # Should use no-op instruments
        assert isinstance(metrics._events_processed_counter, NoOpCounter)
        assert isinstance(metrics._events_failed_counter, NoOpCounter)
        assert isinstance(metrics._processing_duration_histogram, NoOpHistogram)

    def test_noop_instruments_dont_raise(self):
        """No-op instruments don't raise when called."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(
            subscription_name="TestSubscription",
            enable_metrics=False,
        )

        # These should not raise
        metrics.record_event_processed("Event1", 10.0)
        metrics.record_event_failed("Event2", "Error")
        metrics.record_lag(100)
        metrics.record_state("live")

        # Snapshot should still work
        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 1
        assert snapshot.events_failed == 1


class TestMetricsWithMockedOTel:
    """Tests for metrics with mocked OpenTelemetry."""

    @pytest.fixture(autouse=True)
    def reset_meter(self):
        """Reset global meter state between tests."""
        from eventsource.subscriptions.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    @pytest.fixture
    def mock_counter(self):
        """Create mock counter."""
        counter = Mock()
        counter.add = Mock()
        return counter

    @pytest.fixture
    def mock_histogram(self):
        """Create mock histogram."""
        histogram = Mock()
        histogram.record = Mock()
        return histogram

    def test_record_event_processed_calls_counter(self, mock_counter, mock_histogram):
        """record_event_processed calls counter and histogram."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

        metrics = SubscriptionMetrics(subscription_name="TestSubscription")
        metrics._events_processed_counter = mock_counter
        metrics._processing_duration_histogram = mock_histogram

        metrics.record_event_processed("OrderCreated", 15.5, status="success")

        mock_counter.add.assert_called_once_with(
            1,
            {
                "subscription": "TestSubscription",
                "event.type": "OrderCreated",
                "status": "success",
            },
        )
        mock_histogram.record.assert_called_once_with(
            15.5,
            {
                "subscription": "TestSubscription",
                "event.type": "OrderCreated",
                "status": "success",
            },
        )

    def test_record_event_failed_calls_counter(self, mock_counter):
        """record_event_failed calls counter."""
        from eventsource.subscriptions.metrics import SubscriptionMetrics

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


class TestMetricsRegistry:
    """Tests for metrics registry functions."""

    @pytest.fixture(autouse=True)
    def reset_registry(self):
        """Reset registry between tests."""
        from eventsource.subscriptions.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_get_metrics_creates_new(self):
        """get_metrics creates new instance."""
        from eventsource.subscriptions.metrics import get_metrics

        metrics = get_metrics("TestSubscription")

        assert metrics.subscription_name == "TestSubscription"
        assert metrics.enable_metrics is True

    def test_get_metrics_returns_same_instance(self):
        """get_metrics returns same instance for same name."""
        from eventsource.subscriptions.metrics import get_metrics

        metrics1 = get_metrics("TestSubscription")
        metrics2 = get_metrics("TestSubscription")

        assert metrics1 is metrics2

    def test_get_metrics_different_names(self):
        """get_metrics returns different instances for different names."""
        from eventsource.subscriptions.metrics import get_metrics

        metrics1 = get_metrics("Subscription1")
        metrics2 = get_metrics("Subscription2")

        assert metrics1 is not metrics2
        assert metrics1.subscription_name == "Subscription1"
        assert metrics2.subscription_name == "Subscription2"

    def test_get_metrics_with_disabled(self):
        """get_metrics respects enable_metrics parameter."""
        from eventsource.subscriptions.metrics import get_metrics

        metrics = get_metrics("TestSubscription", enable_metrics=False)

        assert metrics.enable_metrics is False

    def test_clear_metrics_registry(self):
        """clear_metrics_registry clears all instances."""
        from eventsource.subscriptions.metrics import (
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
        from eventsource.subscriptions.metrics import _get_meter, reset_meter

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
        from eventsource.subscriptions.metrics import _Timer

        timer = _Timer()
        assert timer.duration_ms == 0.0

    def test_timer_start_stop(self):
        """Timer measures duration between start and stop."""
        from eventsource.subscriptions.metrics import _Timer

        timer = _Timer()
        timer.start()
        time.sleep(0.01)  # 10ms
        timer.stop()

        assert timer.duration_ms >= 10.0

    def test_timer_stop_idempotent(self):
        """Timer stop is idempotent."""
        from eventsource.subscriptions.metrics import _Timer

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


class TestImports:
    """Test module imports work correctly."""

    def test_import_from_subscriptions(self):
        """All exports are importable from subscriptions module."""
        from eventsource.subscriptions import (
            OTEL_METRICS_AVAILABLE,
            STATE_MAPPING,
            MetricSnapshot,
            NoOpCounter,
            NoOpGauge,
            NoOpHistogram,
            StateValue,
            SubscriptionMetrics,
            clear_metrics_registry,
            get_metrics,
            reset_meter,
        )

        assert OTEL_METRICS_AVAILABLE is not None
        assert SubscriptionMetrics is not None
        assert MetricSnapshot is not None
        assert StateValue is not None
        assert STATE_MAPPING is not None
        assert NoOpCounter is not None
        assert NoOpHistogram is not None
        assert NoOpGauge is not None
        assert get_metrics is not None
        assert clear_metrics_registry is not None
        assert reset_meter is not None

    def test_import_from_metrics_module(self):
        """All exports are importable from metrics module."""
        from eventsource.subscriptions.metrics import (
            OTEL_METRICS_AVAILABLE,
            STATE_MAPPING,
            MetricSnapshot,
            NoOpCounter,
            NoOpGauge,
            NoOpHistogram,
            StateValue,
            SubscriptionMetrics,
            clear_metrics_registry,
            get_metrics,
            reset_meter,
        )

        assert OTEL_METRICS_AVAILABLE is not None
        assert SubscriptionMetrics is not None
        assert MetricSnapshot is not None
        assert StateValue is not None
        assert STATE_MAPPING is not None
        assert NoOpCounter is not None
        assert NoOpHistogram is not None
        assert NoOpGauge is not None
        assert get_metrics is not None
        assert clear_metrics_registry is not None
        assert reset_meter is not None


class TestObservableGaugeCallbacks:
    """Tests for observable gauge callbacks."""

    @pytest.fixture(autouse=True)
    def reset_meter(self):
        """Reset global meter state between tests."""
        from eventsource.subscriptions.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_observe_lag_callback(self):
        """_observe_lag callback yields correct observation."""
        from eventsource.subscriptions.metrics import (
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
        from eventsource.subscriptions.metrics import (
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
