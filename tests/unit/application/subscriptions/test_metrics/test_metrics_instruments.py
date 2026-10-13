"""Tests for OTel availability, state values, no-op instruments, snapshots, and imports."""

from __future__ import annotations


class TestOTELMetricsAvailable:
    """Tests for OTEL_METRICS_AVAILABLE constant."""

    def test_otel_metrics_available_is_boolean(self):
        """OTEL_METRICS_AVAILABLE is a boolean."""
        from eventsource.application.subscriptions.metrics import OTEL_METRICS_AVAILABLE

        assert isinstance(OTEL_METRICS_AVAILABLE, bool)

    def test_otel_metrics_available_reflects_import(self):
        """OTEL_METRICS_AVAILABLE is True when opentelemetry is installed."""
        from eventsource.application.subscriptions.metrics import OTEL_METRICS_AVAILABLE

        # In test environment, OpenTelemetry should be available
        assert OTEL_METRICS_AVAILABLE is True


class TestStateValue:
    """Tests for StateValue enum."""

    def test_state_values(self):
        """State values are correctly defined."""
        from eventsource.application.subscriptions.metrics import StateValue

        assert StateValue.UNKNOWN == 0
        assert StateValue.STARTING == 1
        assert StateValue.CATCHING_UP == 2
        assert StateValue.LIVE == 3
        assert StateValue.PAUSED == 4
        assert StateValue.STOPPED == 5
        assert StateValue.ERROR == 6

    def test_state_mapping(self):
        """STATE_MAPPING maps strings to correct values."""
        from eventsource.application.subscriptions.metrics import STATE_MAPPING, StateValue

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
        from eventsource.application.subscriptions.metrics import NoOpCounter

        counter = NoOpCounter()
        # Should not raise
        counter.add(1)
        counter.add(10, {"key": "value"})

    def test_noop_histogram_record_does_nothing(self):
        """NoOpHistogram.record does nothing."""
        from eventsource.application.subscriptions.metrics import NoOpHistogram

        histogram = NoOpHistogram()
        # Should not raise
        histogram.record(1.5)
        histogram.record(10.0, {"key": "value"})

    def test_noop_gauge_init_does_nothing(self):
        """NoOpGauge init does nothing."""
        from eventsource.application.subscriptions.metrics import NoOpGauge

        gauge = NoOpGauge()
        # Should not raise - just verify we can create it
        assert gauge is not None


class TestMetricSnapshot:
    """Tests for MetricSnapshot dataclass."""

    def test_snapshot_default_values(self):
        """MetricSnapshot has sensible defaults."""
        from eventsource.application.subscriptions.metrics import MetricSnapshot, StateValue

        snapshot = MetricSnapshot()

        assert snapshot.events_processed == 0
        assert snapshot.events_failed == 0
        assert snapshot.total_processing_time_ms == 0.0
        assert snapshot.current_lag == 0
        assert snapshot.current_state == StateValue.UNKNOWN
        assert snapshot.current_state_name == "unknown"

    def test_snapshot_with_values(self):
        """MetricSnapshot stores provided values."""
        from eventsource.application.subscriptions.metrics import MetricSnapshot, StateValue

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
        from eventsource.application.subscriptions.metrics import MetricSnapshot, StateValue

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


class TestImports:
    """Test module imports work correctly."""

    def test_import_from_subscriptions(self):
        """All exports are importable from subscriptions module."""
        from eventsource.application.subscriptions import (
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
        from eventsource.application.subscriptions.metrics import (
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
