"""Tests for no-op instruments and OpenTelemetry integration."""

from __future__ import annotations

from eventsource.application.migration.metrics import (
    OTEL_METRICS_AVAILABLE,
    MigrationMetrics,
    NoOpCounter,
    NoOpGauge,
    NoOpHistogram,
)


class TestOTELMetricsAvailable:
    """Tests for OTEL_METRICS_AVAILABLE constant."""

    def test_otel_metrics_available_is_boolean(self):
        """OTEL_METRICS_AVAILABLE is a boolean."""
        assert isinstance(OTEL_METRICS_AVAILABLE, bool)

    def test_otel_metrics_available_reflects_import(self):
        """OTEL_METRICS_AVAILABLE is True when opentelemetry is installed."""
        # In test environment, OpenTelemetry should be available
        assert OTEL_METRICS_AVAILABLE is True


class TestNoOpInstruments:
    """Tests for no-op metric instruments."""

    def test_noop_counter_add_does_nothing(self):
        """NoOpCounter.add does nothing."""
        counter = NoOpCounter()
        # Should not raise
        counter.add(1)
        counter.add(10, {"key": "value"})

    def test_noop_histogram_record_does_nothing(self):
        """NoOpHistogram.record does nothing."""
        histogram = NoOpHistogram()
        # Should not raise
        histogram.record(1.5)
        histogram.record(10.0, {"key": "value"})

    def test_noop_gauge_init_does_nothing(self):
        """NoOpGauge init does nothing."""
        gauge = NoOpGauge()
        # Should not raise - just verify we can create it
        assert gauge is not None


class TestMigrationMetricsNoOTel:
    """Tests for MigrationMetrics when OTel is not available."""

    def test_metrics_disabled_uses_noop(self):
        """Metrics with enable_metrics=False uses no-op instruments."""
        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
            enable_metrics=False,
        )

        # Should use no-op instruments
        assert isinstance(metrics._events_copied_counter, NoOpCounter)
        assert isinstance(metrics._phase_duration_histogram, NoOpHistogram)
        assert isinstance(metrics._cutover_duration_histogram, NoOpHistogram)
        assert isinstance(metrics._failed_target_writes_counter, NoOpCounter)
        assert isinstance(metrics._verification_failures_counter, NoOpCounter)

    def test_noop_instruments_dont_raise(self):
        """No-op instruments don't raise when called."""
        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
            enable_metrics=False,
        )

        # These should not raise
        metrics.record_events_copied(1000, 500.0)
        metrics.record_sync_lag(100)
        metrics.record_phase_duration("bulk_copy", 300.0)
        metrics.record_cutover_duration(45.0, success=True)
        metrics.record_failed_target_write()
        metrics.record_verification_failure()

        # Snapshot should still work
        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied == 1000
        assert snapshot.sync_lag_events == 100


class TestMetricsWithMockedOTel:
    """Tests for metrics with mocked OpenTelemetry."""

    def test_record_events_copied_calls_counter(self, mock_counter):
        """record_events_copied calls counter."""
        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )
        metrics._events_copied_counter = mock_counter

        metrics.record_events_copied(1000, 500.0)

        mock_counter.add.assert_called_once_with(
            1000,
            {
                "migration_id": "migration-123",
                "tenant_id": "tenant-456",
            },
        )

    def test_record_phase_duration_calls_histogram(self, mock_histogram):
        """record_phase_duration calls histogram."""
        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )
        metrics._phase_duration_histogram = mock_histogram

        metrics.record_phase_duration("bulk_copy", 300.0)

        mock_histogram.record.assert_called_once_with(
            300.0,
            {
                "migration_id": "migration-123",
                "tenant_id": "tenant-456",
                "phase": "bulk_copy",
            },
        )

    def test_record_cutover_duration_calls_histogram(self, mock_histogram):
        """record_cutover_duration calls histogram."""
        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )
        metrics._cutover_duration_histogram = mock_histogram

        metrics.record_cutover_duration(45.0, success=True)

        mock_histogram.record.assert_called_once_with(
            45.0,
            {
                "migration_id": "migration-123",
                "tenant_id": "tenant-456",
                "success": "true",
            },
        )

    def test_record_failed_target_write_calls_counter(self, mock_counter):
        """record_failed_target_write calls counter."""
        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )
        metrics._failed_target_writes_counter = mock_counter

        metrics.record_failed_target_write(error_type="ConnectionError")

        mock_counter.add.assert_called_once_with(
            1,
            {
                "migration_id": "migration-123",
                "tenant_id": "tenant-456",
                "error_type": "ConnectionError",
            },
        )

    def test_record_verification_failure_calls_counter(self, mock_counter):
        """record_verification_failure calls counter."""
        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )
        metrics._verification_failures_counter = mock_counter

        metrics.record_verification_failure(failure_type="HashMismatch")

        mock_counter.add.assert_called_once_with(
            1,
            {
                "migration_id": "migration-123",
                "tenant_id": "tenant-456",
                "failure_type": "HashMismatch",
            },
        )
