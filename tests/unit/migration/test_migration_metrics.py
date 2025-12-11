"""
Unit tests for migration metrics module (P4-003).

Tests for:
- MigrationMetrics class
- NoOp instruments when OTel unavailable
- Metric recording methods
- Metric snapshot functionality
- Sync lag and rate tracking
- Timer context managers (phase and cutover)
- ActiveMigrationsTracker
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
        from eventsource.migration.metrics import OTEL_METRICS_AVAILABLE

        assert isinstance(OTEL_METRICS_AVAILABLE, bool)

    def test_otel_metrics_available_reflects_import(self):
        """OTEL_METRICS_AVAILABLE is True when opentelemetry is installed."""
        from eventsource.migration.metrics import OTEL_METRICS_AVAILABLE

        # In test environment, OpenTelemetry should be available
        assert OTEL_METRICS_AVAILABLE is True


class TestNoOpInstruments:
    """Tests for no-op metric instruments."""

    def test_noop_counter_add_does_nothing(self):
        """NoOpCounter.add does nothing."""
        from eventsource.migration.metrics import NoOpCounter

        counter = NoOpCounter()
        # Should not raise
        counter.add(1)
        counter.add(10, {"key": "value"})

    def test_noop_histogram_record_does_nothing(self):
        """NoOpHistogram.record does nothing."""
        from eventsource.migration.metrics import NoOpHistogram

        histogram = NoOpHistogram()
        # Should not raise
        histogram.record(1.5)
        histogram.record(10.0, {"key": "value"})

    def test_noop_gauge_init_does_nothing(self):
        """NoOpGauge init does nothing."""
        from eventsource.migration.metrics import NoOpGauge

        gauge = NoOpGauge()
        # Should not raise - just verify we can create it
        assert gauge is not None


class TestMigrationMetricSnapshot:
    """Tests for MigrationMetricSnapshot dataclass."""

    def test_snapshot_default_values(self):
        """MigrationMetricSnapshot has sensible defaults."""
        from eventsource.migration.metrics import MigrationMetricSnapshot

        snapshot = MigrationMetricSnapshot()

        assert snapshot.events_copied == 0
        assert snapshot.events_copied_rate == 0.0
        assert snapshot.sync_lag_events == 0
        assert snapshot.failed_target_writes == 0
        assert snapshot.verification_failures == 0
        assert snapshot.phase_durations == {}
        assert snapshot.cutover_durations == []

    def test_snapshot_with_values(self):
        """MigrationMetricSnapshot stores provided values."""
        from eventsource.migration.metrics import MigrationMetricSnapshot

        snapshot = MigrationMetricSnapshot(
            events_copied=1000,
            events_copied_rate=500.0,
            sync_lag_events=50,
            failed_target_writes=2,
            verification_failures=1,
            phase_durations={"bulk_copy": 300.0},
            cutover_durations=[45.0, 50.0],
        )

        assert snapshot.events_copied == 1000
        assert snapshot.events_copied_rate == 500.0
        assert snapshot.sync_lag_events == 50
        assert snapshot.failed_target_writes == 2
        assert snapshot.verification_failures == 1
        assert snapshot.phase_durations == {"bulk_copy": 300.0}
        assert snapshot.cutover_durations == [45.0, 50.0]

    def test_snapshot_to_dict(self):
        """MigrationMetricSnapshot.to_dict returns correct dictionary."""
        from eventsource.migration.metrics import MigrationMetricSnapshot

        snapshot = MigrationMetricSnapshot(
            events_copied=1000,
            events_copied_rate=500.0,
            sync_lag_events=50,
            failed_target_writes=2,
            verification_failures=1,
            phase_durations={"bulk_copy": 300.0},
            cutover_durations=[45.0],
        )

        result = snapshot.to_dict()

        assert result["events_copied"] == 1000
        assert result["events_copied_rate"] == 500.0
        assert result["sync_lag_events"] == 50
        assert result["failed_target_writes"] == 2
        assert result["verification_failures"] == 1
        assert result["phase_durations"] == {"bulk_copy": 300.0}
        assert result["cutover_durations"] == [45.0]


class TestMigrationMetrics:
    """Tests for MigrationMetrics class."""

    @pytest.fixture(autouse=True)
    def reset_meter(self):
        """Reset global meter state between tests."""
        from eventsource.migration.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_init_with_ids(self):
        """Migration metrics initializes with IDs."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        assert metrics.migration_id == "migration-123"
        assert metrics.tenant_id == "tenant-456"
        assert metrics.enable_metrics is True

    def test_init_with_metrics_disabled(self):
        """Migration metrics initializes with metrics disabled."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
            enable_metrics=False,
        )

        assert metrics.migration_id == "migration-123"
        assert metrics.tenant_id == "tenant-456"
        assert metrics.enable_metrics is False
        assert metrics.metrics_enabled is False

    def test_metrics_enabled_property(self):
        """metrics_enabled reflects actual state."""
        from eventsource.migration.metrics import (
            OTEL_METRICS_AVAILABLE,
            MigrationMetrics,
        )

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
            enable_metrics=True,
        )

        # Should be True when OTel is available
        assert metrics.metrics_enabled == OTEL_METRICS_AVAILABLE

    def test_record_events_copied(self):
        """record_events_copied updates internal counters."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_events_copied(1000, 500.0)
        metrics.record_events_copied(500)

        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied == 1500
        assert snapshot.events_copied_rate == 500.0
        assert metrics.current_copy_rate == 500.0

    def test_record_events_copied_without_rate(self):
        """record_events_copied works without rate."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_events_copied(1000)

        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied == 1000
        assert snapshot.events_copied_rate == 0.0

    def test_record_sync_lag(self):
        """record_sync_lag updates sync lag value."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_sync_lag(100)
        assert metrics.current_sync_lag == 100
        assert metrics.get_snapshot().sync_lag_events == 100

        metrics.record_sync_lag(50)
        assert metrics.current_sync_lag == 50

    def test_record_sync_lag_negative_becomes_zero(self):
        """record_sync_lag with negative value becomes zero."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_sync_lag(-10)
        assert metrics.current_sync_lag == 0

    def test_record_phase_duration(self):
        """record_phase_duration updates internal tracking."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_phase_duration("bulk_copy", 300.0)
        metrics.record_phase_duration("dual_write", 120.0)
        metrics.record_phase_duration("bulk_copy", 50.0)  # Additional time

        snapshot = metrics.get_snapshot()
        assert snapshot.phase_durations["bulk_copy"] == 350.0
        assert snapshot.phase_durations["dual_write"] == 120.0

    def test_record_cutover_duration(self):
        """record_cutover_duration updates internal tracking."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_cutover_duration(45.0, success=True)
        metrics.record_cutover_duration(55.0, success=False)

        snapshot = metrics.get_snapshot()
        assert snapshot.cutover_durations == [45.0, 55.0]

    def test_record_failed_target_write(self):
        """record_failed_target_write updates internal counter."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_failed_target_write()
        metrics.record_failed_target_write(error_type="ConnectionError")

        snapshot = metrics.get_snapshot()
        assert snapshot.failed_target_writes == 2

    def test_record_verification_failure(self):
        """record_verification_failure updates internal counter."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_verification_failure()
        metrics.record_verification_failure(failure_type="HashMismatch")

        snapshot = metrics.get_snapshot()
        assert snapshot.verification_failures == 2

    def test_time_phase_context_manager(self):
        """time_phase context manager measures and records duration."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        with metrics.time_phase("bulk_copy") as timer:
            time.sleep(0.01)  # 10ms

        # Should measure at least 10ms = 0.01 seconds
        assert timer.duration_seconds >= 0.01
        assert timer.duration_seconds < 1.0  # Reasonable upper bound

        # Should be recorded
        snapshot = metrics.get_snapshot()
        assert "bulk_copy" in snapshot.phase_durations
        assert snapshot.phase_durations["bulk_copy"] >= 0.01

    def test_time_cutover_context_manager(self):
        """time_cutover context manager measures and records duration."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        with metrics.time_cutover() as timer:
            time.sleep(0.01)  # 10ms

        # Should measure at least 10ms
        assert timer.duration_ms >= 10.0
        assert timer.duration_ms < 100.0  # Reasonable upper bound

        # Should be recorded with default success=True
        snapshot = metrics.get_snapshot()
        assert len(snapshot.cutover_durations) == 1
        assert snapshot.cutover_durations[0] >= 10.0

    def test_time_cutover_with_failure(self):
        """time_cutover tracks failure status."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        with metrics.time_cutover() as timer:
            time.sleep(0.005)
            timer.success = False

        assert timer.success is False
        # Duration should still be recorded
        snapshot = metrics.get_snapshot()
        assert len(snapshot.cutover_durations) == 1

    def test_get_snapshot(self):
        """get_snapshot returns accurate snapshot."""
        from eventsource.migration.metrics import MigrationMetrics

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )

        metrics.record_events_copied(1000, 500.0)
        metrics.record_sync_lag(50)
        metrics.record_phase_duration("bulk_copy", 300.0)
        metrics.record_cutover_duration(45.0, success=True)
        metrics.record_failed_target_write()
        metrics.record_verification_failure()

        snapshot = metrics.get_snapshot()

        assert snapshot.events_copied == 1000
        assert snapshot.events_copied_rate == 500.0
        assert snapshot.sync_lag_events == 50
        assert snapshot.failed_target_writes == 1
        assert snapshot.verification_failures == 1
        assert snapshot.phase_durations["bulk_copy"] == 300.0
        assert snapshot.cutover_durations == [45.0]


class TestMigrationMetricsNoOTel:
    """Tests for MigrationMetrics when OTel is not available."""

    @pytest.fixture(autouse=True)
    def reset_meter(self):
        """Reset global meter state between tests."""
        from eventsource.migration.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_metrics_disabled_uses_noop(self):
        """Metrics with enable_metrics=False uses no-op instruments."""
        from eventsource.migration.metrics import (
            MigrationMetrics,
            NoOpCounter,
            NoOpHistogram,
        )

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
        from eventsource.migration.metrics import MigrationMetrics

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

    @pytest.fixture(autouse=True)
    def reset_meter(self):
        """Reset global meter state between tests."""
        from eventsource.migration.metrics import clear_metrics_registry

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

    def test_record_events_copied_calls_counter(self, mock_counter):
        """record_events_copied calls counter."""
        from eventsource.migration.metrics import MigrationMetrics

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
        from eventsource.migration.metrics import MigrationMetrics

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
        from eventsource.migration.metrics import MigrationMetrics

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
        from eventsource.migration.metrics import MigrationMetrics

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
        from eventsource.migration.metrics import MigrationMetrics

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


class TestActiveMigrationsTracker:
    """Tests for ActiveMigrationsTracker class."""

    @pytest.fixture(autouse=True)
    def reset_tracker(self):
        """Reset tracker between tests."""
        from eventsource.migration.metrics import (
            clear_metrics_registry,
        )

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_singleton_pattern(self):
        """ActiveMigrationsTracker is a singleton."""
        from eventsource.migration.metrics import ActiveMigrationsTracker

        tracker1 = ActiveMigrationsTracker.get_instance()
        tracker2 = ActiveMigrationsTracker.get_instance()

        assert tracker1 is tracker2

    def test_register_migration(self):
        """register_migration adds migration to active set."""
        from eventsource.migration.metrics import ActiveMigrationsTracker

        tracker = ActiveMigrationsTracker.get_instance()

        tracker.register_migration("migration-1")
        tracker.register_migration("migration-2")

        assert tracker.active_count == 2
        assert "migration-1" in tracker.active_migrations
        assert "migration-2" in tracker.active_migrations

    def test_register_migration_idempotent(self):
        """register_migration is idempotent."""
        from eventsource.migration.metrics import ActiveMigrationsTracker

        tracker = ActiveMigrationsTracker.get_instance()

        tracker.register_migration("migration-1")
        tracker.register_migration("migration-1")  # Duplicate

        assert tracker.active_count == 1

    def test_unregister_migration(self):
        """unregister_migration removes migration from active set."""
        from eventsource.migration.metrics import ActiveMigrationsTracker

        tracker = ActiveMigrationsTracker.get_instance()

        tracker.register_migration("migration-1")
        tracker.register_migration("migration-2")

        tracker.unregister_migration("migration-1")

        assert tracker.active_count == 1
        assert "migration-1" not in tracker.active_migrations
        assert "migration-2" in tracker.active_migrations

    def test_unregister_migration_not_registered(self):
        """unregister_migration is safe for unknown migrations."""
        from eventsource.migration.metrics import ActiveMigrationsTracker

        tracker = ActiveMigrationsTracker.get_instance()

        # Should not raise
        tracker.unregister_migration("unknown-migration")

        assert tracker.active_count == 0

    def test_reset(self):
        """reset clears the singleton."""
        from eventsource.migration.metrics import ActiveMigrationsTracker

        tracker1 = ActiveMigrationsTracker.get_instance()
        tracker1.register_migration("migration-1")

        ActiveMigrationsTracker.reset()

        tracker2 = ActiveMigrationsTracker.get_instance()

        assert tracker1 is not tracker2
        assert tracker2.active_count == 0


class TestMetricsRegistry:
    """Tests for metrics registry functions."""

    @pytest.fixture(autouse=True)
    def reset_registry(self):
        """Reset registry between tests."""
        from eventsource.migration.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_get_migration_metrics_creates_new(self):
        """get_migration_metrics creates new instance."""
        from eventsource.migration.metrics import get_migration_metrics

        metrics = get_migration_metrics("migration-123", "tenant-456")

        assert metrics.migration_id == "migration-123"
        assert metrics.tenant_id == "tenant-456"
        assert metrics.enable_metrics is True

    def test_get_migration_metrics_returns_same_instance(self):
        """get_migration_metrics returns same instance for same ID."""
        from eventsource.migration.metrics import get_migration_metrics

        metrics1 = get_migration_metrics("migration-123", "tenant-456")
        metrics2 = get_migration_metrics("migration-123", "tenant-456")

        assert metrics1 is metrics2

    def test_get_migration_metrics_different_ids(self):
        """get_migration_metrics returns different instances for different IDs."""
        from eventsource.migration.metrics import get_migration_metrics

        metrics1 = get_migration_metrics("migration-1", "tenant-1")
        metrics2 = get_migration_metrics("migration-2", "tenant-2")

        assert metrics1 is not metrics2
        assert metrics1.migration_id == "migration-1"
        assert metrics2.migration_id == "migration-2"

    def test_get_migration_metrics_registers_active(self):
        """get_migration_metrics registers migration as active."""
        from eventsource.migration.metrics import (
            ActiveMigrationsTracker,
            get_migration_metrics,
        )

        get_migration_metrics("migration-123", "tenant-456")

        tracker = ActiveMigrationsTracker.get_instance()
        assert "migration-123" in tracker.active_migrations

    def test_release_migration_metrics(self):
        """release_migration_metrics removes from registry and tracker."""
        from eventsource.migration.metrics import (
            ActiveMigrationsTracker,
            get_migration_metrics,
            release_migration_metrics,
        )

        metrics = get_migration_metrics("migration-123", "tenant-456")

        release_migration_metrics("migration-123")

        tracker = ActiveMigrationsTracker.get_instance()
        assert "migration-123" not in tracker.active_migrations

        # Should create new instance after release
        metrics_new = get_migration_metrics("migration-123", "tenant-456")
        assert metrics_new is not metrics

    def test_release_migration_metrics_unknown(self):
        """release_migration_metrics is safe for unknown migrations."""
        from eventsource.migration.metrics import release_migration_metrics

        # Should not raise
        release_migration_metrics("unknown-migration")

    def test_clear_metrics_registry(self):
        """clear_metrics_registry clears all instances."""
        from eventsource.migration.metrics import (
            clear_metrics_registry,
            get_migration_metrics,
        )

        metrics1 = get_migration_metrics("migration-1", "tenant-1")
        get_migration_metrics("migration-2", "tenant-2")

        clear_metrics_registry()

        # Should create new instances after clear
        metrics1_new = get_migration_metrics("migration-1", "tenant-1")
        assert metrics1_new is not metrics1


class TestPhaseTimer:
    """Tests for internal _PhaseTimer class."""

    def test_timer_not_started(self):
        """Timer returns 0 when not started."""
        from eventsource.migration.metrics import _PhaseTimer

        timer = _PhaseTimer()
        assert timer.duration_seconds == 0.0

    def test_timer_start_stop(self):
        """Timer measures duration between start and stop."""
        from eventsource.migration.metrics import _PhaseTimer

        timer = _PhaseTimer()
        timer.start()
        time.sleep(0.01)  # 10ms
        timer.stop()

        assert timer.duration_seconds >= 0.01

    def test_timer_stop_idempotent(self):
        """Timer stop is idempotent."""
        from eventsource.migration.metrics import _PhaseTimer

        timer = _PhaseTimer()
        timer.start()
        time.sleep(0.01)
        timer.stop()
        duration1 = timer.duration_seconds

        time.sleep(0.01)
        timer.stop()  # Second stop
        duration2 = timer.duration_seconds

        # Duration should not change after first stop
        assert duration1 == duration2


class TestCutoverTimer:
    """Tests for internal _CutoverTimer class."""

    def test_timer_not_started(self):
        """Timer returns 0 when not started."""
        from eventsource.migration.metrics import _CutoverTimer

        timer = _CutoverTimer()
        assert timer.duration_ms == 0.0

    def test_timer_start_stop(self):
        """Timer measures duration between start and stop."""
        from eventsource.migration.metrics import _CutoverTimer

        timer = _CutoverTimer()
        timer.start()
        time.sleep(0.01)  # 10ms
        timer.stop()

        assert timer.duration_ms >= 10.0

    def test_timer_default_success(self):
        """Timer defaults to success=True."""
        from eventsource.migration.metrics import _CutoverTimer

        timer = _CutoverTimer()
        assert timer.success is True

    def test_timer_success_can_be_set(self):
        """Timer success can be set."""
        from eventsource.migration.metrics import _CutoverTimer

        timer = _CutoverTimer()
        timer.success = False
        assert timer.success is False


class TestObservableGaugeCallbacks:
    """Tests for observable gauge callbacks."""

    @pytest.fixture(autouse=True)
    def reset_meter(self):
        """Reset global meter state between tests."""
        from eventsource.migration.metrics import clear_metrics_registry

        clear_metrics_registry()
        yield
        clear_metrics_registry()

    def test_observe_events_copied_rate_callback(self):
        """_observe_events_copied_rate callback yields correct observation."""
        from eventsource.migration.metrics import (
            OTEL_METRICS_AVAILABLE,
            MigrationMetrics,
        )

        if not OTEL_METRICS_AVAILABLE:
            pytest.skip("OpenTelemetry not available")

        from opentelemetry.metrics import Observation

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )
        metrics.record_events_copied(1000, 500.0)

        # Call the callback
        observations = list(metrics._observe_events_copied_rate(None))

        assert len(observations) == 1
        obs = observations[0]
        assert isinstance(obs, Observation)
        assert obs.value == 500.0
        assert obs.attributes["migration_id"] == "migration-123"
        assert obs.attributes["tenant_id"] == "tenant-456"

    def test_observe_sync_lag_callback(self):
        """_observe_sync_lag callback yields correct observation."""
        from eventsource.migration.metrics import (
            OTEL_METRICS_AVAILABLE,
            MigrationMetrics,
        )

        if not OTEL_METRICS_AVAILABLE:
            pytest.skip("OpenTelemetry not available")

        from opentelemetry.metrics import Observation

        metrics = MigrationMetrics(
            migration_id="migration-123",
            tenant_id="tenant-456",
        )
        metrics.record_sync_lag(50)

        # Call the callback
        observations = list(metrics._observe_sync_lag(None))

        assert len(observations) == 1
        obs = observations[0]
        assert isinstance(obs, Observation)
        assert obs.value == 50
        assert obs.attributes["migration_id"] == "migration-123"
        assert obs.attributes["tenant_id"] == "tenant-456"


class TestImports:
    """Test module imports work correctly."""

    def test_import_from_migration_module(self):
        """All exports are importable from migration module."""
        from eventsource.migration import (
            ActiveMigrationsTracker,
            MigrationMetrics,
            MigrationMetricSnapshot,
            clear_metrics_registry,
            get_migration_metrics,
            release_migration_metrics,
        )

        assert MigrationMetrics is not None
        assert MigrationMetricSnapshot is not None
        assert ActiveMigrationsTracker is not None
        assert get_migration_metrics is not None
        assert release_migration_metrics is not None
        assert clear_metrics_registry is not None

    def test_import_from_metrics_module(self):
        """All exports are importable from metrics module."""
        from eventsource.migration.metrics import (
            OTEL_METRICS_AVAILABLE,
            ActiveMigrationsTracker,
            MigrationMetrics,
            MigrationMetricSnapshot,
            NoOpCounter,
            NoOpGauge,
            NoOpHistogram,
            clear_metrics_registry,
            get_migration_metrics,
            release_migration_metrics,
            reset_meter,
        )

        assert OTEL_METRICS_AVAILABLE is not None
        assert MigrationMetrics is not None
        assert MigrationMetricSnapshot is not None
        assert ActiveMigrationsTracker is not None
        assert NoOpCounter is not None
        assert NoOpHistogram is not None
        assert NoOpGauge is not None
        assert get_migration_metrics is not None
        assert release_migration_metrics is not None
        assert clear_metrics_registry is not None
        assert reset_meter is not None
