"""Tests for migration trackers, metrics registry, timers, gauge callbacks, and imports."""

from __future__ import annotations

import time

import pytest

from eventsource.application.migration.metrics import (
    OTEL_METRICS_AVAILABLE,
    ActiveMigrationsTracker,
    MigrationMetrics,
    _CutoverTimer,
    _PhaseTimer,
    clear_metrics_registry,
    get_migration_metrics,
    release_migration_metrics,
)


class TestActiveMigrationsTracker:
    """Tests for ActiveMigrationsTracker class."""

    def test_singleton_pattern(self):
        """ActiveMigrationsTracker is a singleton."""
        tracker1 = ActiveMigrationsTracker.get_instance()
        tracker2 = ActiveMigrationsTracker.get_instance()

        assert tracker1 is tracker2

    def test_register_migration(self):
        """register_migration adds migration to active set."""
        tracker = ActiveMigrationsTracker.get_instance()

        tracker.register_migration("migration-1")
        tracker.register_migration("migration-2")

        assert tracker.active_count == 2
        assert "migration-1" in tracker.active_migrations
        assert "migration-2" in tracker.active_migrations

    def test_register_migration_idempotent(self):
        """register_migration is idempotent."""
        tracker = ActiveMigrationsTracker.get_instance()

        tracker.register_migration("migration-1")
        tracker.register_migration("migration-1")  # Duplicate

        assert tracker.active_count == 1

    def test_unregister_migration(self):
        """unregister_migration removes migration from active set."""
        tracker = ActiveMigrationsTracker.get_instance()

        tracker.register_migration("migration-1")
        tracker.register_migration("migration-2")

        tracker.unregister_migration("migration-1")

        assert tracker.active_count == 1
        assert "migration-1" not in tracker.active_migrations
        assert "migration-2" in tracker.active_migrations

    def test_unregister_migration_not_registered(self):
        """unregister_migration is safe for unknown migrations."""
        tracker = ActiveMigrationsTracker.get_instance()

        # Should not raise
        tracker.unregister_migration("unknown-migration")

        assert tracker.active_count == 0

    def test_reset(self):
        """reset clears the singleton."""
        tracker1 = ActiveMigrationsTracker.get_instance()
        tracker1.register_migration("migration-1")

        ActiveMigrationsTracker.reset()

        tracker2 = ActiveMigrationsTracker.get_instance()

        assert tracker1 is not tracker2
        assert tracker2.active_count == 0


class TestMetricsRegistry:
    """Tests for metrics registry functions."""

    def test_get_migration_metrics_creates_new(self):
        """get_migration_metrics creates new instance."""
        metrics = get_migration_metrics("migration-123", "tenant-456")

        assert metrics.migration_id == "migration-123"
        assert metrics.tenant_id == "tenant-456"
        assert metrics.enable_metrics is True

    def test_get_migration_metrics_returns_same_instance(self):
        """get_migration_metrics returns same instance for same ID."""
        metrics1 = get_migration_metrics("migration-123", "tenant-456")
        metrics2 = get_migration_metrics("migration-123", "tenant-456")

        assert metrics1 is metrics2

    def test_get_migration_metrics_different_ids(self):
        """get_migration_metrics returns different instances for different IDs."""
        metrics1 = get_migration_metrics("migration-1", "tenant-1")
        metrics2 = get_migration_metrics("migration-2", "tenant-2")

        assert metrics1 is not metrics2
        assert metrics1.migration_id == "migration-1"
        assert metrics2.migration_id == "migration-2"

    def test_get_migration_metrics_registers_active(self):
        """get_migration_metrics registers migration as active."""
        get_migration_metrics("migration-123", "tenant-456")

        tracker = ActiveMigrationsTracker.get_instance()
        assert "migration-123" in tracker.active_migrations

    def test_release_migration_metrics(self):
        """release_migration_metrics removes from registry and tracker."""
        metrics = get_migration_metrics("migration-123", "tenant-456")

        release_migration_metrics("migration-123")

        tracker = ActiveMigrationsTracker.get_instance()
        assert "migration-123" not in tracker.active_migrations

        # Should create new instance after release
        metrics_new = get_migration_metrics("migration-123", "tenant-456")
        assert metrics_new is not metrics

    def test_release_migration_metrics_unknown(self):
        """release_migration_metrics is safe for unknown migrations."""
        # Should not raise
        release_migration_metrics("unknown-migration")

    def test_clear_metrics_registry(self):
        """clear_metrics_registry clears all instances."""
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
        timer = _PhaseTimer()
        assert timer.duration_seconds == 0.0

    def test_timer_start_stop(self):
        """Timer measures duration between start and stop."""
        timer = _PhaseTimer()
        timer.start()
        time.sleep(0.01)  # 10ms
        timer.stop()

        assert timer.duration_seconds >= 0.01

    def test_timer_stop_idempotent(self):
        """Timer stop is idempotent."""
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
        timer = _CutoverTimer()
        assert timer.duration_ms == 0.0

    def test_timer_start_stop(self):
        """Timer measures duration between start and stop."""
        timer = _CutoverTimer()
        timer.start()
        time.sleep(0.01)  # 10ms
        timer.stop()

        assert timer.duration_ms >= 10.0

    def test_timer_default_success(self):
        """Timer defaults to success=True."""
        timer = _CutoverTimer()
        assert timer.success is True

    def test_timer_success_can_be_set(self):
        """Timer success can be set."""
        timer = _CutoverTimer()
        timer.success = False
        assert timer.success is False


class TestObservableGaugeCallbacks:
    """Tests for observable gauge callbacks."""

    def test_observe_events_copied_rate_callback(self):
        """_observe_events_copied_rate callback yields correct observation."""
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
        from eventsource.application.migration import (
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
        from eventsource.application.migration.metrics import (
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
