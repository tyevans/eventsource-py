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
    def test_singleton_pattern(self):
        assert ActiveMigrationsTracker.get_instance() is ActiveMigrationsTracker.get_instance()

    def test_register_migration(self):
        tracker = ActiveMigrationsTracker.get_instance()
        tracker.register_migration("migration-1")
        tracker.register_migration("migration-2")
        assert tracker.active_count == 2
        assert {"migration-1", "migration-2"}.issubset(tracker.active_migrations)

    def test_register_migration_idempotent(self):
        tracker = ActiveMigrationsTracker.get_instance()
        tracker.register_migration("migration-1")
        tracker.register_migration("migration-1")
        assert tracker.active_count == 1

    def test_unregister_migration(self):
        tracker = ActiveMigrationsTracker.get_instance()
        tracker.register_migration("migration-1")
        tracker.register_migration("migration-2")
        tracker.unregister_migration("migration-1")
        assert tracker.active_count == 1
        assert "migration-1" not in tracker.active_migrations
        assert "migration-2" in tracker.active_migrations

    def test_unregister_migration_not_registered(self):
        tracker = ActiveMigrationsTracker.get_instance()
        tracker.unregister_migration("unknown-migration")
        assert tracker.active_count == 0

    def test_reset(self):
        tracker1 = ActiveMigrationsTracker.get_instance()
        tracker1.register_migration("migration-1")
        ActiveMigrationsTracker.reset()
        tracker2 = ActiveMigrationsTracker.get_instance()
        assert tracker1 is not tracker2 and tracker2.active_count == 0


class TestMetricsRegistry:
    def test_get_migration_metrics_creates_new(self):
        metrics = get_migration_metrics("migration-123", "tenant-456")
        assert (metrics.migration_id, metrics.tenant_id) == ("migration-123", "tenant-456")
        assert metrics.enable_metrics is True

    def test_get_migration_metrics_returns_same_instance(self):
        m1 = get_migration_metrics("migration-123", "tenant-456")
        m2 = get_migration_metrics("migration-123", "tenant-456")
        assert m1 is m2

    def test_get_migration_metrics_different_ids(self):
        m1 = get_migration_metrics("migration-1", "tenant-1")
        m2 = get_migration_metrics("migration-2", "tenant-2")
        assert m1 is not m2
        assert m1.migration_id == "migration-1" and m2.migration_id == "migration-2"

    def test_get_migration_metrics_registers_active(self):
        get_migration_metrics("migration-123", "tenant-456")
        assert "migration-123" in ActiveMigrationsTracker.get_instance().active_migrations

    def test_release_migration_metrics(self):
        metrics = get_migration_metrics("migration-123", "tenant-456")
        release_migration_metrics("migration-123")
        assert "migration-123" not in ActiveMigrationsTracker.get_instance().active_migrations
        assert get_migration_metrics("migration-123", "tenant-456") is not metrics

    def test_release_migration_metrics_unknown(self):
        release_migration_metrics("unknown-migration")

    def test_clear_metrics_registry(self):
        metrics1 = get_migration_metrics("migration-1", "tenant-1")
        get_migration_metrics("migration-2", "tenant-2")
        clear_metrics_registry()
        assert get_migration_metrics("migration-1", "tenant-1") is not metrics1


class TestPhaseTimer:
    def test_timer_not_started(self):
        assert _PhaseTimer().duration_seconds == 0.0

    def test_timer_start_stop(self):
        timer = _PhaseTimer()
        timer.start()
        time.sleep(0.01)
        timer.stop()
        assert timer.duration_seconds >= 0.01

    def test_timer_stop_idempotent(self):
        timer = _PhaseTimer()
        timer.start()
        time.sleep(0.01)
        timer.stop()
        duration1 = timer.duration_seconds
        time.sleep(0.01)
        timer.stop()
        assert duration1 == timer.duration_seconds


class TestCutoverTimer:
    def test_timer_not_started(self):
        assert _CutoverTimer().duration_ms == 0.0

    def test_timer_start_stop(self):
        timer = _CutoverTimer()
        timer.start()
        time.sleep(0.01)
        timer.stop()
        assert timer.duration_ms >= 10.0

    def test_timer_default_success(self):
        assert _CutoverTimer().success is True

    def test_timer_success_can_be_set(self):
        timer = _CutoverTimer()
        timer.success = False
        assert timer.success is False


class TestObservableGaugeCallbacks:
    def test_observe_events_copied_rate_callback(self):
        if not OTEL_METRICS_AVAILABLE:
            pytest.skip("OpenTelemetry not available")
        from opentelemetry.metrics import Observation

        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_events_copied(1000, 500.0)
        observations = list(metrics._observe_events_copied_rate(None))
        assert len(observations) == 1
        obs = observations[0]
        assert isinstance(obs, Observation) and obs.value == 500.0
        assert obs.attributes == {"migration_id": "migration-123", "tenant_id": "tenant-456"}

    def test_observe_sync_lag_callback(self):
        if not OTEL_METRICS_AVAILABLE:
            pytest.skip("OpenTelemetry not available")
        from opentelemetry.metrics import Observation

        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_sync_lag(50)
        observations = list(metrics._observe_sync_lag(None))
        assert len(observations) == 1
        obs = observations[0]
        assert isinstance(obs, Observation) and obs.value == 50
        assert obs.attributes == {"migration_id": "migration-123", "tenant_id": "tenant-456"}


class TestImports:
    def test_import_from_migration_module(self):
        import eventsource.application.migration as mod

        names = (
            "MigrationMetrics",
            "MigrationMetricSnapshot",
            "ActiveMigrationsTracker",
            "get_migration_metrics",
            "release_migration_metrics",
            "clear_metrics_registry",
        )
        assert all(getattr(mod, name) is not None for name in names)

    def test_import_from_metrics_module(self):
        from eventsource.application.migration import metrics as mod

        names = (
            "OTEL_METRICS_AVAILABLE",
            "MigrationMetrics",
            "MigrationMetricSnapshot",
            "ActiveMigrationsTracker",
            "NoOpCounter",
            "NoOpHistogram",
            "NoOpGauge",
            "get_migration_metrics",
            "release_migration_metrics",
            "clear_metrics_registry",
            "reset_meter",
        )
        assert all(getattr(mod, name) is not None for name in names)
