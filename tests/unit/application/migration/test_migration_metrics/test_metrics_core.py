"""Tests for migration metric snapshots and MigrationMetrics core functionality."""

from __future__ import annotations

import time

from eventsource.application.migration.metrics import (
    OTEL_METRICS_AVAILABLE,
    MigrationMetrics,
    MigrationMetricSnapshot,
)


class TestMigrationMetricSnapshot:
    """Tests for MigrationMetricSnapshot dataclass."""

    def test_snapshot_default_values(self):
        snapshot = MigrationMetricSnapshot()
        assert snapshot.events_copied == 0
        assert snapshot.events_copied_rate == 0.0
        assert snapshot.sync_lag_events == 0
        assert snapshot.failed_target_writes == 0
        assert snapshot.verification_failures == 0
        assert snapshot.phase_durations == {}
        assert snapshot.cutover_durations == []

    def test_snapshot_with_values(self):
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

    def test_init_with_ids(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        assert metrics.migration_id == "migration-123"
        assert metrics.tenant_id == "tenant-456"
        assert metrics.enable_metrics is True

    def test_init_with_metrics_disabled(self):
        metrics = MigrationMetrics(
            migration_id="migration-123", tenant_id="tenant-456", enable_metrics=False
        )
        assert metrics.migration_id == "migration-123"
        assert metrics.tenant_id == "tenant-456"
        assert metrics.enable_metrics is False
        assert metrics.metrics_enabled is False

    def test_metrics_enabled_property(self):
        metrics = MigrationMetrics(
            migration_id="migration-123", tenant_id="tenant-456", enable_metrics=True
        )
        assert metrics.metrics_enabled == OTEL_METRICS_AVAILABLE

    def test_record_events_copied(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_events_copied(1000, 500.0)
        metrics.record_events_copied(500)
        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied == 1500
        assert snapshot.events_copied_rate == 500.0
        assert metrics.current_copy_rate == 500.0

    def test_record_events_copied_without_rate(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_events_copied(1000)
        snapshot = metrics.get_snapshot()
        assert snapshot.events_copied == 1000
        assert snapshot.events_copied_rate == 0.0

    def test_record_sync_lag(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_sync_lag(100)
        assert metrics.current_sync_lag == 100
        assert metrics.get_snapshot().sync_lag_events == 100
        metrics.record_sync_lag(50)
        assert metrics.current_sync_lag == 50

    def test_record_sync_lag_negative_becomes_zero(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_sync_lag(-10)
        assert metrics.current_sync_lag == 0

    def test_record_phase_duration(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_phase_duration("bulk_copy", 300.0)
        metrics.record_phase_duration("dual_write", 120.0)
        metrics.record_phase_duration("bulk_copy", 50.0)
        snapshot = metrics.get_snapshot()
        assert snapshot.phase_durations["bulk_copy"] == 350.0
        assert snapshot.phase_durations["dual_write"] == 120.0

    def test_record_cutover_duration(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_cutover_duration(45.0, success=True)
        metrics.record_cutover_duration(55.0, success=False)
        snapshot = metrics.get_snapshot()
        assert snapshot.cutover_durations == [45.0, 55.0]

    def test_record_failed_target_write(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_failed_target_write()
        metrics.record_failed_target_write(error_type="ConnectionError")
        snapshot = metrics.get_snapshot()
        assert snapshot.failed_target_writes == 2

    def test_record_verification_failure(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        metrics.record_verification_failure()
        metrics.record_verification_failure(failure_type="HashMismatch")
        snapshot = metrics.get_snapshot()
        assert snapshot.verification_failures == 2

    def test_time_phase_context_manager(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        with metrics.time_phase("bulk_copy") as timer:
            time.sleep(0.01)
        assert timer.duration_seconds >= 0.01
        assert timer.duration_seconds < 1.0
        snapshot = metrics.get_snapshot()
        assert "bulk_copy" in snapshot.phase_durations
        assert snapshot.phase_durations["bulk_copy"] >= 0.01

    def test_time_cutover_context_manager(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        with metrics.time_cutover() as timer:
            time.sleep(0.01)
        assert timer.duration_ms >= 10.0
        assert timer.duration_ms < 100.0
        snapshot = metrics.get_snapshot()
        assert len(snapshot.cutover_durations) == 1
        assert snapshot.cutover_durations[0] >= 10.0

    def test_time_cutover_with_failure(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
        with metrics.time_cutover() as timer:
            time.sleep(0.005)
            timer.success = False
        assert timer.success is False
        snapshot = metrics.get_snapshot()
        assert len(snapshot.cutover_durations) == 1

    def test_get_snapshot(self):
        metrics = MigrationMetrics(migration_id="migration-123", tenant_id="tenant-456")
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
