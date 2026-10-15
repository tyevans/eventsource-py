"""Lifecycle and status change detection tests for StatusStreamer."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.status_streamer import StatusStreamer
from eventsource.ports.migration.models import MigrationPhase, MigrationStatus


class TestStatusStreamerInit:
    """Tests for StatusStreamer initialization."""

    def test_init_with_required_args(self) -> None:
        coordinator = MagicMock()
        migration_id = uuid4()
        streamer = StatusStreamer(
            coordinator=coordinator,
            migration_id=migration_id,
            enable_tracing=False,
        )
        assert streamer._coordinator == coordinator
        assert streamer._migration_id == migration_id
        assert streamer._subscribers == {}
        assert not streamer._closed

    def test_init_with_tracing_disabled(self) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )
        assert streamer._enable_tracing is False

    def test_migration_id_property(self) -> None:
        migration_id = uuid4()
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=migration_id,
            enable_tracing=False,
        )
        assert streamer.migration_id == migration_id

    def test_subscriber_count_initial(self) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )
        assert streamer.subscriber_count == 0

    def test_is_closed_initial(self) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )
        assert streamer.is_closed is False


class TestStatusStreamerClose:
    """Tests for StatusStreamer.close() method."""

    @pytest.mark.asyncio
    async def test_close_marks_streamer_closed(self) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )
        assert not streamer.is_closed
        await streamer.close()
        assert streamer.is_closed

    @pytest.mark.asyncio
    async def test_close_clears_subscribers(self) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )
        await streamer._register_subscriber()
        assert streamer.subscriber_count == 1
        await streamer.close()
        assert streamer.subscriber_count == 0


class TestStatusStreamerStatusChanged:
    """Tests for StatusStreamer._status_changed() method."""

    def test_phase_change_is_detected(self, base_status: MigrationStatus) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )
        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.DUAL_WRITE,
            progress_percent=base_status.progress_percent,
            events_total=base_status.events_total,
            events_copied=base_status.events_copied,
            events_remaining=base_status.events_remaining,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )
        assert streamer._status_changed(base_status, new_status) is True

    def test_progress_change_1_percent_is_detected(self, base_status: MigrationStatus) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )
        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=51.0,
            events_total=base_status.events_total,
            events_copied=510,
            events_remaining=490,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )
        assert streamer._status_changed(base_status, new_status) is True

    def test_small_progress_change_not_detected(self, base_status: MigrationStatus) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )
        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=50.5,
            events_total=base_status.events_total,
            events_copied=505,
            events_remaining=495,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )
        assert streamer._status_changed(base_status, new_status) is False

    def test_sync_lag_change_is_detected(self, base_status: MigrationStatus) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )
        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=base_status.progress_percent,
            events_total=base_status.events_total,
            events_copied=base_status.events_copied,
            events_remaining=base_status.events_remaining,
            sync_lag_events=15,
            sync_lag_ms=15.0,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )
        assert streamer._status_changed(base_status, new_status) is True

    def test_error_count_increase_is_detected(self, base_status: MigrationStatus) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )
        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=base_status.progress_percent,
            events_total=base_status.events_total,
            events_copied=base_status.events_copied,
            events_remaining=base_status.events_remaining,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=1,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )
        assert streamer._status_changed(base_status, new_status) is True

    def test_pause_state_change_is_detected(self, base_status: MigrationStatus) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )
        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=base_status.progress_percent,
            events_total=base_status.events_total,
            events_copied=base_status.events_copied,
            events_remaining=base_status.events_remaining,
            sync_lag_events=base_status.sync_lag_events,
            sync_lag_ms=base_status.sync_lag_ms,
            error_count=base_status.error_count,
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=True,
        )
        assert streamer._status_changed(base_status, new_status) is True

    def test_no_change_not_detected(self, base_status: MigrationStatus) -> None:
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )
        assert streamer._status_changed(base_status, base_status) is False
