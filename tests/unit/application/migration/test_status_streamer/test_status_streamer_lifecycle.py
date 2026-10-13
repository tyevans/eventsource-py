"""Lifecycle and status change detection tests for StatusStreamer."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.status_streamer import StatusStreamer
from eventsource.ports.migration.models import (
    MigrationPhase,
    MigrationStatus,
)


class TestStatusStreamerInit:
    """Tests for StatusStreamer initialization."""

    def test_init_with_required_args(self) -> None:
        """Test initialization with required arguments."""
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
        """Test initialization with tracing disabled."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )

        assert streamer._enable_tracing is False

    def test_migration_id_property(self) -> None:
        """Test migration_id property."""
        migration_id = uuid4()
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=migration_id,
            enable_tracing=False,
        )

        assert streamer.migration_id == migration_id

    def test_subscriber_count_initial(self) -> None:
        """Test subscriber_count is 0 initially."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )

        assert streamer.subscriber_count == 0

    def test_is_closed_initial(self) -> None:
        """Test is_closed is False initially."""
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
        """Test close() marks the streamer as closed."""
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
        """Test close() clears all subscribers."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=uuid4(),
            enable_tracing=False,
        )

        # Register a subscriber manually
        await streamer._register_subscriber()
        assert streamer.subscriber_count == 1

        await streamer.close()

        assert streamer.subscriber_count == 0


class TestStatusStreamerStatusChanged:
    """Tests for StatusStreamer._status_changed() method."""

    def test_phase_change_is_detected(self, base_status: MigrationStatus) -> None:
        """Test that phase change is detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.DUAL_WRITE,  # Changed
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
        """Test that progress change of 1% is detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=51.0,  # Changed by 1%
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
        """Test that small progress change (<1%) is not detected."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        new_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=base_status.phase,
            progress_percent=50.5,  # Changed by only 0.5%
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
        """Test that sync lag change of 10+ events is detected."""
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
            sync_lag_events=15,  # Changed by 15 events
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
        """Test that error count increase is detected."""
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
            error_count=1,  # Increased from 0
            started_at=base_status.started_at,
            phase_started_at=base_status.phase_started_at,
            estimated_completion=base_status.estimated_completion,
            current_rate_events_per_sec=base_status.current_rate_events_per_sec,
            is_paused=base_status.is_paused,
        )

        assert streamer._status_changed(base_status, new_status) is True

    def test_pause_state_change_is_detected(self, base_status: MigrationStatus) -> None:
        """Test that pause state change is detected."""
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
            is_paused=True,  # Changed from False
        )

        assert streamer._status_changed(base_status, new_status) is True

    def test_no_change_not_detected(self, base_status: MigrationStatus) -> None:
        """Test that identical status is not detected as changed."""
        streamer = StatusStreamer(
            coordinator=MagicMock(),
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        assert streamer._status_changed(base_status, base_status) is False
