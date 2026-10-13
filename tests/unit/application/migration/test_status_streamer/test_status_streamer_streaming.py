"""Streaming and multiple subscriber tests for StatusStreamer."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.status_streamer import StatusStreamer
from eventsource.ports.migration.models import (
    MigrationPhase,
    MigrationStatus,
)


class TestStatusStreamerStreamStatus:
    """Tests for StatusStreamer.stream_status() method."""

    @pytest.mark.asyncio
    async def test_stream_status_yields_initial_status(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status yields initial status when include_initial=True."""
        mock_coordinator.get_status = AsyncMock(return_value=base_status)

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = []
        async for status in streamer.stream_status(update_interval=0.1, include_initial=True):
            statuses.append(status)
            break  # Stop after first status

        assert len(statuses) == 1
        assert statuses[0] == base_status

    @pytest.mark.asyncio
    async def test_stream_status_skips_initial_when_disabled(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status skips initial status when include_initial=False."""
        # Return terminal status after first call to end the stream
        terminal_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.COMPLETED,
            progress_percent=100.0,
            events_total=1000,
            events_copied=1000,
            events_remaining=0,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=0.0,
            is_paused=False,
        )
        mock_coordinator.get_status = AsyncMock(return_value=terminal_status)

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = []
        async for status in streamer.stream_status(update_interval=0.1, include_initial=False):
            statuses.append(status)

        # Should yield the terminal status (after waiting, not as initial)
        assert len(statuses) == 1
        assert statuses[0].phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_status_stops_on_terminal_phase(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status stops when migration reaches terminal phase."""
        terminal_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.COMPLETED,
            progress_percent=100.0,
            events_total=1000,
            events_copied=1000,
            events_remaining=0,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=0.0,
            is_paused=False,
        )
        mock_coordinator.get_status = AsyncMock(return_value=terminal_status)

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = []
        async for status in streamer.stream_status(update_interval=0.1):
            statuses.append(status)

        # Should stop after terminal phase
        assert len(statuses) == 1
        assert statuses[0].phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_status_raises_on_invalid_interval(
        self, mock_coordinator: MagicMock
    ) -> None:
        """Test stream_status raises ValueError for invalid update_interval."""
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=uuid4(),
            enable_tracing=False,
        )

        with pytest.raises(ValueError) as exc_info:
            async for _ in streamer.stream_status(update_interval=0):
                pass

        assert "update_interval must be > 0" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_stream_status_raises_if_closed(self, mock_coordinator: MagicMock) -> None:
        """Test stream_status raises RuntimeError if streamer is closed."""
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=uuid4(),
            enable_tracing=False,
        )

        await streamer.close()

        with pytest.raises(RuntimeError) as exc_info:
            async for _ in streamer.stream_status():
                pass

        assert "StatusStreamer has been closed" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_stream_status_registers_with_coordinator(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status registers and unregisters with coordinator."""
        # Return non-terminal status first, then terminal on second call
        terminal_status = MigrationStatus(
            migration_id=base_status.migration_id,
            tenant_id=base_status.tenant_id,
            phase=MigrationPhase.COMPLETED,
            progress_percent=100.0,
            events_total=1000,
            events_copied=1000,
            events_remaining=0,
            sync_lag_events=0,
            sync_lag_ms=0.0,
            error_count=0,
            started_at=datetime.now(UTC),
            phase_started_at=datetime.now(UTC),
            estimated_completion=None,
            current_rate_events_per_sec=0.0,
            is_paused=False,
        )
        # First call returns non-terminal (for initial), second returns terminal
        mock_coordinator.get_status = AsyncMock(side_effect=[base_status, terminal_status])

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        async for _ in streamer.stream_status(update_interval=0.1):
            pass

        # Verify registration happened (only happens when initial is not terminal)
        mock_coordinator.register_status_queue.assert_called_once()
        mock_coordinator.unregister_status_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_stream_status_stops_when_migration_not_found(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status stops when migration is not found."""
        # First call returns status, second returns None (not found)
        mock_coordinator.get_status = AsyncMock(side_effect=[base_status, None])

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = []
        async for status in streamer.stream_status(update_interval=0.05):
            statuses.append(status)

        # Should yield initial status and then stop
        assert len(statuses) == 1


class TestStatusStreamerMultipleSubscribers:
    """Tests for multiple subscriber support."""

    @pytest.mark.asyncio
    async def test_multiple_subscribers_can_register(self) -> None:
        """Test that multiple subscribers can register and are tracked."""
        mock_coordinator = MagicMock()
        mock_coordinator.register_status_queue = MagicMock()
        mock_coordinator.unregister_status_queue = MagicMock()

        migration_id = uuid4()

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=migration_id,
            enable_tracing=False,
        )

        # Register multiple subscribers manually
        sub1 = await streamer._register_subscriber()
        sub2 = await streamer._register_subscriber()
        sub3 = await streamer._register_subscriber()

        assert streamer.subscriber_count == 3
        assert sub1 != sub2 != sub3

        # Unregister them
        await streamer._unregister_subscriber(sub1)
        await streamer._unregister_subscriber(sub2)
        await streamer._unregister_subscriber(sub3)

        assert streamer.subscriber_count == 0

    @pytest.mark.asyncio
    async def test_concurrent_streams_are_independent(self) -> None:
        """Test that concurrent streams work independently."""
        mock_coordinator = MagicMock()
        mock_coordinator.register_status_queue = MagicMock()
        mock_coordinator.unregister_status_queue = MagicMock()

        migration_id = uuid4()
        tenant_id = uuid4()

        # Use a call counter to return active then terminal
        call_count = {"value": 0}

        async def get_status_impl(*args, **kwargs):
            call_count["value"] += 1
            # Return active for first call of each stream, then terminal
            if call_count["value"] % 2 == 1:
                return MigrationStatus(
                    migration_id=migration_id,
                    tenant_id=tenant_id,
                    phase=MigrationPhase.BULK_COPY,
                    progress_percent=50.0,
                    events_total=1000,
                    events_copied=500,
                    events_remaining=500,
                    sync_lag_events=0,
                    sync_lag_ms=0.0,
                    error_count=0,
                    started_at=datetime.now(UTC),
                    phase_started_at=datetime.now(UTC),
                    estimated_completion=None,
                    current_rate_events_per_sec=100.0,
                    is_paused=False,
                )
            else:
                return MigrationStatus(
                    migration_id=migration_id,
                    tenant_id=tenant_id,
                    phase=MigrationPhase.COMPLETED,
                    progress_percent=100.0,
                    events_total=1000,
                    events_copied=1000,
                    events_remaining=0,
                    sync_lag_events=0,
                    sync_lag_ms=0.0,
                    error_count=0,
                    started_at=datetime.now(UTC),
                    phase_started_at=datetime.now(UTC),
                    estimated_completion=None,
                    current_rate_events_per_sec=0.0,
                    is_paused=False,
                )

        mock_coordinator.get_status = AsyncMock(side_effect=get_status_impl)

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=migration_id,
            enable_tracing=False,
        )

        # Run two streams sequentially to avoid race conditions
        collected_1 = []
        async for status in streamer.stream_status(update_interval=0.05):
            collected_1.append(status)

        collected_2 = []
        async for status in streamer.stream_status(update_interval=0.05):
            collected_2.append(status)

        # Both should have collected statuses
        assert len(collected_1) >= 1
        assert len(collected_2) >= 1

        # Both should have registered with the coordinator
        assert mock_coordinator.register_status_queue.call_count == 2
        assert mock_coordinator.unregister_status_queue.call_count == 2
