"""Streaming and multiple subscriber tests for StatusStreamer."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.status_streamer import StatusStreamer
from eventsource.ports.migration.models import MigrationPhase, MigrationStatus


def _make_status(
    migration_id: UUID,
    tenant_id: UUID,
    phase: MigrationPhase = MigrationPhase.COMPLETED,
    progress_percent: float = 100.0,
    events_copied: int = 1000,
    events_remaining: int = 0,
    rate: float = 0.0,
) -> MigrationStatus:
    """Create a MigrationStatus instance for testing."""
    return MigrationStatus(
        migration_id=migration_id,
        tenant_id=tenant_id,
        phase=phase,
        progress_percent=progress_percent,
        events_total=1000,
        events_copied=events_copied,
        events_remaining=events_remaining,
        sync_lag_events=0,
        sync_lag_ms=0.0,
        error_count=0,
        started_at=datetime.now(UTC),
        phase_started_at=datetime.now(UTC),
        estimated_completion=None,
        current_rate_events_per_sec=rate,
        is_paused=False,
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
        terminal_status = _make_status(base_status.migration_id, base_status.tenant_id)
        mock_coordinator.get_status = AsyncMock(return_value=terminal_status)
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = [
            s async for s in streamer.stream_status(update_interval=0.1, include_initial=False)
        ]

        assert len(statuses) == 1
        assert statuses[0].phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_status_stops_on_terminal_phase(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status stops when migration reaches terminal phase."""
        terminal_status = _make_status(base_status.migration_id, base_status.tenant_id)
        mock_coordinator.get_status = AsyncMock(return_value=terminal_status)
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = [s async for s in streamer.stream_status(update_interval=0.1)]

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
        terminal_status = _make_status(base_status.migration_id, base_status.tenant_id)
        mock_coordinator.get_status = AsyncMock(side_effect=[base_status, terminal_status])
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        async for _ in streamer.stream_status(update_interval=0.1):
            pass

        mock_coordinator.register_status_queue.assert_called_once()
        mock_coordinator.unregister_status_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_stream_status_stops_when_migration_not_found(
        self, mock_coordinator: MagicMock, base_status: MigrationStatus
    ) -> None:
        """Test stream_status stops when migration is not found."""
        mock_coordinator.get_status = AsyncMock(side_effect=[base_status, None])
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=base_status.migration_id,
            enable_tracing=False,
        )

        statuses = [s async for s in streamer.stream_status(update_interval=0.05)]
        assert len(statuses) == 1


class TestStatusStreamerMultipleSubscribers:
    """Tests for multiple subscriber support."""

    @pytest.mark.asyncio
    async def test_multiple_subscribers_can_register(self) -> None:
        """Test that multiple subscribers can register and are tracked."""
        mock_coordinator = MagicMock()
        mock_coordinator.register_status_queue = MagicMock()
        mock_coordinator.unregister_status_queue = MagicMock()

        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=uuid4(),
            enable_tracing=False,
        )

        sub1 = await streamer._register_subscriber()
        sub2 = await streamer._register_subscriber()
        sub3 = await streamer._register_subscriber()

        assert streamer.subscriber_count == 3
        assert sub1 != sub2 != sub3

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
        call_count = {"value": 0}

        async def get_status_impl(*args, **kwargs):
            call_count["value"] += 1
            if call_count["value"] % 2 == 1:
                return _make_status(
                    migration_id,
                    tenant_id,
                    phase=MigrationPhase.BULK_COPY,
                    progress_percent=50.0,
                    events_copied=500,
                    events_remaining=500,
                    rate=100.0,
                )
            return _make_status(migration_id, tenant_id)

        mock_coordinator.get_status = AsyncMock(side_effect=get_status_impl)
        streamer = StatusStreamer(
            coordinator=mock_coordinator,
            migration_id=migration_id,
            enable_tracing=False,
        )

        collected_1 = [s async for s in streamer.stream_status(update_interval=0.05)]
        collected_2 = [s async for s in streamer.stream_status(update_interval=0.05)]

        assert len(collected_1) >= 1
        assert len(collected_2) >= 1
        assert mock_coordinator.register_status_queue.call_count == 2
        assert mock_coordinator.unregister_status_queue.call_count == 2
