"""Tests for StatusStreamManager and MigrationCoordinator status streaming integration."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.status_streamer import (
    StatusStreamer,
    StatusStreamManager,
)
from eventsource.ports.migration.models import (
    Migration,
    MigrationPhase,
    MigrationStatus,
)


class TestStatusStreamManagerInit:
    """Tests for StatusStreamManager initialization."""

    def test_init_with_required_args(self) -> None:
        """Test initialization with required arguments."""
        coordinator = MagicMock()

        manager = StatusStreamManager(
            coordinator=coordinator,
            enable_tracing=False,
        )

        assert manager._coordinator == coordinator
        assert manager._streamers == {}

    def test_active_streamers_initial(self) -> None:
        """Test active_streamers is 0 initially."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        assert manager.active_streamers == 0


class TestStatusStreamManagerGetStreamer:
    """Tests for StatusStreamManager.get_streamer() method."""

    @pytest.mark.asyncio
    async def test_get_streamer_creates_new(self) -> None:
        """Test get_streamer creates new streamer if not exists."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        streamer = await manager.get_streamer(migration_id)

        assert isinstance(streamer, StatusStreamer)
        assert streamer.migration_id == migration_id
        assert manager.active_streamers == 1

    @pytest.mark.asyncio
    async def test_get_streamer_returns_existing(self) -> None:
        """Test get_streamer returns existing streamer."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        streamer1 = await manager.get_streamer(migration_id)
        streamer2 = await manager.get_streamer(migration_id)

        assert streamer1 is streamer2
        assert manager.active_streamers == 1


class TestStatusStreamManagerClose:
    """Tests for StatusStreamManager close methods."""

    @pytest.mark.asyncio
    async def test_close_streamer_removes_it(self) -> None:
        """Test close_streamer removes the streamer."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        await manager.get_streamer(migration_id)
        assert manager.active_streamers == 1

        await manager.close_streamer(migration_id)

        assert manager.active_streamers == 0

    @pytest.mark.asyncio
    async def test_close_streamer_noop_if_not_exists(self) -> None:
        """Test close_streamer is noop if streamer doesn't exist."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        # Should not raise
        await manager.close_streamer(uuid4())

    @pytest.mark.asyncio
    async def test_close_all_removes_all(self) -> None:
        """Test close_all removes all streamers."""
        manager = StatusStreamManager(
            coordinator=MagicMock(),
            enable_tracing=False,
        )

        await manager.get_streamer(uuid4())
        await manager.get_streamer(uuid4())
        await manager.get_streamer(uuid4())
        assert manager.active_streamers == 3

        await manager.close_all()

        assert manager.active_streamers == 0


class TestStatusStreamManagerCleanup:
    """Tests for StatusStreamManager.cleanup_terminal_migrations() method."""

    @pytest.mark.asyncio
    async def test_cleanup_removes_terminal_migrations(self) -> None:
        """Test cleanup removes streamers for terminal migrations."""
        mock_coordinator = MagicMock()

        active_status = MigrationStatus(
            migration_id=uuid4(),
            tenant_id=uuid4(),
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

        terminal_status = MigrationStatus(
            migration_id=uuid4(),
            tenant_id=uuid4(),
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

        # Return appropriate status based on migration_id
        async def get_status_side_effect(migration_id):
            if migration_id == active_status.migration_id:
                return active_status
            return terminal_status

        mock_coordinator.get_status = AsyncMock(side_effect=get_status_side_effect)

        manager = StatusStreamManager(
            coordinator=mock_coordinator,
            enable_tracing=False,
        )

        # Create streamers for both migrations
        await manager.get_streamer(active_status.migration_id)
        await manager.get_streamer(terminal_status.migration_id)
        assert manager.active_streamers == 2

        # Cleanup
        removed = await manager.cleanup_terminal_migrations()

        assert removed == 1
        assert manager.active_streamers == 1

    @pytest.mark.asyncio
    async def test_cleanup_removes_not_found_migrations(self) -> None:
        """Test cleanup removes streamers for migrations not found."""
        mock_coordinator = MagicMock()
        mock_coordinator.get_status = AsyncMock(side_effect=Exception("Not found"))

        manager = StatusStreamManager(
            coordinator=mock_coordinator,
            enable_tracing=False,
        )

        await manager.get_streamer(uuid4())
        assert manager.active_streamers == 1

        removed = await manager.cleanup_terminal_migrations()

        assert removed == 1
        assert manager.active_streamers == 0


class TestCoordinatorIntegration:
    """Tests for MigrationCoordinator integration with status streaming."""

    def test_create_status_streamer(self, mock_deps: dict) -> None:
        """Test create_status_streamer creates StatusStreamer."""
        coordinator = MigrationCoordinator(
            **mock_deps,
            enable_tracing=False,
        )

        migration_id = uuid4()
        streamer = coordinator.create_status_streamer(migration_id)

        assert isinstance(streamer, StatusStreamer)
        assert streamer.migration_id == migration_id

    @pytest.mark.asyncio
    async def test_stream_status_yields_statuses(self, mock_deps: dict) -> None:
        """Test stream_status yields MigrationStatus objects."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
            events_total=1000,
            events_copied=1000,
            started_at=datetime.now(UTC),
        )

        mock_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **mock_deps,
            enable_tracing=False,
        )

        statuses = []
        async for status in coordinator.stream_status(migration_id, update_interval=0.1):
            statuses.append(status)

        assert len(statuses) == 1
        assert statuses[0].migration_id == migration_id
        assert statuses[0].phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_status_with_custom_interval(self, mock_deps: dict) -> None:
        """Test stream_status respects custom update interval."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
            events_total=1000,
            events_copied=1000,
            started_at=datetime.now(UTC),
        )

        mock_deps["migration_repo"].get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            **mock_deps,
            enable_tracing=False,
        )

        statuses = []
        async for status in coordinator.stream_status(
            migration_id,
            update_interval=0.05,
            include_initial=True,
        ):
            statuses.append(status)

        assert len(statuses) >= 1
