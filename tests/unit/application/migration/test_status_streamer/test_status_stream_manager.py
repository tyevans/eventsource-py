"""Tests for StatusStreamManager and MigrationCoordinator status streaming integration."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.status_streamer import StatusStreamer, StatusStreamManager
from eventsource.ports.migration.models import Migration, MigrationPhase, MigrationStatus


def _make_status(phase: MigrationPhase, pct: float = 50.0, rate: float = 100.0) -> MigrationStatus:
    now = datetime.now(UTC)
    copied = int(10 * pct)
    return MigrationStatus(
        migration_id=uuid4(),
        tenant_id=uuid4(),
        phase=phase,
        progress_percent=pct,
        events_total=1000,
        events_copied=copied,
        events_remaining=1000 - copied,
        sync_lag_events=0,
        sync_lag_ms=0.0,
        error_count=0,
        started_at=now,
        phase_started_at=now,
        estimated_completion=None,
        current_rate_events_per_sec=rate,
        is_paused=False,
    )


class TestStatusStreamManagerInit:
    def test_init_with_required_args(self) -> None:
        coordinator = MagicMock()
        manager = StatusStreamManager(coordinator=coordinator, enable_tracing=False)
        assert manager._coordinator == coordinator and manager._streamers == {}

    def test_active_streamers_initial(self) -> None:
        manager = StatusStreamManager(coordinator=MagicMock(), enable_tracing=False)
        assert manager.active_streamers == 0


class TestStatusStreamManagerGetStreamer:
    @pytest.mark.asyncio
    async def test_get_streamer_creates_new(self) -> None:
        manager = StatusStreamManager(coordinator=MagicMock(), enable_tracing=False)
        migration_id = uuid4()
        streamer = await manager.get_streamer(migration_id)
        assert isinstance(streamer, StatusStreamer) and streamer.migration_id == migration_id
        assert manager.active_streamers == 1

    @pytest.mark.asyncio
    async def test_get_streamer_returns_existing(self) -> None:
        manager = StatusStreamManager(coordinator=MagicMock(), enable_tracing=False)
        migration_id = uuid4()
        streamer1 = await manager.get_streamer(migration_id)
        streamer2 = await manager.get_streamer(migration_id)
        assert streamer1 is streamer2 and manager.active_streamers == 1


class TestStatusStreamManagerClose:
    @pytest.mark.asyncio
    async def test_close_streamer_removes_it(self) -> None:
        manager = StatusStreamManager(coordinator=MagicMock(), enable_tracing=False)
        migration_id = uuid4()
        await manager.get_streamer(migration_id)
        assert manager.active_streamers == 1
        await manager.close_streamer(migration_id)
        assert manager.active_streamers == 0

    @pytest.mark.asyncio
    async def test_close_streamer_noop_if_not_exists(self) -> None:
        manager = StatusStreamManager(coordinator=MagicMock(), enable_tracing=False)
        await manager.close_streamer(uuid4())

    @pytest.mark.asyncio
    async def test_close_all_removes_all(self) -> None:
        manager = StatusStreamManager(coordinator=MagicMock(), enable_tracing=False)
        for _ in range(3):
            await manager.get_streamer(uuid4())
        assert manager.active_streamers == 3
        await manager.close_all()
        assert manager.active_streamers == 0


class TestStatusStreamManagerCleanup:
    @pytest.mark.asyncio
    async def test_cleanup_removes_terminal_migrations(self) -> None:
        mock_coordinator = MagicMock()
        active_status = _make_status(MigrationPhase.BULK_COPY, 50.0, 100.0)
        terminal_status = _make_status(MigrationPhase.COMPLETED, 100.0, 0.0)
        mock_coordinator.get_status = AsyncMock(
            side_effect=lambda mid: (
                active_status if mid == active_status.migration_id else terminal_status
            )
        )
        manager = StatusStreamManager(coordinator=mock_coordinator, enable_tracing=False)
        await manager.get_streamer(active_status.migration_id)
        await manager.get_streamer(terminal_status.migration_id)
        assert manager.active_streamers == 2
        assert await manager.cleanup_terminal_migrations() == 1
        assert manager.active_streamers == 1

    @pytest.mark.asyncio
    async def test_cleanup_removes_not_found_migrations(self) -> None:
        mock_coordinator = MagicMock(get_status=AsyncMock(side_effect=Exception("Not found")))
        manager = StatusStreamManager(coordinator=mock_coordinator, enable_tracing=False)
        await manager.get_streamer(uuid4())
        assert manager.active_streamers == 1
        assert await manager.cleanup_terminal_migrations() == 1
        assert manager.active_streamers == 0


class TestCoordinatorIntegration:
    def test_create_status_streamer(self, mock_deps: dict) -> None:
        coordinator = MigrationCoordinator(**mock_deps, enable_tracing=False)
        migration_id = uuid4()
        streamer = coordinator.create_status_streamer(migration_id)
        assert isinstance(streamer, StatusStreamer) and streamer.migration_id == migration_id

    @pytest.mark.asyncio
    async def test_stream_status_yields_statuses(self, mock_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
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
        coordinator = MigrationCoordinator(**mock_deps, enable_tracing=False)
        statuses = [s async for s in coordinator.stream_status(migration_id, update_interval=0.1)]
        assert len(statuses) == 1
        assert statuses[0].migration_id == migration_id
        assert statuses[0].phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_stream_status_with_custom_interval(self, mock_deps: dict) -> None:
        migration_id, tenant_id = uuid4(), uuid4()
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
        coordinator = MigrationCoordinator(**mock_deps, enable_tracing=False)
        statuses = [
            s
            async for s in coordinator.stream_status(
                migration_id, update_interval=0.05, include_initial=True
            )
        ]
        assert len(statuses) >= 1
