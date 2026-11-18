"""Bounds on migration status streaming: update_interval floor and subscriber cap (TASK-0486)."""

from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.status_streamer import StatusStreamer
from eventsource.ports.migration.models import MigrationPhase, MigrationStatus


def _status(migration_id, tenant_id, phase: MigrationPhase) -> MigrationStatus:
    return MigrationStatus(
        migration_id=migration_id,
        tenant_id=tenant_id,
        phase=phase,
        progress_percent=10.0,
        events_total=100,
        events_copied=10,
        events_remaining=90,
        sync_lag_events=0,
        sync_lag_ms=0.0,
        error_count=0,
        started_at=datetime.now(UTC),
        phase_started_at=datetime.now(UTC),
        estimated_completion=None,
        current_rate_events_per_sec=0.0,
        is_paused=False,
    )


def _coordinator(migration_id, phase: MigrationPhase) -> MagicMock:
    coordinator = MagicMock()
    status = _status(migration_id, uuid4(), phase)
    coordinator.get_status = AsyncMock(return_value=status)
    return coordinator


@pytest.mark.asyncio
async def test_update_interval_below_floor_is_rejected() -> None:
    migration_id = uuid4()
    streamer = StatusStreamer(
        coordinator=_coordinator(migration_id, MigrationPhase.BULK_COPY),
        migration_id=migration_id,
        enable_tracing=False,
    )
    with pytest.raises(ValueError, match="update_interval must be >= 0.05"):
        async for _ in streamer.stream_status(update_interval=0.01):
            pass


@pytest.mark.asyncio
async def test_update_interval_at_floor_is_accepted() -> None:
    migration_id = uuid4()
    streamer = StatusStreamer(
        coordinator=_coordinator(migration_id, MigrationPhase.COMPLETED),
        migration_id=migration_id,
        enable_tracing=False,
    )
    statuses = [s async for s in streamer.stream_status(update_interval=0.05)]
    assert [s.phase for s in statuses] == [MigrationPhase.COMPLETED]


@pytest.mark.asyncio
async def test_subscriber_cap_rejects_the_next_stream() -> None:
    migration_id = uuid4()
    streamer = StatusStreamer(
        coordinator=_coordinator(migration_id, MigrationPhase.BULK_COPY),
        migration_id=migration_id,
        enable_tracing=False,
    )
    streams = [streamer.stream_status(update_interval=0.05) for _ in range(100)]
    try:
        for stream in streams:
            # Each open stream yields the initial status and stays registered.
            await stream.__anext__()
        assert streamer.subscriber_count == 100

        overflow = streamer.stream_status(update_interval=0.05)
        with pytest.raises(RuntimeError, match="already has 100 subscribers"):
            await overflow.__anext__()
        assert streamer.subscriber_count == 100
    finally:
        for stream in streams:
            await stream.aclose()
    assert streamer.subscriber_count == 0
