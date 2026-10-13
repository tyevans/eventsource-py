"""Tests for WritePauseManager functionality."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

if TYPE_CHECKING:
    from eventsource.application.migration.write_pause import WritePauseManager


class TestWritePauseManager:
    """Tests for WritePauseManager functionality."""

    @pytest.mark.asyncio
    async def test_pause_and_resume_flow(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test basic pause and resume flow."""
        # Initially not paused
        assert not write_pause_manager.is_paused(tenant_id)

        # Pause
        created = await write_pause_manager.pause_writes(tenant_id)
        assert created is True
        assert write_pause_manager.is_paused(tenant_id)

        # Idempotent pause
        created_again = await write_pause_manager.pause_writes(tenant_id)
        assert created_again is False

        # Resume
        metrics = await write_pause_manager.resume_writes(tenant_id)
        assert metrics is not None
        assert metrics.duration_ms > 0
        assert not write_pause_manager.is_paused(tenant_id)

    @pytest.mark.asyncio
    async def test_wait_if_not_paused_returns_immediately(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test that wait returns immediately if not paused."""
        wait_time = await write_pause_manager.wait_if_paused(tenant_id)
        assert wait_time == 0.0

    @pytest.mark.asyncio
    async def test_pause_metrics_tracking(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test that pause metrics are properly tracked."""
        await write_pause_manager.pause_writes(tenant_id)
        await asyncio.sleep(0.05)  # Brief pause
        metrics = await write_pause_manager.resume_writes(tenant_id)

        assert metrics is not None
        assert metrics.tenant_id == tenant_id
        assert metrics.duration_ms >= 50  # At least 50ms
        assert metrics.started_at < metrics.ended_at

        # Check history
        history = write_pause_manager.get_metrics_history()
        assert len(history) == 1
        assert history[0].tenant_id == tenant_id


__all__ = ["TestWritePauseManager"]
