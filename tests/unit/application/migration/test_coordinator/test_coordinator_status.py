"""Unit tests for MigrationCoordinator status reporting and queue management."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from eventsource.application.migration.exceptions import MigrationNotFoundError
from eventsource.ports.migration.models import (
    MigrationPhase,
    MigrationStatus,
)
from tests.unit.application.migration.test_coordinator._fixtures import (
    create_coordinator,
    create_test_migration,
)


class TestGetStatus:
    """Tests for MigrationCoordinator.get_status() method."""

    @pytest.mark.asyncio
    async def test_get_status_returns_status(self) -> None:
        """Test get_status returns MigrationStatus."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = create_test_migration(
            id=migration_id,
            tenant_id=tenant_id,
            phase=MigrationPhase.BULK_COPY,
            events_total=1000,
            events_copied=500,
            started_at=datetime.now(UTC),
        )

        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        status = await coordinator.get_status(migration_id)

        assert isinstance(status, MigrationStatus)
        assert status.migration_id == migration_id
        assert status.tenant_id == tenant_id
        assert status.phase == MigrationPhase.BULK_COPY
        assert status.events_total == 1000
        assert status.events_copied == 500
        assert status.progress_percent == 50.0

    @pytest.mark.asyncio
    async def test_get_status_raises_if_not_found(self) -> None:
        """Test get_status raises MigrationNotFoundError if not found."""
        migration_id = uuid4()
        migration_repo = AsyncMock(get=AsyncMock(return_value=None))
        coordinator = create_coordinator(migration_repo=migration_repo)

        with pytest.raises(MigrationNotFoundError) as exc_info:
            await coordinator.get_status(migration_id)

        assert exc_info.value.migration_id == migration_id


class TestBuildStatus:
    """Tests for MigrationCoordinator._build_status() method."""

    def test_build_status_calculates_rate(self) -> None:
        """Test _build_status calculates events per second rate."""
        migration = create_test_migration(
            events_total=1000,
            events_copied=500,
            started_at=datetime.now(UTC),
        )
        coordinator = create_coordinator()

        status = coordinator._build_status(migration)

        assert status.progress_percent == 50.0
        assert status.events_remaining == 500

    def test_build_status_with_bulk_copy_phase(self) -> None:
        """Test _build_status with bulk copy phase."""
        bulk_copy_started = datetime.now(UTC)
        migration = create_test_migration(
            bulk_copy_started_at=bulk_copy_started,
        )
        coordinator = create_coordinator()

        status = coordinator._build_status(migration)

        assert status.phase_started_at == bulk_copy_started


class TestStatusQueueManagement:
    """Tests for status queue management methods."""

    def test_register_status_queue(self) -> None:
        """Test register_status_queue adds queue."""
        coordinator = create_coordinator()
        migration_id = uuid4()
        queue: asyncio.Queue[UUID] = asyncio.Queue()

        coordinator.register_status_queue(migration_id, queue)

        assert migration_id in coordinator._status_queues
        assert queue in coordinator._status_queues[migration_id]

    def test_unregister_status_queue(self) -> None:
        """Test unregister_status_queue removes queue."""
        coordinator = create_coordinator()
        migration_id = uuid4()
        queue: asyncio.Queue[UUID] = asyncio.Queue()

        coordinator.register_status_queue(migration_id, queue)
        coordinator.unregister_status_queue(migration_id, queue)

        assert queue not in coordinator._status_queues.get(migration_id, [])

    @pytest.mark.asyncio
    async def test_notify_status_update(self) -> None:
        """Test _notify_status_update sends to queues."""
        coordinator = create_coordinator()
        migration_id = uuid4()
        queue: asyncio.Queue[UUID] = asyncio.Queue()

        coordinator.register_status_queue(migration_id, queue)

        await coordinator._notify_status_update(migration_id)

        assert not queue.empty()
        assert await queue.get() == migration_id

    def test_cleanup_status_queues(self) -> None:
        """Test _cleanup_status_queues removes all queues."""
        coordinator = create_coordinator()
        migration_id = uuid4()
        queue: asyncio.Queue[UUID] = asyncio.Queue()

        coordinator.register_status_queue(migration_id, queue)
        coordinator._cleanup_status_queues(migration_id)

        assert migration_id not in coordinator._status_queues
