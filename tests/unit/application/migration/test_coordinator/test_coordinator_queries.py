"""Unit tests for MigrationCoordinator query methods."""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from eventsource.application.migration.exceptions import MigrationNotFoundError
from eventsource.ports.migration.models import (
    Migration,
    MigrationPhase,
    MigrationStatus,
)
from tests.unit.application.migration.test_coordinator._fixtures import (
    create_coordinator,
    create_test_migration,
)


class TestGetMigration:
    """Tests for MigrationCoordinator.get_migration() method."""

    @pytest.mark.asyncio
    async def test_get_migration_returns_migration(self) -> None:
        """Test get_migration returns migration if found."""
        migration_id = uuid4()
        migration = create_test_migration(id=migration_id)
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        result = await coordinator.get_migration(migration_id)

        assert result == migration

    @pytest.mark.asyncio
    async def test_get_migration_returns_none_if_not_found(self) -> None:
        """Test get_migration returns None if not found."""
        migration_repo = AsyncMock(get=AsyncMock(return_value=None))
        coordinator = create_coordinator(migration_repo=migration_repo)

        result = await coordinator.get_migration(uuid4())

        assert result is None


class TestGetMigrationForTenant:
    """Tests for MigrationCoordinator.get_migration_for_tenant() method."""

    @pytest.mark.asyncio
    async def test_get_migration_for_tenant_returns_active(self) -> None:
        """Test get_migration_for_tenant returns active migration."""
        tenant_id = uuid4()
        migration = create_test_migration(tenant_id=tenant_id)
        migration_repo = AsyncMock(get_by_tenant=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        result = await coordinator.get_migration_for_tenant(tenant_id)

        assert result == migration


class TestListActiveMigrations:
    """Tests for MigrationCoordinator.list_active_migrations() method."""

    @pytest.mark.asyncio
    async def test_list_active_migrations_returns_statuses(self) -> None:
        """Test list_active_migrations returns list of statuses."""
        migrations = [
            create_test_migration(
                target_store_id="dedicated-1",
                phase=MigrationPhase.BULK_COPY,
            ),
            create_test_migration(
                target_store_id="dedicated-2",
                phase=MigrationPhase.DUAL_WRITE,
            ),
        ]
        migration_repo = AsyncMock(list_active=AsyncMock(return_value=migrations))
        coordinator = create_coordinator(migration_repo=migration_repo)

        statuses = await coordinator.list_active_migrations()

        assert len(statuses) == 2
        assert all(isinstance(s, MigrationStatus) for s in statuses)
        assert statuses[0].phase == MigrationPhase.BULK_COPY
        assert statuses[1].phase == MigrationPhase.DUAL_WRITE

    @pytest.mark.asyncio
    async def test_list_active_migrations_empty(self) -> None:
        """Test list_active_migrations with no active migrations."""
        migration_repo = AsyncMock(list_active=AsyncMock(return_value=[]))
        coordinator = create_coordinator(migration_repo=migration_repo)

        statuses = await coordinator.list_active_migrations()

        assert statuses == []


class TestWaitForPhase:
    """Tests for MigrationCoordinator.wait_for_phase() method."""

    @pytest.mark.asyncio
    async def test_wait_for_phase_returns_when_phase_reached(self) -> None:
        """Test wait_for_phase returns when target phase is reached."""
        migration_id = uuid4()
        migration = create_test_migration(id=migration_id, phase=MigrationPhase.COMPLETED)
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        result = await coordinator.wait_for_phase(
            migration_id,
            MigrationPhase.COMPLETED,
            timeout=1.0,
        )

        assert result.phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_wait_for_phase_returns_on_terminal_state(self) -> None:
        """Test wait_for_phase returns on terminal state."""
        migration_id = uuid4()
        migration = create_test_migration(id=migration_id, phase=MigrationPhase.FAILED)
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        result = await coordinator.wait_for_phase(
            migration_id,
            MigrationPhase.COMPLETED,  # Waiting for COMPLETED but got FAILED
            timeout=1.0,
        )

        # Should return on terminal state even if not the target phase
        assert result.phase == MigrationPhase.FAILED

    @pytest.mark.asyncio
    async def test_wait_for_phase_raises_timeout(self) -> None:
        """Test wait_for_phase raises TimeoutError on timeout."""
        migration_id = uuid4()
        migration = create_test_migration(id=migration_id, phase=MigrationPhase.BULK_COPY)
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        with pytest.raises(asyncio.TimeoutError):
            await coordinator.wait_for_phase(
                migration_id,
                MigrationPhase.COMPLETED,
                timeout=0.1,
                poll_interval=0.05,
            )

    @pytest.mark.asyncio
    async def test_wait_for_phase_raises_if_not_found(self) -> None:
        """Test wait_for_phase raises if migration not found."""
        migration_id = uuid4()
        migration_repo = AsyncMock(get=AsyncMock(return_value=None))
        coordinator = create_coordinator(migration_repo=migration_repo)

        with pytest.raises(MigrationNotFoundError):
            await coordinator.wait_for_phase(
                migration_id,
                MigrationPhase.COMPLETED,
                timeout=1.0,
            )


class TestCalculateDuration:
    """Tests for MigrationCoordinator._calculate_duration() method."""

    def test_calculate_duration_with_completed_migration(self) -> None:
        """Test duration calculation for completed migration."""
        started = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        completed = datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC)  # 60 seconds later

        migration = create_test_migration(
            phase=MigrationPhase.COMPLETED,
            started_at=started,
            completed_at=completed,
        )
        coordinator = create_coordinator()

        duration = coordinator._calculate_duration(migration)

        assert duration == 60.0

    def test_calculate_duration_with_none_migration(self) -> None:
        """Test duration calculation with None migration."""
        coordinator = create_coordinator()

        duration = coordinator._calculate_duration(None)

        assert duration == 0.0

    def test_calculate_duration_with_not_started(self) -> None:
        """Test duration calculation with not started migration."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
            started_at=None,
        )
        coordinator = create_coordinator()

        duration = coordinator._calculate_duration(migration)

        assert duration == 0.0
