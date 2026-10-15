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
    @pytest.mark.asyncio
    async def test_get_migration_returns_migration(self) -> None:
        migration_id = uuid4()
        migration = create_test_migration(id=migration_id)
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)
        assert await coordinator.get_migration(migration_id) == migration

    @pytest.mark.asyncio
    async def test_get_migration_returns_none_if_not_found(self) -> None:
        migration_repo = AsyncMock(get=AsyncMock(return_value=None))
        coordinator = create_coordinator(migration_repo=migration_repo)
        assert await coordinator.get_migration(uuid4()) is None


class TestGetMigrationForTenant:
    @pytest.mark.asyncio
    async def test_get_migration_for_tenant_returns_active(self) -> None:
        tenant_id = uuid4()
        migration = create_test_migration(tenant_id=tenant_id)
        migration_repo = AsyncMock(get_by_tenant=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)
        assert await coordinator.get_migration_for_tenant(tenant_id) == migration


class TestListActiveMigrations:
    @pytest.mark.asyncio
    async def test_list_active_migrations_returns_statuses(self) -> None:
        migrations = [
            create_test_migration(target_store_id="dedicated-1", phase=MigrationPhase.BULK_COPY),
            create_test_migration(target_store_id="dedicated-2", phase=MigrationPhase.DUAL_WRITE),
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
        migration_repo = AsyncMock(list_active=AsyncMock(return_value=[]))
        coordinator = create_coordinator(migration_repo=migration_repo)
        assert await coordinator.list_active_migrations() == []


class TestWaitForPhase:
    @pytest.mark.asyncio
    async def test_wait_for_phase_returns_when_phase_reached(self) -> None:
        migration_id = uuid4()
        migration = create_test_migration(id=migration_id, phase=MigrationPhase.COMPLETED)
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        result = await coordinator.wait_for_phase(
            migration_id, MigrationPhase.COMPLETED, timeout=1.0
        )
        assert result.phase == MigrationPhase.COMPLETED

    @pytest.mark.asyncio
    async def test_wait_for_phase_returns_on_terminal_state(self) -> None:
        migration_id = uuid4()
        migration = create_test_migration(id=migration_id, phase=MigrationPhase.FAILED)
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        result = await coordinator.wait_for_phase(
            migration_id, MigrationPhase.COMPLETED, timeout=1.0
        )
        assert result.phase == MigrationPhase.FAILED

    @pytest.mark.asyncio
    async def test_wait_for_phase_raises_timeout(self) -> None:
        migration_id = uuid4()
        migration = create_test_migration(id=migration_id, phase=MigrationPhase.BULK_COPY)
        migration_repo = AsyncMock(get=AsyncMock(return_value=migration))
        coordinator = create_coordinator(migration_repo=migration_repo)

        with pytest.raises(asyncio.TimeoutError):
            await coordinator.wait_for_phase(
                migration_id, MigrationPhase.COMPLETED, timeout=0.1, poll_interval=0.05
            )

    @pytest.mark.asyncio
    async def test_wait_for_phase_raises_if_not_found(self) -> None:
        migration_id = uuid4()
        migration_repo = AsyncMock(get=AsyncMock(return_value=None))
        coordinator = create_coordinator(migration_repo=migration_repo)

        with pytest.raises(MigrationNotFoundError):
            await coordinator.wait_for_phase(migration_id, MigrationPhase.COMPLETED, timeout=1.0)


class TestCalculateDuration:
    def test_calculate_duration_with_completed_migration(self) -> None:
        started = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        completed = datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC)
        migration = create_test_migration(
            phase=MigrationPhase.COMPLETED, started_at=started, completed_at=completed
        )
        coordinator = create_coordinator()
        assert coordinator._calculate_duration(migration) == 60.0

    def test_calculate_duration_with_none_migration(self) -> None:
        coordinator = create_coordinator()
        assert coordinator._calculate_duration(None) == 0.0

    def test_calculate_duration_with_not_started(self) -> None:
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
            started_at=None,
        )
        coordinator = create_coordinator()
        assert coordinator._calculate_duration(migration) == 0.0
