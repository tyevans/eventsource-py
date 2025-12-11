"""
Unit tests for MigrationCoordinator implementation.

Tests cover:
- MigrationCoordinator initialization
- start_migration() method
- get_status() method
- pause_migration() method
- resume_migration() method
- abort_migration() method
- list_active_migrations() method
- wait_for_phase() method
- Bulk copy phase orchestration
- Error handling and state transitions
- Status queue management
"""

import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.migration.coordinator import MigrationCoordinator
from eventsource.migration.exceptions import (
    MigrationAlreadyExistsError,
    MigrationError,
    MigrationNotFoundError,
)
from eventsource.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
    MigrationResult,
    MigrationStatus,
    TenantMigrationState,
    TenantRouting,
)


# Test event class for testing
class TestEvent(DomainEvent):
    """Test event for unit tests."""

    event_type: str = "TestEvent"
    aggregate_type: str = "TestAggregate"
    value: str = "test"


class TestMigrationCoordinatorInit:
    """Tests for MigrationCoordinator initialization."""

    def test_init_with_required_args(self) -> None:
        """Test initialization with required arguments."""
        source_store = MagicMock()
        migration_repo = MagicMock()
        routing_repo = MagicMock()
        router = MagicMock()

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            enable_tracing=False,
        )

        assert coordinator._source_store == source_store
        assert coordinator._migration_repo == migration_repo
        assert coordinator._routing_repo == routing_repo
        assert coordinator._router == router
        assert coordinator._source_store_id == "default"
        assert coordinator._active_copiers == {}
        assert coordinator._active_tasks == {}
        assert coordinator._status_queues == {}

    def test_init_with_custom_source_store_id(self) -> None:
        """Test initialization with custom source store ID."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            source_store_id="shared-postgres",
            enable_tracing=False,
        )

        assert coordinator._source_store_id == "shared-postgres"

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        assert coordinator._enable_tracing is False


class TestStartMigration:
    """Tests for MigrationCoordinator.start_migration() method."""

    @pytest.fixture
    def coordinator_deps(self) -> dict:
        """Create coordinator dependencies."""
        return {
            "source_store": AsyncMock(),
            "migration_repo": AsyncMock(),
            "routing_repo": AsyncMock(),
            "router": MagicMock(),
        }

    @pytest.mark.asyncio
    async def test_start_migration_creates_migration(self, coordinator_deps: dict) -> None:
        """Test start_migration creates and returns migration."""
        tenant_id = uuid4()
        target_store = AsyncMock()
        target_store_id = "dedicated-tenant"

        # Mock repository methods
        coordinator_deps["migration_repo"].get_by_tenant = AsyncMock(return_value=None)
        coordinator_deps["migration_repo"].create = AsyncMock()

        # Create a mock migration to return from get()
        created_migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id=target_store_id,
            phase=MigrationPhase.BULK_COPY,
            started_at=datetime.now(UTC),
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=created_migration)
        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator_deps["routing_repo"].get_or_default = AsyncMock(
            return_value=TenantRouting(tenant_id=tenant_id, store_id="default")
        )
        coordinator_deps["routing_repo"].set_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        # Mock the bulk copy to avoid running in background
        with patch.object(coordinator, "_run_bulk_copy", new_callable=AsyncMock):
            migration = await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=target_store,
                target_store_id=target_store_id,
            )

        assert migration.tenant_id == tenant_id
        assert migration.target_store_id == target_store_id
        coordinator_deps["migration_repo"].create.assert_called_once()
        coordinator_deps["router"].register_store.assert_called_once_with(
            target_store_id, target_store
        )

    @pytest.mark.asyncio
    async def test_start_migration_with_config(self, coordinator_deps: dict) -> None:
        """Test start_migration with custom configuration."""
        tenant_id = uuid4()
        target_store = AsyncMock()
        config = MigrationConfig(batch_size=500, max_bulk_copy_rate=5000)

        coordinator_deps["migration_repo"].get_by_tenant = AsyncMock(return_value=None)
        coordinator_deps["migration_repo"].create = AsyncMock()

        created_migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            config=config,
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=created_migration)
        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator_deps["routing_repo"].get_or_default = AsyncMock(
            return_value=TenantRouting(tenant_id=tenant_id, store_id="default")
        )
        coordinator_deps["routing_repo"].set_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        with patch.object(coordinator, "_run_bulk_copy", new_callable=AsyncMock):
            migration = await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=target_store,
                target_store_id="dedicated",
                config=config,
            )

        assert migration.config.batch_size == 500
        assert migration.config.max_bulk_copy_rate == 5000

    @pytest.mark.asyncio
    async def test_start_migration_raises_if_active_migration_exists(
        self, coordinator_deps: dict
    ) -> None:
        """Test start_migration raises if active migration exists."""
        tenant_id = uuid4()
        existing_migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="existing",
            phase=MigrationPhase.BULK_COPY,
        )

        coordinator_deps["migration_repo"].get_by_tenant = AsyncMock(
            return_value=existing_migration
        )

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        with pytest.raises(MigrationAlreadyExistsError) as exc_info:
            await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=AsyncMock(),
                target_store_id="new-target",
            )

        assert exc_info.value.tenant_id == tenant_id
        assert exc_info.value.existing_migration_id == existing_migration.id

    @pytest.mark.asyncio
    async def test_start_migration_updates_routing_state(self, coordinator_deps: dict) -> None:
        """Test start_migration updates routing state to BULK_COPY."""
        tenant_id = uuid4()
        migration_id = uuid4()

        coordinator_deps["migration_repo"].get_by_tenant = AsyncMock(return_value=None)
        coordinator_deps["migration_repo"].create = AsyncMock()

        created_migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )
        coordinator_deps["migration_repo"].get = AsyncMock(return_value=created_migration)
        coordinator_deps["migration_repo"].update_phase = AsyncMock()
        coordinator_deps["routing_repo"].get_or_default = AsyncMock(
            return_value=TenantRouting(tenant_id=tenant_id, store_id="default")
        )
        coordinator_deps["routing_repo"].set_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            **coordinator_deps,
            enable_tracing=False,
        )

        with patch.object(coordinator, "_run_bulk_copy", new_callable=AsyncMock):
            await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=AsyncMock(),
                target_store_id="dedicated",
            )

        # Verify routing state was updated
        coordinator_deps["routing_repo"].set_migration_state.assert_called()
        call_args = coordinator_deps["routing_repo"].set_migration_state.call_args
        assert call_args[0][0] == tenant_id
        assert call_args[0][1] == TenantMigrationState.BULK_COPY


class TestGetStatus:
    """Tests for MigrationCoordinator.get_status() method."""

    @pytest.mark.asyncio
    async def test_get_status_returns_status(self) -> None:
        """Test get_status returns MigrationStatus."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            events_total=1000,
            events_copied=500,
            started_at=datetime.now(UTC),
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

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

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=None)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationNotFoundError) as exc_info:
            await coordinator.get_status(migration_id)

        assert exc_info.value.migration_id == migration_id


class TestPauseMigration:
    """Tests for MigrationCoordinator.pause_migration() method."""

    @pytest.mark.asyncio
    async def test_pause_migration_pauses_copier(self) -> None:
        """Test pause_migration pauses the active copier."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            is_paused=False,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)
        migration_repo.set_paused = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add a mock copier
        mock_copier = MagicMock()
        coordinator._active_copiers[migration_id] = mock_copier

        await coordinator.pause_migration(migration_id)

        mock_copier.pause.assert_called_once()
        migration_repo.set_paused.assert_called_once_with(
            migration_id,
            paused=True,
            reason="Operator requested",
        )

    @pytest.mark.asyncio
    async def test_pause_migration_raises_if_terminal(self) -> None:
        """Test pause_migration raises if migration is terminal."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.pause_migration(migration_id)

        assert "Cannot pause a completed migration" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_pause_migration_noop_if_already_paused(self) -> None:
        """Test pause_migration is noop if already paused."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            is_paused=True,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)
        migration_repo.set_paused = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        await coordinator.pause_migration(migration_id)

        # Should not call set_paused again
        migration_repo.set_paused.assert_not_called()


class TestResumeMigration:
    """Tests for MigrationCoordinator.resume_migration() method."""

    @pytest.mark.asyncio
    async def test_resume_migration_resumes_copier(self) -> None:
        """Test resume_migration resumes the active copier."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            is_paused=True,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)
        migration_repo.set_paused = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        # Add a mock copier
        mock_copier = MagicMock()
        coordinator._active_copiers[migration_id] = mock_copier

        await coordinator.resume_migration(migration_id)

        mock_copier.resume.assert_called_once()
        migration_repo.set_paused.assert_called_once_with(migration_id, paused=False)

    @pytest.mark.asyncio
    async def test_resume_migration_raises_if_not_paused(self) -> None:
        """Test resume_migration raises if migration is not paused."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            is_paused=False,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.resume_migration(migration_id)

        assert "Migration is not paused" in str(exc_info.value)


class TestAbortMigration:
    """Tests for MigrationCoordinator.abort_migration() method."""

    @pytest.mark.asyncio
    async def test_abort_migration_cancels_copier_and_task(self) -> None:
        """Test abort_migration cancels copier and background task."""
        migration_id = uuid4()
        tenant_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            events_copied=100,
            started_at=datetime.now(UTC),
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)
        migration_repo.update_phase = AsyncMock()
        migration_repo.record_error = AsyncMock()

        routing_repo = AsyncMock()
        routing_repo.clear_migration_state = AsyncMock()

        router = MagicMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            enable_tracing=False,
        )

        # Add mock copier
        mock_copier = MagicMock()
        coordinator._active_copiers[migration_id] = mock_copier

        # Create a real asyncio task that can be cancelled
        async def dummy_task():
            await asyncio.sleep(100)  # Long running task

        task = asyncio.create_task(dummy_task())
        coordinator._active_tasks[migration_id] = task

        result = await coordinator.abort_migration(migration_id, reason="Test abort")

        assert isinstance(result, MigrationResult)
        assert result.success is False
        assert result.final_phase == MigrationPhase.ABORTED
        assert result.error_message == "Test abort"

        mock_copier.cancel.assert_called_once()
        assert task.cancelled()  # Task should be cancelled
        routing_repo.clear_migration_state.assert_called_once_with(tenant_id)
        router.clear_dual_write_interceptor.assert_called_once_with(tenant_id)
        migration_repo.update_phase.assert_called_once_with(migration_id, MigrationPhase.ABORTED)

    @pytest.mark.asyncio
    async def test_abort_migration_raises_if_terminal(self) -> None:
        """Test abort_migration raises if migration is terminal."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationError) as exc_info:
            await coordinator.abort_migration(migration_id)

        assert "Cannot abort a completed migration" in str(exc_info.value)


class TestListActiveMigrations:
    """Tests for MigrationCoordinator.list_active_migrations() method."""

    @pytest.mark.asyncio
    async def test_list_active_migrations_returns_statuses(self) -> None:
        """Test list_active_migrations returns list of statuses."""
        migrations = [
            Migration(
                id=uuid4(),
                tenant_id=uuid4(),
                source_store_id="default",
                target_store_id="dedicated-1",
                phase=MigrationPhase.BULK_COPY,
            ),
            Migration(
                id=uuid4(),
                tenant_id=uuid4(),
                source_store_id="default",
                target_store_id="dedicated-2",
                phase=MigrationPhase.DUAL_WRITE,
            ),
        ]

        migration_repo = AsyncMock()
        migration_repo.list_active = AsyncMock(return_value=migrations)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        statuses = await coordinator.list_active_migrations()

        assert len(statuses) == 2
        assert all(isinstance(s, MigrationStatus) for s in statuses)
        assert statuses[0].phase == MigrationPhase.BULK_COPY
        assert statuses[1].phase == MigrationPhase.DUAL_WRITE

    @pytest.mark.asyncio
    async def test_list_active_migrations_empty(self) -> None:
        """Test list_active_migrations with no active migrations."""
        migration_repo = AsyncMock()
        migration_repo.list_active = AsyncMock(return_value=[])

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        statuses = await coordinator.list_active_migrations()

        assert statuses == []


class TestWaitForPhase:
    """Tests for MigrationCoordinator.wait_for_phase() method."""

    @pytest.mark.asyncio
    async def test_wait_for_phase_returns_when_phase_reached(self) -> None:
        """Test wait_for_phase returns when target phase is reached."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

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
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.FAILED,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

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
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

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

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=None)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationNotFoundError):
            await coordinator.wait_for_phase(
                migration_id,
                MigrationPhase.COMPLETED,
                timeout=1.0,
            )


class TestGetMigration:
    """Tests for MigrationCoordinator.get_migration() method."""

    @pytest.mark.asyncio
    async def test_get_migration_returns_migration(self) -> None:
        """Test get_migration returns migration if found."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        result = await coordinator.get_migration(migration_id)

        assert result == migration

    @pytest.mark.asyncio
    async def test_get_migration_returns_none_if_not_found(self) -> None:
        """Test get_migration returns None if not found."""
        migration_repo = AsyncMock()
        migration_repo.get = AsyncMock(return_value=None)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        result = await coordinator.get_migration(uuid4())

        assert result is None


class TestGetMigrationForTenant:
    """Tests for MigrationCoordinator.get_migration_for_tenant() method."""

    @pytest.mark.asyncio
    async def test_get_migration_for_tenant_returns_active(self) -> None:
        """Test get_migration_for_tenant returns active migration."""
        tenant_id = uuid4()
        migration = Migration(
            id=uuid4(),
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.get_by_tenant = AsyncMock(return_value=migration)

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        result = await coordinator.get_migration_for_tenant(tenant_id)

        assert result == migration


class TestBuildStatus:
    """Tests for MigrationCoordinator._build_status() method."""

    def test_build_status_calculates_rate(self) -> None:
        """Test _build_status calculates events per second rate."""
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            events_total=1000,
            events_copied=500,
            started_at=datetime.now(UTC),
        )

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        status = coordinator._build_status(migration)

        assert status.progress_percent == 50.0
        assert status.events_remaining == 500

    def test_build_status_with_bulk_copy_phase(self) -> None:
        """Test _build_status with bulk copy phase."""
        bulk_copy_started = datetime.now(UTC)
        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
            bulk_copy_started_at=bulk_copy_started,
        )

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        status = coordinator._build_status(migration)

        assert status.phase_started_at == bulk_copy_started


class TestStatusQueueManagement:
    """Tests for status queue management methods."""

    def test_register_status_queue(self) -> None:
        """Test register_status_queue adds queue."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        queue: asyncio.Queue[UUID] = asyncio.Queue()

        coordinator.register_status_queue(migration_id, queue)

        assert migration_id in coordinator._status_queues
        assert queue in coordinator._status_queues[migration_id]

    def test_unregister_status_queue(self) -> None:
        """Test unregister_status_queue removes queue."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        queue: asyncio.Queue[UUID] = asyncio.Queue()

        coordinator.register_status_queue(migration_id, queue)
        coordinator.unregister_status_queue(migration_id, queue)

        assert queue not in coordinator._status_queues.get(migration_id, [])

    @pytest.mark.asyncio
    async def test_notify_status_update(self) -> None:
        """Test _notify_status_update sends to queues."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        queue: asyncio.Queue[UUID] = asyncio.Queue()

        coordinator.register_status_queue(migration_id, queue)

        await coordinator._notify_status_update(migration_id)

        assert not queue.empty()
        assert await queue.get() == migration_id

    def test_cleanup_status_queues(self) -> None:
        """Test _cleanup_status_queues removes all queues."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        migration_id = uuid4()
        queue: asyncio.Queue[UUID] = asyncio.Queue()

        coordinator.register_status_queue(migration_id, queue)
        coordinator._cleanup_status_queues(migration_id)

        assert migration_id not in coordinator._status_queues


class TestCalculateDuration:
    """Tests for MigrationCoordinator._calculate_duration() method."""

    def test_calculate_duration_with_completed_migration(self) -> None:
        """Test duration calculation for completed migration."""
        started = datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC)
        completed = datetime(2024, 1, 1, 0, 1, 0, tzinfo=UTC)  # 60 seconds later

        migration = Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.COMPLETED,
            started_at=started,
            completed_at=completed,
        )

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        duration = coordinator._calculate_duration(migration)

        assert duration == 60.0

    def test_calculate_duration_with_none_migration(self) -> None:
        """Test duration calculation with None migration."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

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

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        duration = coordinator._calculate_duration(migration)

        assert duration == 0.0


class TestCompleteMigration:
    """Tests for MigrationCoordinator._complete_migration() method."""

    @pytest.mark.asyncio
    async def test_complete_migration_updates_phase_and_routing(self) -> None:
        """Test _complete_migration updates phase and routing."""
        migration_id = uuid4()
        tenant_id = uuid4()
        target_store_id = "dedicated-target"

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id=target_store_id,
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.update_phase = AsyncMock()

        routing_repo = AsyncMock()
        routing_repo.set_migration_state = AsyncMock()
        routing_repo.set_routing = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=MagicMock(),
            enable_tracing=False,
        )

        await coordinator._complete_migration(migration)

        migration_repo.update_phase.assert_called_once_with(migration_id, MigrationPhase.COMPLETED)
        routing_repo.set_migration_state.assert_called_once_with(
            tenant_id, TenantMigrationState.MIGRATED, migration_id
        )
        routing_repo.set_routing.assert_called_once_with(tenant_id, target_store_id)


class TestFailMigration:
    """Tests for MigrationCoordinator._fail_migration() method."""

    @pytest.mark.asyncio
    async def test_fail_migration_updates_phase_and_clears_routing(self) -> None:
        """Test _fail_migration updates phase and clears routing."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        migration_repo = AsyncMock()
        migration_repo.update_phase = AsyncMock()
        migration_repo.record_error = AsyncMock()

        routing_repo = AsyncMock()
        routing_repo.clear_migration_state = AsyncMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=MagicMock(),
            enable_tracing=False,
        )

        await coordinator._fail_migration(migration, "Test error")

        migration_repo.update_phase.assert_called_once_with(migration_id, MigrationPhase.FAILED)
        migration_repo.record_error.assert_called_once_with(migration_id, "Test error")
        routing_repo.clear_migration_state.assert_called_once_with(tenant_id)
