"""Tests for sync lag calculation, status building, and cutover manager access."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.exceptions import MigrationError, MigrationNotFoundError
from eventsource.ports.migration.models import Migration, MigrationPhase, MigrationStatus, SyncLag


class TestGetSyncLag:
    """Tests for MigrationCoordinator.get_sync_lag() method."""

    @pytest.mark.asyncio
    async def test_get_sync_lag_returns_lag(self) -> None:
        """Test get_sync_lag returns current sync lag."""
        migration_id = uuid4()
        tenant_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
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

        expected_lag = SyncLag(
            events=50,
            source_position=1000,
            target_position=950,
            timestamp=datetime.now(UTC),
        )
        lag_tracker = MagicMock()
        lag_tracker.calculate_lag = AsyncMock(return_value=expected_lag)
        coordinator._lag_trackers[migration_id] = lag_tracker

        result = await coordinator.get_sync_lag(migration_id)

        assert result == expected_lag
        assert result.events == 50
        lag_tracker.calculate_lag.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_sync_lag_returns_none_if_no_tracker(self) -> None:
        """Test get_sync_lag returns None if no tracker found."""
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

        result = await coordinator.get_sync_lag(migration_id)

        assert result is None

    @pytest.mark.asyncio
    async def test_get_sync_lag_raises_if_not_found(self) -> None:
        """Test get_sync_lag raises if migration not found."""
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
            await coordinator.get_sync_lag(migration_id)


class TestBuildStatusWithSyncLag:
    """Tests for MigrationCoordinator._build_status() with sync lag."""

    def test_build_status_includes_sync_lag(self) -> None:
        """Test _build_status includes sync lag from tracker."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.DUAL_WRITE,
            events_total=1000,
            events_copied=1000,
            started_at=datetime.now(UTC),
        )

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        lag = SyncLag(
            events=25,
            source_position=1000,
            target_position=975,
            timestamp=datetime.now(UTC),
        )
        lag_tracker = MagicMock()
        lag_tracker.current_lag = lag
        coordinator._lag_trackers[migration_id] = lag_tracker

        status = coordinator._build_status(migration)

        assert isinstance(status, MigrationStatus)
        assert status.sync_lag_events == 25
        assert status.sync_lag_ms == 25.0

    def test_build_status_zero_lag_without_tracker(self) -> None:
        """Test _build_status returns zero lag without tracker."""
        migration_id = uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        status = coordinator._build_status(migration)

        assert status.sync_lag_events == 0
        assert status.sync_lag_ms == 0.0


class TestGetCutoverManager:
    """Tests for MigrationCoordinator._get_cutover_manager() method."""

    def test_get_cutover_manager_creates_instance(self) -> None:
        """Test _get_cutover_manager creates CutoverManager instance."""
        lock_manager = MagicMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            lock_manager=lock_manager,
            enable_tracing=False,
        )

        cutover_manager = coordinator._get_cutover_manager()

        assert cutover_manager is not None
        assert coordinator._cutover_manager is cutover_manager

    def test_get_cutover_manager_returns_same_instance(self) -> None:
        """Test _get_cutover_manager returns same instance on repeated calls."""
        lock_manager = MagicMock()

        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            lock_manager=lock_manager,
            enable_tracing=False,
        )

        cutover_manager1 = coordinator._get_cutover_manager()
        cutover_manager2 = coordinator._get_cutover_manager()

        assert cutover_manager1 is cutover_manager2

    def test_get_cutover_manager_raises_without_lock_manager(self) -> None:
        """Test _get_cutover_manager raises if no lock_manager."""
        coordinator = MigrationCoordinator(
            source_store=MagicMock(),
            migration_repo=MagicMock(),
            routing_repo=MagicMock(),
            router=MagicMock(),
            enable_tracing=False,
        )

        with pytest.raises(MigrationError) as exc_info:
            coordinator._get_cutover_manager()

        assert "lock_manager not provided" in str(exc_info.value)
