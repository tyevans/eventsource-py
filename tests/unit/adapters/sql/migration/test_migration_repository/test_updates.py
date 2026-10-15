"""Tests for updating migration phases, progress, errors, and pause state."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.migration import PostgreSQLMigrationRepository
from eventsource.application.migration.exceptions import (
    InvalidPhaseTransitionError,
    MigrationNotFoundError,
)
from eventsource.ports.migration.models import Migration, MigrationPhase
from eventsource.ports.positions import Position


@pytest.fixture
def mock_conn() -> MagicMock:
    return MagicMock()


@pytest.fixture
def repo(mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
    return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)


class TestPostgreSQLMigrationRepositoryUpdatePhase:
    """Tests for PostgreSQLMigrationRepository.update_phase method."""

    @pytest.mark.asyncio
    async def test_update_phase_raises_not_found_error(
        self, repo: PostgreSQLMigrationRepository
    ) -> None:
        repo.get = AsyncMock(return_value=None)
        migration_id = uuid4()
        with pytest.raises(MigrationNotFoundError) as exc_info:
            await repo.update_phase(migration_id, MigrationPhase.BULK_COPY)
        assert exc_info.value.migration_id == migration_id

    @pytest.mark.asyncio
    async def test_update_phase_raises_invalid_transition_error(
        self, repo: PostgreSQLMigrationRepository
    ) -> None:
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="shared",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
        )
        repo.get = AsyncMock(return_value=migration)
        with pytest.raises(InvalidPhaseTransitionError) as exc_info:
            await repo.update_phase(migration_id, MigrationPhase.CUTOVER)
        assert exc_info.value.current_phase == MigrationPhase.PENDING
        assert exc_info.value.target_phase == MigrationPhase.CUTOVER

    @pytest.mark.asyncio
    async def test_update_phase_success(self, repo: PostgreSQLMigrationRepository) -> None:
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="shared",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
        )
        repo.get = AsyncMock(return_value=migration)
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.update_phase(migration_id, MigrationPhase.BULK_COPY)
            mock_conn.execute.assert_called_once()


class TestPostgreSQLMigrationRepositoryUpdateProgress:
    """Tests for PostgreSQLMigrationRepository.update_progress method."""

    @pytest.mark.asyncio
    async def test_update_progress_without_target_position(
        self, repo: PostgreSQLMigrationRepository
    ) -> None:
        migration_id = uuid4()
        source_position = Position(store_id="source", key=(500,))
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.update_progress(
                migration_id=migration_id,
                events_copied=500,
                last_source_position=source_position,
            )
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert "last_target_position_token" not in params
            assert params["last_source_position_token"] == source_position.to_str()

    @pytest.mark.asyncio
    async def test_update_progress_with_target_position(
        self, repo: PostgreSQLMigrationRepository
    ) -> None:
        migration_id = uuid4()
        source_position = Position(store_id="source", key=(500,))
        target_position = Position(store_id="target", key=(250,))
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.update_progress(
                migration_id=migration_id,
                events_copied=500,
                last_source_position=source_position,
                last_target_position=target_position,
            )
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["last_target_position_token"] == target_position.to_str()


class TestPostgreSQLMigrationRepositorySetEventsTotal:
    """Tests for PostgreSQLMigrationRepository.set_events_total method."""

    @pytest.mark.asyncio
    async def test_set_events_total(self, repo: PostgreSQLMigrationRepository) -> None:
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.set_events_total(migration_id, 10000)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["events_total"] == 10000 and params["id"] == migration_id


class TestPostgreSQLMigrationRepositoryRecordError:
    """Tests for PostgreSQLMigrationRepository.record_error method."""

    @pytest.mark.asyncio
    async def test_record_error(self, repo: PostgreSQLMigrationRepository) -> None:
        migration_id = uuid4()
        error_message = "Connection timeout"
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.record_error(migration_id, error_message)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["error"] == error_message and params["id"] == migration_id

    @pytest.mark.asyncio
    async def test_record_error_truncates_long_messages(
        self, repo: PostgreSQLMigrationRepository
    ) -> None:
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.record_error(migration_id, "x" * 2000)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert len(params["error"]) == 1000


class TestPostgreSQLMigrationRepositorySetPaused:
    """Tests for PostgreSQLMigrationRepository.set_paused method."""

    @pytest.mark.asyncio
    async def test_set_paused_true(self, repo: PostgreSQLMigrationRepository) -> None:
        migration_id = uuid4()
        reason = "Manual pause for maintenance"
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.set_paused(migration_id, paused=True, reason=reason)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert params["reason"] == reason and "paused_at" in params

    @pytest.mark.asyncio
    async def test_set_paused_false(self, repo: PostgreSQLMigrationRepository) -> None:
        migration_id = uuid4()
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            await repo.set_paused(migration_id, paused=False)
            mock_conn.execute.assert_called_once()
            params = mock_conn.execute.call_args[0][1]
            assert "reason" not in params and "paused_at" not in params
