"""Tests for helper methods and phase transition validation."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.migration import (
    PostgreSQLMigrationRepository,
)
from eventsource.application.migration.exceptions import (
    InvalidPhaseTransitionError,
)
from eventsource.ports.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
)
from eventsource.ports.positions import Position


class TestPostgreSQLMigrationRepositoryHelpers:
    """Tests for PostgreSQLMigrationRepository helper methods."""

    @pytest.fixture
    def repo(self) -> PostgreSQLMigrationRepository:
        """Create a repository for testing."""
        mock_conn = MagicMock()
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    def test_get_phase_timestamp_updates_bulk_copy(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp updates for BULK_COPY phase."""
        sql = repo._get_phase_timestamp_updates(MigrationPhase.BULK_COPY)
        assert "started_at = :phase_time" in sql
        assert "bulk_copy_started_at = :phase_time" in sql

    def test_get_phase_timestamp_updates_dual_write(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp updates for DUAL_WRITE phase."""
        sql = repo._get_phase_timestamp_updates(MigrationPhase.DUAL_WRITE)
        assert "bulk_copy_completed_at = :phase_time" in sql
        assert "dual_write_started_at = :phase_time" in sql

    def test_get_phase_timestamp_updates_cutover(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp updates for CUTOVER phase."""
        sql = repo._get_phase_timestamp_updates(MigrationPhase.CUTOVER)
        assert "cutover_started_at = :phase_time" in sql

    def test_get_phase_timestamp_updates_completed(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp updates for COMPLETED phase."""
        sql = repo._get_phase_timestamp_updates(MigrationPhase.COMPLETED)
        assert "completed_at = :phase_time" in sql

    def test_get_phase_timestamp_updates_aborted(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp updates for ABORTED phase."""
        sql = repo._get_phase_timestamp_updates(MigrationPhase.ABORTED)
        assert "completed_at = :phase_time" in sql

    def test_get_phase_timestamp_updates_failed(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp updates for FAILED phase."""
        sql = repo._get_phase_timestamp_updates(MigrationPhase.FAILED)
        assert "completed_at = :phase_time" in sql

    def test_get_phase_timestamp_updates_pending(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp updates for PENDING phase returns empty string."""
        sql = repo._get_phase_timestamp_updates(MigrationPhase.PENDING)
        assert sql == ""

    def test_get_phase_timestamp_params_bulk_copy(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp params for BULK_COPY phase."""
        now = datetime.now(UTC)
        params = repo._get_phase_timestamp_params(MigrationPhase.BULK_COPY, now)
        assert params["phase_time"] == now

    def test_get_phase_timestamp_params_pending(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test timestamp params for PENDING phase returns empty dict."""
        now = datetime.now(UTC)
        params = repo._get_phase_timestamp_params(MigrationPhase.PENDING, now)
        assert params == {}

    def test_row_to_migration(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test converting a database row to Migration."""
        migration_id = uuid4()
        tenant_id = uuid4()
        config = MigrationConfig(batch_size=500).to_dict()
        created_at = datetime.now(UTC)
        source_position = Position(store_id="source", key=(400,))
        target_position = Position(store_id="target", key=(200,))

        row = (
            migration_id,  # id
            tenant_id,  # tenant_id
            "shared",  # source_store_id
            "dedicated",  # target_store_id
            "dual_write",  # phase
            1000,  # events_total
            750,  # events_copied
            source_position.to_str(),  # last_source_position_token
            target_position.to_str(),  # last_target_position_token
            created_at,  # started_at
            created_at,  # bulk_copy_started_at
            created_at,  # bulk_copy_completed_at
            created_at,  # dual_write_started_at
            None,  # cutover_started_at
            None,  # completed_at
            json.dumps(config),  # config
            2,  # error_count
            "Test error",  # last_error
            created_at,  # last_error_at
            True,  # is_paused
            created_at,  # paused_at
            "Maintenance",  # pause_reason
            created_at,  # created_at
            created_at,  # updated_at
            "admin@example.com",  # created_by
        )

        migration = repo._row_to_migration(row)

        assert migration.id == migration_id
        assert migration.tenant_id == tenant_id
        assert migration.source_store_id == "shared"
        assert migration.target_store_id == "dedicated"
        assert migration.phase == MigrationPhase.DUAL_WRITE
        assert migration.events_total == 1000
        assert migration.events_copied == 750
        assert migration.last_source_position == source_position
        assert migration.last_target_position == target_position
        assert migration.config.batch_size == 500
        assert migration.error_count == 2
        assert migration.last_error == "Test error"
        assert migration.is_paused is True
        assert migration.pause_reason == "Maintenance"
        assert migration.created_by == "admin@example.com"

    def test_row_to_migration_with_dict_config(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test converting a row with config already as dict."""
        migration_id = uuid4()
        tenant_id = uuid4()
        config = {"batch_size": 500}  # Already a dict
        created_at = datetime.now(UTC)

        row = (
            migration_id,
            tenant_id,
            "shared",
            "dedicated",
            "pending",
            0,
            0,
            None,  # last_source_position_token
            None,  # last_target_position_token
            None,
            None,
            None,
            None,
            None,
            None,
            config,  # dict instead of JSON string
            0,
            None,
            None,
            False,
            None,
            None,
            created_at,
            created_at,
            None,
        )

        migration = repo._row_to_migration(row)

        assert migration.config.batch_size == 500

    def test_row_to_migration_with_null_values(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test converting a row with null values."""
        migration_id = uuid4()
        tenant_id = uuid4()
        config = MigrationConfig().to_dict()

        row = (
            migration_id,
            tenant_id,
            "shared",
            "dedicated",
            "pending",
            None,  # events_total
            None,  # events_copied
            None,  # last_source_position_token
            None,  # last_target_position_token
            None,
            None,
            None,
            None,
            None,
            None,
            json.dumps(config),
            None,  # error_count
            None,
            None,
            None,  # is_paused
            None,
            None,
            None,
            None,
            None,
        )

        migration = repo._row_to_migration(row)

        assert migration.events_total == 0
        assert migration.events_copied == 0
        assert migration.last_source_position is None
        assert migration.last_target_position is None
        assert migration.error_count == 0
        assert migration.is_paused is False


class TestPhaseTransitionValidation:
    """Tests for phase transition validation logic."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "current_phase,new_phase,should_succeed",
        [
            # Valid forward transitions
            (MigrationPhase.PENDING, MigrationPhase.BULK_COPY, True),
            (MigrationPhase.BULK_COPY, MigrationPhase.DUAL_WRITE, True),
            (MigrationPhase.DUAL_WRITE, MigrationPhase.CUTOVER, True),
            (MigrationPhase.CUTOVER, MigrationPhase.COMPLETED, True),
            # Valid rollback
            (MigrationPhase.CUTOVER, MigrationPhase.DUAL_WRITE, True),
            # Valid abort transitions
            (MigrationPhase.PENDING, MigrationPhase.ABORTED, True),
            (MigrationPhase.BULK_COPY, MigrationPhase.ABORTED, True),
            (MigrationPhase.DUAL_WRITE, MigrationPhase.ABORTED, True),
            (MigrationPhase.CUTOVER, MigrationPhase.ABORTED, False),  # No abort from cutover
            # Valid fail transitions
            (MigrationPhase.BULK_COPY, MigrationPhase.FAILED, True),
            (MigrationPhase.DUAL_WRITE, MigrationPhase.FAILED, True),
            (MigrationPhase.CUTOVER, MigrationPhase.FAILED, True),
            # Invalid skip transitions
            (MigrationPhase.PENDING, MigrationPhase.DUAL_WRITE, False),
            (MigrationPhase.PENDING, MigrationPhase.CUTOVER, False),
            (MigrationPhase.PENDING, MigrationPhase.COMPLETED, False),
            (MigrationPhase.BULK_COPY, MigrationPhase.CUTOVER, False),
            (MigrationPhase.BULK_COPY, MigrationPhase.COMPLETED, False),
            # Invalid backward transitions
            (MigrationPhase.DUAL_WRITE, MigrationPhase.BULK_COPY, False),
            (MigrationPhase.DUAL_WRITE, MigrationPhase.PENDING, False),
            # Terminal phases cannot transition
            (MigrationPhase.COMPLETED, MigrationPhase.PENDING, False),
            (MigrationPhase.COMPLETED, MigrationPhase.BULK_COPY, False),
            (MigrationPhase.ABORTED, MigrationPhase.PENDING, False),
            (MigrationPhase.FAILED, MigrationPhase.PENDING, False),
        ],
    )
    async def test_phase_transition(
        self,
        repo: PostgreSQLMigrationRepository,
        current_phase: MigrationPhase,
        new_phase: MigrationPhase,
        should_succeed: bool,
    ) -> None:
        """Test phase transition validation."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="shared",
            target_store_id="dedicated",
            phase=current_phase,
        )
        repo.get = AsyncMock(return_value=migration)

        if should_succeed:
            with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
                mock_conn = AsyncMock()
                mock_ctx.return_value.__aenter__.return_value = mock_conn

                await repo.update_phase(migration_id, new_phase)

                mock_conn.execute.assert_called_once()
        else:
            with pytest.raises(InvalidPhaseTransitionError) as exc_info:
                await repo.update_phase(migration_id, new_phase)

            assert exc_info.value.current_phase == current_phase
            assert exc_info.value.target_phase == new_phase
