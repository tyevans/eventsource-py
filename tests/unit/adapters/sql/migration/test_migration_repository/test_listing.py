"""Tests for listing active migrations."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.migration import (
    PostgreSQLMigrationRepository,
)
from eventsource.ports.migration.models import (
    MigrationConfig,
    MigrationPhase,
)
from eventsource.ports.positions import Position


class TestPostgreSQLMigrationRepositoryListActive:
    """Tests for PostgreSQLMigrationRepository.list_active method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_list_active_returns_empty_list(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test list_active returns empty list when no active migrations."""
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = []
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_active()

            assert result == []

    @pytest.mark.asyncio
    async def test_list_active_returns_migrations(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test list_active returns active migrations."""
        config = MigrationConfig().to_dict()
        source_position1 = Position(store_id="source", key=(250,))
        target_position1 = Position(store_id="target", key=(125,))
        row1 = (
            uuid4(),  # id
            uuid4(),  # tenant_id
            "shared",  # source_store_id
            "dedicated1",  # target_store_id
            "bulk_copy",  # phase
            1000,  # events_total
            500,  # events_copied
            source_position1.to_str(),  # last_source_position_token
            target_position1.to_str(),  # last_target_position_token
            datetime.now(UTC),  # started_at
            datetime.now(UTC),  # bulk_copy_started_at
            None,  # bulk_copy_completed_at
            None,  # dual_write_started_at
            None,  # cutover_started_at
            None,  # completed_at
            json.dumps(config),  # config
            0,  # error_count
            None,  # last_error
            None,  # last_error_at
            False,  # is_paused
            None,  # paused_at
            None,  # pause_reason
            datetime.now(UTC),  # created_at
            datetime.now(UTC),  # updated_at
            "test@example.com",  # created_by
        )
        source_position2 = Position(store_id="source", key=(750,))
        target_position2 = Position(store_id="target", key=(375,))
        row2 = (
            uuid4(),  # id
            uuid4(),  # tenant_id
            "shared",  # source_store_id
            "dedicated2",  # target_store_id
            "dual_write",  # phase
            2000,  # events_total
            1500,  # events_copied
            source_position2.to_str(),  # last_source_position_token
            target_position2.to_str(),  # last_target_position_token
            datetime.now(UTC),  # started_at
            datetime.now(UTC),  # bulk_copy_started_at
            datetime.now(UTC),  # bulk_copy_completed_at
            datetime.now(UTC),  # dual_write_started_at
            None,  # cutover_started_at
            None,  # completed_at
            json.dumps(config),  # config
            1,  # error_count
            "Timeout",  # last_error
            datetime.now(UTC),  # last_error_at
            False,  # is_paused
            None,  # paused_at
            None,  # pause_reason
            datetime.now(UTC),  # created_at
            datetime.now(UTC),  # updated_at
            "test@example.com",  # created_by
        )

        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = [row1, row2]
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_active()

            assert len(result) == 2
            assert result[0].phase == MigrationPhase.BULK_COPY
            assert result[1].phase == MigrationPhase.DUAL_WRITE
