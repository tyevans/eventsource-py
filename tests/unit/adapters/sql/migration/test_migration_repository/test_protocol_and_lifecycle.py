"""Tests for migration repository protocol, lifecycle (init, create, get)."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.sql.migration.migration import (
    VALID_TRANSITIONS,
    PostgreSQLMigrationRepository,
)
from eventsource.application.migration.exceptions import (
    MigrationAlreadyExistsError,
)
from eventsource.ports.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
)
from eventsource.ports.migration.repositories import MigrationRepository
from eventsource.ports.positions import Position


class TestValidTransitions:
    def test_pending_can_transition_to_bulk_copy(self) -> None:
        assert MigrationPhase.BULK_COPY in VALID_TRANSITIONS[MigrationPhase.PENDING]

    def test_pending_can_transition_to_aborted(self) -> None:
        assert MigrationPhase.ABORTED in VALID_TRANSITIONS[MigrationPhase.PENDING]

    def test_pending_cannot_skip_to_dual_write(self) -> None:
        assert MigrationPhase.DUAL_WRITE not in VALID_TRANSITIONS[MigrationPhase.PENDING]

    def test_bulk_copy_valid_transitions(self) -> None:
        valid = VALID_TRANSITIONS[MigrationPhase.BULK_COPY]
        assert MigrationPhase.DUAL_WRITE in valid
        assert MigrationPhase.ABORTED in valid
        assert MigrationPhase.FAILED in valid

    def test_dual_write_valid_transitions(self) -> None:
        valid = VALID_TRANSITIONS[MigrationPhase.DUAL_WRITE]
        assert MigrationPhase.CUTOVER in valid
        assert MigrationPhase.ABORTED in valid
        assert MigrationPhase.FAILED in valid

    def test_cutover_valid_transitions(self) -> None:
        valid = VALID_TRANSITIONS[MigrationPhase.CUTOVER]
        assert MigrationPhase.COMPLETED in valid
        assert MigrationPhase.DUAL_WRITE in valid  # Rollback
        assert MigrationPhase.FAILED in valid

    def test_terminal_phases_have_no_transitions(self) -> None:
        assert VALID_TRANSITIONS[MigrationPhase.COMPLETED] == set()
        assert VALID_TRANSITIONS[MigrationPhase.ABORTED] == set()
        assert VALID_TRANSITIONS[MigrationPhase.FAILED] == set()


class TestMigrationRepositoryProtocol:
    def test_postgresql_repository_implements_protocol(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationRepository(mock_conn)
        assert isinstance(repo, MigrationRepository)

    def test_protocol_has_required_methods(self) -> None:
        assert hasattr(MigrationRepository, "create")
        assert hasattr(MigrationRepository, "get")
        assert hasattr(MigrationRepository, "get_by_tenant")
        assert hasattr(MigrationRepository, "update_phase")
        assert hasattr(MigrationRepository, "update_progress")
        assert hasattr(MigrationRepository, "set_events_total")
        assert hasattr(MigrationRepository, "record_error")
        assert hasattr(MigrationRepository, "set_paused")
        assert hasattr(MigrationRepository, "list_active")


class TestPostgreSQLMigrationRepositoryInit:
    def test_init_with_connection(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationRepository(mock_conn)
        assert repo._conn == mock_conn

    def test_init_with_tracing_enabled(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationRepository(mock_conn, enable_tracing=True)
        assert repo._conn == mock_conn

    def test_init_with_tracing_disabled(self) -> None:
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)
        assert repo._conn == mock_conn


class TestPostgreSQLMigrationRepositoryCreate:
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.fixture
    def sample_migration(self) -> Migration:
        return Migration(
            id=uuid4(),
            tenant_id=uuid4(),
            source_store_id="shared",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
            events_total=1000,
            created_by="test@example.com",
        )

    @pytest.mark.asyncio
    async def test_create_success(
        self,
        repo: PostgreSQLMigrationRepository,
        sample_migration: Migration,
    ) -> None:
        repo.get_by_tenant = AsyncMock(return_value=None)
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            result = await repo.create(sample_migration)
            assert result == sample_migration.id
            mock_conn.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_raises_error_when_active_exists(
        self,
        repo: PostgreSQLMigrationRepository,
        sample_migration: Migration,
    ) -> None:
        existing_migration = Migration(
            id=uuid4(),
            tenant_id=sample_migration.tenant_id,
            source_store_id="shared",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )
        repo.get_by_tenant = AsyncMock(return_value=existing_migration)
        with pytest.raises(MigrationAlreadyExistsError) as exc_info:
            await repo.create(sample_migration)
        assert exc_info.value.tenant_id == sample_migration.tenant_id
        assert exc_info.value.existing_migration_id == existing_migration.id


class TestPostgreSQLMigrationRepositoryGet:
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_returns_none_when_not_found(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            result = await repo.get(uuid4())
            assert result is None

    @pytest.mark.asyncio
    async def test_get_returns_migration_when_found(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        migration_id = uuid4()
        tenant_id = uuid4()
        config = MigrationConfig().to_dict()
        source_position = Position(store_id="source", key=(250,))
        target_position = Position(store_id="target", key=(125,))
        row = (
            migration_id,
            tenant_id,
            "shared",
            "dedicated",
            "pending",
            1000,
            500,
            source_position.to_str(),
            target_position.to_str(),
            None,
            None,
            None,
            None,
            None,
            None,
            json.dumps(config),
            0,
            None,
            None,
            False,
            None,
            None,
            datetime.now(UTC),
            datetime.now(UTC),
            "test@example.com",
        )
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = row
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            result = await repo.get(migration_id)
            assert result is not None
            assert result.id == migration_id
            assert result.tenant_id == tenant_id
            assert result.source_store_id == "shared"
            assert result.target_store_id == "dedicated"
            assert result.phase == MigrationPhase.PENDING


class TestPostgreSQLMigrationRepositoryGetByTenant:
    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_by_tenant_returns_none_when_no_active(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        with patch("eventsource.adapters.sql.migration.migration.sql_connection") as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn
            result = await repo.get_by_tenant(uuid4())
            assert result is None
