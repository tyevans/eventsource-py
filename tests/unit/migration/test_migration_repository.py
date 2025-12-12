"""
Unit tests for MigrationRepository implementations.

Tests cover:
- MigrationRepository protocol compliance
- PostgreSQLMigrationRepository CRUD operations
- Phase transition validation
- Progress tracking updates
- Error recording
- Pause/resume functionality
- Active migration listing
- Valid phase transitions map
"""

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.migration.exceptions import (
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    MigrationNotFoundError,
)
from eventsource.migration.models import (
    Migration,
    MigrationConfig,
    MigrationPhase,
)
from eventsource.migration.repositories.migration import (
    VALID_TRANSITIONS,
    MigrationRepository,
    PostgreSQLMigrationRepository,
)


class TestValidTransitions:
    """Tests for the VALID_TRANSITIONS state machine definition."""

    def test_pending_can_transition_to_bulk_copy(self) -> None:
        """Test PENDING can transition to BULK_COPY."""
        assert MigrationPhase.BULK_COPY in VALID_TRANSITIONS[MigrationPhase.PENDING]

    def test_pending_can_transition_to_aborted(self) -> None:
        """Test PENDING can transition to ABORTED."""
        assert MigrationPhase.ABORTED in VALID_TRANSITIONS[MigrationPhase.PENDING]

    def test_pending_cannot_skip_to_dual_write(self) -> None:
        """Test PENDING cannot skip to DUAL_WRITE."""
        assert MigrationPhase.DUAL_WRITE not in VALID_TRANSITIONS[MigrationPhase.PENDING]

    def test_bulk_copy_valid_transitions(self) -> None:
        """Test BULK_COPY valid transitions."""
        valid = VALID_TRANSITIONS[MigrationPhase.BULK_COPY]
        assert MigrationPhase.DUAL_WRITE in valid
        assert MigrationPhase.ABORTED in valid
        assert MigrationPhase.FAILED in valid

    def test_dual_write_valid_transitions(self) -> None:
        """Test DUAL_WRITE valid transitions."""
        valid = VALID_TRANSITIONS[MigrationPhase.DUAL_WRITE]
        assert MigrationPhase.CUTOVER in valid
        assert MigrationPhase.ABORTED in valid
        assert MigrationPhase.FAILED in valid

    def test_cutover_valid_transitions(self) -> None:
        """Test CUTOVER valid transitions including rollback."""
        valid = VALID_TRANSITIONS[MigrationPhase.CUTOVER]
        assert MigrationPhase.COMPLETED in valid
        assert MigrationPhase.DUAL_WRITE in valid  # Rollback
        assert MigrationPhase.FAILED in valid

    def test_terminal_phases_have_no_transitions(self) -> None:
        """Test terminal phases cannot transition to any other phase."""
        assert VALID_TRANSITIONS[MigrationPhase.COMPLETED] == set()
        assert VALID_TRANSITIONS[MigrationPhase.ABORTED] == set()
        assert VALID_TRANSITIONS[MigrationPhase.FAILED] == set()


class TestMigrationRepositoryProtocol:
    """Tests for MigrationRepository protocol."""

    def test_postgresql_repository_implements_protocol(self) -> None:
        """Test PostgreSQLMigrationRepository implements the protocol."""
        # Create a mock connection
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationRepository(mock_conn)

        # The protocol is runtime checkable
        assert isinstance(repo, MigrationRepository)

    def test_protocol_has_required_methods(self) -> None:
        """Test protocol defines all required methods."""
        # Verify protocol methods exist
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
    """Tests for PostgreSQLMigrationRepository initialization."""

    def test_init_with_connection(self) -> None:
        """Test initialization with a connection."""
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationRepository(mock_conn)
        assert repo._conn == mock_conn

    def test_init_with_tracing_enabled(self) -> None:
        """Test initialization with tracing enabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationRepository(mock_conn, enable_tracing=True)
        assert repo._conn == mock_conn

    def test_init_with_tracing_disabled(self) -> None:
        """Test initialization with tracing disabled."""
        mock_conn = MagicMock()
        repo = PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)
        assert repo._conn == mock_conn


class TestPostgreSQLMigrationRepositoryCreate:
    """Tests for PostgreSQLMigrationRepository.create method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.fixture
    def sample_migration(self) -> Migration:
        """Create a sample migration for testing."""
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
        """Test successful migration creation."""
        # Mock get_by_tenant to return None (no existing migration)
        repo.get_by_tenant = AsyncMock(return_value=None)

        # Mock execute_with_connection
        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
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
        """Test create raises error when active migration exists."""
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
    """Tests for PostgreSQLMigrationRepository.get method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_returns_none_when_not_found(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test get returns None when migration not found."""
        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
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
        """Test get returns migration when found."""
        migration_id = uuid4()
        tenant_id = uuid4()
        config = MigrationConfig().to_dict()

        # Create a row tuple matching the SELECT query
        row = (
            migration_id,  # id
            tenant_id,  # tenant_id
            "shared",  # source_store_id
            "dedicated",  # target_store_id
            "pending",  # phase
            1000,  # events_total
            500,  # events_copied
            250,  # last_source_position
            125,  # last_target_position
            None,  # started_at
            None,  # bulk_copy_started_at
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

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
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
    """Tests for PostgreSQLMigrationRepository.get_by_tenant method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_get_by_tenant_returns_none_when_no_active(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test get_by_tenant returns None when no active migration."""
        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchone.return_value = None
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_by_tenant(uuid4())

            assert result is None


class TestPostgreSQLMigrationRepositoryUpdatePhase:
    """Tests for PostgreSQLMigrationRepository.update_phase method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_update_phase_raises_not_found_error(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test update_phase raises error when migration not found."""
        repo.get = AsyncMock(return_value=None)
        migration_id = uuid4()

        with pytest.raises(MigrationNotFoundError) as exc_info:
            await repo.update_phase(migration_id, MigrationPhase.BULK_COPY)

        assert exc_info.value.migration_id == migration_id

    @pytest.mark.asyncio
    async def test_update_phase_raises_invalid_transition_error(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test update_phase raises error for invalid transition."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="shared",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
        )
        repo.get = AsyncMock(return_value=migration)

        # PENDING cannot transition directly to CUTOVER
        with pytest.raises(InvalidPhaseTransitionError) as exc_info:
            await repo.update_phase(migration_id, MigrationPhase.CUTOVER)

        assert exc_info.value.current_phase == MigrationPhase.PENDING
        assert exc_info.value.target_phase == MigrationPhase.CUTOVER

    @pytest.mark.asyncio
    async def test_update_phase_success(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test successful phase update."""
        migration_id = uuid4()
        migration = Migration(
            id=migration_id,
            tenant_id=uuid4(),
            source_store_id="shared",
            target_store_id="dedicated",
            phase=MigrationPhase.PENDING,
        )
        repo.get = AsyncMock(return_value=migration)

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.update_phase(migration_id, MigrationPhase.BULK_COPY)

            mock_conn.execute.assert_called_once()


class TestPostgreSQLMigrationRepositoryUpdateProgress:
    """Tests for PostgreSQLMigrationRepository.update_progress method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_update_progress_without_target_position(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test update_progress without target position."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.update_progress(
                migration_id=migration_id,
                events_copied=500,
                last_source_position=500,
            )

            mock_conn.execute.assert_called_once()
            # Check that the query does not include last_target_position
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert "last_target_position" not in params

    @pytest.mark.asyncio
    async def test_update_progress_with_target_position(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test update_progress with target position."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.update_progress(
                migration_id=migration_id,
                events_copied=500,
                last_source_position=500,
                last_target_position=250,
            )

            mock_conn.execute.assert_called_once()
            # Check that the query includes last_target_position
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["last_target_position"] == 250


class TestPostgreSQLMigrationRepositorySetEventsTotal:
    """Tests for PostgreSQLMigrationRepository.set_events_total method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_set_events_total(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test setting events total."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.set_events_total(migration_id, 10000)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["events_total"] == 10000
            assert params["id"] == migration_id


class TestPostgreSQLMigrationRepositoryRecordError:
    """Tests for PostgreSQLMigrationRepository.record_error method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_record_error(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test recording an error."""
        migration_id = uuid4()
        error_message = "Connection timeout"

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.record_error(migration_id, error_message)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["error"] == error_message
            assert params["id"] == migration_id

    @pytest.mark.asyncio
    async def test_record_error_truncates_long_messages(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test that error messages are truncated to 1000 characters."""
        migration_id = uuid4()
        long_error = "x" * 2000  # 2000 character error

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.record_error(migration_id, long_error)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert len(params["error"]) == 1000


class TestPostgreSQLMigrationRepositorySetPaused:
    """Tests for PostgreSQLMigrationRepository.set_paused method."""

    @pytest.fixture
    def mock_conn(self) -> MagicMock:
        """Create a mock connection."""
        return MagicMock()

    @pytest.fixture
    def repo(self, mock_conn: MagicMock) -> PostgreSQLMigrationRepository:
        """Create a repository with mock connection."""
        return PostgreSQLMigrationRepository(mock_conn, enable_tracing=False)

    @pytest.mark.asyncio
    async def test_set_paused_true(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test pausing a migration."""
        migration_id = uuid4()
        reason = "Manual pause for maintenance"

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.set_paused(migration_id, paused=True, reason=reason)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert params["reason"] == reason
            assert "paused_at" in params

    @pytest.mark.asyncio
    async def test_set_paused_false(
        self,
        repo: PostgreSQLMigrationRepository,
    ) -> None:
        """Test resuming a migration."""
        migration_id = uuid4()

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            await repo.set_paused(migration_id, paused=False)

            mock_conn.execute.assert_called_once()
            call_args = mock_conn.execute.call_args
            params = call_args[0][1]
            assert "reason" not in params
            assert "paused_at" not in params


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
        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
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
        row1 = (
            uuid4(),  # id
            uuid4(),  # tenant_id
            "shared",  # source_store_id
            "dedicated1",  # target_store_id
            "bulk_copy",  # phase
            1000,  # events_total
            500,  # events_copied
            250,  # last_source_position
            125,  # last_target_position
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
        row2 = (
            uuid4(),  # id
            uuid4(),  # tenant_id
            "shared",  # source_store_id
            "dedicated2",  # target_store_id
            "dual_write",  # phase
            2000,  # events_total
            1500,  # events_copied
            750,  # last_source_position
            375,  # last_target_position
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

        with patch(
            "eventsource.migration.repositories.migration.execute_with_connection"
        ) as mock_ctx:
            mock_conn = AsyncMock()
            mock_result = MagicMock()
            mock_result.fetchall.return_value = [row1, row2]
            mock_conn.execute.return_value = mock_result
            mock_ctx.return_value.__aenter__.return_value = mock_conn

            result = await repo.list_active()

            assert len(result) == 2
            assert result[0].phase == MigrationPhase.BULK_COPY
            assert result[1].phase == MigrationPhase.DUAL_WRITE


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

        row = (
            migration_id,  # id
            tenant_id,  # tenant_id
            "shared",  # source_store_id
            "dedicated",  # target_store_id
            "dual_write",  # phase
            1000,  # events_total
            750,  # events_copied
            400,  # last_source_position
            200,  # last_target_position
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
        assert migration.last_source_position == 400
        assert migration.last_target_position == 200
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
            0,
            0,
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
            None,  # last_source_position
            None,  # last_target_position
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
        assert migration.last_source_position == 0
        assert migration.last_target_position == 0
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
            with patch(
                "eventsource.migration.repositories.migration.execute_with_connection"
            ) as mock_ctx:
                mock_conn = AsyncMock()
                mock_ctx.return_value.__aenter__.return_value = mock_conn

                await repo.update_phase(migration_id, new_phase)

                mock_conn.execute.assert_called_once()
        else:
            with pytest.raises(InvalidPhaseTransitionError) as exc_info:
                await repo.update_phase(migration_id, new_phase)

            assert exc_info.value.current_phase == current_phase
            assert exc_info.value.target_phase == new_phase
