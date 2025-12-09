"""
Unit tests for the PostgreSQLSnapshotStore implementation.

Tests cover:
- Basic CRUD operations (save, get, delete, exists)
- Upsert semantics (save overwrites existing)
- Bulk delete by aggregate type
- Schema version filtering
- OpenTelemetry tracing (optional)
- JSON state serialization/deserialization

All database interactions are mocked using unittest.mock.
"""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from eventsource.snapshots import PostgreSQLSnapshotStore, Snapshot

# --- Fixtures ---


@pytest.fixture
def mock_session() -> AsyncMock:
    """Create a mock async session."""
    session = AsyncMock(spec=AsyncSession)
    return session


@pytest.fixture
def mock_session_factory(mock_session: AsyncMock) -> MagicMock:
    """Create a mock session factory that returns the mock session."""
    factory = MagicMock(spec=async_sessionmaker)

    # Create async context manager for session
    session_context = AsyncMock()
    session_context.__aenter__.return_value = mock_session
    session_context.__aexit__.return_value = None
    factory.return_value = session_context

    # Create async context manager for begin()
    begin_context = AsyncMock()
    begin_context.__aenter__.return_value = None
    begin_context.__aexit__.return_value = None
    mock_session.begin.return_value = begin_context

    return factory


@pytest.fixture
def store(mock_session_factory: MagicMock) -> PostgreSQLSnapshotStore:
    """Create a PostgreSQLSnapshotStore with mocked session factory."""
    return PostgreSQLSnapshotStore(
        session_factory=mock_session_factory,
        enable_tracing=False,
    )


@pytest.fixture
def aggregate_id() -> UUID:
    """Create a random aggregate ID."""
    return uuid4()


@pytest.fixture
def sample_snapshot(aggregate_id: UUID) -> Snapshot:
    """Create a sample snapshot for testing."""
    return Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="TestAggregate",
        version=10,
        schema_version=1,
        state={"value": "test", "count": 5, "items": ["a", "b"]},
        created_at=datetime.now(UTC),
    )


# --- Helper Functions ---


def create_mock_result(rows: list[Any] | None = None, rowcount: int = 0) -> MagicMock:
    """Create a mock SQLAlchemy result."""
    result = MagicMock()
    if rows is not None:
        result.fetchone.return_value = rows[0] if rows else None
        result.fetchall.return_value = rows
    result.rowcount = rowcount
    result.scalar.return_value = None
    return result


def create_scalar_result(value: Any) -> MagicMock:
    """Create a mock result for scalar queries."""
    result = MagicMock()
    result.scalar.return_value = value
    return result


def create_snapshot_row(snapshot: Snapshot) -> MagicMock:
    """Create a mock database row from a snapshot."""
    row = MagicMock()
    row.aggregate_id = snapshot.aggregate_id
    row.aggregate_type = snapshot.aggregate_type
    row.version = snapshot.version
    row.schema_version = snapshot.schema_version
    row.state = snapshot.state  # Can be dict or JSON string
    row.created_at = snapshot.created_at
    return row


# --- Basic CRUD Tests ---


class TestPostgreSQLSnapshotStoreBasic:
    """Tests for basic CRUD operations."""

    @pytest.mark.asyncio
    async def test_save_snapshot(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        """Test saving a snapshot."""
        mock_session.execute.return_value = create_mock_result()

        await store.save_snapshot(sample_snapshot)

        # Verify execute was called with INSERT ... ON CONFLICT
        mock_session.execute.assert_called_once()
        call_args = mock_session.execute.call_args
        sql_text = str(call_args[0][0])
        assert "INSERT INTO snapshots" in sql_text
        assert "ON CONFLICT" in sql_text
        assert "DO UPDATE SET" in sql_text

    @pytest.mark.asyncio
    async def test_save_snapshot_params(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        """Test that save_snapshot passes correct parameters."""
        mock_session.execute.return_value = create_mock_result()

        await store.save_snapshot(sample_snapshot)

        call_args = mock_session.execute.call_args
        params = call_args[0][1]
        assert params["aggregate_id"] == sample_snapshot.aggregate_id
        assert params["aggregate_type"] == sample_snapshot.aggregate_type
        assert params["version"] == sample_snapshot.version
        assert params["schema_version"] == sample_snapshot.schema_version
        assert params["created_at"] == sample_snapshot.created_at
        # State should be JSON-encoded
        assert '"value": "test"' in params["state"]

    @pytest.mark.asyncio
    async def test_get_snapshot_found(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        """Test getting an existing snapshot."""
        row = create_snapshot_row(sample_snapshot)
        mock_session.execute.return_value = create_mock_result([row])

        result = await store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        assert result is not None
        assert result.aggregate_id == sample_snapshot.aggregate_id
        assert result.aggregate_type == sample_snapshot.aggregate_type
        assert result.version == sample_snapshot.version
        assert result.schema_version == sample_snapshot.schema_version
        assert result.state == sample_snapshot.state

    @pytest.mark.asyncio
    async def test_get_snapshot_not_found(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test getting a non-existent snapshot returns None."""
        mock_result = MagicMock()
        mock_result.fetchone.return_value = None
        mock_session.execute.return_value = mock_result

        result = await store.get_snapshot(aggregate_id, "TestAggregate")

        assert result is None

    @pytest.mark.asyncio
    async def test_get_snapshot_json_string_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test getting snapshot with JSON string state (vs dict)."""
        row = MagicMock()
        row.aggregate_id = aggregate_id
        row.aggregate_type = "TestAggregate"
        row.version = 5
        row.schema_version = 1
        row.state = '{"value": "from_json", "count": 10}'  # JSON string
        row.created_at = datetime.now(UTC)

        mock_session.execute.return_value = create_mock_result([row])

        result = await store.get_snapshot(aggregate_id, "TestAggregate")

        assert result is not None
        assert result.state == {"value": "from_json", "count": 10}

    @pytest.mark.asyncio
    async def test_delete_snapshot_exists(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test deleting an existing snapshot."""
        mock_session.execute.return_value = create_mock_result(rowcount=1)

        result = await store.delete_snapshot(aggregate_id, "TestAggregate")

        assert result is True
        mock_session.execute.assert_called_once()
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "DELETE FROM snapshots" in sql_text

    @pytest.mark.asyncio
    async def test_delete_snapshot_not_exists(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test deleting a non-existent snapshot returns False."""
        mock_session.execute.return_value = create_mock_result(rowcount=0)

        result = await store.delete_snapshot(aggregate_id, "TestAggregate")

        assert result is False

    @pytest.mark.asyncio
    async def test_snapshot_exists_true(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test snapshot_exists returns True when snapshot exists."""
        mock_session.execute.return_value = create_scalar_result(True)

        result = await store.snapshot_exists(aggregate_id, "TestAggregate")

        assert result is True
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "SELECT EXISTS" in sql_text

    @pytest.mark.asyncio
    async def test_snapshot_exists_false(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test snapshot_exists returns False when snapshot doesn't exist."""
        mock_session.execute.return_value = create_scalar_result(False)

        result = await store.snapshot_exists(aggregate_id, "TestAggregate")

        assert result is False


# --- Upsert Tests ---


class TestUpsertSemantics:
    """Tests for upsert (INSERT ON CONFLICT) behavior."""

    @pytest.mark.asyncio
    async def test_save_overwrites_existing(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that saving a snapshot overwrites existing one."""
        # First save
        snapshot1 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            version=5,
            schema_version=1,
            state={"value": "first"},
            created_at=datetime.now(UTC),
        )
        mock_session.execute.return_value = create_mock_result()
        await store.save_snapshot(snapshot1)

        # Second save with same aggregate_id and type
        snapshot2 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="TestAggregate",
            version=10,
            schema_version=1,
            state={"value": "second"},
            created_at=datetime.now(UTC),
        )
        await store.save_snapshot(snapshot2)

        # Verify both used upsert SQL
        assert mock_session.execute.call_count == 2
        for call in mock_session.execute.call_args_list:
            sql_text = str(call[0][0])
            assert "ON CONFLICT" in sql_text

    @pytest.mark.asyncio
    async def test_different_aggregate_types_are_separate(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test snapshots for different aggregate types don't conflict."""
        mock_session.execute.return_value = create_mock_result()

        # Save snapshot for "Order" type
        snapshot1 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=5,
            schema_version=1,
            state={"status": "pending"},
            created_at=datetime.now(UTC),
        )
        await store.save_snapshot(snapshot1)

        # Save snapshot for "User" type with same aggregate_id
        snapshot2 = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="User",
            version=3,
            schema_version=1,
            state={"name": "test"},
            created_at=datetime.now(UTC),
        )
        await store.save_snapshot(snapshot2)

        # Both should be separate inserts (different aggregate_type)
        assert mock_session.execute.call_count == 2


# --- Bulk Delete Tests ---


class TestBulkDelete:
    """Tests for delete_snapshots_by_type."""

    @pytest.mark.asyncio
    async def test_delete_by_type(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test deleting all snapshots of a type."""
        mock_session.execute.return_value = create_mock_result(rowcount=5)

        count = await store.delete_snapshots_by_type("TestAggregate")

        assert count == 5
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "DELETE FROM snapshots" in sql_text
        assert "aggregate_type = :aggregate_type" in sql_text
        assert "schema_version" not in sql_text

    @pytest.mark.asyncio
    async def test_delete_by_type_with_schema_version_filter(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test deleting snapshots by type with schema version filter."""
        mock_session.execute.return_value = create_mock_result(rowcount=3)

        count = await store.delete_snapshots_by_type(
            "TestAggregate",
            schema_version_below=2,
        )

        assert count == 3
        sql_text = str(mock_session.execute.call_args[0][0])
        assert "DELETE FROM snapshots" in sql_text
        assert "schema_version < :schema_version_below" in sql_text

    @pytest.mark.asyncio
    async def test_delete_by_type_no_matches(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
    ) -> None:
        """Test deleting when no snapshots match returns 0."""
        mock_session.execute.return_value = create_mock_result(rowcount=0)

        count = await store.delete_snapshots_by_type("NonExistentType")

        assert count == 0


# --- Properties Tests ---


class TestProperties:
    """Tests for store properties."""

    def test_session_factory_property(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session_factory: MagicMock,
    ) -> None:
        """Test session_factory property returns the factory."""
        assert store.session_factory == mock_session_factory

    def test_repr_without_tracing(
        self,
        store: PostgreSQLSnapshotStore,
    ) -> None:
        """Test __repr__ without tracing."""
        assert "PostgreSQLSnapshotStore" in repr(store)
        assert "tracing=disabled" in repr(store)

    def test_repr_with_tracing(
        self,
        mock_session_factory: MagicMock,
    ) -> None:
        """Test __repr__ with tracing enabled."""
        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            enable_tracing=True,
        )
        # Note: tracing_enabled will be True only if OTEL is available
        # The repr should reflect _enable_tracing, which is set to enable_tracing && OTEL_AVAILABLE
        assert "PostgreSQLSnapshotStore" in repr(store)


# --- OpenTelemetry Tracing Tests ---


class TestOpenTelemetryTracing:
    """Tests for optional OpenTelemetry tracing using TracingMixin."""

    @pytest.mark.asyncio
    async def test_save_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        """Test that save_snapshot creates a span when tracing is enabled."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_span_context = MagicMock()
        mock_span_context.__enter__ = MagicMock(return_value=mock_span)
        mock_span_context.__exit__ = MagicMock(return_value=None)
        mock_tracer.start_as_current_span.return_value = mock_span_context

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            enable_tracing=True,
        )
        # Inject mock tracer
        store._tracer = mock_tracer
        store._enable_tracing = True
        mock_session.execute.return_value = create_mock_result()

        await store.save_snapshot(sample_snapshot)

        mock_tracer.start_as_current_span.assert_called_once()
        call_kwargs = mock_tracer.start_as_current_span.call_args
        assert call_kwargs[0][0] == "eventsource.snapshot.save"
        assert "eventsource.aggregate.id" in call_kwargs[1]["attributes"]

    @pytest.mark.asyncio
    async def test_get_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
        sample_snapshot: Snapshot,
    ) -> None:
        """Test that get_snapshot creates a span when tracing is enabled."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_span_context = MagicMock()
        mock_span_context.__enter__ = MagicMock(return_value=mock_span)
        mock_span_context.__exit__ = MagicMock(return_value=None)
        mock_tracer.start_as_current_span.return_value = mock_span_context

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            enable_tracing=True,
        )
        # Inject mock tracer
        store._tracer = mock_tracer
        store._enable_tracing = True
        row = create_snapshot_row(sample_snapshot)
        mock_session.execute.return_value = create_mock_result([row])

        await store.get_snapshot(
            sample_snapshot.aggregate_id,
            sample_snapshot.aggregate_type,
        )

        mock_tracer.start_as_current_span.assert_called_once()
        call_kwargs = mock_tracer.start_as_current_span.call_args
        assert call_kwargs[0][0] == "eventsource.snapshot.get"
        assert "eventsource.aggregate.id" in call_kwargs[1]["attributes"]

    @pytest.mark.asyncio
    async def test_delete_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that delete_snapshot creates a span when tracing is enabled."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_span_context = MagicMock()
        mock_span_context.__enter__ = MagicMock(return_value=mock_span)
        mock_span_context.__exit__ = MagicMock(return_value=None)
        mock_tracer.start_as_current_span.return_value = mock_span_context

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            enable_tracing=True,
        )
        # Inject mock tracer
        store._tracer = mock_tracer
        store._enable_tracing = True
        mock_session.execute.return_value = create_mock_result(rowcount=1)

        await store.delete_snapshot(aggregate_id, "TestAggregate")

        mock_tracer.start_as_current_span.assert_called_once()
        call_kwargs = mock_tracer.start_as_current_span.call_args
        assert call_kwargs[0][0] == "eventsource.snapshot.delete"
        assert "eventsource.aggregate.id" in call_kwargs[1]["attributes"]

    @pytest.mark.asyncio
    async def test_exists_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test that snapshot_exists creates a span when tracing is enabled."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_span_context = MagicMock()
        mock_span_context.__enter__ = MagicMock(return_value=mock_span)
        mock_span_context.__exit__ = MagicMock(return_value=None)
        mock_tracer.start_as_current_span.return_value = mock_span_context

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            enable_tracing=True,
        )
        # Inject mock tracer
        store._tracer = mock_tracer
        store._enable_tracing = True
        mock_session.execute.return_value = create_scalar_result(True)

        await store.snapshot_exists(aggregate_id, "TestAggregate")

        mock_tracer.start_as_current_span.assert_called_once()
        call_kwargs = mock_tracer.start_as_current_span.call_args
        assert call_kwargs[0][0] == "eventsource.snapshot.exists"
        assert "eventsource.aggregate.id" in call_kwargs[1]["attributes"]

    @pytest.mark.asyncio
    async def test_delete_by_type_creates_span_when_tracing_enabled(
        self,
        mock_session_factory: MagicMock,
        mock_session: AsyncMock,
    ) -> None:
        """Test that delete_snapshots_by_type creates a span when tracing is enabled."""
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_span_context = MagicMock()
        mock_span_context.__enter__ = MagicMock(return_value=mock_span)
        mock_span_context.__exit__ = MagicMock(return_value=None)
        mock_tracer.start_as_current_span.return_value = mock_span_context

        store = PostgreSQLSnapshotStore(
            session_factory=mock_session_factory,
            enable_tracing=True,
        )
        # Inject mock tracer
        store._tracer = mock_tracer
        store._enable_tracing = True
        mock_session.execute.return_value = create_mock_result(rowcount=5)

        await store.delete_snapshots_by_type("TestAggregate", schema_version_below=2)

        mock_tracer.start_as_current_span.assert_called_once()
        call_kwargs = mock_tracer.start_as_current_span.call_args
        assert call_kwargs[0][0] == "eventsource.snapshot.delete_by_type"
        assert "eventsource.aggregate.type" in call_kwargs[1]["attributes"]

    def test_no_span_created_when_tracing_disabled(
        self,
        store: PostgreSQLSnapshotStore,
    ) -> None:
        """Test that no tracing occurs when tracing is disabled."""
        # Store was created with enable_tracing=False
        assert store._enable_tracing is False
        assert store._tracer is None


# --- Import Tests ---


class TestImports:
    """Tests for verifying correct module exports."""

    def test_import_from_snapshots_module(self) -> None:
        """Test importing PostgreSQLSnapshotStore from snapshots module."""
        from eventsource.snapshots import PostgreSQLSnapshotStore

        assert PostgreSQLSnapshotStore is not None

    def test_import_from_main_module(self) -> None:
        """Test importing from main eventsource module."""
        # Note: PostgreSQLSnapshotStore may not be in main exports
        # This test verifies the snapshots module is accessible
        from eventsource import snapshots

        assert hasattr(snapshots, "PostgreSQLSnapshotStore")


# --- Complex State Tests ---


class TestComplexState:
    """Tests for handling complex state objects."""

    @pytest.mark.asyncio
    async def test_save_complex_nested_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test saving snapshot with deeply nested state."""
        complex_state = {
            "user": {
                "name": "John",
                "address": {
                    "street": "123 Main St",
                    "city": "Test City",
                },
            },
            "items": [
                {"id": 1, "name": "Item 1"},
                {"id": 2, "name": "Item 2"},
            ],
            "metadata": {
                "created_by": "system",
                "tags": ["important", "verified"],
            },
        }
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="ComplexAggregate",
            version=1,
            schema_version=1,
            state=complex_state,
            created_at=datetime.now(UTC),
        )
        mock_session.execute.return_value = create_mock_result()

        await store.save_snapshot(snapshot)

        params = mock_session.execute.call_args[0][1]
        # Verify JSON encoding preserved structure
        import json

        decoded = json.loads(params["state"])
        assert decoded == complex_state

    @pytest.mark.asyncio
    async def test_get_complex_nested_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test retrieving snapshot with deeply nested state."""
        complex_state = {
            "orders": [{"id": 1, "total": 100.50}, {"id": 2, "total": 200.00}],
            "settings": {"notifications": True},
        }
        row = MagicMock()
        row.aggregate_id = aggregate_id
        row.aggregate_type = "ComplexAggregate"
        row.version = 5
        row.schema_version = 1
        row.state = complex_state  # Dict state
        row.created_at = datetime.now(UTC)

        mock_session.execute.return_value = create_mock_result([row])

        result = await store.get_snapshot(aggregate_id, "ComplexAggregate")

        assert result is not None
        assert result.state == complex_state
        assert result.state["orders"][0]["total"] == 100.50

    @pytest.mark.asyncio
    async def test_empty_state(
        self,
        store: PostgreSQLSnapshotStore,
        mock_session: AsyncMock,
        aggregate_id: UUID,
    ) -> None:
        """Test handling empty state dict."""
        snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="EmptyAggregate",
            version=1,
            schema_version=1,
            state={},
            created_at=datetime.now(UTC),
        )
        mock_session.execute.return_value = create_mock_result()

        await store.save_snapshot(snapshot)

        params = mock_session.execute.call_args[0][1]
        assert params["state"] == "{}"
