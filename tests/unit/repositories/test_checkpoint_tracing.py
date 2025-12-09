"""
Unit tests for CheckpointRepository tracing functionality.

Tests for:
- TracingMixin integration for all checkpoint repository implementations
- Span creation for get_checkpoint, update_checkpoint, get_lag_metrics, reset_checkpoint
- Correct span attributes using standard ATTR_* constants
- Tracing disabled behavior
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, Mock
from uuid import uuid4

import pytest

from eventsource.observability import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
    TracingMixin,
)
from eventsource.repositories.checkpoint import (
    InMemoryCheckpointRepository,
)

# ============================================================================
# TracingMixin Integration Tests - InMemoryCheckpointRepository
# ============================================================================


class TestInMemoryCheckpointRepositoryTracingMixin:
    """Tests for InMemoryCheckpointRepository TracingMixin integration."""

    def test_inherits_from_tracing_mixin(self):
        """InMemoryCheckpointRepository inherits from TracingMixin."""
        assert issubclass(InMemoryCheckpointRepository, TracingMixin)

    def test_tracing_enabled_by_default(self):
        """Tracing is enabled by default when OTEL is available."""
        repo = InMemoryCheckpointRepository()

        # Check that tracing was initialized
        assert hasattr(repo, "_enable_tracing")
        assert hasattr(repo, "_tracer")

    def test_tracing_disabled_when_requested(self):
        """Tracing can be disabled via constructor parameter."""
        repo = InMemoryCheckpointRepository(enable_tracing=False)

        assert repo._enable_tracing is False
        assert repo._tracer is None

    def test_has_tracing_enabled_property(self):
        """Store exposes tracing_enabled property from mixin."""
        repo = InMemoryCheckpointRepository(enable_tracing=True)

        # tracing_enabled is a property from TracingMixin
        assert hasattr(repo, "tracing_enabled")
        assert isinstance(repo.tracing_enabled, bool)

    def test_backward_compatible_constructor(self):
        """Constructor without enable_tracing should work (default True)."""
        repo = InMemoryCheckpointRepository()
        # Should not raise, tracing defaults to enabled
        assert hasattr(repo, "_enable_tracing")


# ============================================================================
# Span Creation Tests - InMemoryCheckpointRepository
# ============================================================================


class TestInMemoryCheckpointRepositorySpanCreation:
    """Tests for span creation in InMemoryCheckpointRepository operations."""

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer.start_as_current_span.return_value = span
        return tracer

    @pytest.fixture
    def traced_repo(self, mock_tracer):
        """Create a repository with injected mock tracer."""
        repo = InMemoryCheckpointRepository(enable_tracing=True)
        # Inject mock tracer
        repo._tracer = mock_tracer
        repo._enable_tracing = True
        return repo

    @pytest.mark.asyncio
    async def test_get_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """get_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.get_checkpoint(projection_name)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.get_checkpoint"

    @pytest.mark.asyncio
    async def test_get_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """get_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"

        await traced_repo.get_checkpoint(projection_name)

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_update_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """update_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await traced_repo.update_checkpoint(projection_name, event_id, event_type)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.update_checkpoint"

    @pytest.mark.asyncio
    async def test_update_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """update_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await traced_repo.update_checkpoint(projection_name, event_id, event_type)

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name
        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == event_type

    @pytest.mark.asyncio
    async def test_get_lag_metrics_creates_span(self, traced_repo, mock_tracer):
        """get_lag_metrics creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.get_lag_metrics(projection_name)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.get_lag_metrics"

    @pytest.mark.asyncio
    async def test_get_lag_metrics_span_attributes(self, traced_repo, mock_tracer):
        """get_lag_metrics span includes correct standard attributes."""
        projection_name = "TestProjection"

        await traced_repo.get_lag_metrics(projection_name)

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_reset_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """reset_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.reset_checkpoint(projection_name)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.reset_checkpoint"

    @pytest.mark.asyncio
    async def test_reset_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """reset_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"

        await traced_repo.reset_checkpoint(projection_name)

        # Verify correct attributes using standard constants
        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_creates_span(self, traced_repo, mock_tracer):
        """get_all_checkpoints creates a span with correct name."""
        await traced_repo.get_all_checkpoints()

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.get_all_checkpoints"

    @pytest.mark.asyncio
    async def test_clear_creates_span(self, traced_repo, mock_tracer):
        """clear creates a span with correct name."""
        await traced_repo.clear()

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.clear"


# ============================================================================
# Tracing Disabled Tests - InMemoryCheckpointRepository
# ============================================================================


class TestInMemoryCheckpointRepositoryTracingDisabled:
    """Tests for InMemoryCheckpointRepository behavior when tracing is disabled."""

    @pytest.mark.asyncio
    async def test_get_checkpoint_works_without_tracing(self):
        """get_checkpoint works correctly when tracing is disabled."""
        repo = InMemoryCheckpointRepository(enable_tracing=False)

        result = await repo.get_checkpoint("TestProjection")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_works_without_tracing(self):
        """update_checkpoint works correctly when tracing is disabled."""
        repo = InMemoryCheckpointRepository(enable_tracing=False)

        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await repo.update_checkpoint(projection_name, event_id, event_type)

        result = await repo.get_checkpoint(projection_name)
        assert result == event_id

    @pytest.mark.asyncio
    async def test_get_lag_metrics_works_without_tracing(self):
        """get_lag_metrics works correctly when tracing is disabled."""
        repo = InMemoryCheckpointRepository(enable_tracing=False)

        projection_name = "TestProjection"
        event_id = uuid4()
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        result = await repo.get_lag_metrics(projection_name)
        assert result is not None
        assert result.projection_name == projection_name

    @pytest.mark.asyncio
    async def test_reset_checkpoint_works_without_tracing(self):
        """reset_checkpoint works correctly when tracing is disabled."""
        repo = InMemoryCheckpointRepository(enable_tracing=False)

        projection_name = "TestProjection"
        event_id = uuid4()
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        await repo.reset_checkpoint(projection_name)

        result = await repo.get_checkpoint(projection_name)
        assert result is None

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_works_without_tracing(self):
        """get_all_checkpoints works correctly when tracing is disabled."""
        repo = InMemoryCheckpointRepository(enable_tracing=False)

        result = await repo.get_all_checkpoints()
        assert result == []

    @pytest.mark.asyncio
    async def test_clear_works_without_tracing(self):
        """clear works correctly when tracing is disabled."""
        repo = InMemoryCheckpointRepository(enable_tracing=False)

        projection_name = "TestProjection"
        event_id = uuid4()
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        await repo.clear()

        result = await repo.get_all_checkpoints()
        assert result == []


# ============================================================================
# SQLite Checkpoint Repository Tracing Tests
# ============================================================================

# Check if aiosqlite is available
try:
    import aiosqlite

    from eventsource.repositories.checkpoint import SQLiteCheckpointRepository

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    SQLiteCheckpointRepository = None  # type: ignore[assignment,misc]
    AIOSQLITE_AVAILABLE = False


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteCheckpointRepositoryTracingMixin:
    """Tests for SQLiteCheckpointRepository TracingMixin integration."""

    @pytest.fixture
    async def db_connection(self) -> aiosqlite.Connection:
        """Create an in-memory SQLite database with schema for each test."""
        conn = await aiosqlite.connect(":memory:")

        # Create the projection_checkpoints table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS projection_checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_event_id TEXT,
                last_event_type TEXT,
                last_processed_at TEXT,
                events_processed INTEGER NOT NULL DEFAULT 0,
                global_position INTEGER,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        # Create the events table for lag metrics tests
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                tenant_id TEXT,
                actor_id TEXT,
                version INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        await conn.commit()

        yield conn

        await conn.close()

    def test_inherits_from_tracing_mixin(self, db_connection: aiosqlite.Connection):
        """SQLiteCheckpointRepository inherits from TracingMixin."""
        assert issubclass(SQLiteCheckpointRepository, TracingMixin)

    def test_tracing_enabled_by_default(self, db_connection: aiosqlite.Connection):
        """Tracing is enabled by default when OTEL is available."""
        repo = SQLiteCheckpointRepository(db_connection)

        # Check that tracing was initialized
        assert hasattr(repo, "_enable_tracing")
        assert hasattr(repo, "_tracer")

    def test_tracing_disabled_when_requested(self, db_connection: aiosqlite.Connection):
        """Tracing can be disabled via constructor parameter."""
        repo = SQLiteCheckpointRepository(db_connection, enable_tracing=False)

        assert repo._enable_tracing is False
        assert repo._tracer is None

    def test_has_tracing_enabled_property(self, db_connection: aiosqlite.Connection):
        """Store exposes tracing_enabled property from mixin."""
        repo = SQLiteCheckpointRepository(db_connection, enable_tracing=True)

        # tracing_enabled is a property from TracingMixin
        assert hasattr(repo, "tracing_enabled")
        assert isinstance(repo.tracing_enabled, bool)


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteCheckpointRepositorySpanCreation:
    """Tests for span creation in SQLiteCheckpointRepository operations."""

    @pytest.fixture
    async def db_connection(self) -> aiosqlite.Connection:
        """Create an in-memory SQLite database with schema for each test."""
        conn = await aiosqlite.connect(":memory:")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS projection_checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_event_id TEXT,
                last_event_type TEXT,
                last_processed_at TEXT,
                events_processed INTEGER NOT NULL DEFAULT 0,
                global_position INTEGER,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL UNIQUE,
                event_type TEXT NOT NULL,
                aggregate_type TEXT NOT NULL,
                aggregate_id TEXT NOT NULL,
                tenant_id TEXT,
                actor_id TEXT,
                version INTEGER NOT NULL,
                timestamp TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        await conn.commit()

        yield conn

        await conn.close()

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer with span context manager."""
        tracer = Mock()
        span = MagicMock()
        span.__enter__ = Mock(return_value=span)
        span.__exit__ = Mock(return_value=None)
        tracer.start_as_current_span.return_value = span
        return tracer

    @pytest.fixture
    def traced_repo(self, db_connection: aiosqlite.Connection, mock_tracer):
        """Create a repository with injected mock tracer."""
        repo = SQLiteCheckpointRepository(db_connection, enable_tracing=True)
        # Inject mock tracer
        repo._tracer = mock_tracer
        repo._enable_tracing = True
        return repo

    @pytest.mark.asyncio
    async def test_get_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """get_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.get_checkpoint(projection_name)

        # Verify span was created with correct name
        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.get_checkpoint"

    @pytest.mark.asyncio
    async def test_get_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """get_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"

        await traced_repo.get_checkpoint(projection_name)

        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_update_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """update_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await traced_repo.update_checkpoint(projection_name, event_id, event_type)

        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.update_checkpoint"

    @pytest.mark.asyncio
    async def test_update_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """update_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await traced_repo.update_checkpoint(projection_name, event_id, event_type)

        call_args = mock_tracer.start_as_current_span.call_args
        attributes = call_args[1]["attributes"]

        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name
        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == event_type

    @pytest.mark.asyncio
    async def test_get_lag_metrics_creates_span(self, traced_repo, mock_tracer):
        """get_lag_metrics creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.get_lag_metrics(projection_name)

        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.get_lag_metrics"

    @pytest.mark.asyncio
    async def test_reset_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """reset_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.reset_checkpoint(projection_name)

        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.reset_checkpoint"

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_creates_span(self, traced_repo, mock_tracer):
        """get_all_checkpoints creates a span with correct name."""
        await traced_repo.get_all_checkpoints()

        mock_tracer.start_as_current_span.assert_called()
        call_args = mock_tracer.start_as_current_span.call_args
        assert call_args[0][0] == "eventsource.checkpoint.get_all_checkpoints"


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLiteCheckpointRepositoryTracingDisabled:
    """Tests for SQLiteCheckpointRepository behavior when tracing is disabled."""

    @pytest.fixture
    async def db_connection(self) -> aiosqlite.Connection:
        """Create an in-memory SQLite database with schema for each test."""
        conn = await aiosqlite.connect(":memory:")

        await conn.execute("""
            CREATE TABLE IF NOT EXISTS projection_checkpoints (
                projection_name TEXT PRIMARY KEY,
                last_event_id TEXT,
                last_event_type TEXT,
                last_processed_at TEXT,
                events_processed INTEGER NOT NULL DEFAULT 0,
                global_position INTEGER,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)

        await conn.commit()

        yield conn

        await conn.close()

    @pytest.mark.asyncio
    async def test_get_checkpoint_works_without_tracing(self, db_connection: aiosqlite.Connection):
        """get_checkpoint works correctly when tracing is disabled."""
        repo = SQLiteCheckpointRepository(db_connection, enable_tracing=False)

        result = await repo.get_checkpoint("TestProjection")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_works_without_tracing(
        self, db_connection: aiosqlite.Connection
    ):
        """update_checkpoint works correctly when tracing is disabled."""
        repo = SQLiteCheckpointRepository(db_connection, enable_tracing=False)

        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await repo.update_checkpoint(projection_name, event_id, event_type)

        result = await repo.get_checkpoint(projection_name)
        assert result == event_id

    @pytest.mark.asyncio
    async def test_reset_checkpoint_works_without_tracing(
        self, db_connection: aiosqlite.Connection
    ):
        """reset_checkpoint works correctly when tracing is disabled."""
        repo = SQLiteCheckpointRepository(db_connection, enable_tracing=False)

        projection_name = "TestProjection"
        event_id = uuid4()
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")

        await repo.reset_checkpoint(projection_name)

        result = await repo.get_checkpoint(projection_name)
        assert result is None


# ============================================================================
# Standard Attributes Tests
# ============================================================================


class TestCheckpointRepositoryStandardAttributes:
    """Tests for standard attribute usage in CheckpointRepository implementations."""

    def test_uses_standard_attribute_constants(self):
        """Verify CheckpointRepository implementations use ATTR_* constants."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/repositories/checkpoint.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 10, f"Expected at least 10 ATTR_* usages, found {count}"

    def test_no_duplicate_otel_available(self):
        """Verify no duplicate OTEL_AVAILABLE definition in checkpoint.py."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "OTEL_AVAILABLE = ", "src/eventsource/repositories/checkpoint.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be 0 - no local definition
        assert result.stdout.strip() == "0", (
            f"Found {result.stdout.strip()} definitions of OTEL_AVAILABLE in checkpoint.py"
        )

    def test_imports_from_observability_module(self):
        """Verify checkpoint.py imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/repositories/checkpoint.py",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "checkpoint.py should import from eventsource.observability"
