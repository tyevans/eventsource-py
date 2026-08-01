"""
Unit tests for CheckpointRepository tracing functionality.

Tests for:
- Tracer protocol integration for all checkpoint repository implementations
- Span creation for get_checkpoint, update_checkpoint, get_lag_metrics, reset_checkpoint
- Correct span attributes using standard ATTR_* constants
- Tracing disabled behavior
"""

from __future__ import annotations

from pathlib import Path
from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.observability import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.observability.tracer import MockTracer, NullTracer

# ============================================================================
# Tracer Protocol Integration Tests - InMemoryCheckpointRepository
# ============================================================================


class TestInMemoryCheckpointRepositoryTracerIntegration:
    """Tests for InMemoryCheckpointRepository Tracer protocol integration."""

    def test_accepts_custom_tracer(self):
        """InMemoryCheckpointRepository accepts a custom tracer."""
        tracer = MockTracer()
        repo = InMemoryCheckpointRepository(tracer=tracer)

        assert repo._tracer is tracer

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
        assert isinstance(repo._tracer, NullTracer)

    def test_tracer_enabled_property(self):
        """Store exposes tracer.enabled property."""
        repo = InMemoryCheckpointRepository(enable_tracing=False)

        # tracer.enabled reflects the tracer state
        assert repo._tracer.enabled is False

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
        """Create a mock tracer that records span calls."""
        return MockTracer()

    @pytest.fixture
    def traced_repo(self, mock_tracer):
        """Create a repository with injected mock tracer."""
        return InMemoryCheckpointRepository(tracer=mock_tracer)

    @pytest.mark.asyncio
    async def test_get_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """get_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.get_checkpoint(projection_name)

        # Verify span was created with correct name
        assert "eventsource.checkpoint.get_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_get_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """get_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"

        await traced_repo.get_checkpoint(projection_name)

        # Find the span and verify attributes
        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.get_checkpoint"
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
        assert "eventsource.checkpoint.update_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_update_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """update_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await traced_repo.update_checkpoint(projection_name, event_id, event_type)

        # Find the span and verify attributes
        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.update_checkpoint"
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
        assert "eventsource.checkpoint.get_lag_metrics" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_get_lag_metrics_span_attributes(self, traced_repo, mock_tracer):
        """get_lag_metrics span includes correct standard attributes."""
        projection_name = "TestProjection"

        await traced_repo.get_lag_metrics(projection_name)

        # Find the span and verify attributes
        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.get_lag_metrics"
        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_reset_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """reset_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.reset_checkpoint(projection_name)

        # Verify span was created with correct name
        assert "eventsource.checkpoint.reset_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_reset_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """reset_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"

        await traced_repo.reset_checkpoint(projection_name)

        # Find the span and verify attributes
        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.reset_checkpoint"
        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_creates_span(self, traced_repo, mock_tracer):
        """get_all_checkpoints creates a span with correct name."""
        await traced_repo.get_all_checkpoints()

        # Verify span was created with correct name
        assert "eventsource.checkpoint.get_all_checkpoints" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_clear_creates_span(self, traced_repo, mock_tracer):
        """clear creates a span with correct name."""
        await traced_repo.clear()

        # Verify span was created with correct name
        assert "eventsource.checkpoint.clear" in mock_tracer.span_names


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
# SQL Checkpoint Repository Tracing Tests
# ============================================================================

# Check if aiosqlite is available
try:
    import aiosqlite

    from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    SQLCheckpointRepository = None  # type: ignore[assignment,misc]
    AIOSQLITE_AVAILABLE = False


async def _sqlite_checkpoint_engine(tmp_path, *, with_events: bool = True):
    """Build a SQLite engine with the checkpoints (and optionally events) schema."""
    from eventsource import create_async_engine
    from eventsource.adapters.sql.schemas import get_schema

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/tracing.db")
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
        if with_events:
            await raw.driver_connection.executescript(get_schema("events", backend="sqlite"))
    return engine


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLCheckpointRepositoryTracerIntegration:
    """Tests for SQLCheckpointRepository Tracer protocol integration."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        """Create a SQLite engine with schema for each test."""
        engine = await _sqlite_checkpoint_engine(tmp_path)
        yield engine
        await engine.dispose()

    def test_accepts_custom_tracer(self, sqlite_engine):
        """SQLCheckpointRepository accepts a custom tracer."""
        tracer = MockTracer()
        repo = SQLCheckpointRepository(sqlite_engine, tracer=tracer)

        assert repo._tracer is tracer

    def test_tracing_enabled_by_default(self, sqlite_engine):
        """Tracing is enabled by default when OTEL is available."""
        repo = SQLCheckpointRepository(sqlite_engine)

        # `enable_tracing`'s actual default (True) must be honored, not just
        # be present as an attribute -- a NullTracer would also satisfy
        # hasattr() but is the opposite of "enabled by default".
        assert repo._enable_tracing is True
        assert not isinstance(repo._tracer, NullTracer)

    def test_tracing_disabled_when_requested(self, sqlite_engine):
        """Tracing can be disabled via constructor parameter."""
        repo = SQLCheckpointRepository(sqlite_engine, enable_tracing=False)

        assert repo._enable_tracing is False
        assert isinstance(repo._tracer, NullTracer)

    def test_tracer_enabled_property(self, sqlite_engine):
        """Store exposes tracer.enabled property."""
        repo = SQLCheckpointRepository(sqlite_engine, enable_tracing=False)

        # tracer.enabled reflects the tracer state
        assert repo._tracer.enabled is False

    def test_conn_backward_compat_attribute_is_the_constructor_arg(self, sqlite_engine):
        """`repo.conn` is kept for backwards-compatible attribute access and
        must be the exact object passed to the constructor, not None or a
        copy."""
        repo = SQLCheckpointRepository(sqlite_engine)

        assert repo.conn is sqlite_engine


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLCheckpointRepositorySpanCreation:
    """Tests for span creation in SQLCheckpointRepository operations."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        """Create a SQLite engine with schema for each test."""
        engine = await _sqlite_checkpoint_engine(tmp_path)
        yield engine
        await engine.dispose()

    @pytest.fixture
    def mock_tracer(self):
        """Create a mock tracer that records span calls."""
        return MockTracer()

    @pytest.fixture
    def traced_repo(self, sqlite_engine, mock_tracer):
        """Create a repository with injected mock tracer."""
        return SQLCheckpointRepository(sqlite_engine, tracer=mock_tracer)

    @pytest.mark.asyncio
    async def test_get_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """get_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.get_checkpoint(projection_name)

        # Verify span was created with correct name
        assert "eventsource.checkpoint.get_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_get_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """get_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"

        await traced_repo.get_checkpoint(projection_name)

        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.get_checkpoint"
        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_update_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """update_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await traced_repo.update_checkpoint(projection_name, event_id, event_type)

        assert "eventsource.checkpoint.update_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_update_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        """update_checkpoint span includes correct standard attributes."""
        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await traced_repo.update_checkpoint(projection_name, event_id, event_type)

        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.update_checkpoint"
        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name
        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == event_type

    @pytest.mark.asyncio
    async def test_get_lag_metrics_creates_span(self, traced_repo, mock_tracer):
        """get_lag_metrics creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.get_lag_metrics(projection_name)

        assert "eventsource.checkpoint.get_lag_metrics" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_reset_checkpoint_creates_span(self, traced_repo, mock_tracer):
        """reset_checkpoint creates a span with correct name."""
        projection_name = "TestProjection"

        await traced_repo.reset_checkpoint(projection_name)

        assert "eventsource.checkpoint.reset_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_creates_span(self, traced_repo, mock_tracer):
        """get_all_checkpoints creates a span with correct name."""
        await traced_repo.get_all_checkpoints()

        assert "eventsource.checkpoint.get_all_checkpoints" in mock_tracer.span_names


@pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
class TestSQLCheckpointRepositoryTracingDisabled:
    """Tests for SQLCheckpointRepository behavior when tracing is disabled."""

    @pytest.fixture
    async def sqlite_engine(self, tmp_path):
        """Create a SQLite engine with schema for each test."""
        engine = await _sqlite_checkpoint_engine(tmp_path, with_events=False)
        yield engine
        await engine.dispose()

    @pytest.mark.asyncio
    async def test_get_checkpoint_works_without_tracing(self, sqlite_engine):
        """get_checkpoint works correctly when tracing is disabled."""
        repo = SQLCheckpointRepository(sqlite_engine, enable_tracing=False)

        result = await repo.get_checkpoint("TestProjection")
        assert result is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_works_without_tracing(self, sqlite_engine):
        """update_checkpoint works correctly when tracing is disabled."""
        repo = SQLCheckpointRepository(sqlite_engine, enable_tracing=False)

        projection_name = "TestProjection"
        event_id = uuid4()
        event_type = "TestEvent"

        await repo.update_checkpoint(projection_name, event_id, event_type)

        result = await repo.get_checkpoint(projection_name)
        assert result == event_id

    @pytest.mark.asyncio
    async def test_reset_checkpoint_works_without_tracing(self, sqlite_engine):
        """reset_checkpoint works correctly when tracing is disabled."""
        repo = SQLCheckpointRepository(sqlite_engine, enable_tracing=False)

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
            ["grep", "-c", "ATTR_", "src/eventsource/adapters/sql/checkpoints.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should find multiple ATTR_* usages
        count = int(result.stdout.strip())
        assert count >= 10, f"Expected at least 10 ATTR_* usages, found {count}"

    def test_no_duplicate_otel_available(self):
        """Verify no duplicate OTEL_AVAILABLE definition in checkpoints.py."""
        import subprocess

        result = subprocess.run(
            ["grep", "-c", "OTEL_AVAILABLE = ", "src/eventsource/adapters/sql/checkpoints.py"],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be 0 - no local definition
        assert result.stdout.strip() == "0", (
            f"Found {result.stdout.strip()} definitions of OTEL_AVAILABLE in checkpoints.py"
        )

    def test_imports_from_observability_module(self):
        """Verify checkpoints.py imports tracing from observability module."""
        import subprocess

        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/adapters/sql/checkpoints.py",
            ],
            capture_output=True,
            text=True,
            cwd=Path(__file__).parents[3],
        )
        # Should be at least 1 - imports from observability
        count = int(result.stdout.strip())
        assert count >= 1, "checkpoints.py should import from eventsource.observability"
