"""SQLCheckpointRepository tracing tests."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.observability import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.observability.tracer import MockTracer, NullTracer

from .test_sql_checkpoint_tracing_engine import _sqlite_checkpoint_engine

# Check if aiosqlite is available
try:
    import aiosqlite

    from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]
    SQLCheckpointRepository = None  # type: ignore[assignment,misc]
    AIOSQLITE_AVAILABLE = False


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

        assert repo._tracer.enabled is False


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
