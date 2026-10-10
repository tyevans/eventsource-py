"""InMemoryCheckpointRepository tracing tests."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.observability import (
    ATTR_EVENT_TYPE,
    ATTR_PROJECTION_NAME,
)
from eventsource.observability.tracer import MockTracer, NullTracer


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
