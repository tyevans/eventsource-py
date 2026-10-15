"""InMemoryCheckpointRepository tracing tests."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.observability import ATTR_EVENT_TYPE, ATTR_PROJECTION_NAME
from eventsource.observability.tracer import MockTracer, NullTracer


class TestInMemoryCheckpointRepositoryTracerIntegration:
    def test_accepts_custom_tracer(self):
        tracer = MockTracer()
        repo = InMemoryCheckpointRepository(tracer=tracer)
        assert repo._tracer is tracer

    def test_tracing_enabled_by_default(self):
        repo = InMemoryCheckpointRepository()
        assert hasattr(repo, "_enable_tracing")
        assert hasattr(repo, "_tracer")

    def test_tracing_disabled_when_requested(self):
        repo = InMemoryCheckpointRepository(enable_tracing=False)
        assert repo._enable_tracing is False
        assert isinstance(repo._tracer, NullTracer)

    def test_tracer_enabled_property(self):
        repo = InMemoryCheckpointRepository(enable_tracing=False)
        assert repo._tracer.enabled is False

    def test_backward_compatible_constructor(self):
        repo = InMemoryCheckpointRepository()
        assert hasattr(repo, "_enable_tracing")


class TestInMemoryCheckpointRepositorySpanCreation:
    @pytest.fixture
    def mock_tracer(self):
        return MockTracer()

    @pytest.fixture
    def traced_repo(self, mock_tracer):
        return InMemoryCheckpointRepository(tracer=mock_tracer)

    @pytest.mark.asyncio
    async def test_get_checkpoint_creates_span(self, traced_repo, mock_tracer):
        await traced_repo.get_checkpoint("TestProjection")
        assert "eventsource.checkpoint.get_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_get_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        projection_name = "TestProjection"
        await traced_repo.get_checkpoint(projection_name)
        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.get_checkpoint"
        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_update_checkpoint_creates_span(self, traced_repo, mock_tracer):
        await traced_repo.update_checkpoint("TestProjection", uuid4(), "TestEvent")
        assert "eventsource.checkpoint.update_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_update_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        projection_name, event_id, event_type = "TestProjection", uuid4(), "TestEvent"
        await traced_repo.update_checkpoint(projection_name, event_id, event_type)
        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.update_checkpoint"
        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name
        assert ATTR_EVENT_TYPE in attributes
        assert attributes[ATTR_EVENT_TYPE] == event_type

    @pytest.mark.asyncio
    async def test_get_lag_metrics_creates_span(self, traced_repo, mock_tracer):
        await traced_repo.get_lag_metrics("TestProjection")
        assert "eventsource.checkpoint.get_lag_metrics" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_get_lag_metrics_span_attributes(self, traced_repo, mock_tracer):
        projection_name = "TestProjection"
        await traced_repo.get_lag_metrics(projection_name)
        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.get_lag_metrics"
        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_reset_checkpoint_creates_span(self, traced_repo, mock_tracer):
        await traced_repo.reset_checkpoint("TestProjection")
        assert "eventsource.checkpoint.reset_checkpoint" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_reset_checkpoint_span_attributes(self, traced_repo, mock_tracer):
        projection_name = "TestProjection"
        await traced_repo.reset_checkpoint(projection_name)
        span_name, attributes = mock_tracer.spans[0]
        assert span_name == "eventsource.checkpoint.reset_checkpoint"
        assert ATTR_PROJECTION_NAME in attributes
        assert attributes[ATTR_PROJECTION_NAME] == projection_name

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_creates_span(self, traced_repo, mock_tracer):
        await traced_repo.get_all_checkpoints()
        assert "eventsource.checkpoint.get_all_checkpoints" in mock_tracer.span_names

    @pytest.mark.asyncio
    async def test_clear_creates_span(self, traced_repo, mock_tracer):
        await traced_repo.clear()
        assert "eventsource.checkpoint.clear" in mock_tracer.span_names


class TestInMemoryCheckpointRepositoryTracingDisabled:
    @pytest.mark.asyncio
    async def test_get_checkpoint_works_without_tracing(self):
        repo = InMemoryCheckpointRepository(enable_tracing=False)
        assert await repo.get_checkpoint("TestProjection") is None

    @pytest.mark.asyncio
    async def test_update_checkpoint_works_without_tracing(self):
        repo = InMemoryCheckpointRepository(enable_tracing=False)
        projection_name, event_id = "TestProjection", uuid4()
        await repo.update_checkpoint(projection_name, event_id, "TestEvent")
        assert await repo.get_checkpoint(projection_name) == event_id

    @pytest.mark.asyncio
    async def test_get_lag_metrics_works_without_tracing(self):
        repo = InMemoryCheckpointRepository(enable_tracing=False)
        projection_name = "TestProjection"
        await repo.update_checkpoint(projection_name, uuid4(), "TestEvent")
        result = await repo.get_lag_metrics(projection_name)
        assert result is not None and result.projection_name == projection_name

    @pytest.mark.asyncio
    async def test_reset_checkpoint_works_without_tracing(self):
        repo = InMemoryCheckpointRepository(enable_tracing=False)
        projection_name = "TestProjection"
        await repo.update_checkpoint(projection_name, uuid4(), "TestEvent")
        await repo.reset_checkpoint(projection_name)
        assert await repo.get_checkpoint(projection_name) is None

    @pytest.mark.asyncio
    async def test_get_all_checkpoints_works_without_tracing(self):
        repo = InMemoryCheckpointRepository(enable_tracing=False)
        assert await repo.get_all_checkpoints() == []

    @pytest.mark.asyncio
    async def test_clear_works_without_tracing(self):
        repo = InMemoryCheckpointRepository(enable_tracing=False)
        await repo.update_checkpoint("TestProjection", uuid4(), "TestEvent")
        await repo.clear()
        assert await repo.get_all_checkpoints() == []
