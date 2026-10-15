"""Tests for AggregateRepository Tracer composition integration and disabled behavior."""

from __future__ import annotations

import inspect
import subprocess
from pathlib import Path
from uuid import uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.observability import NullTracer
from tests.unit.application.aggregates.test_repository_tracing._fixtures import (
    _REGISTRY,
    TracingTestAggregate,
)


def _find_repo_root() -> Path:
    p = Path(__file__).resolve()
    for parent in p.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return p.parents[5]


class TestAggregateRepositoryTracingComposition:
    """Tests for AggregateRepository Tracer composition integration."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    def test_uses_tracer_composition(self):
        sig = inspect.signature(AggregateRepository.__init__)
        assert "tracer" in sig.parameters

    def test_tracing_enabled_by_default(self, event_store: InMemoryEventStore):
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
        )
        assert hasattr(repo, "_enable_tracing")
        assert hasattr(repo, "_tracer")
        assert repo._tracer is not None

    def test_tracing_disabled_when_requested(self, event_store: InMemoryEventStore):
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            enable_tracing=False,
        )
        assert repo._enable_tracing is False
        assert isinstance(repo._tracer, NullTracer)

    def test_custom_tracer_can_be_injected(self, event_store: InMemoryEventStore):
        custom_tracer = NullTracer()
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            tracer=custom_tracer,
        )
        assert repo._tracer is custom_tracer

    def test_backward_compatible_constructor(self, event_store: InMemoryEventStore):
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
        )
        assert hasattr(repo, "_enable_tracing")
        assert hasattr(repo, "_tracer")


class TestAggregateRepositoryTracingDisabled:
    """Tests for AggregateRepository behavior when tracing is disabled."""

    @pytest.fixture
    def event_store(self) -> InMemoryEventStore:
        return InMemoryEventStore(event_registry=_REGISTRY)

    @pytest.fixture
    def snapshot_store(self) -> InMemorySnapshotStore:
        return InMemorySnapshotStore()

    @pytest.mark.asyncio
    async def test_save_works_without_tracing(self, event_store):
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            enable_tracing=False,
        )
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)
        assert not aggregate.has_uncommitted_events

    @pytest.mark.asyncio
    async def test_load_works_without_tracing(self, event_store):
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            enable_tracing=False,
        )
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        await repo.save(aggregate)

        loaded = await repo.load(aggregate.aggregate_id)
        assert loaded.version == 1
        assert loaded.state.value == "test"

    @pytest.mark.asyncio
    async def test_exists_works_without_tracing(self, event_store):
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            enable_tracing=False,
        )
        aggregate_id = uuid4()
        assert await repo.exists(aggregate_id) is False

        aggregate = TracingTestAggregate(aggregate_id)
        aggregate.do_something("test")
        await repo.save(aggregate)
        assert await repo.exists(aggregate_id) is True

    @pytest.mark.asyncio
    async def test_create_snapshot_works_without_tracing(self, event_store, snapshot_store):
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=TracingTestAggregate,
            snapshot_store=snapshot_store,
            enable_tracing=False,
        )
        aggregate = TracingTestAggregate(uuid4())
        aggregate.do_something("test")
        aggregate._uncommitted_events.clear()

        snapshot = await repo.create_snapshot(aggregate)
        assert snapshot is not None
        assert snapshot.version == 1


class TestAggregateRepositoryStandardAttributes:
    """Tests for standard attribute usage in AggregateRepository."""

    def test_imports_from_observability_module(self):
        result = subprocess.run(
            [
                "grep",
                "-c",
                "from eventsource.observability import",
                "src/eventsource/application/aggregates/repository.py",
            ],
            capture_output=True,
            text=True,
            cwd=_find_repo_root(),
        )
        count = int(result.stdout.strip())
        assert count >= 1, "AggregateRepository should import from eventsource.observability"

    def test_uses_standard_attribute_constants(self):
        result = subprocess.run(
            ["grep", "-c", "ATTR_", "src/eventsource/application/aggregates/repository.py"],
            capture_output=True,
            text=True,
            cwd=_find_repo_root(),
        )
        count = int(result.stdout.strip())
        assert count >= 4, f"Expected at least 4 ATTR_* usages, found {count}"


__all__ = [
    "TestAggregateRepositoryStandardAttributes",
    "TestAggregateRepositoryTracingComposition",
    "TestAggregateRepositoryTracingDisabled",
]
