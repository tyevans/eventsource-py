"""Unit tests confirming ATTR_MIGRATION_* telemetry constants are set on real spans."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.cutover import (
    ATTR_CUTOVER_MIGRATION_ID,
    CutoverManager,
)
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.observability.attributes import (
    ATTR_MIGRATION_ID,
    ATTR_MIGRATION_PHASE,
    ATTR_MIGRATION_SOURCE_STORE,
    ATTR_MIGRATION_SYNC_LAG_EVENTS,
    ATTR_MIGRATION_TARGET_STORE,
    ATTR_MIGRATION_TENANT_ID,
)
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import Migration, MigrationConfig, MigrationPhase

pytestmark = pytest.mark.asyncio


class _IsolatedOtelTracer:
    def __init__(self, otel_tracer: Any) -> None:
        self._tracer = otel_tracer

    @property
    def enabled(self) -> bool:
        return True

    def span(self, name: str, attributes: dict[str, Any] | None = None) -> Any:
        return self._tracer.start_as_current_span(name, attributes=attributes or {})


@pytest.fixture
def isolated_tracer() -> tuple[Any, Any]:
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return _IsolatedOtelTracer(provider.get_tracer("test")), exporter


def _span_attrs(exporter: Any, name: str) -> dict[str, Any]:
    spans = [s for s in exporter.get_finished_spans() if s.name == name]
    assert len(spans) == 1
    return dict(spans[0].attributes or {})


class TestSyncLagTrackerSpanAttributes:
    async def test_calculate_lag_sets_sync_lag_events_attribute(
        self, isolated_tracer: tuple[Any, Any]
    ) -> None:
        tracer, exporter = isolated_tracer
        registry = EventRegistry()

        @register_event(registry=registry)
        class SpanTestEvent(DomainEvent):
            aggregate_type: str = "SpanTestAggregate"

        source_store = InMemoryEventStore("span-source", event_registry=registry)
        target_store = InMemoryEventStore("span-target", event_registry=registry)

        for _ in range(4):
            stream = StreamId(aggregate_id=uuid4(), category="SpanTestAggregate")
            await source_store.append(
                stream, [SpanTestEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_()
            )

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=MigrationConfig(cutover_max_lag_events=100),
            migration_id=uuid4(),
            tracer=tracer,
        )
        await lag_tracker.calculate_lag()
        attrs = _span_attrs(exporter, "eventsource.sync_lag.calculate_lag")
        assert attrs[ATTR_MIGRATION_SYNC_LAG_EVENTS] == 4


class _AcquireCtx:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *exc: Any) -> None:
        return None


class TestCutoverManagerSpanAttributes:
    async def test_execute_cutover_sets_both_migration_id_attributes(
        self, isolated_tracer: tuple[Any, Any]
    ) -> None:
        tracer, exporter = isolated_tracer
        migration_id, tenant_id = uuid4(), uuid4()

        lock_manager = MagicMock(acquire=MagicMock(return_value=_AcquireCtx()))
        router = MagicMock(
            pause_writes=AsyncMock(),
            resume_writes=AsyncMock(),
            get_store=MagicMock(return_value=AsyncMock()),
            clear_dual_write_interceptor=MagicMock(),
        )
        routing_repo = AsyncMock(get_routing=AsyncMock(return_value=None))
        lag = MagicMock(is_within_threshold=MagicMock(return_value=True), events=0)
        lag_tracker = MagicMock(calculate_lag=AsyncMock(), current_lag=lag)

        manager = CutoverManager(
            router=router, routing_repo=routing_repo, lock_manager=lock_manager, tracer=tracer
        )
        await manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
        )

        attrs = _span_attrs(exporter, "eventsource.cutover.execute")
        assert attrs[ATTR_CUTOVER_MIGRATION_ID] == str(migration_id)
        assert attrs[ATTR_MIGRATION_ID] == str(migration_id)


class TestCoordinatorSpanAttributes:
    async def test_transition_to_dual_write_sets_phase_and_ids(
        self, isolated_tracer: tuple[Any, Any]
    ) -> None:
        tracer, exporter = isolated_tracer
        migration_id, tenant_id = uuid4(), uuid4()

        migration = Migration(
            id=migration_id,
            tenant_id=tenant_id,
            source_store_id="default",
            target_store_id="dedicated",
            phase=MigrationPhase.BULK_COPY,
        )
        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=AsyncMock(update_phase=AsyncMock()),
            routing_repo=AsyncMock(set_migration_state=AsyncMock()),
            router=MagicMock(),
            tracer=tracer,
        )
        await coordinator._transition_to_dual_write(migration, AsyncMock())

        attrs = _span_attrs(exporter, "eventsource.coordinator.transition_to_dual_write")
        assert attrs[ATTR_MIGRATION_ID] == str(migration_id)
        assert attrs[ATTR_MIGRATION_TENANT_ID] == str(tenant_id)
        assert attrs[ATTR_MIGRATION_PHASE] == MigrationPhase.DUAL_WRITE.value

    async def test_start_migration_sets_source_and_target_store(
        self, isolated_tracer: tuple[Any, Any]
    ) -> None:
        tracer, exporter = isolated_tracer
        migration_repo = AsyncMock(get_by_tenant=AsyncMock(return_value=None), create=AsyncMock())
        router = MagicMock(register_store=MagicMock(), set_dual_write_interceptor=MagicMock())

        coordinator = MigrationCoordinator(
            source_store=AsyncMock(),
            migration_repo=migration_repo,
            routing_repo=AsyncMock(),
            router=router,
            source_store_id="default",
            tracer=tracer,
        )
        coordinator._run_bulk_copy = AsyncMock()  # type: ignore[method-assign]

        await coordinator.start_migration(
            tenant_id=uuid4(), target_store=AsyncMock(), target_store_id="dedicated"
        )

        attrs = _span_attrs(exporter, "eventsource.coordinator.start_migration")
        assert attrs[ATTR_MIGRATION_SOURCE_STORE] == "default"
        assert attrs[ATTR_MIGRATION_TARGET_STORE] == "dedicated"
