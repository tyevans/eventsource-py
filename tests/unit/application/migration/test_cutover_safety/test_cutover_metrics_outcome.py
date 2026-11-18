"""Cutover metrics follow the terminal outcome: success only on COMPLETED, failure on FAILED."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from uuid import uuid4

import pytest

from eventsource.application.migration import metrics_recorder
from eventsource.application.migration.metrics import clear_metrics_registry, get_migration_metrics
from eventsource.domain import StreamId
from eventsource.ports.migration.models import MigrationConfig, MigrationPhase
from eventsource.ports.positions import ExpectedVersion, Position
from tests.unit.application.migration.test_cutover_safety._harness import (
    build_harness,
    run_to_dual_write,
)
from tests.unit.application.migration.test_phase3_integration._events import OrderCreated

DURATION = "migration.cutover.duration"
FAILURES = "migration.cutover.failures"


@pytest.fixture
def observed(monkeypatch: pytest.MonkeyPatch) -> Iterator[Any]:
    """Give each test its own in-memory meter provider.

    The global OpenTelemetry provider can be set only once per process, so the
    migration recorder is pointed at a per-test provider instead.
    """
    sdk = pytest.importorskip("opentelemetry.sdk.metrics")
    export = pytest.importorskip("opentelemetry.sdk.metrics.export")
    reader = export.InMemoryMetricReader()
    meter = sdk.MeterProvider(metric_readers=[reader]).get_meter("test.migration.metrics")
    monkeypatch.setattr(metrics_recorder, "_get_meter", lambda: meter)
    clear_metrics_registry()
    yield reader
    clear_metrics_registry()


def _points(reader: Any, name: str) -> list[Any]:
    # Collect once per reader. Older SDKs (1.16) reset histogram points after a
    # collect, so a second collect would read an empty histogram.
    if not hasattr(reader, "_collected"):
        reader._collected = reader.get_metrics_data()
    data = reader._collected
    if data is None:
        return []
    return [
        point
        for resource in data.resource_metrics
        for scope in resource.scope_metrics
        for metric in scope.metrics
        if metric.name == name
        for point in metric.data.data_points
    ]


def _duration_count(reader: Any, success: str) -> int:
    return sum(p.count for p in _points(reader, DURATION) if p.attributes.get("success") == success)


def _failure_total(reader: Any) -> int:
    return sum(p.value for p in _points(reader, FAILURES))


@pytest.mark.asyncio
async def test_completed_cutover_records_success_only(observed: Any) -> None:
    h = build_harness()
    config = MigrationConfig(verify_consistency=False, migrate_subscriptions=False)
    migration = await run_to_dual_write(h, config)
    metrics = get_migration_metrics(str(migration.id), str(h.tenant_id))

    result = await h.coordinator.trigger_cutover(migration.id)

    assert result.success is True
    final = await h.migration_repo.get(migration.id)
    assert final is not None and final.phase == MigrationPhase.COMPLETED
    assert _duration_count(observed, "true") == 1
    assert _duration_count(observed, "false") == 0
    assert _failure_total(observed) == 0
    assert metrics.get_snapshot().cutover_failures == 0


@pytest.mark.asyncio
async def test_failed_verification_records_failure_not_success(observed: Any) -> None:
    h = build_harness()
    config = MigrationConfig(verify_consistency=True, migrate_subscriptions=False)
    migration = await run_to_dual_write(h, config)
    # An event that exists only on the target makes the copies disagree.
    extra = OrderCreated(aggregate_id=uuid4(), tenant_id=h.tenant_id)
    await h.target.append(StreamId(aggregate_id=extra.aggregate_id, category="Order"), [extra], ExpectedVersion.no_stream())  # fmt: skip
    metrics = get_migration_metrics(str(migration.id), str(h.tenant_id))

    result = await h.coordinator.trigger_cutover(migration.id)

    assert result.success is False
    final = await h.migration_repo.get(migration.id)
    assert final is not None and final.phase == MigrationPhase.FAILED
    assert _duration_count(observed, "true") == 0
    assert _duration_count(observed, "false") == 1
    assert _failure_total(observed) == 1
    assert metrics.get_snapshot().cutover_failures == 1


@pytest.mark.asyncio
async def test_failed_subscription_migration_records_failure_not_success(observed: Any) -> None:
    h = build_harness()
    config = MigrationConfig(verify_consistency=False, migrate_subscriptions=True)
    migration = await run_to_dual_write(h, config)
    # A checkpoint below every recorded mapping cannot be translated.
    before_all = Position(store_id=h.source.store_id, key=(0,))
    await h.checkpoint_repo.save_position("OrderProjection", before_all, uuid4(), "OrderCreated")
    metrics = get_migration_metrics(str(migration.id), str(h.tenant_id))

    result = await h.coordinator.trigger_cutover(migration.id)

    assert result.success is False
    final = await h.migration_repo.get(migration.id)
    assert final is not None and final.phase == MigrationPhase.FAILED
    assert _duration_count(observed, "true") == 0
    assert _duration_count(observed, "false") == 1
    assert _failure_total(observed) == 1
    assert metrics.get_snapshot().cutover_failures == 1


@pytest.mark.asyncio
async def test_rolled_back_cutover_records_failed_duration_without_failure_count(
    observed: Any, monkeypatch: pytest.MonkeyPatch
) -> None:
    h = build_harness()
    config = MigrationConfig(verify_consistency=False, migrate_subscriptions=False)
    migration = await run_to_dual_write(h, config)

    async def unhealthy() -> None:
        raise RuntimeError("target unreachable")

    monkeypatch.setattr(h.target, "current_position", unhealthy)

    result = await h.coordinator.trigger_cutover(migration.id)

    assert result.success is False and result.rolled_back is True
    final = await h.migration_repo.get(migration.id)
    assert final is not None and final.phase == MigrationPhase.DUAL_WRITE
    assert _duration_count(observed, "true") == 0
    assert _duration_count(observed, "false") == 1
    assert _failure_total(observed) == 0
