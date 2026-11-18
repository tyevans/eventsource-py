"""A failed post-switch verification or subscription migration never ends in COMPLETED."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.domain import StreamId
from eventsource.ports.migration.models import AuditEventType, MigrationConfig, MigrationPhase
from eventsource.ports.positions import ExpectedVersion, Position
from tests.unit.application.migration.test_cutover_safety._harness import (
    TARGET_ID,
    build_harness,
    run_to_dual_write,
)
from tests.unit.application.migration.test_phase3_integration._events import OrderCreated


@pytest.mark.asyncio
async def test_clean_verification_completes_and_reports_verified() -> None:
    h = build_harness()
    config = MigrationConfig(verify_consistency=True, migrate_subscriptions=False)
    migration = await run_to_dual_write(h, config)

    result = await h.coordinator.trigger_cutover(migration.id)

    assert result.success is True and result.consistency_verified is True
    final = await h.migration_repo.get(migration.id)
    assert final is not None and final.phase == MigrationPhase.COMPLETED


@pytest.mark.asyncio
async def test_injected_inconsistency_ends_in_failed_with_visible_outcome() -> None:
    h = build_harness()
    config = MigrationConfig(verify_consistency=True, migrate_subscriptions=False)
    migration = await run_to_dual_write(h, config)
    # An event that exists only on the target makes the copies disagree.
    extra = OrderCreated(aggregate_id=uuid4(), tenant_id=h.tenant_id)
    await h.target.append(StreamId(aggregate_id=extra.aggregate_id, category="Order"), [extra], ExpectedVersion.no_stream())  # fmt: skip

    result = await h.coordinator.trigger_cutover(migration.id)

    assert result.success is False
    assert result.consistency_verified is False
    assert result.error_message is not None and "violations" in result.error_message
    final = await h.migration_repo.get(migration.id)
    assert final is not None and final.phase == MigrationPhase.FAILED
    assert final.last_error is not None and "violations" in final.last_error
    event_types = [e.event_type for e in h.audit.entries]
    assert AuditEventType.CUTOVER_COMPLETED not in event_types
    failed = [e for e in h.audit.entries if e.event_type == AuditEventType.MIGRATION_FAILED]
    assert len(failed) == 1
    assert failed[0].details is not None
    assert failed[0].details["consistency_verified"] is False
    routing = await h.routing.get_routing(h.tenant_id)
    assert routing is not None and routing.store_id == TARGET_ID


@pytest.mark.asyncio
async def test_failed_subscription_migration_ends_in_failed() -> None:
    h = build_harness()
    config = MigrationConfig(verify_consistency=False, migrate_subscriptions=True)
    migration = await run_to_dual_write(h, config)
    # A checkpoint at a source position below every recorded mapping cannot be translated.
    before_all = Position(store_id=h.source.store_id, key=(0,))
    await h.checkpoint_repo.save_position("OrderProjection", before_all, uuid4(), "OrderCreated")

    result = await h.coordinator.trigger_cutover(migration.id)

    assert result.success is False
    assert result.error_message is not None and "subscription migration" in result.error_message
    final = await h.migration_repo.get(migration.id)
    assert final is not None and final.phase == MigrationPhase.FAILED
