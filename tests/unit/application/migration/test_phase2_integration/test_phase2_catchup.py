"""Tests for catch-up rounds cap during bulk copy."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING
from uuid import UUID

import pytest

from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.ports.migration.models import MigrationConfig, MigrationPhase
from tests.unit.application.migration.test_phase2_integration._helpers import create_test_events

if TYPE_CHECKING:
    from eventsource.adapters.memory import InMemoryEventStore
    from eventsource.application.migration.router import TenantStoreRouter
    from tests.unit.application.migration.test_phase2_integration._mocks import MockLockManager
    from tests.unit.application.migration.test_phase2_integration._repositories import (
        InMemoryMigrationRepository,
        InMemoryRoutingRepository,
    )


class TestCatchUpRoundsCap:
    """The post-copy catch-up loop must be bounded, not spin forever.

    A mirror that never drains (every mirrored write keeps failing) would
    otherwise make the `while completed` loop in `_run_bulk_copy` run
    forever waiting for `remaining == 0`. `_MAX_CATCHUP_ROUNDS` caps that:
    once hit, the migration still completes its copy and transitions to
    DUAL_WRITE (fail-open on progress), but the lag anchor -- driven by
    the same unabsorbed-failure bookkeeping exercised in
    `TestSyncLagAnchorOnWriteActiveTenant` -- stays clamped at the
    checkpoint, so cutover keeps refusing until a later pass absorbs the
    remainder.
    """

    @pytest.mark.asyncio
    async def test_cap_hit_still_transitions_with_anchor_clamped(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        migration_repo: InMemoryMigrationRepository,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        lock_manager: MockLockManager,
        tenant_id: UUID,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Force every catch-up round to report failures still outstanding.

        Rather than fighting the copier into a genuine sustained mirror
        failure (which the retry/duplicate handling in `BulkCopier` is
        specifically designed to resolve), fake the always-failing mirror
        at the interceptor's public attestation boundary:
        `mark_copy_pass_complete` never reports zero remaining, and
        `safe_lag_anchor` never advances past the checkpoint -- exactly
        the observable contract a permanently-saturated interceptor
        already has (see `test_saturated_failure_history_never_reports_clean`).
        This isolates the coordinator's bounded-loop control flow from the
        interceptor's own bookkeeping, which is covered elsewhere.
        """
        monkeypatch.setattr(
            DualWriteInterceptor,
            "mark_copy_pass_complete",
            lambda self, checkpoint: 1,
        )
        monkeypatch.setattr(
            DualWriteInterceptor,
            "safe_lag_anchor",
            lambda self, checkpoint: checkpoint,
        )

        await create_test_events(source_store, tenant_id, count=3)

        coordinator = MigrationCoordinator(
            source_store=source_store,
            migration_repo=migration_repo,
            routing_repo=routing_repo,
            router=router,
            lock_manager=lock_manager,
            source_store_id="default",
            enable_tracing=False,
        )
        router.register_store("dedicated", target_store)

        with caplog.at_level(
            logging.WARNING, logger="eventsource.application.migration.coordinator"
        ):
            migration = await coordinator.start_migration(
                tenant_id=tenant_id,
                target_store=target_store,
                target_store_id="dedicated",
                config=MigrationConfig(
                    cutover_max_lag_events=2,
                    verify_consistency=False,
                    migrate_subscriptions=False,
                ),
            )

            # The loop terminates (does not hang) and still reaches DUAL_WRITE.
            migration = await coordinator.wait_for_phase(
                migration.id, MigrationPhase.DUAL_WRITE, timeout=5.0, poll_interval=0.01
            )

        assert migration.phase == MigrationPhase.DUAL_WRITE
        assert any(
            "mirror failures remain unabsorbed" in record.message for record in caplog.records
        )

        # More source events land after the checkpoint. A working anchor
        # would still be safe to advance on a healthy mirror, but with the
        # cap hit and the mirror faked as permanently saturated, the
        # anchor is pinned at the checkpoint -- these count as lag.
        await create_test_events(source_store, tenant_id, count=5)

        # The anchor never advanced past the checkpoint, so cutover refuses.
        result = await coordinator.trigger_cutover(migration.id)
        assert result.success is False


__all__ = ["TestCatchUpRoundsCap"]
