"""Tests for sync lag anchor behavior on write-active tenants."""

from __future__ import annotations

from unittest.mock import MagicMock
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.bulk_copier import BulkCopier
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion, FeedReadOptions
from eventsource.ports.migration.models import MigrationConfig
from tests.unit.application.migration.test_phase2_integration._anchor_base import (
    _TestSyncLagAnchorBase,
)
from tests.unit.application.migration.test_phase2_integration._events import SampleTestEvent
from tests.unit.application.migration.test_phase2_integration._helpers import (
    drive_dual_writes,
    seed_copied_prefix,
)
from tests.unit.application.migration.test_phase2_integration._mocks import FlakyTarget


class TestSyncLagAnchorOnWriteActiveTenant(_TestSyncLagAnchorBase):
    """Lag must converge on a live tenant WITHOUT ever hiding missing events.

    The count-behind reads the source feed after its anchor, which
    includes everything the `DualWriteInterceptor` has already mirrored.
    The anchor therefore advances to the interceptor's synced watermark --
    but never past its first failure, because events at or after that
    point may be missing from the target.
    """

    @pytest.mark.asyncio
    async def test_restart_refuses_until_a_fresh_copy_pass(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """A restarted orchestrator has no watermarks, so it refuses cutover.

        The watermarks live in the interceptor's memory. After a restart
        the anchor falls back to the bulk-copy checkpoint, so already
        mirrored events count as lag and cutover refuses until a fresh
        copy pass advances the checkpoint.
        """
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 4)

        pre_restart = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(pre_restart, tenant_id, 5)

        # The process restarts: a brand-new interceptor, no watermarks.
        restarted = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        assert restarted.last_synced_source_position is None
        assert restarted.safe_lag_anchor(copied_through) == copied_through

        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=MigrationConfig(cutover_max_lag_events=3),
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        lag = await tracker.calculate_lag(since=restarted.safe_lag_anchor(copied_through))

        # The five already-mirrored events read as lag -- conservative,
        # and the documented consequence of not persisting the watermark.
        assert lag.events > 0
        assert tracker.is_sync_ready() is False

    @pytest.mark.asyncio
    async def test_overlap_mirror_conflict_is_absorbed_by_the_copy_pass(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """A mirror refused on a behind stream never leapfrogs the copier.

        The mirror appends at the exact pre-append source version, so on a
        stream the copier has not caught up it fails -- even for an `any`
        writer that would previously have landed out of order. The refusal
        is recorded, the copy pass delivers the event in source order, and
        the completed pass absorbs the recorded failure so the anchor can
        advance again.
        """
        aggregate_id = uuid4()
        stream = StreamId(aggregate_id=aggregate_id, category="SampleAggregate")
        await source_store.append(
            stream,
            [SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)],
            ExpectedVersion.no_stream(),
        )
        await source_store.append(
            stream,
            [SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)],
            ExpectedVersion.exact(1),
        )

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # A live `any` writer appends while the target holds none of the
        # stream yet: the source takes it, the mirror must refuse.
        live = SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        result = await interceptor.append(stream, [live], ExpectedVersion.any_())
        assert result.new_version == 3
        assert await target_store.get_stream_version(stream) == 0
        assert interceptor.first_failed_source_position == result.position

        # The copy pass (which runs with the interceptor installed) then
        # delivers everything, in source stream order.
        copier = BulkCopier(
            source_store=source_store,
            target_store=target_store,
            migration_repo=MagicMock(),
            enable_tracing=False,
        )
        envelopes = [
            env async for env in source_store.read_all(None, FeedReadOptions(tenant_id=tenant_id))
        ]
        await copier._write_batch(uuid4(), tenant_id, envelopes)
        checkpoint = envelopes[-1].position
        assert checkpoint is not None

        source_ids = [env.event.event_id async for env in source_store.read_stream(stream)]
        target_ids = [env.event.event_id async for env in target_store.read_stream(stream)]
        assert target_ids == source_ids

        # The completed covered pass absorbs the failure...
        assert interceptor.mark_copy_pass_complete(checkpoint) == 0
        assert interceptor.first_failed_source_position is None

        # ...so the next mirrored write advances the anchor again.
        follow_up = await interceptor.append(
            stream,
            [SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)],
            ExpectedVersion.exact(3),
        )
        assert interceptor.safe_lag_anchor(checkpoint) == follow_up.position

    @pytest.mark.asyncio
    async def test_failures_beyond_the_checkpoint_stay_unabsorbed(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """`mark_copy_pass_complete` only absorbs what the checkpoint covers.

        A failure past the pass's checkpoint was not re-copied by that
        pass, so it keeps clamping the anchor until a later pass reaches
        it.
        """
        flaky = FlakyTarget(target_store, reject_first=2)
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=flaky,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 2)
        failures = [fw.source_position for fw in interceptor.get_failed_writes()]
        assert len(failures) == 2
        first_failure, second_failure = failures
        assert first_failure is not None and second_failure is not None

        # A pass that only reached the first failure absorbs only it.
        assert interceptor.mark_copy_pass_complete(first_failure) == 1
        assert interceptor.first_failed_source_position == second_failure

        # A later mirror success cannot advance past the remaining hole.
        await drive_dual_writes(interceptor, tenant_id, 1)
        assert interceptor.safe_lag_anchor(first_failure) == first_failure

        # The next pass's checkpoint covers it and the anchor is free.
        assert interceptor.mark_copy_pass_complete(second_failure) == 0
        await drive_dual_writes(interceptor, tenant_id, 1)
        anchor = interceptor.safe_lag_anchor(second_failure)
        assert anchor == interceptor.last_synced_source_position
        assert anchor is not None and anchor > second_failure

    @pytest.mark.asyncio
    async def test_attested_pass_releases_the_install_window_clamp(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """The coordinator's completed-pass attestation replaces the
        first_seen proxy: mirrored writes beyond the checkpoint advance the
        anchor even though the checkpoint never reached first_seen."""
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 2)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 2)

        # Without attestation the proxy clamp holds (checkpoint < first_seen).
        assert interceptor.safe_lag_anchor(copied_through) == copied_through

        # With it, the window is provably empty and the anchor advances.
        assert interceptor.mark_copy_pass_complete(copied_through) == 0
        anchor = interceptor.safe_lag_anchor(copied_through)
        assert anchor == interceptor.last_synced_source_position
        assert anchor is not None and copied_through is not None
        assert anchor > copied_through

    @pytest.mark.asyncio
    async def test_saturated_failure_history_never_reports_clean(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """Once tracked failures exceed `max_failure_history`, the fail-closed
        guard must pin, not just slow, recovery.

        Dropping the oldest unabsorbed failure position to bound memory
        makes a future "fully absorbed" claim unsound -- there is no way
        to prove the dropped position was re-copied. So once saturated,
        `safe_lag_anchor` must clamp to the checkpoint forever, and
        `mark_copy_pass_complete` must never again report zero remaining
        failures, even when its checkpoint covers every position still on
        record.
        """
        always_fails = FlakyTarget(target_store, reject_first=1_000_000)
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=always_fails,
            tenant_id=tenant_id,
            enable_tracing=False,
            max_failure_history=2,
        )

        # Drive more failing mirrors than the cap so the guard saturates.
        await drive_dual_writes(interceptor, tenant_id, 5)
        checkpoint = await source_store.current_position()
        assert checkpoint is not None

        # A checkpoint that covers every recorded failure would normally
        # absorb them all and report clean -- saturation must override that.
        remaining = interceptor.mark_copy_pass_complete(checkpoint)
        assert remaining >= 1

        # Calling it again (another completed pass at the same checkpoint)
        # must still refuse to report clean.
        assert interceptor.mark_copy_pass_complete(checkpoint) >= 1

        # The anchor stays pinned at the checkpoint regardless of how far
        # writes have otherwise synced.
        assert interceptor.safe_lag_anchor(checkpoint) == checkpoint


__all__ = ["TestSyncLagAnchorOnWriteActiveTenant"]
