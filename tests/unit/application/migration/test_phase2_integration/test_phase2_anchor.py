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
    """Lag must converge on a live tenant without hiding missing events."""

    @pytest.mark.asyncio
    async def test_restart_refuses_until_a_fresh_copy_pass(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """A restarted orchestrator has no watermarks, so it refuses cutover."""
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 4)

        pre_restart = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(pre_restart, tenant_id, 5)

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
        assert lag.events > 0
        assert tracker.is_sync_ready() is False

    @pytest.mark.asyncio
    async def test_overlap_mirror_conflict_is_absorbed_by_the_copy_pass(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        tenant_id: UUID,
    ) -> None:
        """A mirror refused on a behind stream never leapfrogs the copier."""
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

        live = SampleTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        result = await interceptor.append(stream, [live], ExpectedVersion.any_())
        assert result.new_version == 3
        assert await target_store.get_stream_version(stream) == 0
        assert interceptor.first_failed_source_position == result.position

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

        assert interceptor.mark_copy_pass_complete(checkpoint) == 0
        assert interceptor.first_failed_source_position is None

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
        """`mark_copy_pass_complete` only absorbs what the checkpoint covers."""
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

        assert interceptor.mark_copy_pass_complete(first_failure) == 1
        assert interceptor.first_failed_source_position == second_failure

        await drive_dual_writes(interceptor, tenant_id, 1)
        assert interceptor.safe_lag_anchor(first_failure) == first_failure

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
        """Completed-pass attestation releases the first_seen proxy clamp."""
        copied_through = await seed_copied_prefix(source_store, target_store, tenant_id, 2)

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        await drive_dual_writes(interceptor, tenant_id, 2)
        assert interceptor.safe_lag_anchor(copied_through) == copied_through

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
        """Once tracked failures exceed `max_failure_history`, recovery stays clamped."""
        always_fails = FlakyTarget(target_store, reject_first=1_000_000)
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=always_fails,
            tenant_id=tenant_id,
            enable_tracing=False,
            max_failure_history=2,
        )

        await drive_dual_writes(interceptor, tenant_id, 5)
        checkpoint = await source_store.current_position()
        assert checkpoint is not None

        remaining = interceptor.mark_copy_pass_complete(checkpoint)
        assert remaining >= 1
        assert interceptor.mark_copy_pass_complete(checkpoint) >= 1
        assert interceptor.safe_lag_anchor(checkpoint) == checkpoint


__all__ = ["TestSyncLagAnchorOnWriteActiveTenant"]
