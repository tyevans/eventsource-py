"""Tests for network partition and process crash simulations during migration."""

from __future__ import annotations

from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion

from ._fixtures import (
    _REGISTRY,
    ChaosTestEvent,
    FailureConfig,
    FailureInjectableStore,
    FailureMode,
    InjectedFailureError,
    create_test_events,
)


class TestNetworkPartitionSimulation:
    """Tests for network partition simulation scenarios."""

    @pytest.mark.asyncio
    async def test_append_fails_during_network_partition(
        self, target_store: FailureInjectableStore, tenant_id: UUID
    ) -> None:
        """Test that appends fail when store is partitioned."""
        target_store.simulate_network_partition()
        aggregate_id = uuid4()
        event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        with pytest.raises(InjectedFailureError) as exc_info:
            await target_store.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
        assert "Network partition" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_reads_fail_during_network_partition(
        self, source_store: FailureInjectableStore, tenant_id: UUID
    ) -> None:
        """Test that reads fail when store is partitioned."""
        await create_test_events(source_store, tenant_id, count=1)
        source_store.simulate_network_partition()

        with pytest.raises(InjectedFailureError) as exc_info:
            await source_store.current_position()
        assert "Network partition" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_partition_heals_and_operations_resume(
        self, source_store: FailureInjectableStore, tenant_id: UUID
    ) -> None:
        """Test that operations resume after partition heals."""
        await create_test_events(source_store, tenant_id, count=5)
        source_store.simulate_network_partition()

        with pytest.raises(InjectedFailureError):
            await source_store.current_position()

        source_store.heal_network_partition()
        count = len([e async for e in source_store.read_all()])
        assert count == 5

    @pytest.mark.asyncio
    async def test_dual_write_handles_target_partition(self, tenant_id: UUID) -> None:
        """Test dual-write continues to source when target is partitioned."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = FailureInjectableStore()
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        target_store.simulate_network_partition()
        aggregate_id = uuid4()
        event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="ChaosAggregate")

        await interceptor.append(stream, [event], ExpectedVersion.no_stream())

        source_events = [envelope async for envelope in source_store.read_stream(stream)]
        assert len(source_events) == 1
        assert interceptor.get_failure_stats().total_failures >= 1


class TestProcessCrashDuringDualWrite:
    """Tests for process crash simulation during dual-write phase."""

    @pytest.mark.asyncio
    async def test_dual_write_no_data_loss_on_source(self, tenant_id: UUID) -> None:
        """Test that dual-write ensures no data loss on source during failures."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = FailureInjectableStore()
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.INTERMITTENT,
                fail_rate=0.5,
                error_message="Random target failure",
            )
        )

        events_written = 0
        for i in range(20):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            events_written += 1

        assert events_written == 20
        source_count = len(
            [
                1
                async for envelope in source_store.read_all()
                if envelope.event.tenant_id == tenant_id
            ]
        )
        assert source_count == 20
        assert interceptor.get_failure_stats().total_failures > 0

    @pytest.mark.asyncio
    async def test_dual_write_source_failure_propagates(
        self, target_store: FailureInjectableStore, tenant_id: UUID
    ) -> None:
        """Test that source store failure propagates to caller."""
        source_store = FailureInjectableStore()
        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        source_store.set_failure(
            FailureConfig(mode=FailureMode.FAIL_ALL, error_message="Source store failure")
        )

        aggregate_id = uuid4()
        event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        with pytest.raises(InjectedFailureError) as exc_info:
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
        assert "Source store failure" in str(exc_info.value)
