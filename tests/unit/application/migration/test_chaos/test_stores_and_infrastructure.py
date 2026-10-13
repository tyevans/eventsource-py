"""Tests for target store failures and failure injection infrastructure."""

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
    count_tenant_events,
)

# =============================================================================
# Target Store Failure During Dual-Write Tests
# =============================================================================


class TestTargetStoreFailures:
    """Tests for target store failures during dual-write."""

    @pytest.mark.asyncio
    async def test_source_writes_succeed_when_target_fails(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that source writes succeed even when target completely fails."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = FailureInjectableStore()

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Complete target failure
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.FAIL_ALL,
                error_message="Target store completely unavailable",
            )
        )

        success_count = 0
        for i in range(10):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )

            # A source-store failure would raise and fail the test; the
            # target's FAIL_ALL is absorbed internally by the interceptor.
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )
            success_count += 1

        # All writes should succeed
        assert success_count == 10

        # All events should be in source
        source_count = 0
        async for envelope in source_store.read_all():
            if envelope.event.tenant_id == tenant_id:
                source_count += 1
        assert source_count == 10

        # No events in target
        target_count = await count_tenant_events(target_store, tenant_id)
        assert target_count == 0

    @pytest.mark.asyncio
    async def test_target_failure_rate_tracked(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that target failure rate is tracked for monitoring."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = FailureInjectableStore()

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # 50% target failure rate
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.INTERMITTENT,
                fail_rate=0.5,
            )
        )

        for i in range(100):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )

            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        stats = interceptor.get_failure_stats()

        # Should have recorded failures (around 50%, but with variance)
        assert stats.total_failures > 20
        assert stats.total_failures < 80

    @pytest.mark.asyncio
    async def test_target_recovers_and_writes_resume(
        self,
        tenant_id: UUID,
    ) -> None:
        """Test that writes to target resume after recovery."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = FailureInjectableStore()

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        # Target initially fails
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.FAIL_ALL,
                error_message="Target unavailable",
            )
        )

        # Write during failure
        for i in range(5):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Target should have no events
        target_count = await count_tenant_events(target_store, tenant_id)
        assert target_count == 0

        # Target recovers
        target_store.clear_failure()

        # Write after recovery
        for i in range(5, 10):
            aggregate_id = uuid4()
            event = ChaosTestEvent(
                aggregate_id=aggregate_id,
                tenant_id=tenant_id,
                sequence=i,
            )
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        # Target should now have the newer events
        target_count = await count_tenant_events(target_store, tenant_id)
        assert target_count == 5  # Only the ones after recovery


# =============================================================================
# Failure Injection Configuration Tests
# =============================================================================


class TestFailureInjectionInfrastructure:
    """Tests for the failure injection infrastructure itself."""

    @pytest.mark.asyncio
    async def test_failure_config_none_mode_passes_through(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that NONE failure mode passes through normally."""
        target_store.set_failure(FailureConfig(mode=FailureMode.NONE))

        aggregate_id = uuid4()
        event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)

        result = await target_store.append(
            StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
            [event],
            ExpectedVersion.no_stream(),
        )

        assert result.new_version == 1

    @pytest.mark.asyncio
    async def test_failure_config_fail_after_n_works(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that FAIL_AFTER_N_OPS mode works correctly."""
        target_store.set_failure(
            FailureConfig(
                mode=FailureMode.FAIL_AFTER_N_OPS,
                fail_after_n=3,
            )
        )

        success_count = 0
        for i in range(5):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            try:
                await target_store.append(
                    StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                    [event],
                    ExpectedVersion.no_stream(),
                )
                success_count += 1
            except InjectedFailureError:
                pass

        # First 3 succeed, then failures start
        assert success_count == 3

    @pytest.mark.asyncio
    async def test_clear_failure_restores_normal_operation(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that clearing failure config restores normal operation."""
        # Set failure
        target_store.set_failure(FailureConfig(mode=FailureMode.FAIL_ALL))

        aggregate_id = uuid4()
        event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id)
        stream = StreamId(aggregate_id=aggregate_id, category="ChaosAggregate")

        # Should fail
        with pytest.raises(InjectedFailureError):
            await target_store.append(stream, [event], ExpectedVersion.no_stream())

        # Clear failure
        target_store.clear_failure()

        # Should succeed now
        result = await target_store.append(stream, [event], ExpectedVersion.no_stream())

        assert result.new_version == 1

    @pytest.mark.asyncio
    async def test_operation_count_tracking(
        self,
        target_store: FailureInjectableStore,
        tenant_id: UUID,
    ) -> None:
        """Test that operation count is properly tracked."""
        config = FailureConfig(mode=FailureMode.NONE)
        target_store.set_failure(config)

        for i in range(5):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            await target_store.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        assert config.operation_count == 5
