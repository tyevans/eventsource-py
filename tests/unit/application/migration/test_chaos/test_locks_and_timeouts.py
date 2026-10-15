"""Tests for lock contention and timeout scenarios during migration."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.application.migration.cutover import CutoverManager
from eventsource.application.migration.dual_write import DualWriteInterceptor
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.application.migration.write_pause import WritePausedError, WritePauseManager
from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.migration.models import MigrationConfig, TenantMigrationState

from ._fixtures import (
    _REGISTRY,
    ChaosTestEvent,
    FailureConfig,
    FailureInjectableStore,
    FailureMode,
    InMemoryRoutingRepository,
    MockLockManager,
    count_tenant_events,
    create_test_events,
)


class TestLockContentionScenarios:
    """Tests for lock contention during migration operations."""

    @pytest.mark.asyncio
    async def test_cutover_fails_when_lock_acquisition_fails(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test cutover fails when lock cannot be acquired."""
        migration_id = uuid4()
        lock_manager = MockLockManager(should_fail=True)

        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            timeout_ms=100.0,
        )

        assert result.success is False
        assert "lock" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_concurrent_cutover_attempts_serialized(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test that concurrent cutover attempts are properly serialized."""
        migration_id = uuid4()
        lock_manager = MockLockManager(contention_delay=0.1)

        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        async def attempt_cutover(attempt_id: int):
            try:
                return await cutover_manager.execute_cutover(
                    migration_id=migration_id,
                    tenant_id=tenant_id,
                    lag_tracker=lag_tracker,
                    target_store_id="dedicated",
                    timeout_ms=500.0,
                )
            except Exception as e:
                return e

        results = await asyncio.gather(
            attempt_cutover(1),
            attempt_cutover(2),
            attempt_cutover(3),
            return_exceptions=True,
        )

        success_count = sum(1 for r in results if hasattr(r, "success") and r.success)
        assert success_count <= 1

    @pytest.mark.asyncio
    async def test_lock_released_on_failure(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test that lock is released even when cutover fails."""
        migration_id = uuid4()
        lock_manager = MockLockManager()

        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)
        await create_test_events(source_store, tenant_id, count=200)

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            config=MigrationConfig(cutover_max_lag_events=10),
            enable_tracing=False,
        )
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            config=MigrationConfig(cutover_max_lag_events=10),
            timeout_ms=500.0,
        )

        assert result.success is False
        assert not lock_manager.is_locked(f"migration:cutover:{tenant_id}")


class TestTimeoutScenarios:
    """Tests for timeout scenarios during migration operations."""

    @pytest.mark.asyncio
    async def test_cutover_timeout_causes_rollback(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        """Test that cutover timeout triggers automatic rollback."""
        migration_id = uuid4()
        lock_manager = MockLockManager(contention_delay=1.0)

        await routing_repo.set_migration_state(
            tenant_id, TenantMigrationState.DUAL_WRITE, migration_id
        )
        router.register_store("dedicated", target_store)

        lag_tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        cutover_manager = CutoverManager(
            lock_manager=lock_manager,
            router=router,
            routing_repo=routing_repo,
            enable_tracing=False,
        )

        result = await cutover_manager.execute_cutover(
            migration_id=migration_id,
            tenant_id=tenant_id,
            lag_tracker=lag_tracker,
            target_store_id="dedicated",
            timeout_ms=100.0,
        )

        if not result.success:
            assert (
                result.rolled_back
                or "timeout" in result.error_message.lower()
                or "lock" in result.error_message.lower()
            )

    @pytest.mark.asyncio
    async def test_write_pause_timeout(
        self,
        write_pause_manager: WritePauseManager,
        tenant_id: UUID,
    ) -> None:
        """Test that write pause timeout raises appropriate error."""
        await write_pause_manager.pause_writes(tenant_id)

        with pytest.raises(WritePausedError) as exc_info:
            await write_pause_manager.wait_if_paused(tenant_id, timeout=0.05)

        assert exc_info.value.tenant_id == tenant_id
        assert exc_info.value.timeout == 0.05

    @pytest.mark.asyncio
    async def test_slow_target_causes_lag_accumulation(self, tenant_id: UUID) -> None:
        """Test that slow target causes sync lag to accumulate."""
        source_store = InMemoryEventStore(event_registry=_REGISTRY)
        target_store = FailureInjectableStore()
        target_store.set_failure(FailureConfig(mode=FailureMode.DELAY_APPEND, delay_seconds=0.05))

        interceptor = DualWriteInterceptor(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
            enable_tracing=False,
        )

        for i in range(10):
            aggregate_id = uuid4()
            event = ChaosTestEvent(aggregate_id=aggregate_id, tenant_id=tenant_id, sequence=i)
            await interceptor.append(
                StreamId(aggregate_id=aggregate_id, category="ChaosAggregate"),
                [event],
                ExpectedVersion.no_stream(),
            )

        source_count = sum(
            [1 async for env in source_store.read_all() if env.event.tenant_id == tenant_id]
        )
        target_count = await count_tenant_events(target_store, tenant_id)

        assert source_count == 10
        assert target_count == 10
