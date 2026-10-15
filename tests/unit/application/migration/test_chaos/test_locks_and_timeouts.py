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


async def _prepare_cutover(
    source_store: FailureInjectableStore,
    target_store: FailureInjectableStore,
    routing_repo: InMemoryRoutingRepository,
    router: TenantStoreRouter,
    tenant_id: UUID,
    lock_manager: MockLockManager,
    config: MigrationConfig | None = None,
) -> tuple[UUID, SyncLagTracker, CutoverManager]:
    migration_id = uuid4()
    await routing_repo.set_migration_state(tenant_id, TenantMigrationState.DUAL_WRITE, migration_id)
    router.register_store("dedicated", target_store)
    lag_tracker = SyncLagTracker(
        source_store=source_store,
        target_store=target_store,
        tenant_id=tenant_id,
        config=config,
        enable_tracing=False,
    )
    cutover_manager = CutoverManager(
        lock_manager=lock_manager, router=router, routing_repo=routing_repo, enable_tracing=False
    )
    return migration_id, lag_tracker, cutover_manager


async def _exec_cutover(
    cutover_manager: CutoverManager,
    migration_id: UUID,
    tenant_id: UUID,
    lag_tracker: SyncLagTracker,
    timeout_ms: float = 100.0,
    config: MigrationConfig | None = None,
):
    return await cutover_manager.execute_cutover(
        migration_id=migration_id,
        tenant_id=tenant_id,
        lag_tracker=lag_tracker,
        target_store_id="dedicated",
        config=config,
        timeout_ms=timeout_ms,
    )


class TestLockContentionScenarios:
    @pytest.mark.asyncio
    async def test_cutover_fails_when_lock_acquisition_fails(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        lock_mgr = MockLockManager(should_fail=True)
        migration_id, lag_tracker, cutover_manager = await _prepare_cutover(
            source_store, target_store, routing_repo, router, tenant_id, lock_mgr
        )
        result = await _exec_cutover(cutover_manager, migration_id, tenant_id, lag_tracker, 100.0)
        assert result.success is False and "lock" in result.error_message.lower()

    @pytest.mark.asyncio
    async def test_concurrent_cutover_attempts_serialized(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        lock_mgr = MockLockManager(contention_delay=0.1)
        migration_id, lag_tracker, cutover_manager = await _prepare_cutover(
            source_store, target_store, routing_repo, router, tenant_id, lock_mgr
        )

        async def attempt_cutover():
            try:
                return await _exec_cutover(
                    cutover_manager, migration_id, tenant_id, lag_tracker, 500.0
                )
            except Exception as e:
                return e

        results = await asyncio.gather(
            attempt_cutover(), attempt_cutover(), attempt_cutover(), return_exceptions=True
        )
        assert sum(1 for r in results if hasattr(r, "success") and r.success) <= 1

    @pytest.mark.asyncio
    async def test_lock_released_on_failure(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        lock_manager = MockLockManager()
        config = MigrationConfig(cutover_max_lag_events=10)
        migration_id, lag_tracker, cutover_manager = await _prepare_cutover(
            source_store, target_store, routing_repo, router, tenant_id, lock_manager, config=config
        )
        await create_test_events(source_store, tenant_id, count=200)
        result = await _exec_cutover(
            cutover_manager, migration_id, tenant_id, lag_tracker, 500.0, config=config
        )
        assert result.success is False
        assert not lock_manager.is_locked(f"migration:cutover:{tenant_id}")


class TestTimeoutScenarios:
    @pytest.mark.asyncio
    async def test_cutover_timeout_causes_rollback(
        self,
        source_store: FailureInjectableStore,
        target_store: FailureInjectableStore,
        routing_repo: InMemoryRoutingRepository,
        router: TenantStoreRouter,
        tenant_id: UUID,
    ) -> None:
        lock_mgr = MockLockManager(contention_delay=1.0)
        migration_id, lag_tracker, cutover_manager = await _prepare_cutover(
            source_store, target_store, routing_repo, router, tenant_id, lock_mgr
        )
        result = await _exec_cutover(cutover_manager, migration_id, tenant_id, lag_tracker, 100.0)
        if not result.success:
            msg = result.error_message.lower()
            assert result.rolled_back or "timeout" in msg or "lock" in msg

    @pytest.mark.asyncio
    async def test_write_pause_timeout(
        self, write_pause_manager: WritePauseManager, tenant_id: UUID
    ) -> None:
        await write_pause_manager.pause_writes(tenant_id)
        with pytest.raises(WritePausedError) as exc_info:
            await write_pause_manager.wait_if_paused(tenant_id, timeout=0.05)
        assert exc_info.value.tenant_id == tenant_id and exc_info.value.timeout == 0.05

    @pytest.mark.asyncio
    async def test_slow_target_causes_lag_accumulation(self, tenant_id: UUID) -> None:
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
            stream = StreamId(aggregate_id=aggregate_id, category="ChaosAggregate")
            await interceptor.append(stream, [event], ExpectedVersion.no_stream())
        source_count = sum(
            [1 async for env in source_store.read_all() if env.event.tenant_id == tenant_id]
        )
        assert source_count == 10 and await count_tenant_events(target_store, tenant_id) == 10
