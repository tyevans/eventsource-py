"""In-memory harness for cutover safety tests, driven through the public coordinator API."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from eventsource.adapters.memory import InMemoryEventStore
from eventsource.adapters.memory.locks import InMemoryLockManager
from eventsource.application.migration.coordinator import MigrationCoordinator
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.application.migration.router import TenantStoreRouter
from eventsource.domain import StreamId
from eventsource.ports.migration.models import (  # fmt: skip
    Migration,
    MigrationConfig,
    MigrationPhase,
    TenantMigrationState,
)
from eventsource.ports.positions import ExpectedVersion
from tests.unit.application.migration.test_migration_audit_log._fakes import FakeAuditLogRepository
from tests.unit.application.migration.test_phase3_integration._events import OrderCreated
from tests.unit.application.migration.test_phase3_integration._mocks import (
    InMemoryPositionMappingRepository,
)
from tests.unit.application.migration.test_phase3_integration._repositories import (
    InMemoryCheckpointRepository,
    InMemoryMigrationRepository,
    InMemoryRoutingRepository,
)

TARGET_ID = "dedicated"
SOURCE_ID = "default"


class GatedRoutingRepository:
    """Routing repository wrapper that holds one named call until released.

    Every other method delegates to the wrapped in-memory repository. The gate
    lets a test start a cutover, pause it at a chosen point, act on the
    migration through the public API, and then let the cutover continue.
    """

    def __init__(self, inner: InMemoryRoutingRepository, *, gate_on: str) -> None:
        self._inner = inner
        self._gate_on = gate_on
        self.armed = False
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def _maybe_gate(self, name: str) -> None:
        if self.armed and name == self._gate_on:
            self.armed = False
            self.reached.set()
            await self.release.wait()

    async def get_routing(self, tenant_id: UUID) -> Any:
        await self._maybe_gate("get_routing")
        return await self._inner.get_routing(tenant_id)

    async def switch_routing(
        self,
        tenant_id: UUID,
        store_id: str,
        state: TenantMigrationState = TenantMigrationState.MIGRATED,
        migration_id: UUID | None = None,
    ) -> None:
        await self._maybe_gate("switch_routing")
        await self._inner.switch_routing(
            tenant_id, store_id, state=state, migration_id=migration_id
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


@dataclass
class Harness:
    tenant_id: UUID
    source: InMemoryEventStore
    target: InMemoryEventStore
    migration_repo: InMemoryMigrationRepository
    routing: GatedRoutingRepository
    router: TenantStoreRouter
    lock_manager: InMemoryLockManager
    audit: FakeAuditLogRepository
    coordinator: MigrationCoordinator
    checkpoint_repo: InMemoryCheckpointRepository
    position_mapping_repo: InMemoryPositionMappingRepository


def build_harness(*, gate_on: str = "switch_routing", with_lock: bool = True) -> Harness:
    source = InMemoryEventStore("source")
    target = InMemoryEventStore("target")
    inner_routing = InMemoryRoutingRepository()
    routing = GatedRoutingRepository(inner_routing, gate_on=gate_on)
    router = TenantStoreRouter(
        default_store=source,
        routing_repo=routing,
        stores={SOURCE_ID: source},
        default_store_id=SOURCE_ID,
        enable_tracing=False,
        write_pause_timeout=0.5,
    )
    router.register_store(TARGET_ID, target)
    migration_repo = InMemoryMigrationRepository()
    audit = FakeAuditLogRepository()
    mapping_repo = InMemoryPositionMappingRepository()
    checkpoint_repo = InMemoryCheckpointRepository()
    lock_manager = InMemoryLockManager()
    coordinator = MigrationCoordinator(
        source_store=source,
        migration_repo=migration_repo,
        routing_repo=routing,
        router=router,
        source_store_id=SOURCE_ID,
        lock_manager=lock_manager if with_lock else None,
        position_mapper=PositionMapper(mapping_repo, enable_tracing=False),
        checkpoint_repo=checkpoint_repo,
        audit_log_repo=audit,
        enable_tracing=False,
    )
    return Harness(
        tenant_id=uuid4(),
        source=source,
        target=target,
        migration_repo=migration_repo,
        routing=routing,
        router=router,
        lock_manager=lock_manager,
        audit=audit,
        coordinator=coordinator,
        checkpoint_repo=checkpoint_repo,
        position_mapping_repo=mapping_repo,
    )


async def create_dual_write_migration(h: Harness, **config_kwargs: Any) -> Migration:
    """Insert a migration already in DUAL_WRITE with its routing record set to match."""
    migration = Migration(
        id=uuid4(),
        tenant_id=h.tenant_id,
        source_store_id=SOURCE_ID,
        target_store_id=TARGET_ID,
        phase=MigrationPhase.DUAL_WRITE,
        config=MigrationConfig(**config_kwargs),
    )
    await h.migration_repo.create(migration)
    await h.routing.set_migration_state(h.tenant_id, TenantMigrationState.DUAL_WRITE, migration.id)
    return migration


async def run_to_dual_write(h: Harness, config: MigrationConfig, event_count: int = 3) -> Migration:
    """Run the real bulk copy through the public API until the migration reaches DUAL_WRITE."""
    for _ in range(event_count):
        await h.source.append(
            StreamId(aggregate_id=uuid4(), category="Order"),
            [OrderCreated(aggregate_id=uuid4(), tenant_id=h.tenant_id)],
            ExpectedVersion.no_stream(),
        )
    migration = await h.coordinator.start_migration(
        tenant_id=h.tenant_id, target_store=h.target, target_store_id=TARGET_ID, config=config
    )
    for _ in range(500):
        current = await h.migration_repo.get(migration.id)
        if current is not None and current.phase == MigrationPhase.DUAL_WRITE:
            return current
        await asyncio.sleep(0.01)
    raise AssertionError("migration did not reach DUAL_WRITE")
