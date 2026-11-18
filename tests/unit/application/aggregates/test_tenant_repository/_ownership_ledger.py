"""Shared ledger aggregate, recording store, and seeding helpers for ownership-load tests (ADR-0113)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from uuid import UUID

from pydantic import BaseModel

from eventsource import DomainEvent, tenant_scope
from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates.repository import AggregateRepository
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event_registry import EventRegistry, register_event
from eventsource.ports.envelopes import EventEnvelope, StreamReadOptions
from eventsource.ports.snapshots import Snapshot

_REGISTRY = EventRegistry()


class LedgerState(BaseModel):
    total: int = 0


@register_event(registry=_REGISTRY)
class LedgerOpened(DomainEvent):
    aggregate_type: str = "Ledger"


@register_event(registry=_REGISTRY)
class LedgerDeposited(DomainEvent):
    aggregate_type: str = "Ledger"
    amount: int


class Ledger(AggregateRoot[LedgerState]):
    __test__ = False
    aggregate_type = "Ledger"

    def _get_initial_state(self) -> LedgerState:
        return LedgerState()

    def _apply(self, event: DomainEvent) -> None:
        if self._state is None:
            self._state = self._get_initial_state()
        if isinstance(event, LedgerDeposited):
            self._state = self._state.model_copy(update={"total": self._state.total + event.amount})

    def open(self) -> None:
        self.create_event(LedgerOpened)

    def deposit(self, amount: int) -> None:
        self.create_event(LedgerDeposited, amount=amount)


class RecordingEventStore(InMemoryEventStore):
    """Records the read options of every stream read, to show which reads a load made."""

    def __init__(self) -> None:
        super().__init__(event_registry=_REGISTRY)
        self.read_options: list[StreamReadOptions | None] = []

    async def read_stream(
        self, stream: StreamId, options: StreamReadOptions | None = None
    ) -> AsyncIterator[EventEnvelope]:
        self.read_options.append(options)
        async for envelope in super().read_stream(stream, options):
            yield envelope


class OwnershipFixture:
    """One event store and snapshot store shared by both tenants, as in production."""

    def __init__(self) -> None:
        self.events = RecordingEventStore()
        self.snapshots = InMemorySnapshotStore()
        self.base: AggregateRepository[Ledger] = AggregateRepository(
            self.events, Ledger, snapshot_store=self.snapshots
        )


async def seed_owned_stream(fx: OwnershipFixture, tenant: UUID, aggregate_id: UUID) -> None:
    """Create a two-event stream owned by ``tenant`` through the public repository."""
    async with tenant_scope(tenant):
        ledger = fx.base.create_new(aggregate_id)
        ledger.open()
        ledger.deposit(5)
        await fx.base.save(ledger)


async def store_snapshot(fx: OwnershipFixture, aggregate_id: UUID, tenant_id: UUID | None) -> None:
    """Store a version-1 snapshot of the ledger with the given tag, through the port."""
    snapshot = Snapshot(
        aggregate_id=aggregate_id,
        aggregate_type="Ledger",
        version=1,
        state={"total": 0},
        schema_version=1,
        created_at=datetime.now(UTC),
        tenant_id=tenant_id,
    )
    await fx.snapshots.save_snapshot(snapshot)


__all__ = [
    "Ledger",
    "OwnershipFixture",
    "RecordingEventStore",
    "seed_owned_stream",
    "store_snapshot",
]
