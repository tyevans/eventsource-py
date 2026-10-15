"""Test harness providing pre-configured in-memory infrastructure."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from typing import Any, TypeVar
from uuid import UUID

from eventsource.adapters.memory import (
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
    InMemoryOutboxRepository,
    InMemoryReadModelRepository,
    InMemorySnapshotStore,
)
from eventsource.adapters.memory.bus import InMemoryEventBus
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.aggregates import AggregateRepository
from eventsource.application.projections.replay import ReplayReport, replay
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion
from eventsource.ports.handlers import EventSubscriber
from eventsource.ports.readmodels.model import ReadModel
from eventsource.testing.assertions import EventAssertions

TEvent = TypeVar("TEvent", bound=DomainEvent)
TAggregate = TypeVar("TAggregate", bound=AggregateRoot[Any])
TModel = TypeVar("TModel", bound=ReadModel)


class InMemoryTestHarness:
    """Pre-configured test infrastructure bundling in-memory stores, bus, and repositories."""

    def __init__(self) -> None:
        self._event_store = InMemoryEventStore()
        self._event_bus = InMemoryEventBus(enable_tracing=False)
        self._checkpoint_repo = InMemoryCheckpointRepository(enable_tracing=False)
        self._dlq_repo = InMemoryDLQRepository(enable_tracing=False)
        self._outbox_repo = InMemoryOutboxRepository(enable_tracing=False)
        self._snapshot_store = InMemorySnapshotStore(enable_tracing=False)

    def __len__(self) -> int:
        return len(self.published_events)

    def __bool__(self) -> bool:
        return True

    def __iter__(self) -> Iterator[DomainEvent]:
        return iter(self.published_events)

    def __contains__(self, target: object) -> bool:
        return target in self.assertions

    @property
    def event_store(self) -> InMemoryEventStore:
        return self._event_store

    @property
    def event_bus(self) -> InMemoryEventBus:
        return self._event_bus

    @property
    def checkpoint_repo(self) -> InMemoryCheckpointRepository:
        return self._checkpoint_repo

    @property
    def dlq_repo(self) -> InMemoryDLQRepository:
        return self._dlq_repo

    @property
    def outbox_repo(self) -> InMemoryOutboxRepository:
        return self._outbox_repo

    @property
    def snapshot_store(self) -> InMemorySnapshotStore:
        return self._snapshot_store

    @property
    def assertions(self) -> EventAssertions:
        return EventAssertions(self.published_events)

    @property
    def published_events(self) -> list[DomainEvent]:
        return self._event_bus.published_events

    async def append_events(
        self, *events: DomainEvent, expected_version: ExpectedVersion | None = None
    ) -> None:
        if not events:
            return
        by_aggregate: dict[tuple[UUID, str], list[DomainEvent]] = {}
        for event in events:
            by_aggregate.setdefault((event.aggregate_id, event.aggregate_type), []).append(event)
        ver = expected_version or ExpectedVersion.any_()
        for (agg_id, agg_type), agg_events in by_aggregate.items():
            await self._event_store.append(StreamId(aggregate_id=agg_id, category=agg_type), agg_events, ver)  # fmt: skip

    async def publish_events(
        self, *events: DomainEvent, persist: bool = False, expected_version: ExpectedVersion | None = None
    ) -> None:  # fmt: skip
        if not events:
            return
        if persist:
            await self.append_events(*events, expected_version=expected_version)
        await self._event_bus.publish(list(events))

    async def replay(
        self, projections: Sequence[EventSubscriber] | EventSubscriber, **kwargs: Any
    ) -> ReplayReport:
        return await replay(self._event_store, projections, **kwargs)

    def create_repository(
        self, aggregate_factory: type[TAggregate], *, snapshot_threshold: int | None = None, with_snapshots: bool = False
    ) -> AggregateRepository[TAggregate]:  # fmt: skip
        use_snaps = with_snapshots or snapshot_threshold is not None
        return AggregateRepository(
            event_store=self._event_store,
            aggregate_factory=aggregate_factory,
            event_publisher=self._event_bus,
            snapshot_store=self._snapshot_store if use_snaps else None,
            snapshot_threshold=snapshot_threshold,
            enable_tracing=False,
        )

    def create_read_model_repository(
        self, model_class: type[TModel]
    ) -> InMemoryReadModelRepository[TModel]:
        return InMemoryReadModelRepository(model_class, enable_tracing=False)

    def reset(self) -> None:
        self._event_store.clear()
        self.__init__()  # type: ignore[misc]

    def clear_published_events(self) -> None:
        self._event_bus.clear_published_events()

    def published_of_type(self, event_type: type[TEvent]) -> list[TEvent]:
        return self._event_bus.published_of_type(event_type)

    def get_events_of_type(self, event_type: type[TEvent]) -> list[TEvent]:
        return self._event_bus.published_of_type(event_type)

    def __repr__(self) -> str:
        return f"InMemoryTestHarness(published={len(self.published_events)})"


__all__ = ["InMemoryTestHarness"]
