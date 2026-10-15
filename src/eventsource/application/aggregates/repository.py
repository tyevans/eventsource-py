"""Repository pattern for event-sourced aggregates."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Literal
from uuid import UUID

from eventsource.application.aggregates.repository_query import AggregateRepositoryQueryMixin
from eventsource.application.aggregates.repository_save import AggregateRepositorySaveMixin
from eventsource.application.aggregates.repository_snapshots import (
    AggregateRepositorySnapshotMixin,
)
from eventsource.application.aggregates.snapshotting import (
    BackgroundScheduler,
    EveryNEvents,
    ImmediateScheduler,
    Never,
    SnapshotPolicy,
    SnapshotScheduler,
)
from eventsource.domain import StreamId
from eventsource.domain.aggregate import AggregateRoot
from eventsource.observability import Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_VERSION,
)
from eventsource.ports.bus import EventPublisher
from eventsource.ports.store import AggregateStore

if TYPE_CHECKING:
    from eventsource.ports.snapshots import SnapshotStore

logger = logging.getLogger(__name__)


class AggregateRepository[TAggregate: AggregateRoot[Any]](
    AggregateRepositoryQueryMixin[TAggregate],
    AggregateRepositorySaveMixin[TAggregate],
    AggregateRepositorySnapshotMixin[TAggregate],
):
    """Repository coordinating aggregate loading, optimistic-locked saving, snapshots, and publishing."""

    def __init__(
        self,
        event_store: AggregateStore,
        aggregate_factory: type[TAggregate],
        event_publisher: EventPublisher | None = None,
        snapshot_store: SnapshotStore | None = None,
        snapshot_threshold: int | None = None,
        snapshot_mode: Literal["sync", "background", "manual"] = "sync",
        snapshot_policy: SnapshotPolicy | None = None,
        snapshot_scheduler: SnapshotScheduler | None = None,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
    ) -> None:
        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
        self._event_store, self._aggregate_factory = event_store, aggregate_factory
        self._event_publisher, self._snapshot_store = event_publisher, snapshot_store
        self._aggregate_type = self._infer_aggregate_type(aggregate_factory)
        self._snapshot_threshold, self._snapshot_mode = snapshot_threshold, snapshot_mode

        if (snapshot_policy is not None or snapshot_scheduler is not None) and (
            snapshot_threshold is not None or snapshot_mode != "sync"
        ):
            raise ValueError(
                "Pass either snapshot_mode/snapshot_threshold or "
                "snapshot_policy/snapshot_scheduler, not both."
            )
        if snapshot_policy is not None:
            self._snapshot_policy: SnapshotPolicy = snapshot_policy
        elif snapshot_mode != "manual" and snapshot_threshold is not None:
            self._snapshot_policy = EveryNEvents(snapshot_threshold)
        else:
            self._snapshot_policy = Never()
        if snapshot_scheduler is not None:
            self._snapshot_scheduler: SnapshotScheduler = snapshot_scheduler
        elif snapshot_mode == "background":
            self._snapshot_scheduler = BackgroundScheduler()
        else:
            self._snapshot_scheduler = ImmediateScheduler()

    def _infer_aggregate_type(self, factory: type[TAggregate]) -> str:
        if hasattr(factory, "aggregate_type"):
            inferred = factory.aggregate_type
            if isinstance(inferred, str) and inferred not in ("", "Unknown"):
                return inferred
        factory_name = factory.__name__ if hasattr(factory, "__name__") else str(factory)
        raise ValueError(
            f"Cannot infer aggregate_type from {factory_name}. "
            f"Add an 'aggregate_type' class attribute to your aggregate class:\n"
            f"     class {factory_name}(DeclarativeAggregate[...]):\n"
            f'         aggregate_type = "YourTypeName"'
        )

    def _stream(self, aggregate_id: UUID) -> StreamId:
        return StreamId(aggregate_id=aggregate_id, category=self._aggregate_type)

    @property
    def aggregate_type(self) -> str:
        return self._aggregate_type

    @property
    def event_store(self) -> AggregateStore:
        return self._event_store

    @property
    def event_publisher(self) -> EventPublisher | None:
        return self._event_publisher

    async def execute(
        self, aggregate_id: UUID, command: Any, *more_commands: Any, create_if_missing: bool = True
    ) -> TAggregate:
        """Load (or create) an aggregate, execute one or more commands on it, and save."""
        agg = await (self.load_or_create(aggregate_id) if create_if_missing else self.load(aggregate_id))  # fmt: skip
        for cmd in (command, *more_commands):
            if callable(cmd) and not isinstance(cmd, type):
                cmd(agg)
            else:
                runner = getattr(agg, "execute", None) or getattr(agg, "apply_command", None)
                if not callable(runner):
                    raise TypeError(
                        f"{type(agg).__name__} does not support execute(); "
                        "pass a callable (e.g. lambda agg: agg.do_something()) or use "
                        "an aggregate supporting execute(command) or apply_command(command), "
                        "such as DeciderAggregate."
                    )
                runner(cmd)
        await self.save(agg)
        return agg


__all__ = [
    "ATTR_AGGREGATE_ID",
    "ATTR_AGGREGATE_TYPE",
    "ATTR_EVENT_COUNT",
    "ATTR_VERSION",
    "AggregateRepository",
]
