"""Snapshotting collaborators for the aggregate repository.

Snapshots are disposable optimizations, never the source of truth (ADR 0021,
superseding ADR 0017): every automatic-path failure degrades to full event
replay instead of raising. Four collaborators replace the former
AggregateSnapshotManager:

- SnapshotPolicy (EveryNEvents / Never): *when* to snapshot — pure predicate.
- SnapshotScheduler (ImmediateScheduler / BackgroundScheduler): *how* the
  write executes — sync-and-swallow or fire-and-forget with a join point.
- take_snapshot(): the single spelling of snapshot construction. Errors
  propagate; degradation is the scheduler's job, so the manual path
  (AggregateRepository.create_snapshot) stays strict.
- read_valid_snapshot(): load-path fetch + schema validation; all failure
  modes collapse to None.
"""

import logging
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from eventsource.application.background_tasks import BackgroundTaskManager
from eventsource.domain.aggregate import AggregateRoot
from eventsource.ports.snapshots import Snapshot, SnapshotStore

logger = logging.getLogger(__name__)


@runtime_checkable
class SnapshotPolicy(Protocol):
    """Decides *when* a snapshot should be taken. Pure and synchronous."""

    def should_snapshot(
        self, aggregate: AggregateRoot[Any], events_since_snapshot: int
    ) -> bool: ...


@dataclass(frozen=True)
class EveryNEvents:
    """Snapshot at deterministic version boundaries: version % n == 0.

    Keyed off the aggregate version (not events_since_snapshot) so that two
    processes saving the same aggregate agree on where boundaries fall. A
    save jumping the version across a boundary without landing on it takes
    no snapshot — acceptable, snapshots are an optimization.
    """

    n: int

    def __post_init__(self) -> None:
        if self.n <= 0:
            raise ValueError(f"EveryNEvents requires n >= 1, got {self.n}")

    def should_snapshot(self, aggregate: AggregateRoot[Any], events_since_snapshot: int) -> bool:
        return aggregate.version > 0 and aggregate.version % self.n == 0


@dataclass(frozen=True)
class Never:
    """Manual mode: automatic snapshotting disabled."""

    def should_snapshot(self, aggregate: AggregateRoot[Any], events_since_snapshot: int) -> bool:
        return False


async def take_snapshot(
    aggregate: AggregateRoot[Any], aggregate_type: str, store: SnapshotStore
) -> Snapshot:
    """Build and persist a snapshot of the aggregate. The single spelling
    of snapshot construction. Errors propagate to the caller."""
    schema_version = getattr(type(aggregate), "schema_version", 1)
    snapshot = Snapshot(
        aggregate_id=aggregate.aggregate_id,
        aggregate_type=aggregate_type,
        version=aggregate.version,
        state=aggregate._serialize_state(),
        schema_version=schema_version,
        created_at=datetime.now(UTC),
    )
    await store.save_snapshot(snapshot)
    logger.info(
        "Created snapshot for %s/%s at version %d (schema_version=%d)",
        aggregate_type,
        aggregate.aggregate_id,
        snapshot.version,
        schema_version,
    )
    return snapshot


async def read_valid_snapshot(
    store: SnapshotStore,
    aggregate_id: UUID,
    aggregate_type: str,
    aggregate_factory: type[AggregateRoot[Any]],
) -> Snapshot | None:
    """Fetch and validate a snapshot for the load path.

    Store error, missing snapshot, and schema mismatch all collapse to
    None: the repository falls back to full event replay."""
    try:
        snapshot = await store.get_snapshot(aggregate_id, aggregate_type)
    except Exception as e:
        logger.warning(
            "Error loading snapshot for %s/%s: %s. Falling back to event replay.",
            aggregate_type,
            aggregate_id,
            e,
        )
        return None
    if snapshot is None:
        return None
    expected = getattr(aggregate_factory, "schema_version", 1)
    if snapshot.schema_version != expected:
        logger.info(
            "Snapshot schema version mismatch for %s/%s: "
            "snapshot has v%d, aggregate expects v%d. "
            "Falling back to full event replay.",
            aggregate_type,
            aggregate_id,
            snapshot.schema_version,
            expected,
        )
        return None
    logger.debug(
        "Loaded valid snapshot for %s/%s at version %d",
        aggregate_type,
        aggregate_id,
        snapshot.version,
    )
    return snapshot


@runtime_checkable
class SnapshotScheduler(Protocol):
    """Decides *how* a snapshot write executes: inline or in background.

    Every implementation carries the full surface — pending_count/
    await_pending are 0/0 for schedulers with nothing in flight — so no
    caller ever needs to sniff the concrete type."""

    async def schedule(
        self,
        write: Coroutine[Any, Any, Snapshot],
        *,
        aggregate_type: str,
        aggregate_id: UUID,
    ) -> Snapshot | None: ...

    @property
    def pending_count(self) -> int: ...

    async def await_pending(self) -> int: ...


class ImmediateScheduler:
    """Awaits the write inline; failures are logged and swallowed so a
    snapshot problem never fails a save whose events already committed."""

    async def schedule(
        self,
        write: Coroutine[Any, Any, Snapshot],
        *,
        aggregate_type: str,
        aggregate_id: UUID,
    ) -> Snapshot | None:
        try:
            return await write
        except Exception as e:
            logger.warning(
                "Failed to create snapshot for %s/%s: %s",
                aggregate_type,
                aggregate_id,
                e,
                exc_info=True,
            )
            return None

    @property
    def pending_count(self) -> int:
        return 0

    async def await_pending(self) -> int:
        return 0


class BackgroundScheduler:
    """Fire-and-forget via BackgroundTaskManager; await_pending() is the
    join point for tests and graceful shutdown."""

    def __init__(self) -> None:
        self._tasks = BackgroundTaskManager()

    async def schedule(
        self,
        write: Coroutine[Any, Any, Snapshot],
        *,
        aggregate_type: str,
        aggregate_id: UUID,
    ) -> Snapshot | None:
        self._tasks.submit(self._guarded(write, aggregate_type, aggregate_id))
        return None

    async def _guarded(
        self,
        write: Coroutine[Any, Any, Snapshot],
        aggregate_type: str,
        aggregate_id: UUID,
    ) -> None:
        try:
            await write
            logger.debug("Background snapshot created for %s/%s", aggregate_type, aggregate_id)
        except Exception as e:
            logger.warning(
                "Background snapshot creation failed for %s/%s: %s",
                aggregate_type,
                aggregate_id,
                e,
                exc_info=True,
            )

    @property
    def pending_count(self) -> int:
        return self._tasks.pending_count

    async def await_pending(self) -> int:
        return await self._tasks.await_all()


__all__ = [
    "BackgroundScheduler",
    "EveryNEvents",
    "ImmediateScheduler",
    "Never",
    "SnapshotPolicy",
    "SnapshotScheduler",
    "read_valid_snapshot",
    "take_snapshot",
]
