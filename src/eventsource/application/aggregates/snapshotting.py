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
from collections import Counter
from collections.abc import Coroutine
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from eventsource.application.background_tasks import BackgroundTaskManager
from eventsource.domain.aggregate import AggregateRoot
from eventsource.domain.exceptions import SnapshotDeserializationError
from eventsource.ports.snapshots import Snapshot, SnapshotStore

# Optional OpenTelemetry import, guarded per ADR 0016: metrics degrade to
# no-ops rather than becoming a hard dependency of the load path.
try:
    from opentelemetry import metrics as _otel_metrics

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    OTEL_METRICS_AVAILABLE = False
    _otel_metrics = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class SnapshotMissReason(StrEnum):
    """Why a snapshot read did not yield a usable snapshot.

    The distinction that matters operationally is permanence, not severity.
    `STORE_ERROR` is usually transient and hits every aggregate at once;
    `DESERIALIZATION_ERROR` is permanent for one row and silently costs a
    full replay on every load of that aggregate until the row is rewritten.
    Collapsed into one bucket -- as they were before -- the second is
    invisible behind the first.
    """

    MISSING = "missing"
    """No snapshot stored. Routine: every aggregate's first load."""

    SCHEMA_MISMATCH = "schema_mismatch"
    """Stored `schema_version` differs from the aggregate's. Routine, and
    usually deliberate -- bumping `schema_version` is how stale snapshots
    are invalidated."""

    STORE_ERROR = "store_error"
    """The store raised something other than `SnapshotDeserializationError`
    -- unreachable backend, timeout, driver error. Not routine."""

    DESERIALIZATION_ERROR = "deserialization_error"
    """The store raised `SnapshotDeserializationError`: the stored state is
    unusable. Not routine, and not self-healing.

    Only stores that raise the documented type land here. In-tree adapters
    surface a corrupt payload later instead, as `STATE_RESTORE_FAILED`."""

    STATE_RESTORE_FAILED = "state_restore_failed"
    """`_restore_from_snapshot()` raised: the payload loaded but the
    aggregate could not rebuild state from it. Same permanence as
    `DESERIALIZATION_ERROR`, detected one step later, and the path a
    corrupt snapshot actually takes with the shipped adapters."""


_MISS_COUNTER_NAME = "eventsource.snapshot.miss"

_meter: Any = None
_miss_counter: Any = None
_miss_tally: Counter[str] = Counter()


def _get_miss_counter() -> Any:
    """Lazily build the OTel counter, or return None when OTel is absent."""
    global _meter, _miss_counter
    if _miss_counter is None and OTEL_METRICS_AVAILABLE and _otel_metrics is not None:
        if _meter is None:
            _meter = _otel_metrics.get_meter("eventsource.application.aggregates", version="1.0.0")
        _miss_counter = _meter.create_counter(
            _MISS_COUNTER_NAME,
            unit="1",
            description="Snapshot reads that fell back to full event replay, by reason",
        )
    return _miss_counter


def record_snapshot_miss(reason: SnapshotMissReason, aggregate_type: str) -> None:
    """Count one snapshot read that fell back to full event replay.

    Instrumentation only: never raises, never changes control flow. Every
    caller degrades to replay whether or not this records anything.
    """
    _miss_tally[reason.value] += 1
    counter = _get_miss_counter()
    if counter is not None:
        counter.add(1, {"reason": reason.value, "aggregate_type": aggregate_type})


def snapshot_miss_counts() -> dict[str, int]:
    """In-process tally of snapshot misses by reason, since last reset.

    Present so the counter is observable without an OpenTelemetry exporter
    -- in tests, and in a REPL when diagnosing why loads got slower.
    """
    return dict(_miss_tally)


def reset_snapshot_miss_counts() -> None:
    """Clear the in-process tally. For tests; does not affect OTel."""
    _miss_tally.clear()


@runtime_checkable
class SnapshotPolicy(Protocol):
    """Decides *when* a snapshot should be taken. Pure and synchronous."""

    def should_snapshot(
        self, aggregate: AggregateRoot[Any], events_since_snapshot: int
    ) -> bool: ...


@dataclass(frozen=True)
class EveryNEvents:
    """Snapshot when a save carries the aggregate past a multiple of n.

    Keyed off the aggregate version rather than a per-repository counter, so
    two processes saving the same aggregate agree on which save owes the
    snapshot.

    Crossing a boundary counts, not landing on one. An aggregate that emits
    several events per command advances its version in strides, and a stride
    that never lands on a multiple of n would otherwise snapshot *never*
    rather than merely late: saves of six events from version 1 leave the
    version permanently odd, so `version % 50 == 0` has no solution at all.
    """

    n: int

    def __post_init__(self) -> None:
        if self.n <= 0:
            raise ValueError(f"EveryNEvents requires n >= 1, got {self.n}")

    def should_snapshot(self, aggregate: AggregateRoot[Any], events_since_snapshot: int) -> bool:
        if aggregate.version <= 0:
            return False
        version_before = aggregate.version - events_since_snapshot
        return aggregate.version // self.n > version_before // self.n


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
    None: the repository falls back to full event replay. Each records a
    distinct `SnapshotMissReason` first, so the degradation is countable
    rather than only greppable (ADR 0017's recorded negative)."""
    try:
        snapshot = await store.get_snapshot(aggregate_id, aggregate_type)
    except SnapshotDeserializationError as e:
        # The store told us *why*: the stored state is unusable. Permanent
        # for this row, so it costs a full replay on every load until the
        # row is rewritten -- unlike a store outage, waiting does not fix it.
        record_snapshot_miss(SnapshotMissReason.DESERIALIZATION_ERROR, aggregate_type)
        logger.warning(
            "Corrupt snapshot for %s/%s: %s. Falling back to event replay.",
            aggregate_type,
            aggregate_id,
            e,
        )
        return None
    except Exception as e:
        record_snapshot_miss(SnapshotMissReason.STORE_ERROR, aggregate_type)
        logger.warning(
            "Error loading snapshot for %s/%s: %s. Falling back to event replay.",
            aggregate_type,
            aggregate_id,
            e,
        )
        return None
    if snapshot is None:
        record_snapshot_miss(SnapshotMissReason.MISSING, aggregate_type)
        return None
    expected = getattr(aggregate_factory, "schema_version", 1)
    if snapshot.schema_version != expected:
        record_snapshot_miss(SnapshotMissReason.SCHEMA_MISMATCH, aggregate_type)
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
