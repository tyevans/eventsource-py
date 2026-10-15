"""Snapshotting collaborators for the aggregate repository (ADR 0021)."""

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

try:
    from opentelemetry import metrics as _otel_metrics

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    OTEL_METRICS_AVAILABLE = False
    _otel_metrics = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)


class SnapshotMissReason(StrEnum):
    MISSING = "missing"
    SCHEMA_MISMATCH = "schema_mismatch"
    STORE_ERROR = "store_error"
    DESERIALIZATION_ERROR = "deserialization_error"
    STATE_RESTORE_FAILED = "state_restore_failed"


_MISS_COUNTER_NAME = "eventsource.snapshot.miss"
_meter: Any = None
_miss_counter: Any = None
_miss_tally: Counter[str] = Counter()
_WriteCoro = Coroutine[Any, Any, Snapshot]
_AnyAgg = AggregateRoot[Any]


def _get_miss_counter() -> Any:
    global _meter, _miss_counter
    if _miss_counter is None and OTEL_METRICS_AVAILABLE and _otel_metrics is not None:
        if _meter is None:
            _meter = _otel_metrics.get_meter("eventsource.application.aggregates", version="1.0.0")
        desc = "Snapshot reads that fell back to full event replay, by reason"
        _miss_counter = _meter.create_counter(_MISS_COUNTER_NAME, unit="1", description=desc)
    return _miss_counter


def record_snapshot_miss(reason: SnapshotMissReason, aggregate_type: str) -> None:
    _miss_tally[reason.value] += 1
    if (counter := _get_miss_counter()) is not None:
        counter.add(1, {"reason": reason.value, "aggregate_type": aggregate_type})


def snapshot_miss_counts() -> dict[str, int]:
    return dict(_miss_tally)


def reset_snapshot_miss_counts() -> None:
    _miss_tally.clear()


@runtime_checkable
class SnapshotPolicy(Protocol):
    def should_snapshot(self, aggregate: _AnyAgg, events_since_snapshot: int) -> bool: ...


@dataclass(frozen=True)
class EveryNEvents:
    n: int

    def __post_init__(self) -> None:
        if self.n <= 0:
            raise ValueError(f"EveryNEvents requires n >= 1, got {self.n}")

    def should_snapshot(self, aggregate: _AnyAgg, events_since_snapshot: int) -> bool:
        prev = aggregate.version - events_since_snapshot
        return aggregate.version > 0 and aggregate.version // self.n > prev // self.n


@dataclass(frozen=True)
class Never:
    def should_snapshot(self, aggregate: _AnyAgg, events_since_snapshot: int) -> bool:
        return False


async def take_snapshot(aggregate: _AnyAgg, aggregate_type: str, store: SnapshotStore) -> Snapshot:
    schema_ver, agg_id = getattr(type(aggregate), "schema_version", 1), aggregate.aggregate_id
    state, now = aggregate._serialize_state(), datetime.now(UTC)
    snapshot = Snapshot(agg_id, aggregate_type, aggregate.version, state, schema_ver, now)
    await store.save_snapshot(snapshot)
    logger.info(f"Created snapshot for {aggregate_type}/{agg_id} at v{snapshot.version}")
    return snapshot


async def read_valid_snapshot(
    store: SnapshotStore,
    aggregate_id: UUID,
    aggregate_type: str,
    aggregate_factory: type[_AnyAgg],
) -> Snapshot | None:
    target = f"{aggregate_type}/{aggregate_id}"
    try:
        snapshot = await store.get_snapshot(aggregate_id, aggregate_type)
    except Exception as e:
        is_deser = isinstance(e, SnapshotDeserializationError)
        miss = (
            SnapshotMissReason.DESERIALIZATION_ERROR if is_deser else SnapshotMissReason.STORE_ERROR
        )
        record_snapshot_miss(miss, aggregate_type)
        logger.warning(f"{'Corrupt' if is_deser else 'Error loading'} snapshot for {target}: {e}.")
        return None
    if snapshot is None:
        record_snapshot_miss(SnapshotMissReason.MISSING, aggregate_type)
        return None
    if snapshot.schema_version != getattr(aggregate_factory, "schema_version", 1):
        record_snapshot_miss(SnapshotMissReason.SCHEMA_MISMATCH, aggregate_type)
        logger.info(f"Snapshot schema mismatch for {target}: v{snapshot.schema_version}")
        return None
    logger.debug(f"Loaded valid snapshot for {target} at version {snapshot.version}")
    return snapshot


@runtime_checkable
class SnapshotScheduler(Protocol):
    async def schedule(
        self, write: _WriteCoro, *, aggregate_type: str, aggregate_id: UUID
    ) -> Snapshot | None: ...
    @property
    def pending_count(self) -> int: ...
    async def await_pending(self) -> int: ...


class ImmediateScheduler:
    pending_count = 0

    async def schedule(
        self, write: _WriteCoro, *, aggregate_type: str, aggregate_id: UUID
    ) -> Snapshot | None:
        try:
            return await write
        except Exception as e:
            logger.warning(
                f"Failed to create snapshot for {aggregate_type}/{aggregate_id}: {e}", exc_info=True
            )
            return None

    async def await_pending(self) -> int:
        return 0


class BackgroundScheduler:
    def __init__(self) -> None:
        self._tasks = BackgroundTaskManager()

    async def schedule(
        self, write: _WriteCoro, *, aggregate_type: str, aggregate_id: UUID
    ) -> Snapshot | None:
        self._tasks.submit(self._guarded(write, f"{aggregate_type}/{aggregate_id}"))
        return None

    async def _guarded(self, write: _WriteCoro, target: str) -> None:
        try:
            await write
            logger.debug(f"Background snapshot created for {target}")
        except Exception as e:
            logger.warning(f"Background snapshot creation failed for {target}: {e}", exc_info=True)

    @property
    def pending_count(self) -> int:
        return self._tasks.pending_count

    async def await_pending(self) -> int:
        return await self._tasks.await_all()


__all__ = ["BackgroundScheduler", "EveryNEvents", "ImmediateScheduler", "Never"]
__all__ += ["SnapshotPolicy", "SnapshotScheduler", "read_valid_snapshot", "take_snapshot"]
