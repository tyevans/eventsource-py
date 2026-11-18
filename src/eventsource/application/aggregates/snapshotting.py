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
from eventsource.observability.error_summary import error_summary
from eventsource.ports.snapshots import Snapshot, SnapshotStore

try:
    from opentelemetry import metrics as _otel_metrics
except ImportError:
    _otel_metrics = None  # type: ignore[assignment]
logger = logging.getLogger(__name__)


class SnapshotMissReason(StrEnum):
    MISSING, SCHEMA_MISMATCH, STORE_ERROR = "missing", "schema_mismatch", "store_error"
    DESERIALIZATION_ERROR, STATE_RESTORE_FAILED = "deserialization_error", "state_restore_failed"
    IDENTITY_MISMATCH, VERSION_AHEAD_OF_STREAM = "identity_mismatch", "version_ahead_of_stream"
    TENANT_MISMATCH = "tenant_mismatch"


_MISS_COUNTER_NAME, _meter, _miss_counter = "eventsource.snapshot.miss", None, None
_miss_tally: Counter[str] = Counter()


def _get_miss_counter() -> Any:
    global _meter, _miss_counter
    if _miss_counter is None and _otel_metrics is not None:
        _meter = _meter or _otel_metrics.get_meter("eventsource.application.aggregates", version="1.0.0")  # fmt: skip
        _miss_counter = _meter.create_counter(_MISS_COUNTER_NAME, unit="1", description="Snapshot reads that fell back to full event replay, by reason")  # fmt: skip
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
    def should_snapshot(self, aggregate: AggregateRoot[Any], events_since_snapshot: int) -> bool: ...  # fmt: skip


@dataclass(frozen=True)
class EveryNEvents:
    n: int

    def __post_init__(self) -> None:
        if self.n <= 0:
            raise ValueError(f"EveryNEvents requires n >= 1, got {self.n}")

    def should_snapshot(self, aggregate: AggregateRoot[Any], events_since_snapshot: int) -> bool:
        return aggregate.version > 0 and aggregate.version // self.n > (aggregate.version - events_since_snapshot) // self.n  # fmt: skip


@dataclass(frozen=True)
class Never:
    def should_snapshot(self, aggregate: AggregateRoot[Any], events_since_snapshot: int) -> bool:
        return False


async def take_snapshot(aggregate: AggregateRoot[Any], aggregate_type: str, store: SnapshotStore, *, tenant: UUID | None = None) -> Snapshot:  # fmt: skip
    snapshot = Snapshot(aggregate.aggregate_id, aggregate_type, aggregate.version, aggregate._serialize_state(), getattr(type(aggregate), "schema_version", 1), datetime.now(UTC), tenant_id=tenant)  # fmt: skip
    await store.save_snapshot(snapshot)
    logger.info(f"Created snapshot for {aggregate_type}/{aggregate.aggregate_id} at v{snapshot.version}")  # fmt: skip
    return snapshot


async def read_valid_snapshot(store: SnapshotStore, aggregate_id: UUID, aggregate_type: str, aggregate_factory: type[AggregateRoot[Any]], *, tenant: UUID | None = None) -> Snapshot | None:  # fmt: skip
    target = f"{aggregate_type}/{aggregate_id}"
    try:
        snapshot = await store.get_snapshot(aggregate_id, aggregate_type)
    except Exception as e:
        is_deser = isinstance(e, SnapshotDeserializationError)
        record_snapshot_miss(SnapshotMissReason.DESERIALIZATION_ERROR if is_deser else SnapshotMissReason.STORE_ERROR, aggregate_type)  # fmt: skip
        logger.warning(f"{'Corrupt' if is_deser else 'Error loading'} snapshot for {target}: {error_summary(e)}.")  # fmt: skip
        return None
    if snapshot is None or snapshot.schema_version != getattr(aggregate_factory, "schema_version", 1):  # fmt: skip
        record_snapshot_miss(SnapshotMissReason.MISSING if snapshot is None else SnapshotMissReason.SCHEMA_MISMATCH, aggregate_type)  # fmt: skip
        if snapshot is not None:
            logger.info(f"Snapshot schema mismatch for {target}: v{snapshot.schema_version}")
        return None
    if snapshot.aggregate_id != aggregate_id or snapshot.aggregate_type != aggregate_type:
        record_snapshot_miss(SnapshotMissReason.IDENTITY_MISMATCH, aggregate_type)
        logger.warning(f"Snapshot identity mismatch for {target}: ignoring snapshot. Falling back to full event replay.")  # fmt: skip
        return None
    if tenant is not None and snapshot.tenant_id != tenant:
        record_snapshot_miss(SnapshotMissReason.TENANT_MISMATCH, aggregate_type)
        logger.info(f"Snapshot for {target} is not owned by tenant: falling back to full event replay.")  # fmt: skip
        return None
    logger.debug(f"Loaded valid snapshot for {target} at version {snapshot.version}")
    return snapshot


@runtime_checkable
class SnapshotScheduler(Protocol):
    async def schedule(self, write: Coroutine[Any, Any, Snapshot], *, aggregate_type: str, aggregate_id: UUID) -> Snapshot | None: ...  # fmt: skip
    @property
    def pending_count(self) -> int: ...
    async def await_pending(self) -> int: ...


class ImmediateScheduler:
    pending_count = 0

    async def schedule(self, write: Coroutine[Any, Any, Snapshot], *, aggregate_type: str, aggregate_id: UUID) -> Snapshot | None:  # fmt: skip
        try:
            return await write
        except Exception as e:
            logger.warning(f"Failed to create snapshot for {aggregate_type}/{aggregate_id}: {error_summary(e)}")  # fmt: skip
            return None

    async def await_pending(self) -> int:
        return 0


class BackgroundScheduler:
    def __init__(self, max_pending: int = 1000) -> None:
        self._tasks = BackgroundTaskManager(max_pending=max_pending)

    async def schedule(self, write: Coroutine[Any, Any, Snapshot], *, aggregate_type: str, aggregate_id: UUID) -> Snapshot | None:  # fmt: skip
        await self._tasks.submit_or_run_inline(self._guarded(write, f"{aggregate_type}/{aggregate_id}"))  # fmt: skip
        return None

    async def _guarded(self, write: Coroutine[Any, Any, Snapshot], target: str) -> None:
        try:
            await write
            logger.debug(f"Background snapshot created for {target}")
        except Exception as e:
            logger.warning(f"Background snapshot creation failed for {target}: {error_summary(e)}")

    @property
    def pending_count(self) -> int:
        return self._tasks.pending_count

    async def await_pending(self) -> int:
        return await self._tasks.await_all()


__all__ = ["BackgroundScheduler", "EveryNEvents", "ImmediateScheduler", "Never", "SnapshotPolicy", "SnapshotScheduler", "read_valid_snapshot", "take_snapshot"]  # fmt: skip
