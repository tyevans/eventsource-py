"""Tests for application.aggregates.snapshotting collaborators."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest

from eventsource.adapters.memory.snapshots import InMemorySnapshotStore
from eventsource.application.aggregates.snapshotting import (
    BackgroundScheduler,
    EveryNEvents,
    ImmediateScheduler,
    Never,
    SnapshotPolicy,
    SnapshotScheduler,
    read_valid_snapshot,
    take_snapshot,
)
from eventsource.ports.snapshots import Snapshot
from tests.fixtures.aggregates import OrderAggregate


def _order_at_version(version: int) -> OrderAggregate:
    order = OrderAggregate(uuid4())
    order.create(uuid4())
    for i in range(version - 1):
        order.add_item(f"item-{i}", Decimal("1.00"))
    return order


@pytest.fixture
def fresh_order() -> OrderAggregate:
    return OrderAggregate(uuid4())


@pytest.fixture
def order_at_version_3() -> OrderAggregate:
    return _order_at_version(3)


@pytest.fixture
def order_at_version_100() -> OrderAggregate:
    return _order_at_version(100)


@pytest.fixture
def order_at_version_101() -> OrderAggregate:
    return _order_at_version(101)


class _FailingSnapshotStore(InMemorySnapshotStore):
    async def save_snapshot(self, snapshot: Snapshot) -> None:
        raise RuntimeError("store down")

    async def get_snapshot(self, aggregate_id: UUID, aggregate_type: str) -> Snapshot | None:
        raise RuntimeError("store down")


@pytest.fixture
def failing_store() -> _FailingSnapshotStore:
    return _FailingSnapshotStore()


class TestEveryNEvents:
    def test_true_on_threshold_boundary(self, order_at_version_100):
        assert EveryNEvents(100).should_snapshot(order_at_version_100, 1) is True

    def test_false_off_boundary(self, order_at_version_101):
        assert EveryNEvents(100).should_snapshot(order_at_version_101, 1) is False

    def test_false_at_version_zero(self, fresh_order):
        assert EveryNEvents(1).should_snapshot(fresh_order, 0) is False

    def test_rejects_nonpositive_n(self):
        with pytest.raises(ValueError):
            EveryNEvents(0)
        with pytest.raises(ValueError):
            EveryNEvents(-5)

    def test_satisfies_policy_protocol(self):
        assert isinstance(EveryNEvents(10), SnapshotPolicy)


class TestNever:
    def test_always_false(self, order_at_version_100):
        assert Never().should_snapshot(order_at_version_100, 10_000) is False

    def test_satisfies_policy_protocol(self):
        assert isinstance(Never(), SnapshotPolicy)


class TestTakeSnapshot:
    async def test_writes_snapshot_with_aggregate_state(self, order_at_version_3):
        store = InMemorySnapshotStore()
        snap = await take_snapshot(order_at_version_3, "Order", store)
        assert snap.version == order_at_version_3.version
        assert snap.aggregate_id == order_at_version_3.aggregate_id
        assert snap.schema_version == getattr(type(order_at_version_3), "schema_version", 1)
        stored = await store.get_snapshot(snap.aggregate_id, "Order")
        assert stored == snap

    async def test_store_errors_propagate(self, order_at_version_3, failing_store):
        with pytest.raises(RuntimeError):
            await take_snapshot(order_at_version_3, "Order", failing_store)


class TestReadValidSnapshot:
    async def test_returns_snapshot_when_schema_matches(self, order_at_version_3):
        store = InMemorySnapshotStore()
        snap = await take_snapshot(order_at_version_3, "Order", store)
        got = await read_valid_snapshot(store, snap.aggregate_id, "Order", type(order_at_version_3))
        assert got == snap

    async def test_returns_none_when_missing(self):
        store = InMemorySnapshotStore()
        assert await read_valid_snapshot(store, uuid4(), "Order", OrderAggregate) is None

    async def test_returns_none_on_schema_mismatch(self):
        store = InMemorySnapshotStore()
        await store.save_snapshot(
            Snapshot(
                aggregate_id=(aid := uuid4()),
                aggregate_type="Order",
                version=5,
                state={},
                schema_version=999,
                created_at=datetime.now(UTC),
            )
        )
        assert await read_valid_snapshot(store, aid, "Order", OrderAggregate) is None

    async def test_returns_none_on_store_error(self, failing_store):
        assert await read_valid_snapshot(failing_store, uuid4(), "Order", OrderAggregate) is None


class TestImmediateScheduler:
    async def test_awaits_write_and_returns_snapshot(self, order_at_version_3):
        store = InMemorySnapshotStore()
        sched = ImmediateScheduler()
        snap = await sched.schedule(
            take_snapshot(order_at_version_3, "Order", store),
            aggregate_type="Order",
            aggregate_id=order_at_version_3.aggregate_id,
        )
        assert snap is not None
        assert await store.get_snapshot(snap.aggregate_id, "Order") == snap

    async def test_swallows_write_failure_returns_none(
        self, order_at_version_3, failing_store, caplog
    ):
        sched = ImmediateScheduler()
        result = await sched.schedule(
            take_snapshot(order_at_version_3, "Order", failing_store),
            aggregate_type="Order",
            aggregate_id=order_at_version_3.aggregate_id,
        )
        assert result is None
        assert "Failed to create snapshot" in caplog.text

    async def test_uniform_pending_surface(self):
        sched = ImmediateScheduler()
        assert sched.pending_count == 0
        assert await sched.await_pending() == 0
        assert isinstance(sched, SnapshotScheduler)


class TestBackgroundScheduler:
    async def test_schedules_and_completes(self, order_at_version_3):
        store = InMemorySnapshotStore()
        sched = BackgroundScheduler()
        result = await sched.schedule(
            take_snapshot(order_at_version_3, "Order", store),
            aggregate_type="Order",
            aggregate_id=order_at_version_3.aggregate_id,
        )
        assert result is None  # deferred
        await sched.await_pending()
        assert await store.get_snapshot(order_at_version_3.aggregate_id, "Order") is not None

    async def test_swallows_background_failure(self, order_at_version_3, failing_store, caplog):
        sched = BackgroundScheduler()
        await sched.schedule(
            take_snapshot(order_at_version_3, "Order", failing_store),
            aggregate_type="Order",
            aggregate_id=order_at_version_3.aggregate_id,
        )
        await sched.await_pending()
        assert "Background snapshot creation failed" in caplog.text

    async def test_satisfies_scheduler_protocol(self):
        assert isinstance(BackgroundScheduler(), SnapshotScheduler)
