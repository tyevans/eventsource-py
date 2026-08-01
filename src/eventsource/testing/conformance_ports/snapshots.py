"""Conformance suites for the `SnapshotStore` port.

`SnapshotStore` (save/get/delete/exists) is a core capability every
snapshot adapter implements. `SnapshotTypeInvalidation` (bulk delete by
aggregate type) is optional -- a store either implements the Protocol or
it doesn't, with no default body or `NotImplementedError` fallback. As
with `ProjectionCheckpointsConformance`/`SubscriptionPositionsConformance`/
`CheckpointRepositoryConformance` in `checkpoints.py`, each capability gets
its own ABC mixin; `SnapshotStoreConformance` combines both for adapters
(all three ship with the library today) that implement both Protocols.

Subclass `SnapshotConformance` alone for a store that only implements the
core Protocol, or `SnapshotStoreConformance` for one that also implements
`SnapshotTypeInvalidation`, and provide a `store` fixture yielding a fresh
adapter instance.
"""

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from typing import Protocol, runtime_checkable
from uuid import uuid4

import pytest

from eventsource.ports.snapshots import Snapshot, SnapshotStore, SnapshotTypeInvalidation


@runtime_checkable
class _InvalidatableSnapshotStore(SnapshotStore, SnapshotTypeInvalidation, Protocol):
    """Test-only intersection: a store satisfying both Protocols.

    Lets the bulk-invalidation tests below call both `save_snapshot`/
    `get_snapshot` (core) and `delete_snapshots_by_type` (optional
    capability) on the same `store` parameter without `type: ignore`
    scattered through the test bodies.
    """


def make_snapshot(
    *,
    aggregate_type: str = "Order",
    version: int = 1,
    schema_version: int = 1,
    state: dict[str, object] | None = None,
) -> Snapshot:
    return Snapshot(
        aggregate_id=uuid4(),
        aggregate_type=aggregate_type,
        version=version,
        state=state if state is not None else {"status": "open"},
        schema_version=schema_version,
        created_at=datetime.now(UTC),
    )


class SnapshotConformance(ABC):
    """Conformance suite for the core `SnapshotStore` Protocol."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `SnapshotStore`."""
        raise NotImplementedError

    async def test_save_and_get_snapshot_round_trips(self, store: SnapshotStore) -> None:
        snapshot = make_snapshot()
        await store.save_snapshot(snapshot)

        loaded = await store.get_snapshot(snapshot.aggregate_id, snapshot.aggregate_type)

        assert loaded == snapshot

    async def test_get_snapshot_returns_none_when_missing(self, store: SnapshotStore) -> None:
        assert await store.get_snapshot(uuid4(), "Order") is None

    async def test_save_overwrites_existing_snapshot(self, store: SnapshotStore) -> None:
        aggregate_id = uuid4()
        first = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=1,
            state={"status": "open"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        second = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=2,
            state={"status": "shipped"},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await store.save_snapshot(first)
        await store.save_snapshot(second)

        loaded = await store.get_snapshot(aggregate_id, "Order")

        assert loaded == second

    async def test_delete_snapshot_returns_true_when_deleted(self, store: SnapshotStore) -> None:
        snapshot = make_snapshot()
        await store.save_snapshot(snapshot)

        deleted = await store.delete_snapshot(snapshot.aggregate_id, snapshot.aggregate_type)

        assert deleted is True
        assert await store.get_snapshot(snapshot.aggregate_id, snapshot.aggregate_type) is None

    async def test_delete_snapshot_returns_false_when_missing(self, store: SnapshotStore) -> None:
        assert await store.delete_snapshot(uuid4(), "Order") is False

    async def test_snapshot_exists(self, store: SnapshotStore) -> None:
        snapshot = make_snapshot()
        assert await store.snapshot_exists(snapshot.aggregate_id, snapshot.aggregate_type) is False

        await store.save_snapshot(snapshot)

        assert await store.snapshot_exists(snapshot.aggregate_id, snapshot.aggregate_type) is True

    async def test_different_aggregate_types_are_independent(self, store: SnapshotStore) -> None:
        aggregate_id = uuid4()
        order_snapshot = Snapshot(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            version=1,
            state={},
            schema_version=1,
            created_at=datetime.now(UTC),
        )
        await store.save_snapshot(order_snapshot)

        assert await store.get_snapshot(aggregate_id, "Order") == order_snapshot
        assert await store.get_snapshot(aggregate_id, "User") is None

    def test_store_satisfies_snapshot_store_protocol(self, store: SnapshotStore) -> None:
        assert isinstance(store, SnapshotStore)


class SnapshotTypeInvalidationConformance(ABC):
    """Conformance suite for the optional `SnapshotTypeInvalidation` capability."""

    @abstractmethod
    @pytest.fixture
    def store(self) -> object:
        """Yield a fresh adapter instance implementing `SnapshotTypeInvalidation`."""
        raise NotImplementedError

    def test_store_satisfies_snapshot_type_invalidation_protocol(
        self, store: SnapshotTypeInvalidation
    ) -> None:
        assert isinstance(store, SnapshotTypeInvalidation)

    async def test_delete_snapshots_by_type_removes_matching_type_only(
        self, store: _InvalidatableSnapshotStore
    ) -> None:
        order_snapshot = make_snapshot(aggregate_type="Order")
        user_snapshot = make_snapshot(aggregate_type="User")
        await store.save_snapshot(order_snapshot)
        await store.save_snapshot(user_snapshot)

        count = await store.delete_snapshots_by_type("Order")

        assert count == 1
        assert (
            await store.get_snapshot(order_snapshot.aggregate_id, order_snapshot.aggregate_type)
            is None
        )
        assert (
            await store.get_snapshot(user_snapshot.aggregate_id, user_snapshot.aggregate_type)
            == user_snapshot
        )

    async def test_delete_snapshots_by_type_filters_by_schema_version(
        self, store: _InvalidatableSnapshotStore
    ) -> None:
        old = make_snapshot(aggregate_type="Order", schema_version=1)
        new = make_snapshot(aggregate_type="Order", schema_version=2)
        await store.save_snapshot(old)
        await store.save_snapshot(new)

        count = await store.delete_snapshots_by_type("Order", schema_version_below=2)

        assert count == 1
        assert await store.get_snapshot(old.aggregate_id, old.aggregate_type) is None
        assert await store.get_snapshot(new.aggregate_id, new.aggregate_type) == new


class SnapshotStoreConformance(SnapshotConformance, SnapshotTypeInvalidationConformance):
    """Both capabilities: core `SnapshotStore` plus `SnapshotTypeInvalidation`.

    All three snapshot adapters shipped with the library (in-memory,
    SQLite, PostgreSQL) implement both Protocols today.
    """
