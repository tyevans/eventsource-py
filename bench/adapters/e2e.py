"""Composite adapter pairing a store backend with its matching snapshot backend."""

from bench.adapters.base import BenchAdapter
from bench.adapters.snapshots import SNAPSHOT_ADAPTERS
from bench.adapters.stores import STORE_ADAPTERS
from eventsource.ports.snapshots import SnapshotStore
from eventsource.stores.interface import EventStore


class E2EAdapter(BenchAdapter[tuple[EventStore, SnapshotStore]]):
    def __init__(
        self,
        store_adapter: BenchAdapter[EventStore],
        snapshot_adapter: BenchAdapter[SnapshotStore],
    ) -> None:
        self._store = store_adapter
        self._snapshot = snapshot_adapter
        # instance attribute shadows the ClassVar on purpose: name = backend pair
        self.name = store_adapter.name

    async def available(self) -> str | None:
        return await self._store.available() or await self._snapshot.available()

    async def setup(self) -> None:
        await self._store.setup()
        await self._snapshot.setup()

    async def teardown(self) -> None:
        await self._snapshot.teardown()
        await self._store.teardown()

    async def create(self) -> tuple[EventStore, SnapshotStore]:
        return (await self._store.create(), await self._snapshot.create())

    async def destroy(self, resource: tuple[EventStore, SnapshotStore]) -> None:
        store, snapshot_store = resource
        await self._store.destroy(store)
        await self._snapshot.destroy(snapshot_store)


def make_e2e_adapters() -> list[E2EAdapter]:
    adapters = []
    for backend, store_cls in STORE_ADAPTERS.items():
        snapshot_cls = SNAPSHOT_ADAPTERS.get(backend)
        if snapshot_cls is not None:
            adapters.append(E2EAdapter(store_cls(), snapshot_cls()))
    return adapters
