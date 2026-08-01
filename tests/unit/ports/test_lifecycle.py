"""Tests for the `SupportsClose` lifecycle port.

Covers the Protocol contract itself, `SyncStoreFacade.close()` now
checking `isinstance(store, SupportsClose)` instead of duck-typing
`getattr`, and `PostgreSQLEventStore`'s explicit `owns_engine` flag --
the engine-ownership hazard this port was introduced to fix.
"""

from unittest.mock import AsyncMock, MagicMock

from eventsource.adapters.postgresql import PostgreSQLEventStore
from eventsource.adapters.sqlite import SQLiteEventStore
from eventsource.ports.lifecycle import SupportsClose
from eventsource.testing.sync_facade import SyncStoreFacade


class _ClosableStore:
    """Structural implementation: no inheritance."""

    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


class _NonClosableStore:
    """Has no close() at all -- e.g. MemoryEventStore."""


def test_structural_implementation_satisfies_supports_close() -> None:
    assert isinstance(_ClosableStore(), SupportsClose)


def test_store_with_no_close_does_not_satisfy_supports_close() -> None:
    assert not isinstance(_NonClosableStore(), SupportsClose)


def test_sqlite_event_store_satisfies_supports_close() -> None:
    store = SQLiteEventStore(":memory:")
    assert isinstance(store, SupportsClose)


def test_postgresql_event_store_satisfies_supports_close() -> None:
    engine = MagicMock()
    engine.url.database = "testdb"
    store = PostgreSQLEventStore(engine)
    assert isinstance(store, SupportsClose)


class TestSyncStoreFacadeClose:
    def test_close_awaits_close_on_a_capable_store(self) -> None:
        store = _ClosableStore()
        facade = SyncStoreFacade(store)  # type: ignore[arg-type]

        facade.close()

        assert store.closed is True
        assert facade._loop.is_closed()

    def test_close_is_a_noop_on_an_incapable_store(self) -> None:
        store = _NonClosableStore()
        facade = SyncStoreFacade(store)  # type: ignore[arg-type]

        facade.close()  # must not raise

        assert facade._loop.is_closed()

    def test_close_is_idempotent_for_a_capable_store(self) -> None:
        store = _ClosableStore()
        facade = SyncStoreFacade(store)  # type: ignore[arg-type]

        facade.close()
        facade.close()  # must not raise on the already-closed loop

        assert store.closed is True

    def test_close_is_idempotent_for_an_incapable_store(self) -> None:
        store = _NonClosableStore()
        facade = SyncStoreFacade(store)  # type: ignore[arg-type]

        facade.close()
        facade.close()


class TestPostgreSQLEventStoreEngineOwnership:
    def _make_store(self, *, owns_engine: bool) -> tuple[PostgreSQLEventStore, AsyncMock]:
        engine = MagicMock()
        engine.url.database = "testdb"
        engine.dispose = AsyncMock()
        store = PostgreSQLEventStore(engine, owns_engine=owns_engine)
        return store, engine.dispose

    async def test_close_does_not_dispose_engine_by_default(self) -> None:
        store, dispose = self._make_store(owns_engine=False)

        await store.close()

        dispose.assert_not_called()

    async def test_close_disposes_engine_when_owns_engine_true(self) -> None:
        store, dispose = self._make_store(owns_engine=True)

        await store.close()

        dispose.assert_called_once()

    async def test_close_is_idempotent_when_owns_engine_true(self) -> None:
        store, dispose = self._make_store(owns_engine=True)

        await store.close()
        await store.close()

        assert dispose.call_count == 2  # each call disposes; dispose() itself is idempotent

    async def test_close_is_idempotent_when_owns_engine_false(self) -> None:
        store, dispose = self._make_store(owns_engine=False)

        await store.close()
        await store.close()

        dispose.assert_not_called()
