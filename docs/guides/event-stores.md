# Event Store Lifecycle and Async Context Managers

All built-in event stores (`InMemoryEventStore`, `SQLiteEventStore`,
`PostgreSQLEventStore`) and snapshot stores (`InMemorySnapshotStore`,
`SQLiteSnapshotStore`, `PostgreSQLSnapshotStore`) implement the
`SupportsAsyncContextManager` and `SupportsClose` protocols from
`eventsource.ports.lifecycle`.

Using `async with store:` guarantees that owned database connections and
background worker threads are deterministically closed when exiting the block.

For choosing a backend and configuring schemas, see
[Choose an Event Store Backend](../how-to/choose-an-event-store-backend.md) and
[Database Schema](database-schema.md).

## Managing store lifecycle with `async with store:`

Connection-owning adapters such as `SQLiteEventStore` and `SQLiteSnapshotStore`
open an `aiosqlite.Connection` lazily on first use. Wrap the store in
`async with` so `await store.close()` runs automatically on scope exit:

```python
from uuid import uuid4
from eventsource import (
    AggregateRepository,
    ExpectedVersion,
    SQLiteEventStore,
    StreamId,
)
from eventsource.adapters.sqlite import SQLiteSnapshotStore


async def run_with_sqlite() -> None:
    async with (
        SQLiteEventStore(":memory:") as event_store,
        SQLiteSnapshotStore(":memory:") as snapshot_store,
    ):
        repo = AggregateRepository(
            event_store=event_store,
            aggregate_factory=OrderAggregate,
            snapshot_store=snapshot_store,
            snapshot_threshold=50,
        )
        order_id = uuid4()
        order = repo.create_new(order_id)
        order.create(customer_id=uuid4())
        await repo.save(order)
    # Both aiosqlite connections and their worker threads are closed here.
```

You can also call `await store.close()` directly in application shutdown hooks
(such as FastAPI lifespan handlers or pytest fixture finalizers). `close()` is
idempotent: calling it multiple times or on a store that was never opened is a
safe no-op.

## Ownership semantics across backends

`SupportsClose` and `SupportsAsyncContextManager` follow a strict ownership
contract: `close()` only tears down resources that the store itself created or
was explicitly given ownership of.

| Store Adapter | Default Ownership | What `close()` / `async with` Does |
| --- | --- | --- |
| `SQLiteEventStore` | Owns its lazy `aiosqlite.Connection` | Closes the `aiosqlite` connection and stops its worker thread |
| `SQLiteSnapshotStore` | Owns its lazy `aiosqlite.Connection` | Closes the `aiosqlite` connection and stops its worker thread |
| `PostgreSQLEventStore` | Caller owns `AsyncEngine` (`owns_engine=False`) | No-op by default; disposes `engine` only when constructed with `owns_engine=True` |
| `PostgreSQLSnapshotStore` | Caller owns `AsyncEngine` (`owns_engine=False`) | No-op by default; disposes `engine` only when constructed with `owns_engine=True` |
| `InMemoryEventStore` | Pure Python memory | No-op (`SupportsAsyncContextManager` supported for uniform wiring) |
| `InMemorySnapshotStore` | Pure Python memory | No-op (`SupportsAsyncContextManager` supported for uniform wiring) |

When a `PostgreSQLEventStore` or `PostgreSQLSnapshotStore` is the sole user of an
`AsyncEngine` and should dispose the connection pool on exit, pass
`owns_engine=True`:

```python
from sqlalchemy.ext.asyncio import create_async_engine
from eventsource import PostgreSQLEventStore

engine = create_async_engine("postgresql+asyncpg://localhost/app")

async with PostgreSQLEventStore(engine, owns_engine=True) as store:
    ...
# engine.dispose() has been awaited on exit
```

## Runtime capability checks with `SupportsAsyncContextManager`

Generic infrastructure code that accepts any `FullEventStore` or `SnapshotStore`
can check `SupportsAsyncContextManager` or `SupportsClose` structurally at
runtime without importing concrete adapter classes:

```python
from eventsource.ports.lifecycle import SupportsAsyncContextManager, SupportsClose

assert isinstance(store, SupportsClose)
assert isinstance(store, SupportsAsyncContextManager)
```

## Unclosed store diagnostics (`ResourceWarning`)

If a `SQLiteEventStore` or `SQLiteSnapshotStore` opens its underlying
`aiosqlite` connection and is later garbage-collected without `await store.close()`
or `async with store:` having completed, its `__del__` finalizer emits a
`ResourceWarning` identifying the leaked store and database path:

```text
ResourceWarning: Unclosed SQLiteEventStore (':memory:') <SQLiteEventStore object at 0x...>;
call 'await store.close()' or use 'async with'
```

To surface leaked store connections immediately as test failures during
development, configure pytest to treat `ResourceWarning` as an error or run
Python with `-W error::ResourceWarning`:

```bash
uv run pytest -W error::ResourceWarning
```
