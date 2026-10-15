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

## `InMemoryEventStore` Ergonomics

In addition to implementing `FullEventStore` and `SupportsAsyncContextManager`,
`InMemoryEventStore` provides convenience helpers for tests, fixtures, and
in-process workflows:

- **$O(1)$ membership check (`event_id in store` or `event in store`)**:
  `InMemoryEventStore.__contains__` checks the internal `_event_ids` set in
  $O(1)$ time. Pass either a `UUID` (`event_id in store`) or a `DomainEvent`
  instance (`event in store`, which checks `event.event_id`); any other type
  returns `False`.
- **Stream existence check (`await store.stream_exists(stream) -> bool`)**:
  Returns `True` if `stream` has at least one appended event, or `False` when
  the stream does not exist.
- **Single-event and default-expected `append`**:
  `await store.append(stream, events, expected=ExpectedVersion.any_())` accepts
  either a single `DomainEvent` or a `Sequence[DomainEvent]` for `events`, and
  defaults `expected` to `ExpectedVersion.any_()` when omitted.

```python
from uuid import uuid4
from eventsource import ExpectedVersion, InMemoryEventStore, StreamId

async with InMemoryEventStore() as store:
    stream = StreamId(aggregate_id=uuid4(), category="Order")
    assert await store.stream_exists(stream) is False

    # Append a single DomainEvent using the default ExpectedVersion.any_()
    created = OrderCreated(aggregate_id=stream.aggregate_id, customer_id=uuid4())
    await store.append(stream, created)

    # Append a sequence with an explicit ExpectedVersion
    shipped = OrderShipped(aggregate_id=stream.aggregate_id, tracking_number="TRACK-1")
    await store.append(stream, [shipped], expected=ExpectedVersion.exact(1))

    assert await store.stream_exists(stream) is True
    assert created.event_id in store
    assert created in store
    assert shipped in store
```
