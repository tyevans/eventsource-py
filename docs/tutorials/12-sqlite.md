# Tutorial 12: Running EventSource on SQLite

In Tutorial 11 you deployed EventSource against PostgreSQL. While PostgreSQL is the production workhorse for multi-instance deployments, SQLite provides a zero-infrastructure, single-file, or in-memory option ideal for local development, embedded applications, and blazingly fast test suites.

EventSource includes a dedicated, first-class SQLite adapter: `SQLiteEventStore`.

In this tutorial, you will:
1. Apply the packaged SQLite schemas with `get_all_schemas(backend="sqlite")`.
2. Configure SQLite with Write-Ahead Logging (`WAL` mode) and busy timeouts for concurrent safety.
3. Persist and read back domain events using `SQLiteEventStore` backed by `aiosqlite`.

---

## 1. Install Dependencies

SQLite support is provided via the `sqlite` optional extra, which installs `aiosqlite`:

```bash
pip install "eventsource-py[sqlite]"
```

---

## 2. Setting Up the Database and Schema

Unlike PostgreSQL, SQLite does not require a database server or network port. You initialize an async SQLAlchemy engine pointing to a file path or in-memory URI:

```python
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine
from eventsource.adapters.sql.schemas import get_all_schemas

# Create async engine for SQLite with WAL pragma configuration
engine = create_async_engine(
    "sqlite+aiosqlite:///events.db",
    echo=False,
)

async def init_schema() -> None:
    # Retrieve the bundled SQLite DDL
    sqlite_schema_sql = get_all_schemas(backend="sqlite")

    async with engine.begin() as conn:
        # SQLite executes multiple statements via raw script
        raw_conn = await conn.get_raw_connection()
        await raw_conn.connection.executescript(sqlite_schema_sql)
```

The bundled `sqlite` schema creates all required tables adapted for SQLite:
- `events` (with autoincrementing integer `global_position`)
- `event_outbox`
- `projection_checkpoints`
- `dead_letter_queue`
- `snapshots`

---

## 3. Configuring SQLite for Concurrent Reliability

SQLite defaults to rollback journal mode, which acquires exclusive locks for writes and blocks concurrent readers. For reliable async operations, configure **Write-Ahead Logging (WAL)** and a **busy timeout**:

```python
from sqlalchemy import event
from sqlalchemy.engine import Engine

@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=5000")  # Wait up to 5s before raising busy error
    cursor.close()
```

---

## 4. Persisting and Reading Events with SQLiteEventStore

Now wire up `SQLiteEventStore` to append and read events:

```python
import asyncio
from uuid import uuid4
from pydantic import BaseModel
from eventsource import DomainEvent, ExpectedVersion
from eventsource.adapters.sqlite import SQLiteEventStore

class OrderPlaced(DomainEvent):
    item_id: str
    price: int

async def main() -> None:
    await init_schema()

    store = SQLiteEventStore(engine=engine)
    order_id = uuid4()

    event = OrderPlaced(
        aggregate_id=order_id,
        aggregate_type="Order",
        aggregate_version=1,
        item_id="item-abc",
        price=100,
    )

    # Append event atomically to stream
    await store.append(
        stream_id=str(order_id),
        events=[event],
        expected_version=ExpectedVersion.NO_STREAM,
    )
    print(f"Event written to SQLite stream: {order_id}")

    # Read events back from stream
    stream_events = await store.read_stream(stream_id=str(order_id))
    print(f"Read {len(stream_events)} event(s) from stream. First event: {stream_events[0].event_type}")

    # Read global event feed
    global_events = await store.read_all(limit=10)
    print(f"Read {len(global_events)} event(s) from global feed.")

if __name__ == "__main__":
    asyncio.run(main())
```

---

## Summary

With `SQLiteEventStore`:
- You have complete parity with the `EventStore` interface.
- Tests can run against in-memory SQLite (`sqlite+aiosqlite:///:memory:`) in milliseconds without external Docker dependencies.
- Single-instance deployments or edge workers can store event streams in lightweight, zero-maintenance local files.
