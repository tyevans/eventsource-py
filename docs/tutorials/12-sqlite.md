# Tutorial 12: Running EventSource on SQLite

In Tutorial 11 you deployed EventSource against PostgreSQL. While PostgreSQL is the production workhorse for multi-instance deployments, SQLite provides a zero-infrastructure, single-file or in-memory option ideal for local development, edge workers, embedded devices, and blazingly fast test suites.

EventSource includes a dedicated, first-class SQLite adapter: `SQLiteEventStore`, backed by `aiosqlite`.

In this tutorial, you will:
1. Understand how `SQLiteEventStore` manages connections, Write-Ahead Logging (WAL mode), and busy timeouts automatically.
2. Apply the packaged SQLite schemas with `get_all_schemas(backend="sqlite")` when using raw SQLAlchemy engines.
3. Persist and read back events from the **Ordering Service** domain.
4. Verify optimistic concurrency control on SQLite using `ExpectedVersion`.

---

## 1. Install Dependencies

SQLite support is provided via the `sqlite` optional extra, which installs `aiosqlite`:

```bash
uv sync --extra sqlite
# or with pip:
pip install "eventsource-py[sqlite]"
```

---

## 2. SQLite Concurrency: WAL Mode & Busy Timeouts

SQLite defaults to rollback journal mode, which acquires an exclusive file lock during writes and blocks concurrent readers. For asynchronous applications, this causes intermittent `sqlite3.OperationalError: database is locked` errors.

Two configurations resolve this:
- **Write-Ahead Logging (`PRAGMA journal_mode = WAL`)**: Allows concurrent readers to proceed uninterrupted while a writer appends to the log.
- **Busy Timeout (`PRAGMA busy_timeout = 5000`)**: Tells SQLite to wait up to 5 seconds for a busy lock to be released before raising an error.

The great news: `SQLiteEventStore` **automatically configures both for you by default** on its connection!

```python
from eventsource.adapters.sqlite import SQLiteEventStore

# wal_mode=True and busy_timeout=5000 are the defaults:
store = SQLiteEventStore(
    database="events.db",  # or ":memory:" for in-process tests
    wal_mode=True,
    busy_timeout=5000,
)
```

Furthermore, on its first operation, `SQLiteEventStore` lazily initializes the SQLite database schema automatically using the bundled SQLite DDL definitions.

---

## 3. Working with Raw SQLAlchemy Engines and Bundled Schemas

If your application uses SQLAlchemy alongside `SQLiteEventStore` (for instance, to manage projection tables or shared connection pools), you can configure WAL mode and apply the bundled schema using `get_all_schemas(backend="sqlite")`:

```python
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.ext.asyncio import create_async_engine
from eventsource.adapters.sql.schemas import get_all_schemas

# Create async engine for SQLite
engine = create_async_engine("sqlite+aiosqlite:///events.db", echo=False)

# Configure WAL mode and busy timeout on every connection
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA busy_timeout=5000")
    cursor.close()

async def init_sqlite_schema() -> None:
    sqlite_schema_sql = get_all_schemas(backend="sqlite")
    async with engine.begin() as conn:
        raw_conn = await conn.get_raw_connection()
        await raw_conn.connection.executescript(sqlite_schema_sql)
```

The bundled `sqlite` schema creates all tables adapted for SQLite's type affinity:
- `events` (with autoincrementing integer `global_position` and JSON-text payload)
- `event_outbox`
- `projection_checkpoints`
- `dead_letter_queue`
- `snapshots`

---

## 4. Persisting and Reading Ordering Service Events

Let's build a runnable script that uses `SQLiteEventStore` to record an order's lifecycle.

Create a file named `order_sqlite.py`:

```python
import asyncio
from datetime import UTC, datetime
from uuid import UUID, uuid4

from eventsource import DomainEvent, ExpectedVersion, OptimisticLockError
from eventsource.adapters.sqlite import SQLiteEventStore


# --- Ordering Domain Events ---

class OrderPlaced(DomainEvent):
    aggregate_type: str = "Order"
    customer_id: str
    item_id: str
    price_cents: int


class OrderPaid(DomainEvent):
    aggregate_type: str = "Order"
    transaction_id: str
    paid_at: str


async def main() -> None:
    # 1. Initialize SQLiteEventStore with a persistent file database
    store = SQLiteEventStore(database="orders.db")

    order_id = uuid4()
    stream_id = str(order_id)
    print(f"Initialized SQLite event store. Stream ID: {stream_id}")

    # 2. Append OrderPlaced event
    placed_event = OrderPlaced(
        aggregate_id=order_id,
        aggregate_version=1,
        customer_id="cust_441",
        item_id="item_ergonomic_chair",
        price_cents=39900,
    )

    result_1 = await store.append(
        stream_id=stream_id,
        events=[placed_event],
        expected_version=ExpectedVersion.NO_STREAM,
    )
    print(f"Appended OrderPlaced. Stream version: {result_1.new_version}")

    # 3. Append OrderPaid event
    paid_event = OrderPaid(
        aggregate_id=order_id,
        aggregate_version=2,
        transaction_id="tx_sqlite_001",
        paid_at=datetime.now(UTC).isoformat(),
    )

    result_2 = await store.append(
        stream_id=stream_id,
        events=[paid_event],
        expected_version=ExpectedVersion.EXACT(1),
    )
    print(f"Appended OrderPaid. Stream version: {result_2.new_version}")

    # 4. Demonstrate Optimistic Concurrency Control
    print("\nTesting optimistic concurrency violation...")
    conflicting_event = OrderPaid(
        aggregate_id=order_id,
        aggregate_version=2,  # Stale version!
        transaction_id="tx_duplicate_stale",
        paid_at=datetime.now(UTC).isoformat(),
    )

    try:
        await store.append(
            stream_id=stream_id,
            events=[conflicting_event],
            expected_version=ExpectedVersion.EXACT(1),  # Current version is already 2
        )
    except OptimisticLockError as err:
        print(f"Caught expected OptimisticLockError: {err}")

    # 5. Read stream back
    print("\nReading aggregate stream:")
    stream_events = await store.read_stream(stream_id)
    for evt in stream_events:
        print(f"  v{evt.aggregate_version}: {evt.event_type} ({evt.occurred_at})")

    # 6. Read global event feed across all aggregates
    print("\nReading global event log:")
    global_events = await store.read_all(limit=5)
    for evt in global_events:
        print(f"  global_pos={evt.global_position}: stream={evt.aggregate_id} ({evt.event_type})")


if __name__ == "__main__":
    asyncio.run(main())
```

Run the script:

```bash
uv run python order_sqlite.py
```

Expected output:
```text
Initialized SQLite event store. Stream ID: edfb5029-79a0-406a-93f5-748439366df0
Appended OrderPlaced. Stream version: 1
Appended OrderPaid. Stream version: 2

Testing optimistic concurrency violation...
Caught expected OptimisticLockError: Stream edfb5029-79a0-406a-93f5-748439366df0: expected version 1, but found 2

Reading aggregate stream:
  v1: OrderPlaced (2026-10-09 21:30:00+00:00)
  v2: OrderPaid (2026-10-09 21:30:01+00:00)

Reading global event log:
  global_pos=1: stream=edfb5029-79a0-406a-93f5-748439366df0 (OrderPlaced)
  global_pos=2: stream=edfb5029-79a0-406a-93f5-748439366df0 (OrderPaid)
```

---

## 5. In-Memory SQLite for Lightning Fast Testing

When writing unit and integration tests, spinning up Docker containers or writing temporary files to disk introduces unnecessary overhead.

You can use an in-memory database by specifying `database=":memory:"`:

```python
import pytest
from eventsource.adapters.sqlite import SQLiteEventStore

@pytest.fixture
def sqlite_store():
    # In-memory SQLite database: zero disk I/O, perfectly isolated per test
    return SQLiteEventStore(database=":memory:")
```

Because `SQLiteEventStore` retains a single open connection for its lifetime, in-memory tables remain accessible across subsequent async calls on that instance without being dropped.

---

## Summary

With `SQLiteEventStore`:
- You have complete interface parity with `PostgreSQLEventStore` and `InMemoryEventStore`.
- WAL mode and busy timeouts are configured out of the box, preventing concurrency lockups.
- Tests can execute against `:memory:` in milliseconds without external services or network ports.
- Edge runtimes and embedded tools can store durable event streams in a single portable file.

Next, continue to [Tutorial 13: Distributed Concurrency with PostgreSQL Advisory Locks](13-locking.md) to manage cross-process coordination, and [Tutorial 14: Snapshotting](14-snapshotting.md) to accelerate aggregate loading.
