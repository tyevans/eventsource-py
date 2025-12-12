# Tutorial 12: Using SQLite for Development and Testing

**Difficulty:** Intermediate
**Progress:** Tutorial 12 of 21 | Phase 3: Production Readiness

SQLite provides zero-configuration persistence for event sourcing, perfect for development, testing, and embedded applications. This tutorial covers in-memory and file-based SQLite backends, pytest fixtures, and migration paths to PostgreSQL.

---

## When to Use SQLite

| Scenario | SQLite | PostgreSQL |
|----------|--------|------------|
| **Local development** | Recommended | Overkill |
| **CI/CD testing** | Recommended | Slower setup |
| **Single-instance production** | Consider | Recommended |
| **Multi-instance production** | Not suitable | Required |
| **Embedded applications** | Recommended | Not suitable |
| **High write concurrency** | Not suitable | Recommended |
| **Edge computing** | Recommended | Not suitable |
| **Desktop applications** | Recommended | Overkill |

**Strengths:** Zero configuration, instant startup, portable single-file storage, embedded in Python.

**Limitations:** Single writer at a time, no network access, limited concurrency for high-traffic systems.

---

## Installation

```bash
pip install eventsource-py[sqlite]
```

```python
from eventsource import SQLITE_AVAILABLE

if SQLITE_AVAILABLE:
    print("SQLite backend is available")
else:
    print("Install with: pip install eventsource-py[sqlite]")
```

---

## In-Memory Databases

Use `:memory:` for fast, isolated test databases that auto-cleanup:

```python
import asyncio
from uuid import uuid4
from eventsource import SQLiteEventStore, AggregateRepository, DomainEvent, register_event
from pydantic import BaseModel


@register_event
class NoteCreated(DomainEvent):
    event_type: str = "NoteCreated"
    aggregate_type: str = "Note"
    title: str
    content: str


async def in_memory_demo():
    """Demonstrate in-memory SQLite usage."""
    # Create in-memory store - database exists only while connection is open
    store = SQLiteEventStore(":memory:")

    # Use async context manager for proper lifecycle
    async with store:
        # Initialize creates the schema
        await store.initialize()

        # Now use it like any event store
        note_id = uuid4()
        event = NoteCreated(
            aggregate_id=note_id,
            aggregate_version=1,
            title="Meeting Notes",
            content="Discuss Q4 roadmap",
        )

        result = await store.append_events(
            aggregate_id=note_id,
            aggregate_type="Note",
            events=[event],
            expected_version=0,
        )

        print(f"Event stored! New version: {result.new_version}")

        # Load it back
        stream = await store.get_events(note_id, "Note")
        print(f"Loaded {len(stream.events)} events")

    # After exiting context manager, database is gone
    print("Database automatically cleaned up")


asyncio.run(in_memory_demo())
```

Always use `async with` for automatic connection management and cleanup.

---

## File-Based Databases

```python
import asyncio
from uuid import uuid4
from eventsource import SQLiteEventStore

async def file_demo():
    db_path = "./my_events.db"

    async with SQLiteEventStore(db_path) as store:
        await store.initialize()
        note_id = uuid4()
        event = NoteCreated(
            aggregate_id=note_id,
            aggregate_version=1,
            title="Persistent Note",
            content="This survives restarts",
        )
        await store.append_events(
            aggregate_id=note_id,
            aggregate_type="Note",
            events=[event],
            expected_version=0,
        )
        return note_id

    # Data persists after context manager exits
    # Reopen and read
    async with SQLiteEventStore(db_path) as store:
        await store.initialize()
        stream = await store.get_events(note_id, "Note")
        print(f"Loaded {len(stream.events)} events")

asyncio.run(file_demo())
```

WAL mode creates `-wal` and `-shm` files. Cleanup helper:

```python
def cleanup_sqlite_db(db_path: str) -> None:
    for suffix in ["", "-wal", "-shm"]:
        path = f"{db_path}{suffix}"
        if os.path.exists(path):
            os.remove(path)
```

---

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | Required | Path to file or `:memory:` |
| `event_registry` | `EventRegistry` | Default registry | Event type lookup for deserialization |
| `wal_mode` | `bool` | `True` | Enable Write-Ahead Logging (not supported for `:memory:`) |
| `busy_timeout` | `int` | `5000` | Milliseconds to wait for locked database |
| `enable_tracing` | `bool` | `True` | OpenTelemetry tracing support |

```python
# WAL mode for better concurrent reads (file-based only)
store = SQLiteEventStore("./events.db", wal_mode=True)

# Disable WAL for in-memory or simpler file handling
store = SQLiteEventStore(":memory:", wal_mode=False)

# Adjust busy timeout for lock contention
store = SQLiteEventStore("./events.db", busy_timeout=10000)
```

---

## Pytest Fixtures for SQLite

```python
# conftest.py
import pytest
import pytest_asyncio
from eventsource import SQLiteEventStore, EventRegistry, AggregateRepository
from my_app.aggregates import NoteAggregate


@pytest_asyncio.fixture
async def event_store():
    """Provide fresh in-memory event store for each test."""
    registry = EventRegistry()
    store = SQLiteEventStore(
        database=":memory:",
        event_registry=registry,
        wal_mode=False,  # WAL not supported for in-memory
    )
    async with store:
        await store.initialize()
        yield store


@pytest_asyncio.fixture
async def note_repository(event_store):
    """Provide repository using the event store fixture."""
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=NoteAggregate,
        aggregate_type="Note",
    )
```

```python
# test_notes.py
import pytest
from uuid import uuid4

class TestNoteAggregate:
    @pytest.mark.asyncio
    async def test_create_note(self, note_repository):
        note_id = uuid4()
        note = note_repository.create_new(note_id)
        note.create("Test Note", "Test content")
        await note_repository.save(note)
        assert note.version == 1
        assert note.state.title == "Test Note"

    @pytest.mark.asyncio
    async def test_load_note(self, note_repository):
        note_id = uuid4()
        note = note_repository.create_new(note_id)
        note.create("Load Test", "Content")
        await note_repository.save(note)

        loaded = await note_repository.load(note_id)
        assert loaded.state.title == "Load Test"

    @pytest.mark.asyncio
    async def test_isolation(self, event_store):
        events = [e async for e in event_store.read_all()]
        assert len(events) == 0  # Fresh database per test
```

---

## Development Workflow

Environment-based configuration for seamless backend switching:

```python
# config.py
import os

def get_event_store_config():
    """Get event store configuration based on environment."""
    env = os.environ.get("APP_ENV", "development")
    if env == "development":
        return {"backend": "sqlite", "database": "./data/events.db"}
    elif env == "testing":
        return {"backend": "sqlite", "database": ":memory:"}
    else:  # production
        return {"backend": "postgresql", "connection_string": os.environ["DATABASE_URL"]}


# infrastructure.py
from eventsource import SQLiteEventStore, PostgreSQLEventStore

async def create_event_store(config: dict):
    """Create event store based on configuration."""
    if config["backend"] == "sqlite":
        store = SQLiteEventStore(config["database"])
        await store.initialize()
        return store
    elif config["backend"] == "postgresql":
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        engine = create_async_engine(config["connection_string"])
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        return PostgreSQLEventStore(session_factory)
    raise ValueError(f"Unknown backend: {config['backend']}")
```

---

## Migration Path to PostgreSQL

Migrate data from SQLite to PostgreSQL:

```python
import asyncio
from eventsource import SQLiteEventStore, PostgreSQLEventStore, ExpectedVersion
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker


async def migrate_to_postgresql(sqlite_path: str, pg_connection_string: str):
    """Migrate events from SQLite to PostgreSQL."""
    async with SQLiteEventStore(sqlite_path) as sqlite_store:
        await sqlite_store.initialize()

        engine = create_async_engine(pg_connection_string)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        pg_store = PostgreSQLEventStore(session_factory)

        count = 0
        async for stored_event in sqlite_store.read_all():
            event = stored_event.event
            await pg_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type=event.aggregate_type,
                events=[event],
                expected_version=ExpectedVersion.ANY,
            )
            count += 1
            if count % 1000 == 0:
                print(f"Migrated {count} events...")

        print(f"Migration complete: {count} events transferred")


asyncio.run(migrate_to_postgresql("./events.db", "postgresql+asyncpg://user:pass@localhost/mydb"))
```

---

## Complete Example

```python
"""
Tutorial 12: Using SQLite for Development and Testing

This example demonstrates SQLite event store configuration.
Run with: python tutorial_12_sqlite.py
"""
import asyncio
import os
from uuid import UUID, uuid4

from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    SQLiteEventStore,
    AggregateRepository,
)


# Events
@register_event
class NoteCreated(DomainEvent):
    event_type: str = "NoteCreated"
    aggregate_type: str = "Note"
    title: str
    content: str


@register_event
class NoteUpdated(DomainEvent):
    event_type: str = "NoteUpdated"
    aggregate_type: str = "Note"
    content: str


# Aggregate

class NoteState(BaseModel):
    note_id: UUID
    title: str = ""
    content: str = ""


class NoteAggregate(AggregateRoot[NoteState]):
    aggregate_type = "Note"

    def _get_initial_state(self) -> NoteState:
        return NoteState(note_id=self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, NoteCreated):
            self._state = NoteState(
                note_id=self.aggregate_id,
                title=event.title,
                content=event.content,
            )
        elif isinstance(event, NoteUpdated):
            if self._state:
                self._state = self._state.model_copy(
                    update={"content": event.content}
                )

    def create(self, title: str, content: str) -> None:
        self.apply_event(NoteCreated(
            aggregate_id=self.aggregate_id,
            title=title,
            content=content,
            aggregate_version=self.get_next_version(),
        ))

    def update(self, content: str) -> None:
        self.apply_event(NoteUpdated(
            aggregate_id=self.aggregate_id,
            content=content,
            aggregate_version=self.get_next_version(),
        ))


# Demo Functions
async def in_memory_demo():
    """Demonstrate in-memory SQLite for testing."""
    print("=== In-Memory SQLite Demo ===\n")

    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=NoteAggregate,
            aggregate_type="Note",
        )

        note_id = uuid4()
        note = repo.create_new(note_id)
        note.create("Meeting Notes", "Discuss Q4 roadmap")
        await repo.save(note)
        print(f"Created note: {note.state.title}")

        loaded = await repo.load(note_id)
        loaded.update("Updated: Discuss Q4 roadmap and budget")
        await repo.save(loaded)
        print(f"Updated note: {loaded.state.content}")
        print(f"Version: {loaded.version}")

    print("Database cleaned up automatically\n")


async def file_based_demo():
    """Demonstrate file-based SQLite for development."""
    print("=== File-Based SQLite Demo ===\n")

    db_path = "./tutorial_notes.db"

    async with SQLiteEventStore(db_path, wal_mode=True) as store:
        await store.initialize()
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=NoteAggregate,
            aggregate_type="Note",
        )

        note_id = uuid4()
        note = repo.create_new(note_id)
        note.create("Development Notes", "Testing SQLite storage")
        await repo.save(note)
        print(f"Note saved to {db_path}")
        print(f"Note ID: {note_id}")

    print(f"Database file exists: {os.path.exists(db_path)}")

    async with SQLiteEventStore(db_path) as store:
        await store.initialize()
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=NoteAggregate,
            aggregate_type="Note",
        )
        loaded = await repo.load(note_id)
        print(f"Reopened and loaded: {loaded.state.title}")

    for suffix in ["", "-wal", "-shm"]:
        path = f"{db_path}{suffix}"
        if os.path.exists(path):
            os.remove(path)
    print("Cleaned up database files\n")


async def configuration_demo():
    """Demonstrate configuration options."""
    print("=== Configuration Options Demo ===\n")

    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()
        print(f"Database: {store.database}")
        print(f"WAL mode: {store.wal_mode}")
        print(f"Busy timeout: {store.busy_timeout}ms")
        print(f"Connected: {store.is_connected}")

    print()

    async with SQLiteEventStore(":memory:", wal_mode=False, busy_timeout=10000, enable_tracing=False) as store:
        await store.initialize()
        print(f"Custom WAL mode: {store.wal_mode}")
        print(f"Custom busy timeout: {store.busy_timeout}ms")
    print()


async def pytest_fixture_pattern():
    """Demonstrate the pattern used in pytest fixtures."""
    print("=== Pytest Fixture Pattern ===\n")

    store = SQLiteEventStore(":memory:")
    async with store:
        await store.initialize()
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=NoteAggregate,
            aggregate_type="Note",
        )

        note = repo.create_new(uuid4())
        note.create("Test Note", "Test content")
        await repo.save(note)
        print(f"Test infrastructure created successfully")
        print(f"Note created with version: {note.version}")

    print("Test cleanup complete\n")


async def main():
    """Run all demos."""
    await in_memory_demo()
    await file_based_demo()
    await configuration_demo()
    await pytest_fixture_pattern()


if __name__ == "__main__":
    asyncio.run(main())
```

Run with: `python tutorial_12_sqlite.py`

---

## Exercises

**Exercise 1:** Create pytest fixtures for an in-memory SQLite event store and repository. Verify test isolation by checking that each test gets a fresh database.

Solution: `docs/tutorials/exercises/solutions/12-1.py`

---

## Next Steps

Continue to [Tutorial 13: Subscription Management](13-subscriptions.md) to learn about coordinated event subscriptions for catch-up processing and real-time event delivery.
