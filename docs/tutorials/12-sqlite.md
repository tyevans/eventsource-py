# Tutorial 12: Using SQLite for Development and Testing

**Estimated Time:** 30-45 minutes
**Difficulty:** Intermediate
**Progress:** Tutorial 12 of 21 | Phase 3: Production Readiness

---

## Prerequisites

Before starting this tutorial, ensure you have:

- Completed [Tutorial 11: Using PostgreSQL Event Store](11-postgresql.md)
- Familiarity with pytest from [Tutorial 8: Testing](08-testing.md)
- Python 3.11+ installed
- eventsource-py installed (`pip install eventsource-py`)

This tutorial introduces **SQLite** as a lightweight backend for event sourcing. SQLite provides zero-configuration persistence that is perfect for development, testing, and embedded applications.

---

## Learning Objectives

By the end of this tutorial, you will be able to:

1. Choose when to use SQLite versus PostgreSQL for your event store
2. Configure in-memory SQLite databases for fast, isolated tests
3. Set up file-based SQLite databases for local development
4. Create pytest fixtures for SQLite event stores
5. Understand WAL mode and its implications for concurrency

---

## When to Use SQLite

SQLite is not just a "toy" database - it powers billions of devices worldwide. For event sourcing, it offers distinct advantages in certain scenarios.

### Decision Matrix

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

### SQLite Strengths

**Zero Configuration**

```bash
# No server to install or configure
pip install eventsource-py[sqlite]

# That's it! SQLite is embedded in Python
```

**Fast Startup**

SQLite opens instantly. No connection pooling, no network latency, no authentication handshakes. This makes it ideal for:

- Unit tests that need fresh databases
- CLI tools that run briefly
- Serverless functions with cold starts

**Portable**

Your entire event store is a single file that you can:

- Copy to another machine
- Email to a colleague
- Include in a bug report
- Back up with a simple `cp` command

**Self-Contained**

Everything runs in your application's process. No external dependencies means:

- Simpler deployment
- Fewer failure modes
- Easier debugging

### SQLite Limitations

**Single Writer**

SQLite allows only one writer at a time. Multiple concurrent writes will queue up, and the `busy_timeout` determines how long they wait before failing.

```python
# Only ONE write can happen at a time
await store.append_events(...)  # Acquires write lock
await store.append_events(...)  # Waits for lock to release
```

**No Network Access**

SQLite is an embedded database. Multiple application instances cannot effectively share a database file over a network. For horizontal scaling, use PostgreSQL.

**Limited Concurrency**

While WAL mode improves read concurrency, SQLite is fundamentally designed for single-process access. High-traffic production systems need PostgreSQL.

---

## Installation

Install eventsource with SQLite support:

```bash
pip install eventsource-py[sqlite]
```

This installs the `aiosqlite` package for async SQLite operations.

Verify the installation:

```python
from eventsource import SQLITE_AVAILABLE

if SQLITE_AVAILABLE:
    print("SQLite backend is available")
else:
    print("Install with: pip install eventsource-py[sqlite]")
```

---

## In-Memory Databases

In-memory SQLite databases are perfect for testing. They are fast, isolated, and automatically cleaned up.

### Creating an In-Memory Store

Use the special path `:memory:` to create an in-memory database:

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

### The Context Manager Pattern

Always use the async context manager (`async with`) for SQLite stores:

```python
# Recommended: Context manager handles connect and cleanup
async with SQLiteEventStore(":memory:") as store:
    await store.initialize()
    # ... use store ...
# Connection automatically closed


# Also valid: Manual lifecycle management
store = SQLiteEventStore(":memory:")
await store.initialize()  # Connects automatically if needed
try:
    # ... use store ...
finally:
    await store.close()
```

The context manager ensures:

1. Connection is established on entry
2. Connection is closed on exit (even if exceptions occur)
3. For in-memory databases: data is automatically discarded

### Why In-Memory for Tests?

In-memory databases provide several testing advantages:

| Benefit | Explanation |
|---------|-------------|
| **Speed** | No disk I/O means extremely fast test execution |
| **Isolation** | Each test gets a completely fresh database |
| **No cleanup** | Database is destroyed when connection closes |
| **Parallelism** | Tests can run in parallel without conflicts |
| **Determinism** | No leftover state from previous test runs |

---

## File-Based Databases

For development and persistence, use a file path instead of `:memory:`:

```python
import asyncio
import os
from uuid import uuid4
from eventsource import SQLiteEventStore


async def file_based_demo():
    """Demonstrate file-based SQLite persistence."""
    db_path = "./my_events.db"

    # First session: create some events
    async with SQLiteEventStore(db_path) as store:
        await store.initialize()

        note_id = uuid4()
        from my_app.events import NoteCreated  # Your event class

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

        print(f"Note {note_id} saved to {db_path}")
        # Save the ID for later
        return note_id

    # Connection closes, but data persists in file


async def reload_demo(note_id):
    """Reload data from file in a new session."""
    db_path = "./my_events.db"

    # Second session: data is still there
    async with SQLiteEventStore(db_path) as store:
        await store.initialize()

        stream = await store.get_events(note_id, "Note")
        print(f"Loaded {len(stream.events)} events from disk")
        print(f"Note title: {stream.events[0].title}")


# Run the demo
note_id = asyncio.run(file_based_demo())
asyncio.run(reload_demo(note_id))
```

### File Cleanup

When using WAL mode (the default), SQLite creates additional files:

```
my_events.db       # Main database file
my_events.db-wal   # Write-ahead log
my_events.db-shm   # Shared memory file (for WAL)
```

To fully remove the database:

```python
import os

def cleanup_sqlite_db(db_path: str) -> None:
    """Remove SQLite database and associated files."""
    for suffix in ["", "-wal", "-shm"]:
        path = f"{db_path}{suffix}"
        if os.path.exists(path):
            os.remove(path)
            print(f"Removed: {path}")
```

---

## Configuration Options

### SQLiteEventStore Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `database` | `str` | Required | Path to file or `:memory:` |
| `event_registry` | `EventRegistry` | Default registry | Event type lookup for deserialization |
| `wal_mode` | `bool` | `True` | Enable Write-Ahead Logging |
| `busy_timeout` | `int` | `5000` | Milliseconds to wait for locked database |
| `enable_tracing` | `bool` | `True` | OpenTelemetry tracing support |

### WAL Mode

Write-Ahead Logging (WAL) improves concurrent read performance:

```python
# WAL mode enabled (default) - better concurrent reads
store = SQLiteEventStore("./events.db", wal_mode=True)

# WAL mode disabled - simpler, but reads block during writes
store = SQLiteEventStore("./events.db", wal_mode=False)
```

**When to enable WAL mode (default):**

- Multiple concurrent readers
- File-based databases
- Better performance for mixed read/write workloads

**When to disable WAL mode:**

- In-memory databases (WAL not supported)
- Single-threaded applications
- When you need simpler file handling (no -wal/-shm files)

### Busy Timeout

When the database is locked, SQLite can wait before failing:

```python
# Wait up to 10 seconds for a locked database
store = SQLiteEventStore("./events.db", busy_timeout=10000)

# Fail immediately if database is locked
store = SQLiteEventStore("./events.db", busy_timeout=0)

# Default: 5 seconds
store = SQLiteEventStore("./events.db")  # busy_timeout=5000
```

The busy timeout is important when:

- Multiple connections might write concurrently
- Long-running read operations might overlap with writes
- You want to detect deadlocks quickly (low timeout)

---

## Pytest Fixtures for SQLite

Creating reusable fixtures makes your tests clean and consistent.

### Basic Event Store Fixture

```python
# conftest.py
import pytest
import pytest_asyncio
from eventsource import SQLiteEventStore, EventRegistry


@pytest_asyncio.fixture
async def event_store():
    """
    Provide an in-memory event store for each test.

    Creates a fresh database for every test, ensuring complete isolation.
    """
    registry = EventRegistry()
    store = SQLiteEventStore(
        database=":memory:",
        event_registry=registry,
        wal_mode=False,  # WAL not supported for in-memory
    )

    async with store:
        await store.initialize()
        yield store

    # Database is automatically cleaned up
```

### Repository Fixture

```python
# conftest.py (continued)
from eventsource import AggregateRepository
from my_app.aggregates import NoteAggregate


@pytest_asyncio.fixture
async def note_repository(event_store):
    """
    Provide a repository using the event store fixture.

    Depends on event_store for proper lifecycle management.
    """
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=NoteAggregate,
        aggregate_type="Note",
    )
```

### Using the Fixtures

```python
# test_notes.py
import pytest
from uuid import uuid4


class TestNoteAggregate:
    """Tests using SQLite fixtures."""

    @pytest.mark.asyncio
    async def test_create_note(self, note_repository):
        """Test creating a note through the repository."""
        note_id = uuid4()
        note = note_repository.create_new(note_id)
        note.create("Test Note", "Test content")
        await note_repository.save(note)

        assert note.version == 1
        assert note.state.title == "Test Note"

    @pytest.mark.asyncio
    async def test_load_note(self, note_repository):
        """Test loading a note from the repository."""
        note_id = uuid4()

        # Create and save
        note = note_repository.create_new(note_id)
        note.create("Load Test", "Content")
        await note_repository.save(note)

        # Load in new instance
        loaded = await note_repository.load(note_id)

        assert loaded.state.title == "Load Test"
        assert loaded.version == 1

    @pytest.mark.asyncio
    async def test_isolation_between_tests(self, event_store):
        """
        Verify each test gets a fresh database.

        This test should NOT see data from other tests.
        """
        # Read all events - should be empty
        events = []
        async for event in event_store.read_all():
            events.append(event)

        assert len(events) == 0, "Database should be empty at test start"
```

### File-Based Fixture for Integration Tests

Sometimes you need persistent storage across test operations:

```python
# conftest.py
import tempfile
import os


@pytest_asyncio.fixture
async def persistent_event_store():
    """
    Provide a file-based event store that persists within the test.

    Useful for testing restart scenarios or file persistence.
    """
    # Create temporary file
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        db_path = f.name

    registry = EventRegistry()
    store = SQLiteEventStore(
        database=db_path,
        event_registry=registry,
        wal_mode=False,  # Avoid extra files
    )

    async with store:
        await store.initialize()
        yield store

    # Cleanup the file after test
    os.unlink(db_path)
```

---

## Development Workflow

SQLite enables a smooth development workflow without external dependencies.

### Local Development Setup

```python
# config.py
import os

def get_event_store_config():
    """Get event store configuration based on environment."""
    env = os.environ.get("APP_ENV", "development")

    if env == "development":
        return {
            "backend": "sqlite",
            "database": "./data/events.db",
        }
    elif env == "testing":
        return {
            "backend": "sqlite",
            "database": ":memory:",
        }
    else:  # production
        return {
            "backend": "postgresql",
            "connection_string": os.environ["DATABASE_URL"],
        }
```

### Factory Function

```python
# infrastructure.py
from eventsource import SQLiteEventStore, PostgreSQLEventStore


async def create_event_store(config: dict):
    """Create event store based on configuration."""
    backend = config["backend"]

    if backend == "sqlite":
        store = SQLiteEventStore(config["database"])
        await store.initialize()
        return store

    elif backend == "postgresql":
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

        engine = create_async_engine(config["connection_string"])
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        store = PostgreSQLEventStore(session_factory)
        # PostgreSQL requires running migrations separately
        return store

    raise ValueError(f"Unknown backend: {backend}")
```

### Switching Between Environments

```python
# main.py
import asyncio
import os
from config import get_event_store_config
from infrastructure import create_event_store


async def main():
    config = get_event_store_config()
    print(f"Using {config['backend']} backend")

    store = await create_event_store(config)

    try:
        # Your application logic here
        # Repository code works identically with both backends
        pass
    finally:
        await store.close()


# Development (default)
asyncio.run(main())

# Testing
# APP_ENV=testing python main.py

# Production
# APP_ENV=production DATABASE_URL=postgresql://... python main.py
```

---

## Migration Path to PostgreSQL

When your application outgrows SQLite, migration is straightforward because both backends implement the same `EventStore` interface.

### Environment-Based Selection

```python
import os
from eventsource import SQLiteEventStore, PostgreSQLEventStore


def create_event_store():
    """Create event store based on environment."""
    if os.environ.get("USE_POSTGRESQL"):
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

        engine = create_async_engine(os.environ["DATABASE_URL"])
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        return PostgreSQLEventStore(session_factory)
    else:
        db_path = os.environ.get("SQLITE_PATH", "./events.db")
        return SQLiteEventStore(db_path)
```

### Data Migration Script

For migrating existing data from SQLite to PostgreSQL:

```python
import asyncio
from eventsource import SQLiteEventStore, PostgreSQLEventStore, ExpectedVersion


async def migrate_to_postgresql(sqlite_path: str, pg_connection_string: str):
    """
    Migrate events from SQLite to PostgreSQL.

    Warning: This is a one-way migration. Back up your data first!
    """
    from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

    # Source: SQLite
    async with SQLiteEventStore(sqlite_path) as sqlite_store:
        await sqlite_store.initialize()

        # Destination: PostgreSQL
        engine = create_async_engine(pg_connection_string)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        pg_store = PostgreSQLEventStore(session_factory)

        # Read all events and copy to PostgreSQL
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


# Run migration
asyncio.run(migrate_to_postgresql(
    "./events.db",
    "postgresql+asyncpg://user:pass@localhost/mydb"
))
```

### Gradual Migration Strategy

1. **Start with SQLite** during development
2. **Run tests** against both backends
3. **Deploy to staging** with PostgreSQL
4. **Migrate production data** using the migration script
5. **Switch production** to PostgreSQL

---

## Complete Example

Here is a complete, runnable example demonstrating SQLite event store usage:

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


# =============================================================================
# Events
# =============================================================================

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


# =============================================================================
# Aggregate
# =============================================================================

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


# =============================================================================
# Demo Functions
# =============================================================================

async def in_memory_demo():
    """Demonstrate in-memory SQLite for testing."""
    print("=== In-Memory SQLite Demo ===\n")

    # Create in-memory store using context manager
    async with SQLiteEventStore(":memory:") as store:
        # Initialize creates the schema
        await store.initialize()

        # Create repository
        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=NoteAggregate,
            aggregate_type="Note",
        )

        # Use it like any other event store
        note_id = uuid4()
        note = repo.create_new(note_id)
        note.create("Meeting Notes", "Discuss Q4 roadmap")
        await repo.save(note)

        print(f"Created note: {note.state.title}")

        # Load and update
        loaded = await repo.load(note_id)
        loaded.update("Updated: Discuss Q4 roadmap and budget")
        await repo.save(loaded)

        print(f"Updated note: {loaded.state.content}")
        print(f"Version: {loaded.version}")

    # Database is gone after context manager exits
    print("Database cleaned up automatically\n")


async def file_based_demo():
    """Demonstrate file-based SQLite for development."""
    print("=== File-Based SQLite Demo ===\n")

    db_path = "./tutorial_notes.db"

    # Create file-based store
    async with SQLiteEventStore(db_path, wal_mode=True) as store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=NoteAggregate,
            aggregate_type="Note",
        )

        # Create a note
        note_id = uuid4()
        note = repo.create_new(note_id)
        note.create("Development Notes", "Testing SQLite storage")
        await repo.save(note)

        print(f"Note saved to {db_path}")
        print(f"Note ID: {note_id}")

    # File persists after context manager
    print(f"Database file exists: {os.path.exists(db_path)}")

    # Can reopen and read
    async with SQLiteEventStore(db_path) as store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=NoteAggregate,
            aggregate_type="Note",
        )

        loaded = await repo.load(note_id)
        print(f"Reopened and loaded: {loaded.state.title}")

    # Cleanup
    for suffix in ["", "-wal", "-shm"]:
        path = f"{db_path}{suffix}"
        if os.path.exists(path):
            os.remove(path)
    print("Cleaned up database files\n")


async def configuration_demo():
    """Demonstrate configuration options."""
    print("=== Configuration Options Demo ===\n")

    # Default configuration
    async with SQLiteEventStore(":memory:") as store:
        await store.initialize()
        print(f"Database: {store.database}")
        print(f"WAL mode: {store.wal_mode}")
        print(f"Busy timeout: {store.busy_timeout}ms")
        print(f"Connected: {store.is_connected}")

    print()

    # Custom configuration
    async with SQLiteEventStore(
        ":memory:",
        wal_mode=False,
        busy_timeout=10000,
        enable_tracing=False,
    ) as store:
        await store.initialize()
        print(f"Custom WAL mode: {store.wal_mode}")
        print(f"Custom busy timeout: {store.busy_timeout}ms")
    print()


async def pytest_fixture_pattern():
    """Demonstrate the pattern used in pytest fixtures."""
    print("=== Pytest Fixture Pattern ===\n")

    # This mimics what a pytest fixture looks like
    async def create_test_infrastructure():
        """Factory function for test infrastructure."""
        store = SQLiteEventStore(":memory:")
        async with store:
            await store.initialize()

            repo = AggregateRepository(
                event_store=store,
                aggregate_factory=NoteAggregate,
                aggregate_type="Note",
            )

            return store, repo

    # Demonstrate fixture usage pattern
    store = SQLiteEventStore(":memory:")
    async with store:
        await store.initialize()

        repo = AggregateRepository(
            event_store=store,
            aggregate_factory=NoteAggregate,
            aggregate_type="Note",
        )

        # Test code
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

Save this as `tutorial_12_sqlite.py` and run it:

```bash
python tutorial_12_sqlite.py
```

---

## Exercises

### Exercise 1: SQLite Test Fixtures

**Objective:** Create reusable pytest fixtures for testing with SQLite.

**Time:** 15-20 minutes

**Requirements:**

1. Create a fixture for an in-memory event store
2. Create a fixture for a repository using the event store
3. Write tests that verify fixture isolation
4. Ensure proper cleanup after each test

**Starter Code:**

```python
"""
Tutorial 12 - Exercise 1: SQLite Test Fixtures

Your task: Create pytest fixtures for SQLite-based testing.
Run with: pytest exercise_12_1.py -v
"""
import pytest
import pytest_asyncio
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    SQLiteEventStore,
    AggregateRepository,
    EventRegistry,
)


# Domain code
@register_event
class ItemCreated(DomainEvent):
    event_type: str = "ItemCreated"
    aggregate_type: str = "Item"
    name: str


class ItemState(BaseModel):
    item_id: str
    name: str = ""


class ItemAggregate(AggregateRoot[ItemState]):
    aggregate_type = "Item"

    def _get_initial_state(self) -> ItemState:
        return ItemState(item_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ItemCreated):
            self._state = ItemState(
                item_id=str(self.aggregate_id),
                name=event.name,
            )

    def create(self, name: str) -> None:
        self.apply_event(ItemCreated(
            aggregate_id=self.aggregate_id,
            name=name,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# TODO: Create your fixtures here
# =============================================================================

@pytest_asyncio.fixture
async def event_store():
    """
    TODO: Create an in-memory SQLite event store fixture.

    Requirements:
    - Use ":memory:" for the database
    - Use a fresh EventRegistry
    - Initialize the schema
    - Yield the store for use in tests
    - Clean up after the test
    """
    pass  # TODO: Implement


@pytest_asyncio.fixture
async def item_repository(event_store):
    """
    TODO: Create an item repository fixture.

    Requirements:
    - Use the event_store fixture
    - Create an AggregateRepository for ItemAggregate
    """
    pass  # TODO: Implement


# =============================================================================
# Tests Using Your Fixtures
# =============================================================================

class TestItemAggregate:
    """Tests demonstrating fixture usage."""

    @pytest.mark.asyncio
    async def test_create_item(self, item_repository):
        """Test creating an item through the repository."""
        item_id = uuid4()
        item = item_repository.create_new(item_id)
        item.create("Test Item")
        await item_repository.save(item)

        assert item.version == 1
        assert item.state.name == "Test Item"

    @pytest.mark.asyncio
    async def test_load_item(self, item_repository):
        """Test loading an item from the repository."""
        item_id = uuid4()

        # Create and save
        item = item_repository.create_new(item_id)
        item.create("Another Item")
        await item_repository.save(item)

        # Load
        loaded = await item_repository.load(item_id)
        assert loaded.state.name == "Another Item"


class TestIsolation:
    """Verify test isolation with separate test class."""

    @pytest.mark.asyncio
    async def test_fresh_database(self, event_store):
        """
        TODO: Verify we get a fresh database.

        If isolation works correctly, there should be no events
        from other tests in this database.
        """
        # Read all events - should be empty
        events = []
        async for event in event_store.read_all():
            events.append(event)

        assert len(events) == 0, "Database should be empty at test start"
```

**Hints:**

- Use `async with` for the store lifecycle
- Remember to call `store.initialize()` to create the schema
- The `event_store` fixture should `yield` the store, not `return` it
- Register the `ItemCreated` event with the fresh registry

<details>
<summary>Click to see the solution</summary>

```python
"""
Tutorial 12 - Exercise 1 Solution: SQLite Test Fixtures

Run with: pytest 12-1.py -v
"""
import pytest
import pytest_asyncio
from uuid import uuid4

from pydantic import BaseModel

from eventsource import (
    DomainEvent,
    register_event,
    AggregateRoot,
    SQLiteEventStore,
    AggregateRepository,
    EventRegistry,
)


# Domain code
@register_event
class ItemCreated(DomainEvent):
    event_type: str = "ItemCreated"
    aggregate_type: str = "Item"
    name: str


class ItemState(BaseModel):
    item_id: str
    name: str = ""


class ItemAggregate(AggregateRoot[ItemState]):
    aggregate_type = "Item"

    def _get_initial_state(self) -> ItemState:
        return ItemState(item_id=str(self.aggregate_id))

    def _apply(self, event: DomainEvent) -> None:
        if isinstance(event, ItemCreated):
            self._state = ItemState(
                item_id=str(self.aggregate_id),
                name=event.name,
            )

    def create(self, name: str) -> None:
        self.apply_event(ItemCreated(
            aggregate_id=self.aggregate_id,
            name=name,
            aggregate_version=self.get_next_version(),
        ))


# =============================================================================
# Pytest Fixtures
# =============================================================================

@pytest_asyncio.fixture
async def event_store():
    """
    Fixture 1: In-memory event store.

    Creates a fresh in-memory SQLite database for each test.
    Automatically initializes and cleans up.
    """
    # Create fresh registry for this test
    registry = EventRegistry()
    registry.register(ItemCreated)

    store = SQLiteEventStore(
        database=":memory:",
        event_registry=registry,
        wal_mode=False,  # WAL not supported for in-memory
    )

    async with store:
        await store.initialize()
        yield store
    # Cleanup is automatic


@pytest_asyncio.fixture
async def item_repository(event_store):
    """
    Fixture 2: Repository using the event store fixture.

    Depends on event_store fixture for proper lifecycle.
    """
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=ItemAggregate,
        aggregate_type="Item",
    )


# =============================================================================
# Tests Using Fixtures
# =============================================================================

class TestItemAggregate:
    """Tests demonstrating fixture usage."""

    @pytest.mark.asyncio
    async def test_create_item(self, item_repository):
        """Test creating an item through the repository."""
        item_id = uuid4()
        item = item_repository.create_new(item_id)
        item.create("Test Item")
        await item_repository.save(item)

        assert item.version == 1
        assert item.state.name == "Test Item"

    @pytest.mark.asyncio
    async def test_load_item(self, item_repository):
        """Test loading an item from the repository."""
        item_id = uuid4()

        # Create and save
        item = item_repository.create_new(item_id)
        item.create("Another Item")
        await item_repository.save(item)

        # Load
        loaded = await item_repository.load(item_id)
        assert loaded.state.name == "Another Item"

    @pytest.mark.asyncio
    async def test_isolation_between_tests(self, item_repository):
        """
        Test that each test gets a fresh database.

        This test creates items that should NOT exist in other tests.
        """
        item_id = uuid4()
        item = item_repository.create_new(item_id)
        item.create("Isolated Item")
        await item_repository.save(item)

        # This item only exists in this test's database
        loaded = await item_repository.load(item_id)
        assert loaded.state.name == "Isolated Item"

    @pytest.mark.asyncio
    async def test_exists_method(self, item_repository):
        """Test checking if aggregate exists."""
        item_id = uuid4()

        # Should not exist yet
        assert not await item_repository.exists(item_id)

        # Create it
        item = item_repository.create_new(item_id)
        item.create("Existing Item")
        await item_repository.save(item)

        # Now should exist
        assert await item_repository.exists(item_id)


# =============================================================================
# Verification of Isolation
# =============================================================================

class TestIsolation:
    """Verify test isolation with separate test class."""

    @pytest.mark.asyncio
    async def test_fresh_database(self, event_store):
        """
        Verify we get a fresh database.

        If isolation works, there should be no events from other tests.
        """
        # Read all events - should be empty
        events = []
        async for event in event_store.read_all():
            events.append(event)

        assert len(events) == 0, "Database should be empty at test start"
```

</details>

The solution file is also available at: `docs/tutorials/exercises/solutions/12-1.py`

---

## Summary

In this tutorial, you learned:

- **SQLite is ideal for development and testing** - zero configuration and fast startup
- **In-memory mode** (`":memory:"`) enables fast, isolated tests
- **File-based mode** provides persistence without PostgreSQL infrastructure
- **WAL mode** is enabled by default for better concurrent read performance
- **Pytest fixtures** make SQLite testing convenient and consistent
- **The same code works with both backends** - easy migration path to PostgreSQL

---

## Key Takeaways

!!! note "Remember"
    - Use `:memory:` for test isolation - each test gets a fresh database
    - Always call `initialize()` or use the context manager to create the schema
    - WAL mode is enabled by default but not supported for in-memory databases
    - SQLite is NOT recommended for multi-instance production deployments

!!! tip "Best Practice"
    Start development with SQLite for its simplicity, then migrate to PostgreSQL when you need horizontal scaling or high write concurrency. The repository pattern makes this transition seamless.

!!! warning "Common Mistake"
    Do not forget to use `wal_mode=False` for in-memory databases. WAL mode creates separate files that do not work with `:memory:`.

---

## Next Steps

Continue to [Tutorial 13: Subscription Management](13-subscriptions.md) to learn about coordinated event subscriptions for catch-up processing and real-time event delivery.

---

## Related Documentation

- [SQLite Backend Guide](../guides/sqlite-backend.md) - Detailed SQLite configuration
- [Testing Guide](../development/testing.md) - Comprehensive testing patterns
- [Tutorial 8: Testing](08-testing.md) - Testing fundamentals
- [Tutorial 11: PostgreSQL](11-postgresql.md) - Production database setup
