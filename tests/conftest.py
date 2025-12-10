"""
Shared pytest fixtures for the eventsource library tests.

This module provides comprehensive test fixtures including:
- Domain event fixtures (event_factory, sample_event, event_stream)
- Event store fixtures (in_memory_store, populated_store)
- Repository fixtures (checkpoint_repo, dlq_repo, outbox_repo)
- Aggregate fixtures (test_aggregate, populated_aggregate)
- Projection fixtures (test_projection)
- Sample data fixtures (aggregate_id, tenant_id)
- SQLite fixtures (sqlite_connection, sqlite_event_store, etc.)
- OpenTelemetry metrics fixtures (metric_reader, reset_kafka_meter)

All fixtures are properly scoped and documented for easy reuse.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio

from eventsource.events.base import DomainEvent
from eventsource.repositories.checkpoint import InMemoryCheckpointRepository
from eventsource.repositories.dlq import InMemoryDLQRepository
from eventsource.repositories.outbox import InMemoryOutboxRepository
from eventsource.stores.in_memory import InMemoryEventStore

# Import shared fixtures from fixtures module
from tests.fixtures import (
    CounterAggregate,
    CounterDecremented,
    CounterIncremented,
    CounterNamed,
    CounterReset,
    CounterState,
    DeclarativeCounterAggregate,
    OrderAggregate,
    OrderCreated,
    OrderItemAdded,
    OrderShipped,
    OrderState,
    SampleEvent,
    create_event,
)

if TYPE_CHECKING:
    import aiosqlite

# ============================================================================
# SQLite Availability Check
# ============================================================================

AIOSQLITE_AVAILABLE = False
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]


# ============================================================================
# OpenTelemetry Metrics Availability Check
# ============================================================================

OTEL_METRICS_AVAILABLE = False
try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    otel_metrics = None  # type: ignore[assignment]
    MeterProvider = None  # type: ignore[assignment, misc]
    InMemoryMetricReader = None  # type: ignore[assignment, misc]


# ============================================================================
# Pytest Configuration
# ============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for tests."""
    config.addinivalue_line("markers", "sqlite: marks tests that require SQLite (aiosqlite)")


# ============================================================================
# Skip Condition
# ============================================================================

skip_if_no_aiosqlite = pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")

skip_if_no_otel_metrics = pytest.mark.skipif(
    not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed"
)


# =============================================================================
# Sample Data Fixtures
# =============================================================================


@pytest.fixture
def aggregate_id() -> UUID:
    """
    Provide a random aggregate ID.

    Returns:
        A new random UUID for use as an aggregate identifier.
    """
    return uuid4()


@pytest.fixture
def tenant_id() -> UUID:
    """
    Provide a random tenant ID for multi-tenant tests.

    Returns:
        A new random UUID for use as a tenant identifier.
    """
    return uuid4()


@pytest.fixture
def customer_id() -> UUID:
    """
    Provide a random customer ID for order tests.

    Returns:
        A new random UUID for use as a customer identifier.
    """
    return uuid4()


# =============================================================================
# Event Factory Fixtures
# =============================================================================


@pytest.fixture
def event_factory() -> Callable[..., DomainEvent]:
    """
    Factory fixture for creating test events with sensible defaults.

    This fixture returns the create_event factory function from the fixtures
    module, allowing tests to easily create events with custom parameters.

    Usage:
        def test_something(event_factory):
            event = event_factory(CounterIncremented, increment=5)
            event = event_factory(aggregate_version=2)  # SampleEvent at v2

    Returns:
        The create_event factory function.
    """
    return create_event


@pytest.fixture
def sample_event(aggregate_id: UUID) -> SampleEvent:
    """
    Provide a pre-created sample event for simple tests.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A SampleEvent with default values.
    """
    return SampleEvent(
        aggregate_id=aggregate_id,
        aggregate_version=1,
        data="test_data",
    )


@pytest.fixture
def counter_event(aggregate_id: UUID) -> CounterIncremented:
    """
    Provide a counter increment event for counter tests.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A CounterIncremented event with increment=5.
    """
    return CounterIncremented(
        aggregate_id=aggregate_id,
        aggregate_version=1,
        increment=5,
    )


@pytest.fixture
def event_stream(aggregate_id: UUID) -> list[DomainEvent]:
    """
    Provide a list of events representing a typical counter lifecycle.

    Returns events: Incremented(10) -> Incremented(5) -> Decremented(3)
    Final counter value would be 12.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A list of 3 counter events with sequential versions.
    """
    return [
        CounterIncremented(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            increment=10,
        ),
        CounterIncremented(
            aggregate_id=aggregate_id,
            aggregate_version=2,
            increment=5,
        ),
        CounterDecremented(
            aggregate_id=aggregate_id,
            aggregate_version=3,
            decrement=3,
        ),
    ]


@pytest.fixture
def order_event_stream(aggregate_id: UUID, customer_id: UUID) -> list[DomainEvent]:
    """
    Provide a list of events representing a complete order lifecycle.

    Returns events: Created -> ItemAdded -> ItemAdded -> Shipped

    Args:
        aggregate_id: The aggregate ID fixture.
        customer_id: The customer ID fixture.

    Returns:
        A list of 4 order events with sequential versions.
    """
    return [
        OrderCreated(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            customer_id=customer_id,
        ),
        OrderItemAdded(
            aggregate_id=aggregate_id,
            aggregate_version=2,
            item_name="Widget A",
            price=10.0,
        ),
        OrderItemAdded(
            aggregate_id=aggregate_id,
            aggregate_version=3,
            item_name="Widget B",
            price=15.0,
        ),
        OrderShipped(
            aggregate_id=aggregate_id,
            aggregate_version=4,
            tracking_number="TRACK123",
        ),
    ]


# =============================================================================
# Event Store Fixtures
# =============================================================================


@pytest.fixture
def in_memory_store() -> InMemoryEventStore:
    """
    Provide a fresh InMemoryEventStore instance.

    Each test gets its own isolated event store.

    Returns:
        A new InMemoryEventStore instance.
    """
    return InMemoryEventStore()


@pytest_asyncio.fixture
async def populated_store(
    in_memory_store: InMemoryEventStore,
    event_stream: list[DomainEvent],
) -> AsyncGenerator[InMemoryEventStore, None]:
    """
    Provide an InMemoryEventStore pre-populated with sample events.

    Contains 3 counter events for a single aggregate (final value = 12).

    Args:
        in_memory_store: The event store fixture.
        event_stream: The event stream fixture.

    Yields:
        The event store populated with events.
    """
    aggregate_id = event_stream[0].aggregate_id
    await in_memory_store.append_events(
        aggregate_id=aggregate_id,
        aggregate_type="Counter",
        events=event_stream,
        expected_version=0,
    )
    yield in_memory_store


# =============================================================================
# Repository Fixtures
# =============================================================================


@pytest.fixture
def checkpoint_repo() -> InMemoryCheckpointRepository:
    """
    Provide a fresh InMemoryCheckpointRepository instance.

    Each test gets its own isolated checkpoint repository.

    Returns:
        A new InMemoryCheckpointRepository instance.
    """
    return InMemoryCheckpointRepository()


@pytest.fixture
def dlq_repo() -> InMemoryDLQRepository:
    """
    Provide a fresh InMemoryDLQRepository instance.

    Each test gets its own isolated dead letter queue repository.

    Returns:
        A new InMemoryDLQRepository instance.
    """
    return InMemoryDLQRepository()


@pytest.fixture
def outbox_repo() -> InMemoryOutboxRepository:
    """
    Provide a fresh InMemoryOutboxRepository instance.

    Each test gets its own isolated outbox repository.

    Returns:
        A new InMemoryOutboxRepository instance.
    """
    return InMemoryOutboxRepository()


# =============================================================================
# Aggregate Fixtures
# =============================================================================


@pytest.fixture
def counter_aggregate(aggregate_id: UUID) -> CounterAggregate:
    """
    Provide a fresh CounterAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new CounterAggregate at version 0.
    """
    return CounterAggregate(aggregate_id)


@pytest.fixture
def declarative_counter_aggregate(aggregate_id: UUID) -> DeclarativeCounterAggregate:
    """
    Provide a fresh DeclarativeCounterAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new DeclarativeCounterAggregate at version 0.
    """
    return DeclarativeCounterAggregate(aggregate_id)


@pytest.fixture
def populated_counter_aggregate(aggregate_id: UUID) -> CounterAggregate:
    """
    Provide a CounterAggregate with initial state.

    The counter is incremented by 10, giving it:
    - version = 1
    - state.value = 10

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A CounterAggregate with one event applied.
    """
    aggregate = CounterAggregate(aggregate_id)
    aggregate.increment(10)
    return aggregate


@pytest.fixture
def order_aggregate(aggregate_id: UUID) -> OrderAggregate:
    """
    Provide a fresh OrderAggregate instance.

    Args:
        aggregate_id: The aggregate ID fixture.

    Returns:
        A new OrderAggregate at version 0.
    """
    return OrderAggregate(aggregate_id)


@pytest.fixture
def populated_order_aggregate(
    aggregate_id: UUID,
    customer_id: UUID,
) -> OrderAggregate:
    """
    Provide an OrderAggregate with items.

    The order has:
    - customer_id set
    - 2 items: Widget A ($10) and Widget B ($15)
    - total = 25.0
    - version = 3

    Args:
        aggregate_id: The aggregate ID fixture.
        customer_id: The customer ID fixture.

    Returns:
        An OrderAggregate with creation and item events applied.
    """
    aggregate = OrderAggregate(aggregate_id)
    aggregate.create(customer_id)
    aggregate.add_item("Widget A", 10.0)
    aggregate.add_item("Widget B", 15.0)
    return aggregate


# =============================================================================
# Mock and Helper Fixtures
# =============================================================================


@pytest.fixture
def mock_event_publisher() -> MockEventPublisher:
    """
    Provide a mock event publisher for testing event publishing.

    Returns:
        A MockEventPublisher that captures published events.
    """
    return MockEventPublisher()


class MockEventPublisher:
    """
    Mock event publisher for testing event publishing behavior.

    Captures all events that would be published, allowing tests to
    verify that the correct events are published at the right times.
    """

    def __init__(self) -> None:
        """Initialize with empty published events list."""
        self.published_events: list[DomainEvent] = []

    async def publish(self, events: list[DomainEvent]) -> None:
        """
        Capture events for later verification.

        Args:
            events: The events to "publish" (capture).
        """
        self.published_events.extend(events)

    def clear(self) -> None:
        """Clear all captured events."""
        self.published_events.clear()


# ============================================================================
# OpenTelemetry Metrics Fixtures
# ============================================================================


@pytest.fixture
def metric_reader() -> Any:
    """
    Provide an InMemoryMetricReader for testing metrics.

    Creates a fresh metric reader and meter provider for each test.
    The provider is set as the global meter provider for the test duration.

    Yields:
        InMemoryMetricReader: Reader for inspecting collected metrics.
    """
    if not OTEL_METRICS_AVAILABLE:
        pytest.skip("opentelemetry-sdk not installed")

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])

    # Store old provider to restore later
    old_provider = otel_metrics.get_meter_provider()
    otel_metrics.set_meter_provider(provider)

    yield reader

    # Restore old provider
    otel_metrics.set_meter_provider(old_provider)


@pytest.fixture
def reset_kafka_meter():
    """
    Reset the kafka module's cached meter between tests.

    The kafka module caches the meter at module level, which can cause
    issues between tests. This fixture resets it.
    """
    try:
        import eventsource.bus.kafka as kafka_module
    except ImportError:
        pytest.skip("eventsource.bus.kafka not available")
        return

    # Reset cached meter
    kafka_module._meter = None

    yield

    # Clean up after test
    kafka_module._meter = None


# ============================================================================
# SQLite Fixtures
# ============================================================================


@pytest_asyncio.fixture
async def sqlite_connection() -> AsyncGenerator[Any, None]:
    """
    Provide a raw aiosqlite connection to an in-memory database.

    Creates a fresh in-memory SQLite database for each test.
    The connection is automatically closed after the test.

    Yields:
        aiosqlite.Connection: Raw database connection
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    conn = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row

    yield conn

    await conn.close()


@pytest_asyncio.fixture
async def sqlite_event_store() -> AsyncGenerator[Any, None]:
    """
    Provide an initialized SQLiteEventStore with in-memory database.

    Creates a fresh in-memory SQLite event store for each test.
    The store is automatically initialized with the schema and
    cleaned up after the test.

    Yields:
        SQLiteEventStore: Initialized event store ready for use
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource import EventRegistry
    from eventsource.stores.sqlite import SQLiteEventStore

    # Create fresh registry for tests
    registry = EventRegistry()

    store = SQLiteEventStore(
        database=":memory:",
        event_registry=registry,
        wal_mode=False,  # WAL mode not supported in-memory
        busy_timeout=5000,
    )

    async with store:
        await store.initialize()
        yield store


@pytest_asyncio.fixture
async def sqlite_checkpoint_repo(
    sqlite_connection: aiosqlite.Connection,
) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLiteCheckpointRepository with schema initialized.

    Creates the projection_checkpoints and events tables in the
    in-memory database for checkpoint testing.

    Args:
        sqlite_connection: Raw aiosqlite connection fixture

    Yields:
        SQLiteCheckpointRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.repositories.checkpoint import SQLiteCheckpointRepository

    # Create the required tables
    await sqlite_connection.execute("""
        CREATE TABLE IF NOT EXISTS projection_checkpoints (
            projection_name TEXT PRIMARY KEY,
            last_event_id TEXT,
            last_event_type TEXT,
            last_processed_at TEXT,
            events_processed INTEGER NOT NULL DEFAULT 0,
            global_position INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    await sqlite_connection.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL UNIQUE,
            event_type TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            tenant_id TEXT,
            actor_id TEXT,
            version INTEGER NOT NULL,
            timestamp TEXT NOT NULL,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    await sqlite_connection.commit()

    repo = SQLiteCheckpointRepository(sqlite_connection)
    yield repo


@pytest_asyncio.fixture
async def sqlite_outbox_repo(
    sqlite_connection: aiosqlite.Connection,
) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLiteOutboxRepository with schema initialized.

    Creates the event_outbox table in the in-memory database
    for outbox testing.

    Args:
        sqlite_connection: Raw aiosqlite connection fixture

    Yields:
        SQLiteOutboxRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.repositories.outbox import SQLiteOutboxRepository

    # Create the event_outbox table
    await sqlite_connection.execute("""
        CREATE TABLE IF NOT EXISTS event_outbox (
            id TEXT PRIMARY KEY,
            event_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            aggregate_id TEXT NOT NULL,
            aggregate_type TEXT NOT NULL,
            tenant_id TEXT,
            event_data TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            published_at TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            last_error TEXT,
            status TEXT NOT NULL DEFAULT 'pending',
            CHECK (status IN ('pending', 'published', 'failed'))
        )
    """)
    await sqlite_connection.commit()

    repo = SQLiteOutboxRepository(sqlite_connection)
    yield repo


@pytest_asyncio.fixture
async def sqlite_dlq_repo(
    sqlite_connection: aiosqlite.Connection,
) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLiteDLQRepository with schema initialized.

    Creates the dead_letter_queue table in the in-memory database
    for DLQ testing.

    Args:
        sqlite_connection: Raw aiosqlite connection fixture

    Yields:
        SQLiteDLQRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.repositories.dlq import SQLiteDLQRepository

    # Create the dead_letter_queue table
    await sqlite_connection.execute("""
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_id TEXT NOT NULL,
            projection_name TEXT NOT NULL,
            event_type TEXT NOT NULL,
            event_data TEXT NOT NULL,
            error_message TEXT NOT NULL,
            error_stacktrace TEXT,
            retry_count INTEGER NOT NULL DEFAULT 0,
            first_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
            last_failed_at TEXT NOT NULL DEFAULT (datetime('now')),
            status TEXT NOT NULL DEFAULT 'failed',
            resolved_at TEXT,
            resolved_by TEXT,
            CHECK (status IN ('failed', 'retrying', 'resolved')),
            UNIQUE (event_id, projection_name)
        )
    """)
    await sqlite_connection.commit()

    repo = SQLiteDLQRepository(sqlite_connection)
    yield repo


# =============================================================================
# Re-export fixtures module items for convenience
# =============================================================================

# These are re-exported so tests can import them from conftest if needed
__all__ = [
    # Events
    "CounterIncremented",
    "CounterDecremented",
    "CounterNamed",
    "CounterReset",
    "OrderCreated",
    "OrderItemAdded",
    "OrderShipped",
    "SampleEvent",
    # States
    "CounterState",
    "OrderState",
    # Aggregates
    "CounterAggregate",
    "DeclarativeCounterAggregate",
    "OrderAggregate",
    # Factory
    "create_event",
    # Mock
    "MockEventPublisher",
    # SQLite
    "AIOSQLITE_AVAILABLE",
    "skip_if_no_aiosqlite",
    # OpenTelemetry Metrics
    "OTEL_METRICS_AVAILABLE",
    "skip_if_no_otel_metrics",
]
