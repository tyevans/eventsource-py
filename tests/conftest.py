"""
Shared pytest fixtures for the eventsource library tests.

This module provides comprehensive test fixtures including:
- Domain event fixtures (event_factory, sample_event, event_stream)
- Event store fixtures (in_memory_store, populated_store)
- Repository fixtures (checkpoint_repo, dlq_repo, outbox_repo)
- Aggregate fixtures (test_aggregate, populated_aggregate)
- Projection fixtures (test_projection)
- Sample data fixtures (aggregate_id, tenant_id)
- SQLite fixtures (sqlite_connection, sqlite_checkpoint_repo, etc.)
- OpenTelemetry metrics fixtures (metric_reader, reset_kafka_meter)

All fixtures are properly scoped and documented for easy reuse.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Callable
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from hypothesis import HealthCheck, settings

from eventsource.adapters.memory.checkpoints import InMemoryCheckpointRepository
from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.memory.outbox import InMemoryOutboxRepository
from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.domain import StreamId
from eventsource.events.base import DomainEvent
from eventsource.ports import ExpectedVersion

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
# Hypothesis Profiles
# ============================================================================
#
# "default" runs locally with a modest example count. "ci" widens the search
# for CI runs where extra time is acceptable. "db" is for property tests that
# exercise real database transactions (deferred to M2's delivery-guarantee
# work) -- fewer examples because each one costs a real transaction, and
# function-scoped fixture reuse across examples is expected there.

settings.register_profile("default", max_examples=100)
settings.register_profile("ci", max_examples=500, deadline=None)
settings.register_profile(
    "db",
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
settings.load_profile("default")


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
    Provide a InMemoryEventStore pre-populated with sample events.

    Contains 3 counter events for a single aggregate (final value = 12).

    Args:
        in_memory_store: The event store fixture.
        event_stream: The event stream fixture.

    Yields:
        The event store populated with events.
    """
    aggregate_id = event_stream[0].aggregate_id
    await in_memory_store.append(
        StreamId(aggregate_id=aggregate_id, category="Counter"),
        event_stream,
        ExpectedVersion.no_stream(),
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
        import eventsource.bus.kafka.bus as kafka_module
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
async def sqlite_checkpoint_repo(tmp_path: Any) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLCheckpointRepository backed by a SQLite engine, schema initialized.

    Creates the projection_checkpoints and events tables in a temporary
    on-disk database for checkpoint testing.

    Yields:
        SQLCheckpointRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
    from eventsource.engine import create_async_engine
    from eventsource.migrations import get_schema

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/checkpoint_repo.db")
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
        await raw.driver_connection.executescript(get_schema("events", backend="sqlite"))

    repo = SQLCheckpointRepository(engine)
    yield repo
    await engine.dispose()


@pytest_asyncio.fixture
async def sqlite_outbox_repo(
    sqlite_connection: aiosqlite.Connection,
) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLiteOutboxRepository with schema initialized.

    Provisions the event_outbox table from
    `get_schema("outbox", backend="sqlite")` in the in-memory database
    for outbox testing.

    Args:
        sqlite_connection: Raw aiosqlite connection fixture

    Yields:
        SQLiteOutboxRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository
    from eventsource.migrations import get_schema

    await sqlite_connection.executescript(get_schema("outbox", backend="sqlite"))
    await sqlite_connection.commit()

    repo = SQLiteOutboxRepository(sqlite_connection)
    yield repo


@pytest_asyncio.fixture
async def sqlite_dlq_repo(tmp_path: Any) -> AsyncGenerator[Any, None]:
    """
    Provide a SQLDLQRepository backed by a SQLite engine, schema initialized.

    Creates the dead_letter_queue table in a temporary on-disk database
    for DLQ testing.

    Yields:
        SQLDLQRepository: Repository ready for testing
    """
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.adapters.sql.dlq import SQLDLQRepository
    from eventsource.engine import create_async_engine
    from eventsource.migrations import get_schema

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dlq_repo.db")
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        await raw.driver_connection.executescript(get_schema("dlq", backend="sqlite"))

    repo = SQLDLQRepository(engine)
    yield repo
    await engine.dispose()


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
