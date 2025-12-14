"""
Shared pytest fixtures for integration tests.

This module provides fixtures for PostgreSQL and Redis test infrastructure
using testcontainers for automatic container management.

If testcontainers or Docker is not available, tests are automatically skipped.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Generator
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from pydantic import BaseModel, Field

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


# ============================================================================
# Pytest Configuration
# ============================================================================


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers for integration tests."""
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests (may require docker)"
    )
    config.addinivalue_line("markers", "postgres: marks tests that require PostgreSQL")
    config.addinivalue_line("markers", "redis: marks tests that require Redis")
    config.addinivalue_line("markers", "rabbitmq: marks tests that require RabbitMQ")
    config.addinivalue_line("markers", "kafka: marks tests that require Kafka")
    config.addinivalue_line("markers", "e2e: marks tests as end-to-end integration tests")


# ============================================================================
# Testcontainers Detection
# ============================================================================

TESTCONTAINERS_AVAILABLE = False
POSTGRES_CONTAINER = None
REDIS_CONTAINER = None

try:
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer

    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    PostgresContainer = None  # type: ignore[assignment, misc]
    RedisContainer = None  # type: ignore[assignment, misc]


def is_docker_available() -> bool:
    """Check if Docker is available for running containers."""
    import subprocess

    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, Exception):
        return False


DOCKER_AVAILABLE = is_docker_available()


# ============================================================================
# Skip Conditions
# ============================================================================

skip_if_no_testcontainers = pytest.mark.skipif(
    not TESTCONTAINERS_AVAILABLE, reason="testcontainers not installed (pip install testcontainers)"
)

skip_if_no_docker = pytest.mark.skipif(
    not DOCKER_AVAILABLE, reason="Docker not available or not running"
)

skip_if_no_postgres_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE),
    reason="PostgreSQL test infrastructure not available",
)

skip_if_no_redis_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE),
    reason="Redis test infrastructure not available",
)


# ============================================================================
# Test Events and Aggregates
# ============================================================================

from eventsource import DomainEvent, register_event  # noqa: E402


@register_event
class TestItemCreated(DomainEvent):
    """Test event for item creation."""

    event_type: str = "TestItemCreated"
    aggregate_type: str = "TestItem"
    name: str
    quantity: int = 1


@register_event
class TestItemUpdated(DomainEvent):
    """Test event for item updates."""

    event_type: str = "TestItemUpdated"
    aggregate_type: str = "TestItem"
    name: str | None = None
    quantity: int | None = None


@register_event
class TestItemDeleted(DomainEvent):
    """Test event for item deletion."""

    event_type: str = "TestItemDeleted"
    aggregate_type: str = "TestItem"


@register_event
class TestOrderCreated(DomainEvent):
    """Test event for order creation (used in e2e tests)."""

    event_type: str = "TestOrderCreated"
    aggregate_type: str = "TestOrder"
    customer_id: UUID
    total_amount: float = 0.0


@register_event
class TestOrderItemAdded(DomainEvent):
    """Test event for adding items to an order."""

    event_type: str = "TestOrderItemAdded"
    aggregate_type: str = "TestOrder"
    item_id: UUID
    item_name: str
    quantity: int
    unit_price: float


@register_event
class TestOrderCompleted(DomainEvent):
    """Test event for order completion."""

    event_type: str = "TestOrderCompleted"
    aggregate_type: str = "TestOrder"
    completed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


# ============================================================================
# Test Aggregate
# ============================================================================

from eventsource import DeclarativeAggregate, handles  # noqa: E402


class TestOrderState(BaseModel):
    """State for TestOrderAggregate."""

    order_id: UUID
    customer_id: UUID | None = None
    items: list[dict[str, Any]] = []
    total_amount: float = 0.0
    status: str = "pending"
    completed_at: datetime | None = None


class TestOrderAggregate(DeclarativeAggregate[TestOrderState]):
    """Test aggregate for integration tests."""

    aggregate_type = "TestOrder"

    def _get_initial_state(self) -> TestOrderState:
        return TestOrderState(order_id=self.aggregate_id)

    @handles(TestOrderCreated)
    def _on_order_created(self, event: TestOrderCreated) -> None:
        self._state = TestOrderState(
            order_id=self.aggregate_id,
            customer_id=event.customer_id,
            total_amount=event.total_amount,
            status="created",
        )

    @handles(TestOrderItemAdded)
    def _on_item_added(self, event: TestOrderItemAdded) -> None:
        if self._state:
            item = {
                "item_id": str(event.item_id),
                "name": event.item_name,
                "quantity": event.quantity,
                "unit_price": event.unit_price,
            }
            new_items = [*self._state.items, item]
            new_total = self._state.total_amount + (event.quantity * event.unit_price)
            self._state = self._state.model_copy(
                update={"items": new_items, "total_amount": new_total}
            )

    @handles(TestOrderCompleted)
    def _on_order_completed(self, event: TestOrderCompleted) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={"status": "completed", "completed_at": event.completed_at}
            )

    def create_order(self, customer_id: UUID, total_amount: float = 0.0) -> None:
        """Create a new order."""
        if self.version > 0:
            raise ValueError("Order already exists")
        event = TestOrderCreated(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
            customer_id=customer_id,
            total_amount=total_amount,
        )
        self._raise_event(event)

    def add_item(
        self,
        item_id: UUID,
        item_name: str,
        quantity: int,
        unit_price: float,
    ) -> None:
        """Add an item to the order."""
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot add items to this order")
        event = TestOrderItemAdded(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
            item_id=item_id,
            item_name=item_name,
            quantity=quantity,
            unit_price=unit_price,
        )
        self._raise_event(event)

    def complete_order(self) -> None:
        """Complete the order."""
        if not self._state or self._state.status != "created":
            raise ValueError("Cannot complete this order")
        event = TestOrderCompleted(
            aggregate_id=self.aggregate_id,
            aggregate_version=self.get_next_version(),
        )
        self._raise_event(event)


# ============================================================================
# PostgreSQL Fixtures
# ============================================================================

# SQL Schema for tests - split into individual statements for asyncpg compatibility
EVENTS_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS events (
        global_position BIGSERIAL PRIMARY KEY,
        event_id UUID NOT NULL UNIQUE,
        event_type VARCHAR(255) NOT NULL,
        aggregate_type VARCHAR(255) NOT NULL,
        aggregate_id UUID NOT NULL,
        tenant_id VARCHAR(255),
        actor_id VARCHAR(255),
        version INTEGER NOT NULL,
        timestamp TIMESTAMPTZ NOT NULL,
        payload JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_events_aggregate_version
    ON events (aggregate_id, aggregate_type, version)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_events_aggregate
    ON events (aggregate_id, aggregate_type)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_events_type
    ON events (event_type)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_events_timestamp
    ON events (timestamp)
    """,
]

CHECKPOINTS_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS projection_checkpoints (
        projection_name VARCHAR(255) PRIMARY KEY,
        last_event_id UUID,
        last_event_type VARCHAR(255),
        last_processed_at TIMESTAMPTZ,
        events_processed BIGINT DEFAULT 0,
        global_position BIGINT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
]

DLQ_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS dead_letter_queue (
        id BIGSERIAL PRIMARY KEY,
        event_id UUID NOT NULL,
        projection_name VARCHAR(255) NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        event_data JSONB NOT NULL,
        error_message TEXT NOT NULL,
        error_stacktrace TEXT,
        retry_count INTEGER DEFAULT 0,
        first_failed_at TIMESTAMPTZ NOT NULL,
        last_failed_at TIMESTAMPTZ NOT NULL,
        status VARCHAR(50) DEFAULT 'failed',
        resolved_at TIMESTAMPTZ,
        resolved_by VARCHAR(255),
        UNIQUE(event_id, projection_name)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dlq_status
    ON dead_letter_queue (status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_dlq_projection
    ON dead_letter_queue (projection_name)
    """,
]

OUTBOX_SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS event_outbox (
        id UUID PRIMARY KEY,
        event_id UUID NOT NULL,
        event_type VARCHAR(255) NOT NULL,
        aggregate_id UUID NOT NULL,
        aggregate_type VARCHAR(255) NOT NULL,
        tenant_id UUID,
        event_data JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL,
        status VARCHAR(50) DEFAULT 'pending',
        published_at TIMESTAMPTZ,
        retry_count INTEGER DEFAULT 0,
        last_error TEXT
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_outbox_status
    ON event_outbox (status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_outbox_created
    ON event_outbox (created_at) WHERE status = 'pending'
    """,
]


@pytest.fixture(scope="session")
def postgres_container() -> Generator[Any, None, None]:
    """
    Provide PostgreSQL container for integration tests.

    Uses testcontainers to automatically start and stop a PostgreSQL container.
    Container is shared across all tests in the session for efficiency.
    """
    if not TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("PostgreSQL testcontainer not available")

    container = PostgresContainer("postgres:15")
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
def postgres_connection_url(postgres_container: Any) -> str:
    """Get PostgreSQL connection URL from container."""
    # testcontainers returns psycopg2 URL, convert to asyncpg
    url = postgres_container.get_connection_url()
    return url.replace("postgresql://", "postgresql+asyncpg://").replace("psycopg2", "asyncpg")


@pytest.fixture(scope="session")
async def postgres_engine(
    postgres_connection_url: str,
) -> AsyncGenerator[AsyncEngine, None]:
    """
    Provide SQLAlchemy async engine connected to PostgreSQL container.

    Creates all required tables on startup.
    """
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(
        postgres_connection_url,
        echo=False,
        pool_size=5,
        max_overflow=10,
    )

    # Create schema - execute each statement individually for asyncpg compatibility
    async with engine.begin() as conn:
        for statement in EVENTS_SCHEMA_STATEMENTS:
            await conn.execute(text(statement))
        for statement in CHECKPOINTS_SCHEMA_STATEMENTS:
            await conn.execute(text(statement))
        for statement in DLQ_SCHEMA_STATEMENTS:
            await conn.execute(text(statement))
        for statement in OUTBOX_SCHEMA_STATEMENTS:
            await conn.execute(text(statement))

    yield engine

    await engine.dispose()


@pytest.fixture
async def postgres_session_factory(
    postgres_engine: AsyncEngine,
) -> AsyncGenerator[async_sessionmaker[AsyncSession], None]:
    """Provide SQLAlchemy async session factory."""
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    factory = async_sessionmaker(
        postgres_engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    yield factory


@pytest.fixture
async def clean_postgres_tables(postgres_engine: AsyncEngine) -> AsyncGenerator[None, None]:
    """Clean all test tables before and after each test."""
    from sqlalchemy import text

    async def truncate_tables() -> None:
        async with postgres_engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE events CASCADE"))
            await conn.execute(text("TRUNCATE TABLE projection_checkpoints CASCADE"))
            await conn.execute(text("TRUNCATE TABLE dead_letter_queue CASCADE"))
            await conn.execute(text("TRUNCATE TABLE event_outbox CASCADE"))

    await truncate_tables()
    yield
    await truncate_tables()


# ============================================================================
# Redis Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def redis_container() -> Generator[Any, None, None]:
    """
    Provide Redis container for integration tests.

    Uses testcontainers to automatically start and stop a Redis container.
    Container is shared across all tests in the session for efficiency.
    """
    if not TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("Redis testcontainer not available")

    container = RedisContainer("redis:7")
    container.start()

    yield container

    container.stop()


@pytest.fixture(scope="session")
def redis_connection_url(redis_container: Any) -> str:
    """Get Redis connection URL from container."""
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}"


@pytest_asyncio.fixture(loop_scope="session")
async def redis_client(redis_connection_url: str) -> AsyncGenerator[Any, None]:
    """
    Provide async Redis client connected to container.

    Flushes database before and after each test for isolation.
    """
    try:
        import redis.asyncio as redis
    except ImportError:
        pytest.skip("redis package not installed")

    # Use single_connection_client=True to avoid connection pool event loop issues
    client = redis.from_url(
        redis_connection_url,
        decode_responses=True,
        single_connection_client=True,
    )

    await client.flushall()

    yield client

    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture(loop_scope="session")
async def clean_redis(redis_connection_url: str) -> AsyncGenerator[None, None]:
    """Clean Redis state before and after each test."""
    try:
        import redis.asyncio as redis
    except ImportError:
        pytest.skip("redis package not installed")

    # Use a separate single-connection client for cleanup to avoid event loop issues
    client = redis.from_url(
        redis_connection_url,
        decode_responses=True,
        single_connection_client=True,
    )
    await client.flushall()
    await client.aclose()

    yield

    # Cleanup after test
    client = redis.from_url(
        redis_connection_url,
        decode_responses=True,
        single_connection_client=True,
    )
    await client.flushall()
    await client.aclose()


# ============================================================================
# Event Store Fixtures
# ============================================================================


@pytest.fixture
async def postgres_event_store(
    postgres_session_factory: async_sessionmaker[AsyncSession],
    clean_postgres_tables: None,
) -> AsyncGenerator[Any, None]:
    """Provide PostgreSQL event store for integration tests."""
    from eventsource import EventRegistry, PostgreSQLEventStore

    # Create fresh registry for tests
    registry = EventRegistry()
    registry.register(TestItemCreated)
    registry.register(TestItemUpdated)
    registry.register(TestItemDeleted)
    registry.register(TestOrderCreated)
    registry.register(TestOrderItemAdded)
    registry.register(TestOrderCompleted)

    store = PostgreSQLEventStore(
        postgres_session_factory,
        event_registry=registry,
        outbox_enabled=False,
    )

    yield store


@pytest.fixture
async def postgres_event_store_with_outbox(
    postgres_session_factory: async_sessionmaker[AsyncSession],
    clean_postgres_tables: None,
) -> AsyncGenerator[Any, None]:
    """Provide PostgreSQL event store with outbox enabled."""
    from eventsource import EventRegistry, PostgreSQLEventStore

    registry = EventRegistry()
    registry.register(TestItemCreated)
    registry.register(TestItemUpdated)
    registry.register(TestItemDeleted)
    registry.register(TestOrderCreated)
    registry.register(TestOrderItemAdded)
    registry.register(TestOrderCompleted)

    store = PostgreSQLEventStore(
        postgres_session_factory,
        event_registry=registry,
        outbox_enabled=True,
    )

    yield store


# ============================================================================
# Redis Event Bus Fixtures
# ============================================================================


@pytest.fixture
def redis_event_bus_factory(
    redis_connection_url: str,
) -> Any:
    """
    Factory fixture for creating Redis event bus instances.

    Returns a factory function that creates a new, unconnected RedisEventBus.
    Each test is responsible for calling connect() and shutdown().

    This avoids event loop issues that occur when connecting in fixture setup.
    """
    from eventsource import (
        REDIS_AVAILABLE,
        EventRegistry,
        RedisEventBus,
        RedisEventBusConfig,
    )

    if not REDIS_AVAILABLE:
        pytest.skip("Redis package not installed")

    def create_bus() -> RedisEventBus:
        registry = EventRegistry()
        registry.register(TestItemCreated)
        registry.register(TestItemUpdated)
        registry.register(TestItemDeleted)
        registry.register(TestOrderCreated)
        registry.register(TestOrderItemAdded)
        registry.register(TestOrderCompleted)

        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix="test_events",
            consumer_group="test_group",
            batch_size=10,
            block_ms=1000,
            single_connection_client=True,  # Avoid event loop issues in tests
        )

        return RedisEventBus(config=config, event_registry=registry)

    return create_bus


@pytest_asyncio.fixture(loop_scope="session")
async def redis_event_bus(
    redis_event_bus_factory: Any,
    clean_redis: None,
) -> AsyncGenerator[Any, None]:
    """
    Provide Redis event bus for integration tests.

    Creates a fresh bus instance, connects it within the test's event loop,
    and ensures proper cleanup after the test.
    """
    bus = redis_event_bus_factory()
    await bus.connect()

    yield bus

    # Only shutdown if still connected
    if bus.is_connected:
        await bus.shutdown()


# ============================================================================
# Repository Fixtures
# ============================================================================


@pytest.fixture
async def postgres_checkpoint_repo(
    postgres_engine: AsyncEngine,
    clean_postgres_tables: None,
) -> AsyncGenerator[Any, None]:
    """Provide PostgreSQL checkpoint repository for integration tests."""
    from eventsource import PostgreSQLCheckpointRepository

    repo = PostgreSQLCheckpointRepository(postgres_engine)
    yield repo


@pytest.fixture
async def postgres_dlq_repo(
    postgres_engine: AsyncEngine,
    clean_postgres_tables: None,
) -> AsyncGenerator[Any, None]:
    """Provide PostgreSQL DLQ repository for integration tests."""
    from eventsource import PostgreSQLDLQRepository

    repo = PostgreSQLDLQRepository(postgres_engine)
    yield repo


@pytest.fixture
async def postgres_outbox_repo(
    postgres_engine: AsyncEngine,
    clean_postgres_tables: None,
) -> AsyncGenerator[Any, None]:
    """Provide PostgreSQL outbox repository for integration tests."""
    from eventsource import PostgreSQLOutboxRepository

    repo = PostgreSQLOutboxRepository(postgres_engine)
    yield repo


# ============================================================================
# Sample Data Fixtures
# ============================================================================


@pytest.fixture
def sample_aggregate_id() -> UUID:
    """Provide a sample aggregate ID."""
    return uuid4()


@pytest.fixture
def sample_tenant_id() -> UUID:
    """Provide a sample tenant ID."""
    return uuid4()


@pytest.fixture
def sample_customer_id() -> UUID:
    """Provide a sample customer ID."""
    return uuid4()


@pytest.fixture
def sample_item_event(sample_aggregate_id: UUID, sample_tenant_id: UUID) -> TestItemCreated:
    """Provide a sample test item created event."""
    return TestItemCreated(
        aggregate_id=sample_aggregate_id,
        tenant_id=sample_tenant_id,
        aggregate_version=1,
        name="Test Item",
        quantity=5,
    )


@pytest.fixture
def sample_order_event(
    sample_aggregate_id: UUID,
    sample_customer_id: UUID,
    sample_tenant_id: UUID,
) -> TestOrderCreated:
    """Provide a sample test order created event."""
    return TestOrderCreated(
        aggregate_id=sample_aggregate_id,
        tenant_id=sample_tenant_id,
        aggregate_version=1,
        customer_id=sample_customer_id,
        total_amount=0.0,
    )
