# Testing Strategy

This guide covers comprehensive testing patterns for event-sourced applications using eventsource. It documents the library's testing infrastructure, best practices, and practical examples for testing aggregates, projections, event stores, and the complete event flow.

## Overview

Event sourcing provides excellent testability because:

- **Events are data** - Easy to create, serialize, and assert on
- **State is derived** - State can be reconstructed from event sequences
- **Deterministic behavior** - Same events always produce the same state
- **InMemory implementations** - Fast, isolated tests without external dependencies
- **Event history replay** - Debug issues by replaying exact event sequences

## Test Structure

```
tests/
+-- unit/                    # Fast, isolated tests
|   +-- test_domain_event.py
|   +-- test_aggregate_root.py
|   +-- test_in_memory_event_store.py
|   +-- test_projection_base.py
|   +-- test_projection_coordinator.py
|   +-- test_aggregate_repository.py
|   +-- test_event_bus.py
|   +-- test_checkpoint_repository.py
|   +-- test_dlq_repository.py
|   +-- test_exceptions.py
|   +-- ...
+-- integration/             # Tests with real dependencies
|   +-- stores/
|   |   +-- test_postgresql.py
|   +-- repositories/
|   |   +-- test_checkpoint.py
|   |   +-- test_dlq.py
|   |   +-- test_outbox.py
|   +-- bus/
|   |   +-- test_redis.py
|   +-- e2e/
|   |   +-- test_full_flow.py
|   +-- conftest.py          # Integration test fixtures
+-- fixtures/                # Shared test utilities
+-- conftest.py              # Shared fixtures
```

## Test Configuration

### pytest Configuration (pyproject.toml)

The project uses pytest with automatic async support:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]
addopts = "-v --cov=src/eventsource --cov-report=term-missing"
asyncio_default_fixture_loop_scope = "session"
markers = [
    "integration: marks tests as integration tests (may require docker)",
    "postgres: marks tests that require PostgreSQL",
    "redis: marks tests that require Redis",
    "e2e: marks tests as end-to-end integration tests",
]
```

Key settings:
- `asyncio_mode = "auto"` - Automatically handles async test functions
- `asyncio_default_fixture_loop_scope = "session"` - Shares event loop across session fixtures
- Custom markers for filtering test types

## InMemory Implementations for Testing

eventsource provides InMemory versions of all major components for fast, isolated testing:

```python
from eventsource import InMemoryEventStore, InMemoryEventBus
from eventsource.repositories import (
    InMemoryCheckpointRepository,
    InMemoryDLQRepository,
)

# Fast, isolated testing - no external dependencies
store = InMemoryEventStore()
bus = InMemoryEventBus()
checkpoint_repo = InMemoryCheckpointRepository()
dlq_repo = InMemoryDLQRepository()
```

### InMemoryEventStore Features

The `InMemoryEventStore` provides:
- Full optimistic locking support (version conflicts, ExpectedVersion constants)
- Idempotent event handling (duplicate event IDs are skipped)
- Thread-safe operations
- Testing helper methods:

```python
# Testing helpers
store.clear()                    # Reset state between tests
store.get_event_count()          # Total events stored
store.get_all_events()           # All events in chronological order
store.get_aggregate_ids()        # All aggregate IDs with events
store.get_global_position()      # Current global position counter
```

## Unit Testing Patterns

### Testing Aggregates

#### Basic Aggregate Test

Test aggregate initialization, commands, and state transitions:

```python
from uuid import uuid4
import pytest
from eventsource import AggregateRoot
from pydantic import BaseModel

# State model
class OrderState(BaseModel):
    order_id: UUID
    customer_id: UUID | None = None
    status: str = "draft"
    total: float = 0.0

# Test aggregate
class TestAggregateInitialization:
    def test_new_aggregate_has_correct_id(self) -> None:
        """New aggregate should have the ID passed to constructor."""
        agg_id = uuid4()
        aggregate = OrderAggregate(agg_id)
        assert aggregate.aggregate_id == agg_id

    def test_new_aggregate_has_version_zero(self) -> None:
        """New aggregate should start at version 0."""
        aggregate = OrderAggregate(uuid4())
        assert aggregate.version == 0

    def test_new_aggregate_has_no_uncommitted_events(self) -> None:
        """New aggregate should have no uncommitted events."""
        aggregate = OrderAggregate(uuid4())
        assert aggregate.uncommitted_events == []
        assert not aggregate.has_uncommitted_events
```

#### Testing Commands and Business Rules

```python
class TestCommandMethods:
    def test_command_creates_and_applies_event(self) -> None:
        """Command methods should create and apply events."""
        aggregate = OrderAggregate(uuid4())
        customer_id = uuid4()

        aggregate.create(customer_id, total_amount=99.99)

        assert aggregate.version == 1
        assert aggregate.state is not None
        assert aggregate.state.customer_id == customer_id
        assert aggregate.state.total_amount == 99.99
        assert aggregate.state.status == "created"

    def test_command_validation(self) -> None:
        """Commands should validate business rules."""
        aggregate = OrderAggregate(uuid4())

        # Cannot create twice
        aggregate.create(uuid4(), 50.0)
        with pytest.raises(ValueError, match="Order already created"):
            aggregate.create(uuid4(), 100.0)

    def test_command_state_validation(self) -> None:
        """Commands should validate state preconditions."""
        aggregate = OrderAggregate(uuid4())

        # Cannot ship without creating first
        with pytest.raises(ValueError, match="must be created"):
            aggregate.ship(tracking_number="TRACK-123")
```

#### Testing Event Application

Test that events correctly update aggregate state:

```python
def test_event_application_updates_state(self) -> None:
    """Applying event should update aggregate state."""
    aggregate = OrderAggregate(uuid4())

    event = OrderCreated(
        aggregate_id=aggregate.aggregate_id,
        aggregate_type="Order",
        aggregate_version=1,
        customer_id=uuid4(),
        total_amount=50.0,
    )
    aggregate.apply_event(event)

    assert aggregate.state is not None
    assert aggregate.state.status == "created"
    assert aggregate.state.total_amount == 50.0

def test_apply_event_tracks_uncommitted_by_default(self) -> None:
    """Applying event with is_new=True should track as uncommitted."""
    aggregate = OrderAggregate(uuid4())

    event = OrderCreated(
        aggregate_id=aggregate.aggregate_id,
        aggregate_type="Order",
        aggregate_version=1,
        customer_id=uuid4(),
        total_amount=50.0,
    )
    aggregate.apply_event(event, is_new=True)

    assert len(aggregate.uncommitted_events) == 1
    assert aggregate.uncommitted_events[0] == event
    assert aggregate.has_uncommitted_events

def test_apply_event_does_not_track_historical(self) -> None:
    """Applying event with is_new=False should not track as uncommitted."""
    aggregate = OrderAggregate(uuid4())

    event = OrderCreated(
        aggregate_id=aggregate.aggregate_id,
        aggregate_type="Order",
        aggregate_version=1,
        customer_id=uuid4(),
        total_amount=50.0,
    )
    aggregate.apply_event(event, is_new=False)

    assert len(aggregate.uncommitted_events) == 0
    assert not aggregate.has_uncommitted_events
```

#### Testing Event History Reconstruction

Test that aggregates correctly rebuild state from event history:

```python
def test_load_from_history_replays_events(self) -> None:
    """load_from_history should replay all events to rebuild state."""
    agg_id = uuid4()
    customer_id = uuid4()

    # Create events as if from event store
    events = [
        OrderCreated(
            aggregate_id=agg_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id=customer_id,
            total_amount=50.0,
        ),
        OrderItemAdded(
            aggregate_id=agg_id,
            aggregate_type="Order",
            aggregate_version=2,
            product_name="Widget",
            quantity=2,
            unit_price=25.0,
        ),
        OrderShipped(
            aggregate_id=agg_id,
            aggregate_type="Order",
            aggregate_version=3,
            tracking_number="TRACK-123",
        ),
    ]

    # Create new aggregate and load history
    aggregate = OrderAggregate(agg_id)
    aggregate.load_from_history(events)

    assert aggregate.version == 3
    assert aggregate.state is not None
    assert aggregate.state.status == "shipped"
    assert aggregate.state.tracking_number == "TRACK-123"
    # Historical events should not be uncommitted
    assert len(aggregate.uncommitted_events) == 0

def test_can_apply_new_events_after_loading_history(self) -> None:
    """After loading history, new events should continue from correct version."""
    agg_id = uuid4()

    events = [
        OrderCreated(
            aggregate_id=agg_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id=uuid4(),
            total_amount=50.0,
        ),
    ]

    aggregate = OrderAggregate(agg_id)
    aggregate.load_from_history(events)

    # Now apply new command
    aggregate.ship("TRACK-456")

    assert aggregate.version == 2
    assert len(aggregate.uncommitted_events) == 1
    assert aggregate.state.status == "shipped"
```

### Testing DeclarativeAggregate with @handles

Test the declarative pattern using the `@handles` decorator:

```python
from eventsource import DeclarativeAggregate, handles

class DeclarativeOrderAggregate(DeclarativeAggregate[OrderState]):
    """Order using declarative pattern with @handles decorator."""

    aggregate_type = "Order"

    def _get_initial_state(self) -> OrderState:
        return OrderState(order_id=self.aggregate_id)

    @handles(OrderCreated)
    def _on_order_created(self, event: OrderCreated) -> None:
        self._state = OrderState(
            order_id=self.aggregate_id,
            customer_id=event.customer_id,
            total_amount=event.total_amount,
            status="created",
        )

    @handles(OrderShipped)
    def _on_order_shipped(self, event: OrderShipped) -> None:
        if self._state:
            self._state = self._state.model_copy(
                update={
                    "status": "shipped",
                    "tracking_number": event.tracking_number,
                }
            )

class TestDeclarativeAggregate:
    def test_handlers_are_registered(self) -> None:
        """Handlers marked with @handles should be registered."""
        aggregate = DeclarativeOrderAggregate(uuid4())

        assert OrderCreated in aggregate._event_handlers
        assert OrderShipped in aggregate._event_handlers

    def test_declarative_apply_calls_correct_handler(self) -> None:
        """_apply should dispatch to the correct handler."""
        aggregate = DeclarativeOrderAggregate(uuid4())

        event = OrderCreated(
            aggregate_id=aggregate.aggregate_id,
            aggregate_type="Order",
            aggregate_version=1,
            customer_id=uuid4(),
            total_amount=100.0,
        )
        aggregate.apply_event(event)

        assert aggregate.state.status == "created"
        assert aggregate.state.total_amount == 100.0
```

### Testing Projections

#### Testing DeclarativeProjection

```python
from eventsource.projections import DeclarativeProjection, handles
from eventsource.repositories import InMemoryCheckpointRepository, InMemoryDLQRepository

class TestOrderProjection(DeclarativeProjection):
    """Test projection that builds order summaries."""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.orders = {}

    @handles(OrderCreated)
    async def _on_created(self, event: OrderCreated) -> None:
        self.orders[event.aggregate_id] = {
            "id": event.aggregate_id,
            "customer_id": event.customer_id,
            "status": "created",
            "total": event.total_amount,
        }

    @handles(OrderShipped)
    async def _on_shipped(self, event: OrderShipped) -> None:
        if event.aggregate_id in self.orders:
            self.orders[event.aggregate_id]["status"] = "shipped"
            self.orders[event.aggregate_id]["tracking"] = event.tracking_number

    async def _truncate_read_models(self) -> None:
        self.orders.clear()


# Test fixtures
@pytest.fixture
def checkpoint_repo():
    return InMemoryCheckpointRepository()

@pytest.fixture
def dlq_repo():
    return InMemoryDLQRepository()

@pytest.fixture
def projection(checkpoint_repo, dlq_repo):
    return TestOrderProjection(
        checkpoint_repo=checkpoint_repo,
        dlq_repo=dlq_repo,
    )


# Tests
class TestProjectionHandling:
    @pytest.mark.asyncio
    async def test_projection_handles_event(self, projection):
        """Projection should update read model on event."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            customer_id=uuid4(),
            total_amount=99.99,
            aggregate_version=1,
        )

        await projection.handle(event)

        assert event.aggregate_id in projection.orders
        assert projection.orders[event.aggregate_id]["status"] == "created"

    @pytest.mark.asyncio
    async def test_projection_handles_multiple_events(self, projection):
        """Projection should handle event sequence correctly."""
        order_id = uuid4()

        await projection.handle(OrderCreated(
            aggregate_id=order_id,
            customer_id=uuid4(),
            total_amount=100.00,
            aggregate_version=1,
        ))
        await projection.handle(OrderShipped(
            aggregate_id=order_id,
            tracking_number="TRACK-123",
            aggregate_version=2,
        ))

        assert projection.orders[order_id]["status"] == "shipped"
        assert projection.orders[order_id]["tracking"] == "TRACK-123"

    @pytest.mark.asyncio
    async def test_projection_updates_checkpoint(self, projection, checkpoint_repo):
        """Successful processing should update checkpoint."""
        event = OrderCreated(
            aggregate_id=uuid4(),
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )

        await projection.handle(event)

        checkpoint = await checkpoint_repo.get_checkpoint("TestOrderProjection")
        assert checkpoint == event.event_id

    @pytest.mark.asyncio
    async def test_projection_reset(self, projection):
        """reset() should clear read models and checkpoint."""
        await projection.handle(OrderCreated(
            aggregate_id=uuid4(),
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        ))

        assert len(projection.orders) == 1

        await projection.reset()

        assert len(projection.orders) == 0
```

#### Testing Projection Error Handling

```python
class TestProjectionErrorHandling:
    @pytest.mark.asyncio
    async def test_failed_event_retries(self, checkpoint_repo, dlq_repo):
        """Failed processing should retry with backoff."""
        attempt_count = 0

        class FailingProjection(DeclarativeProjection):
            MAX_RETRIES = 3
            RETRY_BACKOFF_BASE = 0  # No delay in tests

            @handles(OrderCreated)
            async def _on_created(self, event: OrderCreated) -> None:
                nonlocal attempt_count
                attempt_count += 1
                raise ValueError("Processing failed")

        projection = FailingProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(
            aggregate_id=uuid4(),
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )

        with pytest.raises(ValueError, match="Processing failed"):
            await projection.handle(event)

        assert attempt_count == 3  # Initial + 2 retries

    @pytest.mark.asyncio
    async def test_failed_event_goes_to_dlq(self, checkpoint_repo, dlq_repo):
        """Event goes to DLQ after all retries exhausted."""

        class FailingProjection(DeclarativeProjection):
            MAX_RETRIES = 1
            RETRY_BACKOFF_BASE = 0

            @handles(OrderCreated)
            async def _on_created(self, event: OrderCreated) -> None:
                raise ValueError("Handler error")

        projection = FailingProjection(
            checkpoint_repo=checkpoint_repo,
            dlq_repo=dlq_repo,
        )

        event = OrderCreated(
            aggregate_id=uuid4(),
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )

        with pytest.raises(ValueError):
            await projection.handle(event)

        failed_events = await dlq_repo.get_failed_events()
        assert len(failed_events) == 1
        assert failed_events[0]["event_id"] == str(event.event_id)
        assert "Handler error" in failed_events[0]["error_message"]
```

### Testing Event Store Operations

```python
from eventsource import InMemoryEventStore, OptimisticLockError, ExpectedVersion

@pytest.fixture
def store():
    return InMemoryEventStore()

@pytest.fixture
def aggregate_id():
    return uuid4()


class TestEventStoreAppend:
    @pytest.mark.asyncio
    async def test_append_single_event(self, store, aggregate_id):
        """Test appending a single event."""
        event = OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event],
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 1
        assert result.global_position == 1

    @pytest.mark.asyncio
    async def test_append_multiple_events(self, store, aggregate_id):
        """Test appending multiple events at once."""
        events = [
            OrderCreated(
                aggregate_id=aggregate_id,
                customer_id=uuid4(),
                total_amount=50.0,
                aggregate_version=1,
            ),
            OrderShipped(
                aggregate_id=aggregate_id,
                tracking_number="TRACK-123",
                aggregate_version=2,
            ),
        ]

        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=events,
            expected_version=0,
        )

        assert result.success is True
        assert result.new_version == 2


class TestOptimisticLocking:
    @pytest.mark.asyncio
    async def test_version_conflict_raises_error(self, store, aggregate_id):
        """Version mismatch should raise OptimisticLockError."""
        # First append
        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event1],
            expected_version=0,
        )

        # Try to append with wrong version
        event2 = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRACK-123",
            aggregate_version=2,
        )
        with pytest.raises(OptimisticLockError) as exc_info:
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="Order",
                events=[event2],
                expected_version=0,  # Should be 1
            )

        assert exc_info.value.aggregate_id == aggregate_id
        assert exc_info.value.expected_version == 0
        assert exc_info.value.actual_version == 1

    @pytest.mark.asyncio
    async def test_expected_version_any(self, store, aggregate_id):
        """ExpectedVersion.ANY should skip version check."""
        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event1],
            expected_version=0,
        )

        event2 = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRACK-123",
            aggregate_version=2,
        )
        result = await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event2],
            expected_version=ExpectedVersion.ANY,
        )

        assert result.success is True

    @pytest.mark.asyncio
    async def test_expected_version_no_stream(self, store, aggregate_id):
        """NO_STREAM should fail if stream exists."""
        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event1],
            expected_version=0,
        )

        event2 = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRACK-123",
            aggregate_version=2,
        )
        with pytest.raises(OptimisticLockError):
            await store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="Order",
                events=[event2],
                expected_version=ExpectedVersion.NO_STREAM,
            )


class TestEventRetrieval:
    @pytest.mark.asyncio
    async def test_get_events(self, store, aggregate_id):
        """Test retrieving events for an aggregate."""
        events = [
            OrderCreated(
                aggregate_id=aggregate_id,
                customer_id=uuid4(),
                total_amount=50.0,
                aggregate_version=1,
            ),
            OrderShipped(
                aggregate_id=aggregate_id,
                tracking_number="TRACK-123",
                aggregate_version=2,
            ),
        ]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=events,
            expected_version=0,
        )

        stream = await store.get_events(aggregate_id, "Order")

        assert len(stream.events) == 2
        assert stream.version == 2
        assert stream.events[0].event_type == "OrderCreated"
        assert stream.events[1].event_type == "OrderShipped"

    @pytest.mark.asyncio
    async def test_get_events_from_version(self, store, aggregate_id):
        """Test retrieving events from specific version."""
        events = [
            OrderCreated(aggregate_id=aggregate_id, customer_id=uuid4(),
                        total_amount=50.0, aggregate_version=1),
            OrderItemAdded(aggregate_id=aggregate_id, product_name="Widget",
                          quantity=2, unit_price=25.0, aggregate_version=2),
            OrderShipped(aggregate_id=aggregate_id, tracking_number="TRACK-123",
                        aggregate_version=3),
        ]
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=events,
            expected_version=0,
        )

        # Get events starting from version 1 (skip first event)
        stream = await store.get_events(aggregate_id, "Order", from_version=1)

        assert len(stream.events) == 2
        assert stream.events[0].event_type == "OrderItemAdded"


class TestIdempotency:
    @pytest.mark.asyncio
    async def test_duplicate_event_skipped(self, store, aggregate_id):
        """Duplicate events should be silently skipped."""
        event = OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )

        # First append
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event],
            expected_version=0,
        )

        # Append same event again (simulate retry)
        await store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event],
            expected_version=ExpectedVersion.ANY,
        )

        # Event count should still be 1
        assert store.get_event_count() == 1
```

## Integration Testing

### Setup with docker-compose

The project provides `docker-compose.test.yml` for local integration testing:

```yaml
# docker-compose.test.yml
services:
  postgres:
    image: postgres:15
    container_name: eventsource-test-postgres
    environment:
      POSTGRES_DB: eventsource_test
      POSTGRES_USER: test
      POSTGRES_PASSWORD: test
    ports:
      - "${POSTGRES_PORT:-5433}:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U test -d eventsource_test"]
      interval: 5s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7
    container_name: eventsource-test-redis
    ports:
      - "${REDIS_PORT:-6380}:6379"
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 5s
      retries: 5
```

### Running Integration Tests

```bash
# Start test infrastructure
docker-compose -f docker-compose.test.yml up -d

# Run integration tests
pytest tests/integration/ -v

# Run only PostgreSQL tests
pytest tests/integration/ -v -m postgres

# Run only Redis tests
pytest tests/integration/ -v -m redis

# Run end-to-end tests
pytest tests/integration/e2e/ -v

# Cleanup
docker-compose -f docker-compose.test.yml down -v
```

### Using Testcontainers

The project uses testcontainers for automatic container management in integration tests. Containers are automatically started and stopped.

```python
# tests/integration/conftest.py

import pytest
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

# Check if testcontainers and docker are available
TESTCONTAINERS_AVAILABLE = False
try:
    from testcontainers.postgres import PostgresContainer
    from testcontainers.redis import RedisContainer
    TESTCONTAINERS_AVAILABLE = True
except ImportError:
    pass

def is_docker_available() -> bool:
    """Check if Docker is available."""
    import subprocess
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, timeout=5)
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False

DOCKER_AVAILABLE = is_docker_available()

# Skip conditions
skip_if_no_postgres_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE),
    reason="PostgreSQL test infrastructure not available"
)

skip_if_no_redis_infra = pytest.mark.skipif(
    not (TESTCONTAINERS_AVAILABLE and DOCKER_AVAILABLE),
    reason="Redis test infrastructure not available"
)
```

### PostgreSQL Integration Test Fixtures

```python
@pytest.fixture(scope="session")
def postgres_container():
    """Provide PostgreSQL container for integration tests."""
    if not TESTCONTAINERS_AVAILABLE or not DOCKER_AVAILABLE:
        pytest.skip("PostgreSQL testcontainer not available")

    container = PostgresContainer("postgres:15")
    container.start()
    yield container
    container.stop()


@pytest.fixture(scope="session")
def postgres_connection_url(postgres_container):
    """Get PostgreSQL connection URL from container."""
    url = postgres_container.get_connection_url()
    return url.replace("postgresql://", "postgresql+asyncpg://")


@pytest.fixture(scope="session")
async def postgres_engine(postgres_connection_url):
    """Provide SQLAlchemy async engine."""
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy import text

    engine = create_async_engine(
        postgres_connection_url,
        echo=False,
        pool_size=5,
    )

    # Create schema
    async with engine.begin() as conn:
        await conn.execute(text(EVENTS_SCHEMA))
        await conn.execute(text(CHECKPOINTS_SCHEMA))
        await conn.execute(text(DLQ_SCHEMA))

    yield engine
    await engine.dispose()


@pytest.fixture
async def clean_postgres_tables(postgres_engine):
    """Clean all test tables before and after each test."""
    from sqlalchemy import text

    async def truncate():
        async with postgres_engine.begin() as conn:
            await conn.execute(text("TRUNCATE TABLE events CASCADE"))
            await conn.execute(text("TRUNCATE TABLE projection_checkpoints CASCADE"))
            await conn.execute(text("TRUNCATE TABLE dead_letter_queue CASCADE"))

    await truncate()
    yield
    await truncate()


@pytest.fixture
async def postgres_event_store(postgres_session_factory, clean_postgres_tables):
    """Provide PostgreSQL event store for integration tests."""
    from eventsource import PostgreSQLEventStore, EventRegistry

    registry = EventRegistry()
    registry.register(OrderCreated)
    registry.register(OrderShipped)

    store = PostgreSQLEventStore(
        postgres_session_factory,
        event_registry=registry,
    )
    yield store
```

### PostgreSQL Integration Test Example

```python
pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    skip_if_no_postgres_infra,
]


class TestPostgreSQLEventStore:
    async def test_append_and_retrieve(self, postgres_event_store):
        """Test basic append and retrieve operations."""
        aggregate_id = uuid4()
        event = OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )

        result = await postgres_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event],
            expected_version=0,
        )

        assert result.success is True

        stream = await postgres_event_store.get_events(aggregate_id, "Order")
        assert len(stream.events) == 1
        assert stream.events[0].event_type == "OrderCreated"

    async def test_optimistic_locking_across_connections(
        self, postgres_event_store
    ):
        """Test that optimistic locking works across database connections."""
        from eventsource import OptimisticLockError

        aggregate_id = uuid4()

        # First append
        event1 = OrderCreated(
            aggregate_id=aggregate_id,
            customer_id=uuid4(),
            total_amount=50.0,
            aggregate_version=1,
        )
        await postgres_event_store.append_events(
            aggregate_id=aggregate_id,
            aggregate_type="Order",
            events=[event1],
            expected_version=0,
        )

        # Try to append with wrong version
        event2 = OrderShipped(
            aggregate_id=aggregate_id,
            tracking_number="TRACK-123",
            aggregate_version=2,
        )
        with pytest.raises(OptimisticLockError):
            await postgres_event_store.append_events(
                aggregate_id=aggregate_id,
                aggregate_type="Order",
                events=[event2],
                expected_version=0,  # Wrong - should be 1
            )
```

### End-to-End Integration Tests

Test the complete flow from command to projection:

```python
pytestmark = [
    pytest.mark.integration,
    pytest.mark.postgres,
    pytest.mark.e2e,
    skip_if_no_postgres_infra,
]


class TestFullEventFlow:
    async def test_command_to_projection_flow(
        self, postgres_event_store, sample_customer_id
    ):
        """Test complete flow: command -> aggregate -> store -> bus -> projection."""
        from eventsource import AggregateRepository, InMemoryEventBus

        # Setup
        event_bus = InMemoryEventBus()
        projection = OrderSummaryProjection()
        event_bus.subscribe(OrderCreated, projection)
        event_bus.subscribe(OrderShipped, projection)

        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
            event_publisher=event_bus,
        )

        # Execute command
        order_id = uuid4()
        order = OrderAggregate(order_id)
        order.create(customer_id=sample_customer_id, total_amount=99.99)
        await repo.save(order)

        # Ship order
        order = await repo.load(order_id)
        order.ship(tracking_number="TRACK-123")
        await repo.save(order)

        # Verify aggregate state
        loaded = await repo.load(order_id)
        assert loaded.state.status == "shipped"
        assert loaded.version == 2

        # Verify projection was updated
        assert order_id in projection.orders
        assert projection.orders[order_id]["status"] == "shipped"

    async def test_aggregate_rehydration(
        self, postgres_event_store, sample_customer_id
    ):
        """Test that aggregate fully reconstructs from events."""
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create order with multiple events
        order_id = uuid4()
        order = OrderAggregate(order_id)
        order.create(customer_id=sample_customer_id, total_amount=0.0)
        await repo.save(order)

        order = await repo.load(order_id)
        order.add_item(product_name="Widget", quantity=2, unit_price=15.0)
        order.add_item(product_name="Gadget", quantity=1, unit_price=30.0)
        await repo.save(order)

        # Load fresh instance
        loaded = await repo.load(order_id)

        assert loaded.version == 3
        assert loaded.state.total_amount == 60.0  # 2*15 + 1*30
        assert len(loaded.state.items) == 2

    async def test_concurrent_modifications_conflict(
        self, postgres_event_store, sample_customer_id
    ):
        """Test that concurrent modifications cause optimistic lock failure."""
        from eventsource import OptimisticLockError

        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create order
        order_id = uuid4()
        order = OrderAggregate(order_id)
        order.create(customer_id=sample_customer_id, total_amount=50.0)
        await repo.save(order)

        # Load two instances
        order1 = await repo.load(order_id)
        order2 = await repo.load(order_id)

        # Modify and save first
        order1.add_item(product_name="Item 1", quantity=1, unit_price=10.0)
        await repo.save(order1)

        # Try to modify and save second (should fail)
        order2.add_item(product_name="Item 2", quantity=1, unit_price=20.0)

        with pytest.raises(OptimisticLockError):
            await repo.save(order2)

    async def test_projection_rebuild_from_events(
        self, postgres_event_store, sample_customer_id
    ):
        """Test rebuilding projection by replaying all events."""
        repo = AggregateRepository(
            event_store=postgres_event_store,
            aggregate_factory=OrderAggregate,
            aggregate_type="Order",
        )

        # Create orders without projection
        order_ids = []
        for i in range(3):
            order_id = uuid4()
            order_ids.append(order_id)

            order = OrderAggregate(order_id)
            order.create(customer_id=sample_customer_id, total_amount=float(i * 10))
            await repo.save(order)

        # "Rebuild" projection by reading all events
        projection = OrderSummaryProjection()

        async for stored_event in postgres_event_store.read_all():
            await projection.handle(stored_event.event)

        # Verify projection has correct state
        assert len(projection.orders) == 3
```

## Test Helpers and Utilities

### Event Builder Pattern

Create a helper for building test event sequences:

```python
from uuid import UUID, uuid4
from typing import TypeVar

T = TypeVar("T", bound=DomainEvent)


class EventBuilder:
    """Helper for building test events with consistent aggregate context."""

    def __init__(self, aggregate_id: UUID | None = None):
        self.aggregate_id = aggregate_id or uuid4()
        self.version = 0

    def order_created(self, **kwargs) -> OrderCreated:
        self.version += 1
        return OrderCreated(
            aggregate_id=self.aggregate_id,
            customer_id=kwargs.get("customer_id", uuid4()),
            total_amount=kwargs.get("total_amount", 0.0),
            aggregate_version=self.version,
        )

    def order_shipped(self, **kwargs) -> OrderShipped:
        self.version += 1
        return OrderShipped(
            aggregate_id=self.aggregate_id,
            tracking_number=kwargs.get("tracking_number", "TRACK-TEST"),
            aggregate_version=self.version,
        )

    def item_added(self, **kwargs) -> OrderItemAdded:
        self.version += 1
        return OrderItemAdded(
            aggregate_id=self.aggregate_id,
            product_name=kwargs.get("product_name", "Test Product"),
            quantity=kwargs.get("quantity", 1),
            unit_price=kwargs.get("unit_price", 10.0),
            aggregate_version=self.version,
        )


# Usage in tests
def test_with_builder():
    builder = EventBuilder()

    events = [
        builder.order_created(total_amount=0.0),
        builder.item_added(product_name="Widget", quantity=2, unit_price=15.0),
        builder.item_added(product_name="Gadget", quantity=1, unit_price=30.0),
    ]

    order = OrderAggregate(builder.aggregate_id)
    order.load_from_history(events)

    assert order.version == 3
    assert len(order.state.items) == 2
```

### Aggregate Test Helper

```python
class AggregateTestHelper:
    """Helper for testing aggregates with repository pattern."""

    def __init__(self, aggregate_class, aggregate_type: str):
        self.aggregate_class = aggregate_class
        self.aggregate_type = aggregate_type
        self.store = InMemoryEventStore()
        self.repo = AggregateRepository(
            self.store, aggregate_class, aggregate_type
        )

    def create(self, aggregate_id: UUID | None = None):
        """Create a new aggregate instance."""
        return self.aggregate_class(aggregate_id or uuid4())

    async def save(self, aggregate):
        """Save aggregate to store."""
        await self.repo.save(aggregate)

    async def load(self, aggregate_id: UUID):
        """Load aggregate from store."""
        return await self.repo.load(aggregate_id)

    def get_events(self, aggregate_id: UUID) -> list[DomainEvent]:
        """Get all events for an aggregate (for assertions)."""
        events = self.store._events.get(aggregate_id, [])
        return [e for e in events if e.aggregate_type == self.aggregate_type]

    def clear(self):
        """Reset the store between tests."""
        self.store.clear()


# Usage
@pytest.fixture
def order_helper():
    return AggregateTestHelper(OrderAggregate, "Order")


@pytest.mark.asyncio
async def test_order_flow(order_helper):
    order = order_helper.create()
    order.create(uuid4(), 50.0)
    await order_helper.save(order)

    events = order_helper.get_events(order.aggregate_id)
    assert len(events) == 1
    assert events[0].event_type == "OrderCreated"
```

### Given-When-Then Pattern

Structure tests using the Given-When-Then pattern for clarity:

```python
class TestOrderLifecycle:
    @pytest.mark.asyncio
    async def test_order_can_be_shipped_after_creation(self, order_helper):
        """
        Given: An order that has been created
        When: The order is shipped
        Then: The order status should be 'shipped'
        """
        # Given
        order_id = uuid4()
        order = order_helper.create(order_id)
        order.create(customer_id=uuid4(), total_amount=100.0)
        await order_helper.save(order)

        # When
        loaded = await order_helper.load(order_id)
        loaded.ship(tracking_number="TRACK-123")
        await order_helper.save(loaded)

        # Then
        final = await order_helper.load(order_id)
        assert final.state.status == "shipped"
        assert final.state.tracking_number == "TRACK-123"
        assert final.version == 2

    @pytest.mark.asyncio
    async def test_order_cannot_be_shipped_twice(self, order_helper):
        """
        Given: An order that has already been shipped
        When: Attempting to ship it again
        Then: A ValueError should be raised
        """
        # Given
        order = order_helper.create()
        order.create(uuid4(), 50.0)
        order.ship("TRACK-123")
        await order_helper.save(order)

        # When/Then
        loaded = await order_helper.load(order.aggregate_id)
        with pytest.raises(ValueError, match="must be created"):
            loaded.ship("TRACK-456")
```

## Running Tests

### Common Commands

```bash
# Run all tests
pytest

# Run unit tests only (fast)
pytest tests/unit/

# Run integration tests only
pytest tests/integration/

# Run with coverage report
pytest --cov=eventsource --cov-report=html

# Run specific test file
pytest tests/unit/test_aggregate_root.py

# Run specific test class
pytest tests/unit/test_aggregate_root.py::TestEventApplication

# Run specific test method
pytest tests/unit/test_aggregate_root.py::TestEventApplication::test_apply_event_updates_version

# Run tests with verbose output
pytest -v

# Run tests matching a pattern
pytest -k "aggregate"

# Run tests by marker
pytest -m postgres
pytest -m "not integration"

# Run with parallel execution (requires pytest-xdist)
pytest -n auto
```

### CI/CD Integration

Example GitHub Actions workflow for running tests:

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -e ".[dev]"
      - run: pytest tests/unit/ -v --cov=eventsource

  integration-tests:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_DB: eventsource_test
          POSTGRES_USER: test
          POSTGRES_PASSWORD: test
        ports:
          - 5433:5432
      redis:
        image: redis:7
        ports:
          - 6380:6379
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -e ".[dev,postgresql,redis]"
      - run: pytest tests/integration/ -v -m "postgres or redis"
```

## Best Practices

### 1. Prefer Unit Tests with InMemory Stores

Unit tests using `InMemoryEventStore` are fast and deterministic:

```python
# Fast: ~1ms per test
@pytest.mark.asyncio
async def test_with_in_memory_store():
    store = InMemoryEventStore()
    # ... test logic

# Slow: ~100ms+ per test (database round trips)
@pytest.mark.asyncio
@pytest.mark.integration
async def test_with_postgres_store(postgres_event_store):
    # ... test logic
```

### 2. Test Behavior, Not Implementation

Focus on state changes and business rules, not internal details:

```python
# Good: Tests business behavior
def test_order_total_includes_all_items():
    order = OrderAggregate(uuid4())
    order.create(uuid4(), 0.0)
    order.add_item("Widget", 2, 10.0)
    order.add_item("Gadget", 1, 25.0)

    assert order.state.total_amount == 45.0

# Bad: Tests implementation detail
def test_order_has_two_uncommitted_events():
    order = OrderAggregate(uuid4())
    order.create(uuid4(), 0.0)
    order.add_item("Widget", 2, 10.0)

    assert len(order._uncommitted_events) == 2  # Implementation detail
```

### 3. Use Fixtures for Common Setup

Share setup code via pytest fixtures:

```python
@pytest.fixture
def event_store():
    return InMemoryEventStore()

@pytest.fixture
def order_repo(event_store):
    return AggregateRepository(
        event_store=event_store,
        aggregate_factory=OrderAggregate,
        aggregate_type="Order",
    )

@pytest.fixture
def sample_customer_id():
    return uuid4()
```

### 4. Test Edge Cases

Cover boundary conditions and error scenarios:

```python
class TestEdgeCases:
    def test_empty_order(self):
        """Order with no items should have zero total."""
        order = OrderAggregate(uuid4())
        order.create(uuid4(), 0.0)
        assert order.state.total_amount == 0.0

    def test_large_event_history(self):
        """Aggregate should handle many events."""
        order = OrderAggregate(uuid4())
        order.create(uuid4(), 0.0)

        for i in range(1000):
            order.add_item(f"Item {i}", 1, 1.0)

        assert order.version == 1001
        assert len(order.state.items) == 1000

    def test_concurrent_version_conflict(self):
        """Concurrent modifications should raise error."""
        # ... test optimistic locking
```

### 5. Isolate Tests

Each test should be independent:

```python
@pytest.fixture
def store():
    """Fresh store for each test."""
    return InMemoryEventStore()

# Or clear between tests
@pytest.fixture(autouse=True)
def clean_store(store):
    yield
    store.clear()
```

### 6. Mark Integration Tests

Use markers to categorize tests:

```python
@pytest.mark.integration
@pytest.mark.postgres
async def test_postgresql_operations(postgres_store):
    # ...

@pytest.mark.integration
@pytest.mark.redis
async def test_redis_event_bus(redis_bus):
    # ...
```

### 7. Test Async Code Properly

Always use `pytest.mark.asyncio` for async tests:

```python
@pytest.mark.asyncio
async def test_async_operation():
    store = InMemoryEventStore()
    result = await store.append_events(...)
    assert result.success
```

## Validating Code Examples

The project includes a validation script that ensures all code examples in the `examples/` directory remain valid and runnable.

### Running Example Validation

```bash
# Validate syntax only (fast)
python scripts/validate_examples.py --syntax

# Validate syntax and run examples
python scripts/validate_examples.py

# Also validate Python code blocks in documentation (optional)
python scripts/validate_examples.py --docs
```

### What Gets Validated

1. **Syntax Check**: All `.py` files in `examples/` are parsed with `ast.parse()` to verify valid Python syntax.

2. **Execution Check**: Each example is run as a standalone script. Examples must:
   - Exit with code 0 (success)
   - Complete within 60 seconds
   - Not require external services (use InMemory implementations)

3. **Documentation Code Blocks** (optional with `--docs`): Python code blocks in markdown files are syntax-checked.

### CI Integration

Example validation runs automatically in the CI workflow (`.github/workflows/docs.yml`):

```yaml
validate-examples:
  runs-on: ubuntu-latest
  steps:
    - uses: actions/checkout@v4
    - uses: actions/setup-python@v5
      with:
        python-version: '3.11'
    - run: pip install -e .
    - run: python scripts/validate_examples.py --syntax
    - run: python scripts/validate_examples.py
```

### Writing Valid Examples

Ensure examples follow these patterns:

```python
#!/usr/bin/env python3
"""
Example: Descriptive Title

Brief description of what this example demonstrates.
"""
import asyncio
from eventsource import InMemoryEventStore  # Use InMemory implementations

async def main():
    # Example code here...
    print("Example completed successfully!")

if __name__ == "__main__":
    asyncio.run(main())
```

Key requirements:
- Use InMemory stores for self-contained execution
- Include proper `if __name__ == "__main__":` guard
- Exit cleanly (no infinite loops or input waiting)
- Print success message for verification

## See Also

- [Architecture Overview](../architecture.md) - System design and patterns
- [API Reference - Event Stores](../api/stores.md) - EventStore interface details
- [API Reference - Aggregates](../api/aggregates.md) - AggregateRoot documentation
- [API Reference - Projections](../api/projections.md) - Projection system
- [Examples - Testing Patterns](../examples/testing.md) - Additional examples
