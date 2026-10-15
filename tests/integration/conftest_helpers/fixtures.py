"""PostgreSQL and Redis fixtures, engines, and repositories for integration tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from .domain import (
    TestItemCreated,
    TestItemDeleted,
    TestItemUpdated,
    TestOrderCompleted,
    TestOrderCreated,
    TestOrderItemAdded,
)
from .schemas import (
    CHECKPOINTS_SCHEMA_STATEMENTS,
    DLQ_SCHEMA_STATEMENTS,
    OUTBOX_SCHEMA_STATEMENTS,
)


def _build_test_registry() -> Any:
    from eventsource.domain.event_registry import EventRegistry

    registry = EventRegistry()
    for event_cls in (
        TestItemCreated,
        TestItemUpdated,
        TestItemDeleted,
        TestOrderCreated,
        TestOrderItemAdded,
        TestOrderCompleted,
    ):
        registry.register(event_cls)
    return registry


@pytest.fixture(scope="session")
async def postgres_engine(postgres_connection_url: str) -> AsyncGenerator[AsyncEngine]:
    """Provide SQLAlchemy async engine connected to PostgreSQL container."""
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import create_async_engine

    from eventsource.adapters.sql.schemas import get_schema

    engine = create_async_engine(postgres_connection_url, echo=False, pool_size=5, max_overflow=10)
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        driver_connection = raw.driver_connection
        assert driver_connection is not None
        await driver_connection.execute(get_schema("events"))
        for statement in (
            *CHECKPOINTS_SCHEMA_STATEMENTS,
            *DLQ_SCHEMA_STATEMENTS,
            *OUTBOX_SCHEMA_STATEMENTS,
        ):
            await conn.execute(text(statement))
    yield engine
    await engine.dispose()


@pytest.fixture
async def postgres_session_factory(
    postgres_engine: AsyncEngine,
) -> AsyncGenerator[async_sessionmaker[AsyncSession]]:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    yield async_sessionmaker(postgres_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture
async def clean_postgres_tables(postgres_engine: AsyncEngine) -> AsyncGenerator[None]:
    from sqlalchemy import text

    async def truncate_tables() -> None:
        async with postgres_engine.begin() as conn:
            for tbl in ("events", "projection_checkpoints", "dead_letter_queue", "event_outbox"):
                await conn.execute(text(f"TRUNCATE TABLE {tbl} CASCADE"))

    await truncate_tables()
    yield
    await truncate_tables()


@pytest_asyncio.fixture(loop_scope="session")
async def redis_client(redis_connection_url: str) -> AsyncGenerator[Any]:
    try:
        import redis.asyncio as redis
    except ImportError:
        pytest.skip("redis package not installed")

    client = redis.from_url(
        redis_connection_url, decode_responses=True, single_connection_client=True
    )
    await client.flushall()
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture(loop_scope="session")
async def clean_redis(redis_connection_url: str) -> AsyncGenerator[None]:
    try:
        import redis.asyncio as redis
    except ImportError:
        pytest.skip("redis package not installed")

    for phase in ("before", "after"):
        client = redis.from_url(
            redis_connection_url, decode_responses=True, single_connection_client=True
        )
        await client.flushall()
        await client.aclose()
        if phase == "before":
            yield


@pytest.fixture
async def postgres_event_store(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    from eventsource.adapters.postgresql import PostgreSQLEventStore

    yield PostgreSQLEventStore(
        postgres_engine, event_registry=_build_test_registry(), outbox_enabled=False
    )


@pytest.fixture
async def postgres_event_store_with_outbox(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    from eventsource.adapters.postgresql import PostgreSQLEventStore

    yield PostgreSQLEventStore(
        postgres_engine, event_registry=_build_test_registry(), outbox_enabled=True
    )


@pytest.fixture
def redis_event_bus_factory(redis_connection_url: str) -> Any:
    from eventsource import REDIS_AVAILABLE, RedisEventBus, RedisEventBusConfig

    if not REDIS_AVAILABLE:
        pytest.skip("Redis package not installed")

    def create_bus() -> RedisEventBus:
        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix="test_events",
            consumer_group="test_group",
            stream_read_count=10,
            block_ms=1000,
            single_connection_client=True,
        )
        return RedisEventBus(config=config, event_registry=_build_test_registry())

    return create_bus


@pytest_asyncio.fixture(loop_scope="session")
async def redis_event_bus(redis_event_bus_factory: Any, clean_redis: None) -> AsyncGenerator[Any]:
    bus = redis_event_bus_factory()
    await bus.connect()
    yield bus
    if bus.is_connected:
        await bus.shutdown()


@pytest.fixture
async def postgres_checkpoint_repo(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    from eventsource import SQLCheckpointRepository

    yield SQLCheckpointRepository(postgres_engine)


@pytest.fixture
async def postgres_dlq_repo(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    from eventsource import SQLDLQRepository

    yield SQLDLQRepository(postgres_engine)


@pytest.fixture
async def postgres_outbox_repo(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    from eventsource import PostgreSQLOutboxRepository

    yield PostgreSQLOutboxRepository(postgres_engine)


@pytest.fixture
def sample_aggregate_id() -> UUID:
    return uuid4()


@pytest.fixture
def sample_tenant_id() -> UUID:
    return uuid4()


@pytest.fixture
def sample_customer_id() -> UUID:
    return uuid4()


@pytest.fixture
def sample_item_event(sample_aggregate_id: UUID, sample_tenant_id: UUID) -> TestItemCreated:
    return TestItemCreated(
        aggregate_id=sample_aggregate_id,
        tenant_id=sample_tenant_id,
        aggregate_version=1,
        name="Test Item",
        quantity=5,
    )


@pytest.fixture
def sample_order_event(
    sample_aggregate_id: UUID, sample_customer_id: UUID, sample_tenant_id: UUID
) -> TestOrderCreated:
    return TestOrderCreated(
        aggregate_id=sample_aggregate_id,
        tenant_id=sample_tenant_id,
        aggregate_version=1,
        customer_id=sample_customer_id,
        total_amount=0.0,
    )
