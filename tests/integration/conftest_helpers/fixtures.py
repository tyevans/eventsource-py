"""PostgreSQL and Redis fixtures, engines, and repositories for integration tests."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4

import pytest
import pytest_asyncio
import sqlalchemy.ext.asyncio as sa_async
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

import eventsource as es
from eventsource.adapters.postgresql import PostgreSQLEventStore
from eventsource.adapters.sql.schemas import get_schema
from eventsource.domain.event_registry import EventRegistry

from . import domain as _domain
from . import schemas as _schemas

_EVENT_CLASSES = (
    _domain.TestItemCreated,
    _domain.TestItemUpdated,
    _domain.TestItemDeleted,
    _domain.TestOrderCreated,
    _domain.TestOrderItemAdded,
    _domain.TestOrderCompleted,
)


def _build_test_registry() -> Any:
    registry = EventRegistry()
    for cls in _EVENT_CLASSES:
        registry.register(cls)
    return registry


@pytest.fixture(scope="session")
async def postgres_engine(postgres_connection_url: str) -> AsyncGenerator[AsyncEngine]:
    engine = sa_async.create_async_engine(
        postgres_connection_url, echo=False, pool_size=5, max_overflow=10
    )
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        assert raw.driver_connection is not None
        await raw.driver_connection.execute(get_schema("events"))
        for stmt in (
            *_schemas.CHECKPOINTS_SCHEMA_STATEMENTS,
            *_schemas.DLQ_SCHEMA_STATEMENTS,
            *_schemas.OUTBOX_SCHEMA_STATEMENTS,
        ):
            await conn.execute(text(stmt))
    yield engine
    await engine.dispose()


@pytest.fixture
async def postgres_session_factory(
    postgres_engine: AsyncEngine,
) -> AsyncGenerator[async_sessionmaker[AsyncSession]]:
    yield async_sessionmaker(postgres_engine, class_=AsyncSession, expire_on_commit=False)


@pytest.fixture
async def clean_postgres_tables(postgres_engine: AsyncEngine) -> AsyncGenerator[None]:
    async def truncate_tables() -> None:
        async with postgres_engine.begin() as conn:
            for tbl in ("events", "projection_checkpoints", "dead_letter_queue", "event_outbox"):
                await conn.execute(text(f"TRUNCATE TABLE {tbl} CASCADE"))

    await truncate_tables()
    yield
    await truncate_tables()


def _connect_redis(url: str) -> Any:
    try:
        import redis.asyncio as redis
    except ImportError:
        pytest.skip("redis package not installed")
    return redis.from_url(url, decode_responses=True, single_connection_client=True)


@pytest_asyncio.fixture(loop_scope="session")
async def redis_client(redis_connection_url: str) -> AsyncGenerator[Any]:
    client = _connect_redis(redis_connection_url)
    await client.flushall()
    yield client
    await client.flushall()
    await client.aclose()


@pytest_asyncio.fixture(loop_scope="session")
async def clean_redis(redis_connection_url: str) -> AsyncGenerator[None]:
    for phase in ("before", "after"):
        client = _connect_redis(redis_connection_url)
        await client.flushall()
        await client.aclose()
        if phase == "before":
            yield


@pytest.fixture
async def postgres_event_store(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    yield PostgreSQLEventStore(
        postgres_engine, event_registry=_build_test_registry(), outbox_enabled=False
    )


@pytest.fixture
async def postgres_event_store_with_outbox(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    yield PostgreSQLEventStore(
        postgres_engine, event_registry=_build_test_registry(), outbox_enabled=True
    )


@pytest.fixture
def redis_event_bus_factory(redis_connection_url: str) -> Any:
    if not es.REDIS_AVAILABLE:
        pytest.skip("Redis package not installed")

    def create_bus() -> es.RedisEventBus:
        cfg = es.RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix="test_events",
            consumer_group="test_group",
            stream_read_count=10,
            block_ms=1000,
            single_connection_client=True,
        )
        return es.RedisEventBus(config=cfg, event_registry=_build_test_registry())

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
    yield es.SQLCheckpointRepository(postgres_engine)


@pytest.fixture
async def postgres_dlq_repo(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    yield es.SQLDLQRepository(postgres_engine)


@pytest.fixture
async def postgres_outbox_repo(
    postgres_engine: AsyncEngine, clean_postgres_tables: None
) -> AsyncGenerator[Any]:
    yield es.PostgreSQLOutboxRepository(postgres_engine)


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
def sample_item_event(sample_aggregate_id: UUID, sample_tenant_id: UUID) -> Any:
    kw: dict[str, Any] = {"aggregate_id": sample_aggregate_id, "tenant_id": sample_tenant_id}
    return _domain.TestItemCreated(**kw, aggregate_version=1, name="Test Item", quantity=5)


@pytest.fixture
def sample_order_event(
    sample_aggregate_id: UUID, sample_customer_id: UUID, sample_tenant_id: UUID
) -> Any:
    kw: dict[str, Any] = {"aggregate_id": sample_aggregate_id, "tenant_id": sample_tenant_id}
    return _domain.TestOrderCreated(
        **kw, aggregate_version=1, customer_id=sample_customer_id, total_amount=0.0
    )
