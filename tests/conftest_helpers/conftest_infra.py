"""Infrastructure, SQLite, and OpenTelemetry fixtures for pytest."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import TYPE_CHECKING, Any

import pytest
import pytest_asyncio

if TYPE_CHECKING:
    import aiosqlite

AIOSQLITE_AVAILABLE = False
try:
    import aiosqlite

    AIOSQLITE_AVAILABLE = True
except ImportError:
    aiosqlite = None  # type: ignore[assignment]

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

skip_if_no_aiosqlite = pytest.mark.skipif(not AIOSQLITE_AVAILABLE, reason="aiosqlite not installed")
skip_if_no_otel_metrics = pytest.mark.skipif(
    not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed"
)


@pytest.fixture
def metric_reader() -> Any:
    """Provide an InMemoryMetricReader and global MeterProvider for testing metrics."""
    if not OTEL_METRICS_AVAILABLE:
        pytest.skip("opentelemetry-sdk not installed")

    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    old_provider = otel_metrics.get_meter_provider()
    otel_metrics.set_meter_provider(provider)
    yield reader
    otel_metrics.set_meter_provider(old_provider)


@pytest.fixture
def reset_kafka_meter() -> Any:
    """Reset the kafka module's cached meter between tests."""
    try:
        import eventsource.adapters.kafka.bus as kafka_module
    except ImportError:
        pytest.skip("eventsource.adapters.kafka not available")
        return

    kafka_module._meter = None
    yield
    kafka_module._meter = None


@pytest_asyncio.fixture
async def sqlite_connection() -> AsyncGenerator[Any]:
    """Provide a raw aiosqlite connection to an in-memory database."""
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    conn = await aiosqlite.connect(":memory:")
    conn.row_factory = aiosqlite.Row
    yield conn
    await conn.close()


@pytest_asyncio.fixture
async def sqlite_checkpoint_repo(tmp_path: Any) -> AsyncGenerator[Any]:
    """Provide a SQLCheckpointRepository backed by a SQLite engine, schema initialized."""
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource import create_async_engine
    from eventsource.adapters.sql.checkpoints import SQLCheckpointRepository
    from eventsource.adapters.sql.schemas import get_schema

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/checkpoint_repo.db")
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        assert raw.driver_connection is not None
        await raw.driver_connection.executescript(get_schema("checkpoints", backend="sqlite"))
        await raw.driver_connection.executescript(get_schema("events", backend="sqlite"))

    repo = SQLCheckpointRepository(engine)
    yield repo
    await engine.dispose()


@pytest_asyncio.fixture
async def sqlite_outbox_repo(sqlite_connection: aiosqlite.Connection) -> AsyncGenerator[Any]:
    """Provide a SQLiteOutboxRepository with schema initialized."""
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource.adapters.sql.schemas import get_schema
    from eventsource.adapters.sqlite.outbox import SQLiteOutboxRepository

    await sqlite_connection.executescript(get_schema("outbox", backend="sqlite"))
    await sqlite_connection.commit()

    repo = SQLiteOutboxRepository(sqlite_connection)
    yield repo


@pytest_asyncio.fixture
async def sqlite_dlq_repo(tmp_path: Any) -> AsyncGenerator[Any]:
    """Provide a SQLDLQRepository backed by a SQLite engine, schema initialized."""
    if not AIOSQLITE_AVAILABLE:
        pytest.skip("aiosqlite not installed")

    from eventsource import create_async_engine
    from eventsource.adapters.sql.dlq import SQLDLQRepository
    from eventsource.adapters.sql.schemas import get_schema

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path}/dlq_repo.db")
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        assert raw.driver_connection is not None
        await raw.driver_connection.executescript(get_schema("dlq", backend="sqlite"))

    repo = SQLDLQRepository(engine)
    yield repo
    await engine.dispose()


__all__ = [
    "AIOSQLITE_AVAILABLE",
    "OTEL_METRICS_AVAILABLE",
    "metric_reader",
    "reset_kafka_meter",
    "skip_if_no_aiosqlite",
    "skip_if_no_otel_metrics",
    "sqlite_checkpoint_repo",
    "sqlite_connection",
    "sqlite_dlq_repo",
    "sqlite_outbox_repo",
]
