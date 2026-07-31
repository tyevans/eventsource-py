"""Fixtures private to `tests/integration/e2e/`.

`test_full_flow.py` exercises `AggregateRepository`, which since the
slice-(a) retype (see `docs/superpowers/specs/2026-07-31-legacy-store-
retirement-design.md`) requires the ports `AggregateStore` surface
(`append` / `read_stream` / `get_stream_version`), not the legacy
`EventStore` ABC. These fixtures build the ports adapter
(`eventsource.adapters.postgresql.store.PostgreSQLEventStore`) instead of
the legacy one the shared `tests/integration/conftest.py` fixtures provide.

Mirrors `tests/integration/adapters/conftest.py`: the ports adapter's
`events` table follows the canonical migrations schema (`tenant_id UUID`),
while the legacy fixtures' shared `events` table uses `tenant_id
VARCHAR(255)` (see that module's docstring). Reusing the shared table would
leave behind a schema the other suite can't use, so this suite gets its own
private database on the same session-scoped testcontainer.
"""

from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from eventsource.adapters.postgresql import PostgreSQLEventStore
from eventsource.events.registry import EventRegistry

from ..conftest import TestOrderCompleted, TestOrderCreated, TestOrderItemAdded

_E2E_DATABASE = "e2e_ports"


def _make_registry() -> EventRegistry:
    registry = EventRegistry()
    registry.register(TestOrderCreated)
    registry.register(TestOrderItemAdded)
    registry.register(TestOrderCompleted)
    return registry


@pytest.fixture(scope="session")
async def e2e_postgres_connection_url(postgres_connection_url: str) -> AsyncGenerator[str, None]:
    """Connection URL for a private `e2e_ports` database.

    Created once per session on the shared testcontainer via a raw
    AUTOCOMMIT connection -- `CREATE DATABASE` cannot run inside a
    transaction, and PostgreSQL has no `CREATE DATABASE IF NOT EXISTS`, so
    existence is checked against `pg_database` first.
    """
    admin_engine = create_async_engine(postgres_connection_url, isolation_level="AUTOCOMMIT")
    try:
        async with admin_engine.connect() as conn:
            exists = await conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": _E2E_DATABASE},
            )
            if exists.first() is None:
                await conn.execute(text(f'CREATE DATABASE "{_E2E_DATABASE}"'))
    finally:
        await admin_engine.dispose()

    # `str(url)` masks the password with `***` -- `render_as_string` must be
    # told explicitly to keep it, or asyncpg fails to authenticate.
    url = make_url(postgres_connection_url).set(database=_E2E_DATABASE)
    yield url.render_as_string(hide_password=False)


@pytest.fixture
async def postgres_event_store(
    e2e_postgres_connection_url: str,
) -> AsyncGenerator[PostgreSQLEventStore, None]:
    """Ports-adapter PostgreSQL event store, fresh `events` table per test."""
    engine: AsyncEngine = create_async_engine(e2e_postgres_connection_url)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS events CASCADE"))

    store = PostgreSQLEventStore(engine, event_registry=_make_registry(), create_schema=True)
    yield store
    await store.close()
