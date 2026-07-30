"""Fixtures private to the port-adapter integration tests.

The new-port PostgreSQL conformance/no-skip tests DROP + recreate the
`events` table against the canonical `migrations/schemas/events.sql`
schema (`tenant_id UUID`). The legacy `tests/integration/stores/
test_postgresql.py` suite shares the same session-scoped testcontainer but
provisions `events` itself with `tenant_id VARCHAR(255)` (see
`tests/integration/conftest.py`'s `EVENTS_SCHEMA_STATEMENTS`) and its store
binds `str(...)` for that column. If both suites touch the same database's
`events` table in the same session, whichever runs first leaves behind a
schema the other can't use (`DatatypeMismatchError`).

To avoid that cross-suite drift (a real, but out-of-scope, VARCHAR-vs-UUID
inconsistency between the legacy conftest and the canonical migrations
schema) without touching the legacy fixtures or store, this module gives
the port-adapter tests their own private database on the same
testcontainer.
"""

from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

_PORTS_DATABASE = "ports_conformance"


@pytest.fixture(scope="session")
async def ports_postgres_connection_url(postgres_connection_url: str) -> AsyncGenerator[str, None]:
    """Connection URL for a private `ports_conformance` database.

    Created once per session on the shared testcontainer via a raw
    AUTOCOMMIT connection to the container's default database --
    `CREATE DATABASE` cannot run inside a transaction, and PostgreSQL has
    no `CREATE DATABASE IF NOT EXISTS`, so existence is checked against
    `pg_database` first.
    """
    admin_engine = create_async_engine(postgres_connection_url, isolation_level="AUTOCOMMIT")
    try:
        async with admin_engine.connect() as conn:
            exists = await conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": _PORTS_DATABASE},
            )
            if exists.first() is None:
                await conn.execute(text(f'CREATE DATABASE "{_PORTS_DATABASE}"'))
    finally:
        await admin_engine.dispose()

    # `str(url)` masks the password with `***` -- `render_as_string` must be
    # told explicitly to keep it, or asyncpg fails to authenticate.
    url = make_url(postgres_connection_url).set(database=_PORTS_DATABASE)
    yield url.render_as_string(hide_password=False)
