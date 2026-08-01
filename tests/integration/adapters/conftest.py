"""Fixtures private to the port-adapter integration tests.

These suites DROP and recreate the `events` table between fixtures. The
shared session-scoped `postgres_engine` (tests/integration/conftest.py)
provisions the same canonical `migrations/schemas/events.sql` schema, so
there is no longer any schema drift between them -- but a suite that
recreates tables mid-session would still pull the rug out from under
every other suite sharing the database. This module gives the
port-adapter tests their own private database on the same
testcontainer, so their table churn is invisible to everything else.
"""

from collections.abc import AsyncGenerator

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

_PORTS_DATABASE = "ports_conformance"


@pytest.fixture(scope="session")
async def ports_postgres_connection_url(postgres_connection_url: str) -> AsyncGenerator[str]:
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
