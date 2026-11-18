"""SQL DLQ adapter stores and scopes the owner tenant on a database that predates ADR-0114.

The table is built from the DLQ template without the additive owner column, as an
existing SQLite deployment has it. The adapter adds the column on first use, so
no operator step is needed on SQLite.
"""

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.conftest import skip_if_no_aiosqlite

pytestmark = [pytest.mark.sqlite, skip_if_no_aiosqlite]

from eventsource import create_async_engine  # noqa: E402
from eventsource.adapters.sql.dlq import SQLDLQRepository  # noqa: E402
from eventsource.adapters.sql.schemas import get_schema  # noqa: E402
from eventsource.domain.tenant_context import tenant_scope  # noqa: E402


@pytest.fixture
async def pre_tenancy_engine(tmp_path) -> AsyncIterator:
    url = f"sqlite+aiosqlite:///{tmp_path}/pre_tenancy_dlq.db"
    engine = create_async_engine(url)
    async with engine.begin() as conn:
        raw = await conn.get_raw_connection()
        await raw.driver_connection.executescript(get_schema("dlq", "sqlite", additive=False))
    yield engine
    await engine.dispose()


async def _column_names(engine) -> set[str]:
    async with engine.connect() as conn:
        rows = (await conn.execute(text("PRAGMA table_info(dead_letter_queue)"))).fetchall()
    return {row[1] for row in rows}


async def test_pre_tenancy_sqlite_database_gains_the_column_on_first_use(pre_tenancy_engine):
    assert "tenant_id" not in await _column_names(pre_tenancy_engine)
    repo = SQLDLQRepository(pre_tenancy_engine)
    owner = uuid4()

    await repo.add_failed_event(uuid4(), "P", "Created", {}, RuntimeError("boom"), tenant_id=owner)

    assert "tenant_id" in await _column_names(pre_tenancy_engine)
    (entry,) = await repo.get_failed_events()
    assert entry.tenant_id == owner


async def test_owner_is_visible_only_under_its_own_scope_on_upgraded_database(pre_tenancy_engine):
    repo = SQLDLQRepository(pre_tenancy_engine)
    owner, other = uuid4(), uuid4()
    await repo.add_failed_event(uuid4(), "P", "Created", {}, RuntimeError("boom"), tenant_id=owner)

    async with tenant_scope(owner):
        assert len(await repo.get_failed_events()) == 1
    async with tenant_scope(other):
        assert await repo.get_failed_events() == []
    assert len(await repo.get_failed_events()) == 1


async def test_second_instance_on_upgraded_database_reads_the_owner(pre_tenancy_engine):
    first = SQLDLQRepository(pre_tenancy_engine)
    owner = uuid4()
    await first.add_failed_event(uuid4(), "P", "Created", {}, RuntimeError("x"), tenant_id=owner)

    second = SQLDLQRepository(pre_tenancy_engine)
    async with tenant_scope(owner):
        (entry,) = await second.get_failed_events()
    assert entry.tenant_id == owner


async def test_refailure_keeps_the_original_owner(pre_tenancy_engine):
    repo = SQLDLQRepository(pre_tenancy_engine)
    event_id, owner = uuid4(), uuid4()
    await repo.add_failed_event(event_id, "P", "Created", {}, RuntimeError("one"), tenant_id=owner)
    await repo.add_failed_event(event_id, "P", "Created", {}, RuntimeError("two"), retry_count=1)

    (entry,) = await repo.get_failed_events()
    assert entry.tenant_id == owner
    assert entry.error_message == "RuntimeError: two"


async def test_unscoped_entry_without_owner_stays_untenanted(pre_tenancy_engine):
    repo = SQLDLQRepository(pre_tenancy_engine)
    await repo.add_failed_event(uuid4(), "P", "Created", {}, RuntimeError("boom"))

    (entry,) = await repo.get_failed_events()
    assert entry.tenant_id is None
    async with tenant_scope(uuid4()):
        assert await repo.get_failed_events() == []
