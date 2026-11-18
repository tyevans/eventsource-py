"""PostgreSQL check of the stream audit query in the multi-tenant guide.

The SQL is read from the guide, so this test runs the documented text and not a
copy of it. The guide marks the block with an ``<!-- audit-query -->`` comment.
"""

import re
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from ..conftest import skip_if_no_postgres_infra

pytestmark = [pytest.mark.integration, pytest.mark.postgres, skip_if_no_postgres_infra]

from eventsource.adapters.postgresql import PostgreSQLEventStore  # noqa: E402
from eventsource.ports import ExpectedVersion  # noqa: E402
from eventsource.testing.conformance_ports._fixtures import (  # noqa: E402
    ConformanceEvent,
    make_conformance_registry,
    make_stream,
)

GUIDE = Path(__file__).resolve().parents[3] / "docs" / "guides" / "multi-tenant.md"
TENANT_A = UUID("11111111-1111-1111-1111-111111111111")


def _audit_query() -> str:
    """Return the SQL inside the guide's audit-query block."""
    match = re.search(
        r"<!-- audit-query -->\s*```sql\n(.*?)```", GUIDE.read_text("utf-8"), re.DOTALL
    )
    assert match is not None, "audit-query block is missing from the multi-tenant guide"
    return match.group(1)


async def test_audit_query_returns_only_the_mixed_stream(
    ports_postgres_connection_url: str,
) -> None:
    engine = create_async_engine(ports_postgres_connection_url)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS events CASCADE"))
    store = PostgreSQLEventStore(
        engine, event_registry=make_conformance_registry(), create_schema=True, owns_engine=True
    )
    try:
        clean = make_stream()
        mixed = make_stream()
        await store.append(
            clean,
            [ConformanceEvent(aggregate_id=clean.aggregate_id, tenant_id=TENANT_A)],
            ExpectedVersion.no_stream(),
        )
        await store.append(
            clean,
            [ConformanceEvent(aggregate_id=clean.aggregate_id, tenant_id=TENANT_A)],
            ExpectedVersion.exact(1),
        )
        await store.append(
            mixed,
            [ConformanceEvent(aggregate_id=mixed.aggregate_id, tenant_id=TENANT_A)],
            ExpectedVersion.no_stream(),
        )
        # An untenanted event after tenant A's events.
        await store.append(
            mixed,
            [ConformanceEvent(aggregate_id=mixed.aggregate_id, tenant_id=None)],
            ExpectedVersion.exact(1),
        )

        async with engine.connect() as conn:
            rows = (await conn.execute(text(_audit_query()))).all()
    finally:
        await store.close()

    flagged = {str(row[1]) for row in rows}
    assert flagged == {str(mixed.aggregate_id)}
