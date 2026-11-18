"""DatabaseProjection failures must not log bound SQL parameters.

The handler binds the marker as a parameter of a statement that fails. The
SQLAlchemy message appends ``[parameters: ...]``, so logging ``str(error)``
would leak the marker.
"""

from __future__ import annotations

import logging
from uuid import uuid4

import pytest
from pydantic import Field
from sqlalchemy import text
from sqlalchemy.exc import StatementError
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)

from eventsource.adapters.memory.dlq import InMemoryDLQRepository
from eventsource.adapters.sql.projection import DatabaseProjection
from eventsource.application.projections.retry import ExponentialBackoffRetryPolicy
from eventsource.application.subscriptions.retry_types import RetryConfig
from eventsource.domain.decorators import handles
from eventsource.domain.event import DomainEvent

MARKER = "victim-secret@example.com"


class LedgerEntryRecorded(DomainEvent):
    aggregate_type: str = "Ledger"
    owner: str = Field(default="")


class LedgerProjection(DatabaseProjection):
    @handles(LedgerEntryRecorded)
    async def _on_recorded(self, connection: AsyncConnection, event: LedgerEntryRecorded) -> None:
        # The table does not exist, so the statement fails with the marker bound.
        await connection.execute(
            text("INSERT INTO missing_ledger (owner) VALUES (:owner)"), {"owner": MARKER}
        )


@pytest.fixture
async def engine() -> AsyncEngine:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    yield engine
    await engine.dispose()


async def _fail_one_event(engine: AsyncEngine) -> None:
    projection = LedgerProjection(
        session_factory=async_sessionmaker(engine),
        dlq_repo=InMemoryDLQRepository(),
        retry_policy=ExponentialBackoffRetryPolicy(
            config=RetryConfig(max_retries=0, initial_delay=0.001, max_delay=0.001, jitter=0.0)
        ),
    )
    with pytest.raises(StatementError):
        await projection.handle(LedgerEntryRecorded(aggregate_id=uuid4()))


def _assert_marker_absent(records: list[logging.LogRecord]) -> None:
    assert records, "expected log records from the failure path"
    for record in records:
        assert MARKER not in record.getMessage(), record.getMessage()
        assert MARKER not in repr(record.args)
        assert MARKER not in repr(record.__dict__)


async def test_projection_failure_logs_keep_bound_parameter_out_of_message_and_args(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    # DEBUG for eventsource only: aiosqlite logs bound parameters at DEBUG itself.
    caplog.set_level(logging.DEBUG, logger="eventsource")
    await _fail_one_event(engine)

    projection_records = [
        r for r in caplog.records if r.name == "eventsource.adapters.sql.projection"
    ]
    assert any("sent to DLQ" in r.getMessage() for r in projection_records)
    _assert_marker_absent(projection_records)


async def test_every_eventsource_record_is_free_of_bound_parameter(
    engine: AsyncEngine, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="eventsource")
    await _fail_one_event(engine)

    _assert_marker_absent(list(caplog.records))
