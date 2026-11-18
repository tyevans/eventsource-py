"""error_summary keeps payload values and SQL parameters out of exception summaries."""

from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from eventsource.observability.error_summary import error_summary

MARKER = "victim-secret@example.com"


class _Account(BaseModel):
    owner_id: UUID


def test_validation_error_lists_location_and_type_only() -> None:
    with pytest.raises(Exception) as excinfo:
        _Account.model_validate({"owner_id": MARKER})

    summary = error_summary(excinfo.value)

    assert MARKER not in summary
    assert summary.startswith("ValidationError(")
    assert "('owner_id',)" in summary


async def test_statement_error_omits_sql_text_and_parameters() -> None:
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    try:
        async with engine.connect() as conn:
            with pytest.raises(OperationalError) as excinfo:
                await conn.execute(text("SELECT * FROM missing WHERE v = :v"), {"v": MARKER})
    finally:
        await engine.dispose()

    summary = error_summary(excinfo.value)

    assert MARKER not in summary
    assert "missing" not in summary
    assert summary == "OperationalError(orig=OperationalError)"


def test_other_exceptions_keep_class_and_message() -> None:
    assert error_summary(ValueError("boom")) == "ValueError: boom"
    assert error_summary(RuntimeError()) == "RuntimeError"


def test_control_characters_in_message_are_escaped() -> None:
    summary = error_summary(ValueError("first\nsecond"))

    assert summary == "ValueError: first\\x0asecond"
