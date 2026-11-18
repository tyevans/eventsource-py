"""Migration errors must not carry event payload values into logs or raised messages (TASK-0470).

A pydantic ``ValidationError`` echoes the rejected input value. The marker below stands in
for a personal-data value inside an event. It must not appear in captured log records or in
the message of the exception the migration code raises.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest
from pydantic import BaseModel, ValidationError

from eventsource.application.migration.error_handling import ErrorHandler
from eventsource.application.migration.exceptions import PositionMappingError
from eventsource.application.migration.position_mapper import PositionMapper
from eventsource.ports.positions import Position

MARKER = "victim-secret@example.com"


class _Amount(BaseModel):
    amount: int


def _validation_error_with_marker() -> ValidationError:
    try:
        _Amount.model_validate({"amount": MARKER})
    except ValidationError as exc:
        return exc
    raise AssertionError("expected a ValidationError")


def _pos(n: int, store: str = "src") -> Position:
    return Position(store_id=store, key=(n,))


@pytest.fixture
def failing_repo() -> MagicMock:
    repo = MagicMock()
    repo.create = AsyncMock(side_effect=_validation_error_with_marker())
    repo.create_batch = AsyncMock(side_effect=_validation_error_with_marker())
    return repo


class TestPositionMappingErrorRedaction:
    @pytest.mark.asyncio
    async def test_record_mapping_error_hides_payload_value(
        self, failing_repo: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG)
        mapper = PositionMapper(failing_repo, enable_tracing=False)

        with pytest.raises(PositionMappingError) as info:
            await mapper.record_mapping(uuid4(), _pos(1), _pos(2, "tgt"), uuid4())

        assert MARKER not in str(info.value)
        assert MARKER not in (info.value.reason or "")
        assert "ValidationError" in (info.value.reason or "")
        assert MARKER not in caplog.text
        assert "ValidationError" in caplog.text

    @pytest.mark.asyncio
    async def test_record_mappings_batch_error_hides_payload_value(
        self, failing_repo: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG)
        mapper = PositionMapper(failing_repo, enable_tracing=False)

        with pytest.raises(PositionMappingError) as info:
            await mapper.record_mappings_batch(uuid4(), [(_pos(1), _pos(2, "tgt"), uuid4())])

        assert MARKER not in str(info.value)
        assert MARKER not in (info.value.reason or "")
        assert MARKER not in caplog.text


class TestErrorHandlerUnexpectedErrorRedaction:
    @pytest.mark.asyncio
    async def test_unexpected_error_log_has_no_payload_value_or_traceback(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.DEBUG)
        handler = ErrorHandler()

        async def operation() -> None:
            raise _validation_error_with_marker()

        with pytest.raises(ValidationError):
            await handler.execute_with_retry(operation, "copy-batch")

        assert MARKER not in caplog.text
        assert "ValidationError" in caplog.text
        assert all(record.exc_info is None for record in caplog.records)
