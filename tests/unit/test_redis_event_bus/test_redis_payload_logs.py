"""A failing Redis handler's log records never carry the event payload marker.

The handler validates the event's payload with pydantic, so the raised
``ValidationError`` echoes the marker value. The message is driven through
``_process_message`` with the mocked Redis client, as the pubsub tests do.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig
from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry

from . import _fixtures as fx

config, mock_redis = fx.config, fx.mock_redis

MARKER = "victim-secret@example.com"


class AccountOpened(DomainEvent):
    aggregate_type: str = "Account"
    owner_ref: str


class _Owner(BaseModel):
    owner_id: UUID


def _msg_data(event: DomainEvent) -> dict[str, str]:
    return {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "aggregate_id": str(event.aggregate_id),
        "aggregate_type": event.aggregate_type,
        "occurred_at": datetime.now(UTC).isoformat(),
        "payload": event.model_dump_json(),
    }


async def test_failed_handler_keeps_payload_marker_out_of_log_records(
    caplog: pytest.LogCaptureFixture, config: RedisEventBusConfig, mock_redis: AsyncMock
) -> None:
    registry = EventRegistry()
    registry.register(AccountOpened)

    async def handler(event: AccountOpened) -> None:
        _Owner.model_validate({"owner_id": event.owner_ref})

    with (
        patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
        patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
    ):
        mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
        bus = RedisEventBus(config=config, event_registry=registry)
        await bus.connect()
        bus.subscribe(AccountOpened, handler)  # type: ignore[arg-type]
        event = AccountOpened(aggregate_id=uuid4(), owner_ref=MARKER)
        with caplog.at_level(logging.DEBUG):
            await bus._process_message(f"msg-{event.event_id}", _msg_data(event), "test_consumer")
        await bus.disconnect()

    assert any("failed" in r.getMessage() for r in caplog.records)
    for record in caplog.records:
        assert MARKER not in record.getMessage(), record.getMessage()
        assert MARKER not in repr(record.args)
        assert MARKER not in repr(record.__dict__)
