"""Kafka DLQ listing must survive publisher-controlled header bytes."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.adapters.kafka.config import KafkaEventBusConfig  # noqa: E402
from eventsource.adapters.kafka.connection import KafkaConnectionManager  # noqa: E402
from eventsource.adapters.kafka.dlq import KafkaDLQAdmin  # noqa: E402
from eventsource.adapters.kafka.models import KafkaEventBusStats  # noqa: E402
from eventsource.adapters.kafka.serialization import EventSerializer  # noqa: E402


def _admin() -> KafkaDLQAdmin:
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(bootstrap_servers="localhost:9092")  # type: ignore[arg-type]
    stats = KafkaEventBusStats()
    connection = KafkaConnectionManager(config=config, stats=stats, metrics=None)
    connection._connected = True
    return KafkaDLQAdmin(
        config=config, connection=connection, serializer=EventSerializer(), stats=stats
    )


def _record(headers: list[tuple[str, bytes]] | None, key: bytes | None = b"agg-1") -> Any:
    return SimpleNamespace(
        topic="events.dlq",
        partition=0,
        offset=1,
        key=key,
        timestamp=1000,
        headers=headers,
        value=b'{"foo": "bar"}',
    )


async def _list(records: list[Any]) -> list[dict[str, Any]]:
    consumer = AsyncMock()

    async def fake_aiter(self: Any) -> Any:
        for record in records:
            yield record

    consumer.__aiter__ = fake_aiter
    with patch("eventsource.adapters.kafka.dlq.AIOKafkaConsumer", return_value=consumer):
        return await _admin().get_messages(limit=10)


@pytest.mark.asyncio
async def test_non_utf8_header_is_listed_with_replacement_characters() -> None:
    messages = await _list([_record([("event_type", b"\xff\xfe"), ("dlq_reason", b"ok")])])

    assert len(messages) == 1
    assert messages[0]["headers"]["event_type"] == "��"
    assert messages[0]["headers"]["dlq_reason"] == "ok"
    assert messages[0]["payload"] == {"foo": "bar"}


@pytest.mark.asyncio
async def test_non_utf8_key_is_listed_with_replacement_characters() -> None:
    messages = await _list([_record([], key=b"\xff")])

    assert messages[0]["key"] == "�"


@pytest.mark.parametrize(
    ("raw", "expected"),
    [(b"4", 4), (b"abc", 0), (b"-3", 0), (b"\xff", 0)],
)
@pytest.mark.asyncio
async def test_replay_count_header_is_parsed_safely(raw: bytes, expected: int) -> None:
    messages = await _list([_record([("dlq_replay_count", raw)])])

    assert messages[0]["replay_count"] == expected


@pytest.mark.asyncio
async def test_header_without_value_does_not_break_listing() -> None:
    messages = await _list([_record([("event_type", None)])])  # type: ignore[list-item]

    assert messages[0]["headers"]["event_type"] is None
    assert messages[0]["replay_count"] == 0
