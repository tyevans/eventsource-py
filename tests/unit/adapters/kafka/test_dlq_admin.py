"""Unit tests for the extracted ``KafkaDLQAdmin``.

Covers DLQ message-count aggregation across partitions, the ``get_messages``
limit, replay routing to the main topic via ``require_producer()``, and the
throwaway-consumer helper's guarantee that ``stop()`` always runs -- even
when the wrapped body raises -- in isolation from ``KafkaEventBus``.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.adapters.kafka.config import KafkaEventBusConfig  # noqa: E402
from eventsource.adapters.kafka.connection import KafkaConnectionManager  # noqa: E402
from eventsource.adapters.kafka.dlq import KafkaDLQAdmin  # noqa: E402
from eventsource.adapters.kafka.models import KafkaEventBusStats  # noqa: E402
from eventsource.adapters.kafka.serialization import EventSerializer  # noqa: E402
from eventsource.ports.exceptions import EventBusConnectionError  # noqa: E402


def _make_dlq_admin(producer: Any = None) -> KafkaDLQAdmin:
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(bootstrap_servers="localhost:9092")  # type: ignore[arg-type]
    stats = KafkaEventBusStats()

    connection = KafkaConnectionManager(config=config, stats=stats, metrics=None)
    connection._connected = True
    if producer is not None:
        connection._producer = producer

    return KafkaDLQAdmin(
        config=config,
        connection=connection,
        serializer=EventSerializer(),
        stats=stats,
    )


class TestGetMessageCount:
    async def test_sums_end_minus_beginning_offsets_across_partitions(self) -> None:
        admin = _make_dlq_admin()

        mock_consumer = AsyncMock()
        mock_consumer.partitions_for_topic = MagicMock(return_value={0, 1})

        async def fake_beginning_offsets(tps: list[Any]) -> dict[Any, int]:
            tp = tps[0]
            return {tp: 5} if tp.partition == 0 else {tp: 10}

        async def fake_end_offsets(tps: list[Any]) -> dict[Any, int]:
            tp = tps[0]
            return {tp: 12} if tp.partition == 0 else {tp: 30}

        mock_consumer.beginning_offsets.side_effect = fake_beginning_offsets
        mock_consumer.end_offsets.side_effect = fake_end_offsets
        mock_consumer.assign = MagicMock()

        with patch("eventsource.adapters.kafka.dlq.AIOKafkaConsumer", return_value=mock_consumer):
            count = await admin.get_message_count()

        # partition 0: 12 - 5 = 7, partition 1: 30 - 10 = 20 -> total 27
        assert count == 27
        mock_consumer.start.assert_awaited_once()
        mock_consumer.stop.assert_awaited_once()

    async def test_returns_zero_when_topic_has_no_partitions(self) -> None:
        admin = _make_dlq_admin()

        mock_consumer = AsyncMock()
        mock_consumer.partitions_for_topic = MagicMock(return_value=None)

        with patch("eventsource.adapters.kafka.dlq.AIOKafkaConsumer", return_value=mock_consumer):
            count = await admin.get_message_count()

        assert count == 0
        mock_consumer.stop.assert_awaited_once()

    async def test_raises_when_not_connected(self) -> None:
        admin = _make_dlq_admin()
        admin._connection._connected = False

        with pytest.raises(EventBusConnectionError, match="Not connected"):
            await admin.get_message_count()


class TestGetMessages:
    async def test_honors_limit(self) -> None:
        admin = _make_dlq_admin()

        raw_messages = []
        for i in range(5):
            msg = MagicMock()
            msg.topic = "events.dlq"
            msg.partition = 0
            msg.offset = i
            msg.key = None
            msg.timestamp = 1000 + i
            msg.headers = []
            msg.value = b'{"foo": "bar"}'
            raw_messages.append(msg)

        mock_consumer = AsyncMock()

        async def fake_aiter(self: Any) -> Any:
            for msg in raw_messages:
                yield msg

        mock_consumer.__aiter__ = fake_aiter

        with patch("eventsource.adapters.kafka.dlq.AIOKafkaConsumer", return_value=mock_consumer):
            messages = await admin.get_messages(limit=2)

        assert len(messages) == 2
        assert messages[0]["payload"] == {"foo": "bar"}
        mock_consumer.stop.assert_awaited_once()

    async def test_raises_when_not_connected(self) -> None:
        admin = _make_dlq_admin()
        admin._connection._connected = False

        with pytest.raises(EventBusConnectionError, match="Not connected"):
            await admin.get_messages()


class TestReplayMessage:
    async def test_sends_to_main_topic_via_require_producer(self) -> None:
        mock_producer = AsyncMock()
        admin = _make_dlq_admin(producer=mock_producer)

        message = MagicMock()
        message.offset = 7
        message.headers = [("event_type", b"SomeEvent")]
        message.key = b"agg-1"
        message.value = b'{"payload": true}'

        mock_consumer = AsyncMock()
        mock_consumer.getone = AsyncMock(return_value=message)
        mock_consumer.assign = MagicMock()
        mock_consumer.seek = MagicMock()

        with patch("eventsource.adapters.kafka.dlq.AIOKafkaConsumer", return_value=mock_consumer):
            result = await admin.replay_message(partition=3, offset=7)

        assert result is True
        mock_producer.send.assert_awaited_once()
        _, kwargs = mock_producer.send.call_args
        assert kwargs["topic"] == admin._config.topic_name
        assert kwargs["key"] == b"agg-1"
        assert kwargs["value"] == b'{"payload": true}'
        mock_consumer.stop.assert_awaited_once()

    async def test_raises_when_not_connected(self) -> None:
        admin = _make_dlq_admin(producer=AsyncMock())
        admin._connection._connected = False

        with pytest.raises(EventBusConnectionError, match="Not connected"):
            await admin.replay_message(partition=0, offset=0)


class TestDlqConsumerHelper:
    async def test_always_stops_consumer_even_when_body_raises(self) -> None:
        admin = _make_dlq_admin()

        mock_consumer = AsyncMock()

        with (
            patch("eventsource.adapters.kafka.dlq.AIOKafkaConsumer", return_value=mock_consumer),
            pytest.raises(ValueError, match="boom"),
        ):
            async with admin._dlq_consumer("some-topic"):
                raise ValueError("boom")

        mock_consumer.start.assert_awaited_once()
        mock_consumer.stop.assert_awaited_once()

    async def test_ssl_check_hostname_reaches_dlq_consumer_kwargs(self) -> None:
        """A DLQ consumer must honor ssl_check_hostname like the main consumer.

        Regression test for the security-config drift: DLQ consumers were
        built from a separate, incomplete security dict
        (``KafkaConnectionManager.get_security_config()``) that omitted
        ``ssl_check_hostname`` entirely, so setting it False on the config
        (the documented escape hatch for self-signed / hostname-mismatched
        certs) was silently dropped for every DLQ client while still being
        honored by the main producer/consumer.
        """
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(  # type: ignore[arg-type]
                bootstrap_servers="localhost:9092",
                security_protocol="SSL",
                ssl_check_hostname=False,
            )
        stats = KafkaEventBusStats()
        connection = KafkaConnectionManager(config=config, stats=stats, metrics=None)
        connection._connected = True

        admin = KafkaDLQAdmin(
            config=config,
            connection=connection,
            serializer=EventSerializer(),
            stats=stats,
        )

        mock_consumer = AsyncMock()
        captured_kwargs: dict[str, Any] = {}

        def fake_consumer_ctor(*topics: Any, **kwargs: Any) -> Any:
            captured_kwargs.update(kwargs)
            return mock_consumer

        with patch(
            "eventsource.adapters.kafka.dlq.AIOKafkaConsumer", side_effect=fake_consumer_ctor
        ):
            async with admin._dlq_consumer("some-topic", group_id=None):
                pass

        assert captured_kwargs["ssl_check_hostname"] is False
