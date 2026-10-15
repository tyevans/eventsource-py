"""Kafka dead letter queue administration (inspection, counting, and replay)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

from eventsource.adapters.kafka.consumer_helpers import get_header_value
from eventsource.adapters.serialization import json_loads
from eventsource.ports.exceptions import EventBusConnectionError

if TYPE_CHECKING:
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.models import KafkaEventBusStats
    from eventsource.adapters.kafka.serialization import EventSerializer

try:
    from aiokafka import AIOKafkaConsumer, TopicPartition
except ImportError:
    AIOKafkaConsumer = None
    TopicPartition = None

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaDLQAdmin:
    """Owns dead letter queue inspection, counting, and replay operations."""

    def __init__(
        self,
        config: KafkaEventBusConfig,
        connection: KafkaConnectionManager,
        serializer: EventSerializer,
        stats: KafkaEventBusStats,
    ) -> None:
        self._config = config
        self._connection = connection
        self._serializer = serializer
        self._stats = stats

    @asynccontextmanager
    async def _dlq_consumer(self, *topics: str, **kwargs: Any) -> AsyncIterator[Any]:
        """Build, start, and always stop a throwaway ``AIOKafkaConsumer``."""
        dlq_consumer = AIOKafkaConsumer(*topics, **self._config.get_consumer_config(**kwargs))
        try:
            await dlq_consumer.start()
            yield dlq_consumer
        finally:
            await dlq_consumer.stop()

    async def get_messages(
        self, limit: int = 100, timeout_ms: int = 5000, use_consumer_group: bool = False
    ) -> list[dict[str, Any]]:
        """Retrieve messages from the dead letter queue."""
        if not self._connection.is_connected:
            raise EventBusConnectionError("Not connected to Kafka")

        group_id = None
        if use_consumer_group:
            if not self._config.dlq_consumer_group:
                raise ValueError(
                    "dlq_consumer_group must be set in config to use consumer group mode"
                )
            group_id = self._config.dlq_consumer_group

        messages: list[dict[str, Any]] = []
        try:
            async with self._dlq_consumer(
                self._config.dlq_topic_name,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                consumer_timeout_ms=timeout_ms,
            ) as dlq_consumer:
                async for message in dlq_consumer:
                    if len(messages) >= limit:
                        break
                    headers: dict[str, str] = (
                        {k: v.decode("utf-8") for k, v in message.headers}
                        if message.headers
                        else {}
                    )
                    try:
                        payload = json_loads(message.value) if message.value else None
                    except (ValueError, TypeError):
                        payload = message.value.hex() if message.value else None

                    messages.append(
                        {
                            "topic": message.topic,
                            "partition": message.partition,
                            "offset": message.offset,
                            "key": message.key.decode("utf-8") if message.key else None,
                            "timestamp": message.timestamp,
                            "headers": headers,
                            "payload": payload,
                            "replay_count": int(headers.get("dlq_replay_count", "0")),
                        }
                    )

                if use_consumer_group and messages:
                    await dlq_consumer.commit()
        except TimeoutError:
            pass

        logger.debug(
            "Retrieved DLQ messages",
            extra={"count": len(messages), "limit": limit, "consumer_group": group_id},
        )
        return messages

    async def replay_message(self, partition: int, offset: int, force: bool = False) -> bool:
        """Replay a specific message from the dead letter queue to the main topic."""
        if not self._connection.is_connected:
            raise EventBusConnectionError("Not connected to Kafka")
        producer = self._connection.require_producer()

        async with self._dlq_consumer(group_id=None, enable_auto_commit=False) as dlq_consumer:
            try:
                tp = TopicPartition(self._config.dlq_topic_name, partition)
                dlq_consumer.assign([tp])
                dlq_consumer.seek(tp, offset)

                message = await asyncio.wait_for(dlq_consumer.getone(), timeout=5.0)
                if message.offset != offset:
                    raise ValueError(f"Message not found at offset {offset}")

                current_replay_count = 0
                for key, value in message.headers or []:
                    if key == "dlq_replay_count":
                        with contextlib.suppress(ValueError, UnicodeDecodeError):
                            current_replay_count = int(value.decode("utf-8"))
                        break

                max_replays = self._config.dlq_max_replay_attempts
                if not force and current_replay_count >= max_replays:
                    logger.warning(
                        "Replay rejected: max replay attempts exceeded",
                        extra={
                            "dlq_partition": partition,
                            "dlq_offset": offset,
                            "replay_count": current_replay_count,
                            "max_replay_attempts": max_replays,
                            "event_type": get_header_value(message.headers, "event_type"),
                        },
                    )
                    raise ValueError(
                        f"Message at partition {partition}, offset {offset} has been replayed "
                        f"{current_replay_count} times, exceeding max of {max_replays}. "
                        f"Use force=True to override."
                    )

                original_headers: list[tuple[str, bytes]] = [
                    (k, v)
                    for k, v in (message.headers or [])
                    if not k.startswith("dlq_") and k != "retry_count"
                ]
                new_replay_count = current_replay_count + 1
                original_headers.extend(
                    [
                        ("retry_count", b"0"),
                        ("dlq_replay_count", str(new_replay_count).encode("utf-8")),
                    ]
                )

                await producer.send(
                    topic=self._config.topic_name,
                    key=message.key,
                    value=message.value,
                    headers=original_headers,
                )

                logger.info(
                    "DLQ message replayed",
                    extra={
                        "dlq_partition": partition,
                        "dlq_offset": offset,
                        "target_topic": self._config.topic_name,
                        "event_type": get_header_value(original_headers, "event_type"),
                        "replay_count": new_replay_count,
                    },
                )
                return True
            except TimeoutError as err:
                raise ValueError(
                    f"Timeout reading message at partition {partition}, offset {offset}"
                ) from err

    async def get_message_count(self) -> int:
        """Get the approximate number of messages in the DLQ across all partitions."""
        if not self._connection.is_connected:
            raise EventBusConnectionError("Not connected to Kafka")

        total_count = 0
        async with self._dlq_consumer(self._config.dlq_topic_name, group_id=None) as dlq_consumer:
            partitions = dlq_consumer.partitions_for_topic(self._config.dlq_topic_name)
            if not partitions:
                return 0
            for partition_id in partitions:
                tp = TopicPartition(self._config.dlq_topic_name, partition_id)
                dlq_consumer.assign([tp])
                beginning = await dlq_consumer.beginning_offsets([tp])
                end = await dlq_consumer.end_offsets([tp])
                total_count += max(0, end.get(tp, 0) - beginning.get(tp, 0))
        return total_count
