"""Kafka dead letter queue administration.

Extracted from ``KafkaEventBus`` (bus.py) as part of the bus god-class
decomposition. Owns inspecting, counting, and replaying messages in the
dead letter queue.

The facade still owns the public ``get_dlq_messages`` / ``replay_dlq_message``
/ ``get_dlq_message_count`` signatures and delegates to this collaborator,
which builds throwaway ``AIOKafkaConsumer`` instances via
:meth:`KafkaConnectionManager.get_security_config` and publishes replays via
:meth:`KafkaConnectionManager.require_producer`.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.connection import KafkaConnectionManager
    from eventsource.adapters.kafka.models import KafkaEventBusStats
    from eventsource.adapters.kafka.serialization import EventSerializer

# Optional aiokafka import - fail gracefully if not installed
try:
    from aiokafka import AIOKafkaConsumer, TopicPartition

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaConsumer = None
    TopicPartition = None

# Named explicitly so the logger name is stable and matches the facade's
# pre-extraction "eventsource.bus.kafka" logger.
logger = logging.getLogger("eventsource.bus.kafka")


def _get_header_value(
    headers: list[tuple[str, bytes]] | None,
    key: str,
) -> str | None:
    """Get a header value by key.

    Pure, stateless duplicate of ``KafkaConsumerLoop._get_header_value``.
    Lifted here rather than reused via a ``KafkaConsumerLoop`` instance
    because that collaborator requires handler-routing dependencies (retry
    policy, handler lookup, shutdown event, ...) that have no bearing on DLQ
    administration -- constructing one just to call a pure header-lookup
    helper would be a heavier coupling than duplicating ~8 lines.

    Args:
        headers: List of header tuples.
        key: The header key to find.

    Returns:
        The decoded header value, or None if not found.
    """
    if not headers:
        return None

    for header_key, header_value in headers:
        if header_key == key:
            return header_value.decode("utf-8")

    return None


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
    async def _dlq_consumer(
        self,
        *topics: str,
        **kwargs: Any,
    ) -> AsyncIterator[Any]:
        """Build, start, and always stop a throwaway ``AIOKafkaConsumer``.

        All three DLQ operations need their own short-lived consumer with
        slightly different construction kwargs (topic subscription,
        group_id, timeout, auto-commit). This centralizes the
        build/start/stop boilerplate and guarantees ``stop()`` runs even if
        the caller's body raises.

        Args:
            *topics: Topics to subscribe to at construction time. Omit to
                build an unsubscribed consumer (e.g. for manual ``assign``).
            **kwargs: Additional keyword arguments forwarded to
                ``AIOKafkaConsumer``, merged over the shared bootstrap
                servers and security config.

        Yields:
            The started ``AIOKafkaConsumer``.
        """
        consumer_kwargs: dict[str, Any] = {
            "bootstrap_servers": self._config.bootstrap_servers,
            **self._connection.get_security_config(),
            **kwargs,
        }
        dlq_consumer = AIOKafkaConsumer(*topics, **consumer_kwargs)
        try:
            await dlq_consumer.start()
            yield dlq_consumer
        finally:
            await dlq_consumer.stop()

    async def get_messages(
        self,
        limit: int = 100,
        timeout_ms: int = 5000,
        use_consumer_group: bool = False,
    ) -> list[dict[str, Any]]:
        """Retrieve messages from the dead letter queue.

        Creates a consumer to read DLQ messages. By default, reads without
        committing offsets (inspection mode). When use_consumer_group=True,
        uses the configured DLQ consumer group for coordinated processing.

        Args:
            limit: Maximum number of messages to retrieve.
            timeout_ms: Timeout for polling in milliseconds.
            use_consumer_group: If True, use dlq_consumer_group for coordinated
                DLQ processing. Messages will be committed after retrieval.

        Returns:
            List of DLQ message dictionaries with headers and payload.
            Each message contains:
            - topic: DLQ topic name
            - partition: Partition number
            - offset: Message offset
            - key: Message key (decoded)
            - timestamp: Message timestamp
            - headers: All headers as string dict
            - payload: Deserialized JSON payload or hex-encoded bytes
            - replay_count: Number of times this message has been replayed

        Raises:
            RuntimeError: If not connected to Kafka.
            ValueError: If use_consumer_group=True but dlq_consumer_group not set.
        """
        if not self._connection.is_connected:
            raise RuntimeError("Not connected to Kafka")

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
                count = 0
                async for message in dlq_consumer:
                    if count >= limit:
                        break

                    # Parse headers into dict
                    headers: dict[str, str] = {}
                    if message.headers:
                        for key, value in message.headers:
                            headers[key] = value.decode("utf-8")

                    # Get replay count from headers
                    replay_count = int(headers.get("dlq_replay_count", "0"))

                    # Try to decode value as JSON
                    try:
                        payload = json.loads(message.value.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
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
                            "replay_count": replay_count,
                        }
                    )

                    count += 1

                # Commit offsets if using consumer group
                if use_consumer_group and messages:
                    await dlq_consumer.commit()

        except TimeoutError:
            # Consumer timed out - this is expected when no more messages
            pass

        logger.debug(
            "Retrieved DLQ messages",
            extra={"count": len(messages), "limit": limit, "consumer_group": group_id},
        )

        return messages

    async def replay_message(
        self,
        partition: int,
        offset: int,
        force: bool = False,
    ) -> bool:
        """Replay a specific message from the dead letter queue.

        Reads the message from DLQ and republishes it to the main topic
        for reprocessing. The DLQ message is not deleted (Kafka limitation).

        Replay Loop Protection:
            Each replay increments a dlq_replay_count header. If the count
            exceeds dlq_max_replay_attempts (default 3), the replay is rejected
            to prevent infinite replay loops. Use force=True to override.

        The replayed message:
        - Has all DLQ-specific headers removed except dlq_replay_count
        - Has retry_count reset to 0
        - Has dlq_replay_count incremented
        - Maintains original event headers

        Args:
            partition: The DLQ partition containing the message.
            offset: The offset of the message to replay.
            force: If True, replay even if max replay attempts exceeded.

        Returns:
            True if message was successfully republished.

        Raises:
            RuntimeError: If not connected to Kafka.
            ValueError: If message not found at specified location or max replays exceeded.
        """
        if not self._connection.is_connected:
            raise RuntimeError("Not connected to Kafka")
        producer = self._connection.require_producer()

        async with self._dlq_consumer(
            group_id=None,
            enable_auto_commit=False,
        ) as dlq_consumer:
            try:
                # Assign to specific partition
                tp = TopicPartition(self._config.dlq_topic_name, partition)
                dlq_consumer.assign([tp])

                # Seek to specific offset
                dlq_consumer.seek(tp, offset)

                # Read the message
                message = await asyncio.wait_for(
                    dlq_consumer.getone(),
                    timeout=5.0,
                )

                if message.offset != offset:
                    raise ValueError(f"Message not found at offset {offset}")

                # Check replay count for loop protection
                current_replay_count = 0
                if message.headers:
                    for key, value in message.headers:
                        if key == "dlq_replay_count":
                            with contextlib.suppress(ValueError, UnicodeDecodeError):
                                current_replay_count = int(value.decode("utf-8"))
                            break

                # Enforce replay limit unless forced
                if not force and current_replay_count >= self._config.dlq_max_replay_attempts:
                    event_type = _get_header_value(message.headers, "event_type")
                    logger.warning(
                        "Replay rejected: max replay attempts exceeded",
                        extra={
                            "dlq_partition": partition,
                            "dlq_offset": offset,
                            "replay_count": current_replay_count,
                            "max_replay_attempts": self._config.dlq_max_replay_attempts,
                            "event_type": event_type,
                        },
                    )
                    raise ValueError(
                        f"Message at partition {partition}, offset {offset} has been replayed "
                        f"{current_replay_count} times, exceeding max of "
                        f"{self._config.dlq_max_replay_attempts}. Use force=True to override."
                    )

                # Build headers for republish
                original_headers: list[tuple[str, bytes]] = []
                if message.headers:
                    for key, value in message.headers:
                        # Remove DLQ headers except replay count (we'll update it)
                        if not key.startswith("dlq_") and key != "retry_count":
                            original_headers.append((key, value))

                # Add retry_count header (reset to 0 for fresh attempt)
                original_headers.append(("retry_count", b"0"))

                # Increment and add replay count for loop protection
                new_replay_count = current_replay_count + 1
                original_headers.append(("dlq_replay_count", str(new_replay_count).encode("utf-8")))

                # Republish to main topic
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
                        "event_type": _get_header_value(original_headers, "event_type"),
                        "replay_count": new_replay_count,
                    },
                )

                return True

            except TimeoutError as err:
                raise ValueError(
                    f"Timeout reading message at partition {partition}, offset {offset}"
                ) from err

    async def get_message_count(self) -> int:
        """Get the approximate number of messages in the DLQ.

        Uses consumer lag calculation to estimate DLQ size by comparing
        beginning and end offsets for each partition.

        Returns:
            Approximate count of DLQ messages across all partitions.

        Raises:
            RuntimeError: If not connected to Kafka.
        """
        if not self._connection.is_connected:
            raise RuntimeError("Not connected to Kafka")

        total_count = 0

        async with self._dlq_consumer(
            self._config.dlq_topic_name,
            group_id=None,
        ) as dlq_consumer:
            partitions = dlq_consumer.partitions_for_topic(self._config.dlq_topic_name)
            if not partitions:
                return 0

            for partition_id in partitions:
                tp = TopicPartition(self._config.dlq_topic_name, partition_id)
                dlq_consumer.assign([tp])

                # Get beginning and end offsets
                beginning = await dlq_consumer.beginning_offsets([tp])
                end = await dlq_consumer.end_offsets([tp])

                start_offset = beginning.get(tp, 0)
                end_offset = end.get(tp, 0)
                total_count += max(0, end_offset - start_offset)

        return total_count
