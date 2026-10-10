"""DLQ administration mixin for KafkaEventBus.

Governed by ADR-0002 (<500 lines per module).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from eventsource.adapters.kafka.dlq import KafkaDLQAdmin


class KafkaBusDLQMixin:
    """Mixin providing DLQ inspection, replay, and counting methods."""

    _dlq_admin: KafkaDLQAdmin

    async def get_dlq_messages(
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

        Raises:
            RuntimeError: If not connected to Kafka.
            ValueError: If use_consumer_group=True but dlq_consumer_group not set.
        """
        return await self._dlq_admin.get_messages(
            limit=limit,
            timeout_ms=timeout_ms,
            use_consumer_group=use_consumer_group,
        )

    async def replay_dlq_message(
        self,
        partition: int,
        offset: int,
        force: bool = False,
    ) -> bool:
        """Replay a specific message from the dead letter queue.

        Reads the message from DLQ and republishes it to the main topic
        for reprocessing. The DLQ message is not deleted (Kafka limitation).

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
        return await self._dlq_admin.replay_message(
            partition=partition,
            offset=offset,
            force=force,
        )

    async def get_dlq_message_count(self) -> int:
        """Get the approximate number of messages in the DLQ.

        Uses consumer lag calculation to estimate DLQ size by comparing
        beginning and end offsets for each partition.

        Returns:
            Approximate count of DLQ messages across all partitions.

        Raises:
            RuntimeError: If not connected to Kafka.
        """
        return await self._dlq_admin.get_message_count()
