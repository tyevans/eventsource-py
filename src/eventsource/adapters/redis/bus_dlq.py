"""Dead letter queue and pending message recovery for Redis event bus."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, cast

from eventsource.adapters.redis.config import DecodedEntry, DecodedPending

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eventsource.adapters.redis.config import RedisEventBusConfig
    from eventsource.adapters.redis.models import RedisEventBusStats

logger = logging.getLogger("eventsource.adapters.redis")


class RedisBusDLQMixin:
    """Dead letter queue management and pending message recovery for RedisEventBus."""

    _connected: bool
    _redis: Redis | None
    _config: RedisEventBusConfig
    _stats: RedisEventBusStats
    if TYPE_CHECKING:

        async def connect(self) -> None: ...
        async def _process_message(
            self,
            message_id: str,
            message_data: dict[str, str],
            consumer_name: str,
        ) -> None: ...

    async def recover_pending_messages(
        self,
        min_idle_time_ms: int | None = None,
        max_retries: int | None = None,
        stream_read_count: int | None = None,
    ) -> dict[str, int]:
        """Recover pending messages that have been idle too long via XPENDING and XCLAIM."""
        if not self._connected:
            await self.connect()
        if not self._redis:
            raise RuntimeError("Redis client not initialized")

        min_idle = self._config.pending_idle_ms if min_idle_time_ms is None else min_idle_time_ms
        max_ret = self._config.max_retries if max_retries is None else max_retries
        read_cnt = (
            self._config.stream_read_count if stream_read_count is None else stream_read_count
        )
        stats = {"checked": 0, "claimed": 0, "reprocessed": 0, "dlq": 0, "failed": 0}

        try:
            pending_info = await self._redis.xpending(
                self._config.stream_name, self._config.consumer_group
            )
            if not pending_info or not isinstance(pending_info, dict):
                logger.debug("No pending messages found")
                return stats

            pending_count = pending_info.get("pending", 0)
            if pending_count == 0:
                logger.debug("No pending messages to recover")
                return stats

            stats["checked"] = pending_count
            logger.info(
                f"Found {pending_count} pending messages",
                extra={"pending_count": pending_count},
            )

            pending_messages = cast(
                DecodedPending,
                await self._redis.xpending_range(
                    name=self._config.stream_name,
                    groupname=self._config.consumer_group,
                    min="-",
                    max="+",
                    count=read_cnt,
                ),
            )

            for pending_msg in pending_messages:
                message_id = pending_msg["message_id"]
                idle_time_ms = pending_msg["time_since_delivered"]
                times_delivered = pending_msg["times_delivered"]
                if idle_time_ms < min_idle:
                    continue

                logger.info(
                    f"Processing pending message {message_id}",
                    extra={
                        "message_id": message_id,
                        "idle_time_ms": idle_time_ms,
                        "times_delivered": times_delivered,
                    },
                )

                retry_key = self._config.get_retry_key(message_id)
                retry_count_str = await self._redis.get(retry_key)
                retry_count = int(retry_count_str) if retry_count_str else times_delivered - 1

                try:
                    claimed = cast(
                        list[DecodedEntry],
                        await self._redis.xclaim(
                            name=self._config.stream_name,
                            groupname=self._config.consumer_group,
                            consumername="recovery-worker",
                            min_idle_time=min_idle,
                            message_ids=[message_id],
                        ),
                    )
                    if not claimed:
                        logger.warning(f"Failed to claim message {message_id}")
                        continue

                    stats["claimed"] += 1
                    message_data = claimed[0][1]
                    event_type_name = message_data.get("event_type", "unknown")

                    if retry_count >= max_ret:
                        if self._config.enable_dlq:
                            await self._send_to_dlq(message_id, message_data, retry_count)
                            stats["dlq"] += 1
                            self._stats.messages_sent_to_dlq += 1

                        await self._redis.xack(
                            self._config.stream_name, self._config.consumer_group, message_id
                        )
                        await self._redis.delete(retry_key)
                        logger.warning(
                            f"Message {message_id} sent to DLQ after {retry_count} retries",
                            extra={
                                "message_id": message_id,
                                "event_type": event_type_name,
                                "retry_count": retry_count,
                            },
                        )
                    else:
                        try:
                            await self._process_message(message_id, message_data, "recovery-worker")
                            stats["reprocessed"] += 1
                            self._stats.messages_recovered += 1
                            await self._redis.delete(retry_key)
                            logger.info(
                                f"Successfully reprocessed message {message_id}",
                                extra={
                                    "message_id": message_id,
                                    "event_type": event_type_name,
                                    "retry_count": retry_count,
                                },
                            )
                        except Exception as e:
                            new_retry_count = retry_count + 1
                            await self._redis.setex(
                                retry_key, self._config.retry_key_expiry_seconds, new_retry_count
                            )
                            stats["failed"] += 1
                            logger.error(
                                f"Failed to reprocess message {message_id}: {e}",
                                exc_info=True,
                                extra={
                                    "message_id": message_id,
                                    "event_type": event_type_name,
                                    "retry_count": new_retry_count,
                                },
                            )
                except Exception as e:
                    stats["failed"] += 1
                    logger.error(
                        f"Error recovering message {message_id}: {e}",
                        exc_info=True,
                        extra={"message_id": message_id},
                    )

            logger.info(
                f"Pending message recovery completed: {stats['reprocessed']} reprocessed, "
                f"{stats['dlq']} sent to DLQ, {stats['failed']} failed",
                extra=stats,
            )
        except Exception as e:
            logger.error(f"Pending message recovery failed: {e}", exc_info=True)
            stats["failed"] += 1

        return stats

    async def _send_to_dlq(
        self, message_id: str, message_data: dict[str, str], retry_count: int
    ) -> None:
        """Send a message to the dead letter queue."""
        if not self._redis:
            return
        dlq_data = {
            **message_data,
            "original_message_id": message_id,
            "retry_count": str(retry_count),
            "dlq_timestamp": datetime.now(UTC).isoformat(),
        }
        await self._redis.xadd(
            name=self._config.dlq_stream_name,
            fields=dlq_data,  # type: ignore[arg-type]
        )
        logger.info(
            f"Sent message {message_id} to DLQ",
            extra={
                "message_id": message_id,
                "dlq_stream": self._config.dlq_stream_name,
                "retry_count": retry_count,
            },
        )

    async def get_dlq_messages(
        self, count: int = 100, start: str = "-", end: str = "+"
    ) -> list[dict[str, Any]]:
        """Get messages from the dead letter queue."""
        if not self._connected or not self._redis:
            return []
        try:
            messages = cast(
                list[DecodedEntry],
                await self._redis.xrange(
                    name=self._config.dlq_stream_name, min=start, max=end, count=count
                ),
            )
            return [{"message_id": msg_id, "data": data} for msg_id, data in messages]
        except Exception as e:
            logger.error(f"Failed to get DLQ messages: {e}", exc_info=True)
            return []

    async def replay_dlq_message(self, message_id: str) -> bool:
        """Replay a message from the DLQ back to the main stream."""
        if not self._connected or not self._redis:
            return False
        try:
            messages = cast(
                list[DecodedEntry],
                await self._redis.xrange(
                    name=self._config.dlq_stream_name, min=message_id, max=message_id, count=1
                ),
            )
            if not messages:
                logger.warning(f"DLQ message {message_id} not found")
                return False

            _, data = messages[0]
            replay_data = {
                k: v
                for k, v in data.items()
                if k not in ("original_message_id", "retry_count", "dlq_timestamp")
            }
            new_message_id = cast(
                str,
                await self._redis.xadd(
                    name=self._config.stream_name,
                    fields=cast("dict[Any, Any]", replay_data),
                ),
            )
            await self._redis.xdel(self._config.dlq_stream_name, message_id)
            logger.info(
                f"Replayed DLQ message {message_id} as {new_message_id}",
                extra={"original_message_id": message_id, "new_message_id": new_message_id},
            )
            return True
        except Exception as e:
            logger.error(f"Failed to replay DLQ message {message_id}: {e}", exc_info=True)
            return False


__all__ = ["RedisBusDLQMixin"]
