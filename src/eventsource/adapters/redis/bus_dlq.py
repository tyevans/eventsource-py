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
            self, message_id: str, message_data: dict[str, str], consumer_name: str
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

        cfg, redis = self._config, self._redis
        min_idle = cfg.pending_idle_ms if min_idle_time_ms is None else min_idle_time_ms
        max_ret = cfg.max_retries if max_retries is None else max_retries
        read_cnt = cfg.stream_read_count if stream_read_count is None else stream_read_count
        stats = {"checked": 0, "claimed": 0, "reprocessed": 0, "dlq": 0, "failed": 0}
        stream, group = cfg.stream_name, cfg.consumer_group
        try:
            pending_info = await redis.xpending(stream, group)
            if not pending_info or not isinstance(pending_info, dict):
                logger.debug("No pending messages found")
                return stats
            p_cnt = pending_info.get("pending", 0)
            if p_cnt == 0:
                logger.debug("No pending messages to recover")
                return stats
            stats["checked"] = p_cnt
            logger.info(f"Found {p_cnt} pending messages", extra={"pending_count": p_cnt})
            raw_pend = await redis.xpending_range(stream, group, min="-", max="+", count=read_cnt)
            for pending_msg in cast(DecodedPending, raw_pend):
                msg_id, idle_ms = pending_msg["message_id"], pending_msg["time_since_delivered"]
                delivered = pending_msg["times_delivered"]
                if idle_ms < min_idle:
                    continue
                p_ex = {"message_id": msg_id, "idle_time_ms": idle_ms, "times_delivered": delivered}
                logger.info(f"Processing pending message {msg_id}", extra=p_ex)
                retry_key = cfg.get_retry_key(msg_id)
                retry_str = await redis.get(retry_key)
                retry_cnt = int(retry_str) if retry_str else delivered - 1
                try:
                    raw_claimed = await redis.xclaim(
                        stream, group, "recovery-worker", min_idle, [msg_id]
                    )
                    claimed = cast(list[DecodedEntry], raw_claimed)
                    if not claimed:
                        logger.warning(f"Failed to claim message {msg_id}")
                        continue
                    stats["claimed"] += 1
                    msg_data = claimed[0][1]
                    ev_type = msg_data.get("event_type", "unknown")
                    ex_info = {
                        "message_id": msg_id,
                        "event_type": ev_type,
                        "retry_count": retry_cnt,
                    }
                    if retry_cnt >= max_ret:
                        if cfg.enable_dlq:
                            await self._send_to_dlq(msg_id, msg_data, retry_cnt)
                            stats["dlq"] += 1
                            self._stats.messages_sent_to_dlq += 1
                        await redis.xack(stream, group, msg_id)
                        await redis.delete(retry_key)
                        msg = f"Message {msg_id} sent to DLQ after {retry_cnt} retries"
                        logger.warning(msg, extra=ex_info)
                    else:
                        try:
                            await self._process_message(msg_id, msg_data, "recovery-worker")
                            stats["reprocessed"] += 1
                            self._stats.messages_recovered += 1
                            await redis.delete(retry_key)
                            logger.info(f"Successfully reprocessed message {msg_id}", extra=ex_info)
                        except Exception as e:
                            new_ret = retry_cnt + 1
                            await redis.setex(retry_key, cfg.retry_key_expiry_seconds, new_ret)
                            stats["failed"] += 1
                            err_ex = {**ex_info, "retry_count": new_ret}
                            msg = f"Failed to reprocess message {msg_id}: {e}"
                            logger.error(msg, exc_info=True, extra=err_ex)
                except Exception as e:
                    stats["failed"] += 1
                    msg = f"Error recovering message {msg_id}: {e}"
                    logger.error(msg, exc_info=True, extra={"message_id": msg_id})
            summary = f"Pending message recovery completed: {stats['reprocessed']} reprocessed, {stats['dlq']} sent to DLQ, {stats['failed']} failed"
            logger.info(summary, extra=stats)
        except Exception as e:
            logger.error(f"Pending message recovery failed: {e}", exc_info=True)
            stats["failed"] += 1
        return stats

    async def _send_to_dlq(
        self, message_id: str, message_data: dict[str, str], retry_count: int
    ) -> None:
        if not self._redis:
            return
        now_iso, dlq_stream = datetime.now(UTC).isoformat(), self._config.dlq_stream_name
        dlq_meta = {"original_message_id": message_id, "retry_count": str(retry_count)}
        dlq_data = {**message_data, **dlq_meta, "dlq_timestamp": now_iso}
        await self._redis.xadd(name=dlq_stream, fields=dlq_data)  # type: ignore[arg-type]
        dlq_extra = {"message_id": message_id, "dlq_stream": dlq_stream, "retry_count": retry_count}
        logger.info(f"Sent message {message_id} to DLQ", extra=dlq_extra)

    async def get_dlq_messages(
        self, count: int = 100, start: str = "-", end: str = "+"
    ) -> list[dict[str, Any]]:
        if not self._connected or not self._redis:
            return []
        try:
            dlq_stream = self._config.dlq_stream_name
            raw = await self._redis.xrange(name=dlq_stream, min=start, max=end, count=count)
            entries = cast(list[DecodedEntry], raw)
            return [{"message_id": mid, "data": data} for mid, data in entries]
        except Exception as e:
            logger.error(f"Failed to get DLQ messages: {e}", exc_info=True)
            return []

    async def replay_dlq_message(self, message_id: str) -> bool:
        if not self._connected or not self._redis:
            return False
        try:
            dlq_stream = self._config.dlq_stream_name
            raw = await self._redis.xrange(name=dlq_stream, min=message_id, max=message_id, count=1)
            messages = cast(list[DecodedEntry], raw)
            if not messages:
                logger.warning(f"DLQ message {message_id} not found")
                return False
            dlq_keys = ("original_message_id", "retry_count", "dlq_timestamp")
            replay = {k: v for k, v in messages[0][1].items() if k not in dlq_keys}
            fields = cast("dict[Any, Any]", replay)
            new_id = cast(str, await self._redis.xadd(name=self._config.stream_name, fields=fields))
            await self._redis.xdel(dlq_stream, message_id)
            extra = {"original_message_id": message_id, "new_message_id": new_id}
            logger.info(f"Replayed DLQ message {message_id} as {new_id}", extra=extra)
            return True
        except Exception as e:
            logger.error(f"Failed to replay DLQ message {message_id}: {e}", exc_info=True)
            return False


__all__ = ["RedisBusDLQMixin"]
