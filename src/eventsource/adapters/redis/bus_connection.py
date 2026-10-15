"""Connection and lifecycle mixin for Redis event bus.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import TYPE_CHECKING, Any

from eventsource.adapters.redis.config import (
    ResponseError,
)

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eventsource.adapters.redis.config import RedisEventBusConfig
    from eventsource.adapters.redis.models import RedisEventBusStats


def _get_aioredis() -> Any:
    import sys

    bus_mod = sys.modules.get("eventsource.adapters.redis.bus")
    if bus_mod is not None and hasattr(bus_mod, "aioredis"):
        return bus_mod.aioredis
    from eventsource.adapters.redis.config import aioredis

    return aioredis


logger = logging.getLogger("eventsource.adapters.redis")


class RedisBusConnectionMixin:
    """Connection management, health info, and lifecycle for RedisEventBus."""

    _config: RedisEventBusConfig
    _redis: Redis | None
    _connected: bool
    _consuming: bool
    _consumer_task: asyncio.Task[None] | None
    _stats: RedisEventBusStats
    if TYPE_CHECKING:

        async def _drain_background(self, timeout: float = 30.0) -> None: ...
        async def stop_consuming(self) -> None: ...

    async def connect(self) -> None:
        """Connect to Redis and create consumer group if it doesn't exist."""
        if self._connected:
            logger.warning("RedisEventBus already connected")
            return

        client = _get_aioredis()
        try:
            self._redis = await client.from_url(
                self._config.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=self._config.socket_timeout,
                socket_connect_timeout=self._config.socket_connect_timeout,
                single_connection_client=self._config.single_connection_client,
            )
            if self._redis is None:
                raise RuntimeError("Redis client not initialized")
            await self._redis.ping()
            logger.info(
                "Connected to Redis",
                extra={
                    "redis_url": self._config.redis_url,
                    "stream": self._config.stream_name,
                },
            )
            await self._ensure_consumer_group_exists()
            self._connected = True
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            raise

    async def disconnect(self) -> None:
        """Disconnect from Redis."""
        if self._consumer_task:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
            self._consumer_task = None

        if self._redis:
            await self._redis.aclose()
            self._redis = None
            self._connected = False
            self._consuming = False
            logger.info("Disconnected from Redis")

    async def _ensure_consumer_group_exists(self) -> None:
        """Ensure the consumer group exists, creating it if necessary."""
        if not self._redis:
            return
        try:
            await self._redis.xgroup_create(
                name=self._config.stream_name,
                groupname=self._config.consumer_group,
                id="0",
                mkstream=True,
            )
            logger.info(
                f"Created consumer group '{self._config.consumer_group}' "
                f"on stream '{self._config.stream_name}'"
            )
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{self._config.consumer_group}' already exists")
            else:
                logger.error(f"Failed to create consumer group: {e}")
                raise

    async def get_stream_info(self) -> dict[str, Any]:
        """Get Redis stream statistics and health info."""
        if not self._connected or not self._redis:
            return {"connected": False}

        try:
            stream_info = await self._redis.xinfo_stream(self._config.stream_name)
            groups = await self._redis.xinfo_groups(self._config.stream_name)
            pending = await self._redis.xpending(
                self._config.stream_name,
                self._config.consumer_group,
            )
            try:
                dlq_info = await self._redis.xinfo_stream(self._config.dlq_stream_name)
                dlq_length = dlq_info.get("length", 0)
            except Exception:
                dlq_length = 0

            pending_count = pending.get("pending", 0) if isinstance(pending, dict) else 0
            total_consumers = sum(group["consumers"] for group in groups)

            return {
                "connected": True,
                "stream": {
                    "name": self._config.stream_name,
                    "length": stream_info.get("length", 0),
                    "first_entry_id": stream_info.get("first-entry", [None])[0],
                    "last_entry_id": stream_info.get("last-entry", [None])[0],
                },
                "consumer_groups": [
                    {
                        "name": group["name"],
                        "consumers": group["consumers"],
                        "pending": group["pending"],
                    }
                    for group in groups
                ],
                "pending_messages": pending_count,
                "dlq_messages": dlq_length,
                "active_consumers": total_consumers,
                "stats": self.get_stats_dict(),
            }
        except Exception as e:
            logger.error(f"Failed to get stream info: {e}", exc_info=True)
            return {"connected": True, "error": str(e)}

    def get_stats_dict(self) -> dict[str, int]:
        """Get statistics as a dictionary."""
        return {
            "events_published": self._stats.events_published,
            "events_consumed": self._stats.events_consumed,
            "events_processed_success": self._stats.events_processed_success,
            "events_processed_failed": self._stats.events_processed_failed,
            "messages_recovered": self._stats.messages_recovered,
            "messages_sent_to_dlq": self._stats.messages_sent_to_dlq,
            "handler_errors": self._stats.handler_errors,
            "reconnections": self._stats.reconnections,
        }

    async def shutdown(self, timeout: float = 30.0) -> None:
        """Shutdown the event bus gracefully."""
        logger.info("Shutting down RedisEventBus")
        await self._drain_background(timeout)
        await self.stop_consuming()
        if self._consuming:
            await asyncio.sleep(min(timeout, 5.0))
        await self.disconnect()
        logger.info("RedisEventBus shutdown complete")


__all__ = ["RedisBusConnectionMixin"]
