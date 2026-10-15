"""Connection and lifecycle mixin for Redis event bus."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import sys
from typing import TYPE_CHECKING, Any

from eventsource.adapters.redis.config import ResponseError

if TYPE_CHECKING:
    from redis.asyncio import Redis

    from eventsource.adapters.redis.config import RedisEventBusConfig
    from eventsource.adapters.redis.models import RedisEventBusStats


def _get_aioredis() -> Any:
    if (m := sys.modules.get("eventsource.adapters.redis.bus")) and hasattr(m, "aioredis"):
        return m.aioredis
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
        if self._connected:
            logger.warning("RedisEventBus already connected")
            return
        cfg = self._config
        try:
            self._redis = await _get_aioredis().from_url(
                cfg.redis_url,
                encoding="utf-8",
                decode_responses=True,
                socket_timeout=cfg.socket_timeout,
                socket_connect_timeout=cfg.socket_connect_timeout,
                single_connection_client=cfg.single_connection_client,
            )
            if self._redis is None:
                raise RuntimeError("Redis client not initialized")
            await self._redis.ping()
            logger.info(
                "Connected to Redis", extra={"redis_url": cfg.redis_url, "stream": cfg.stream_name}
            )
            await self._ensure_consumer_group_exists()
            self._connected = True
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}", exc_info=True)
            raise

    async def disconnect(self) -> None:
        if self._consumer_task:
            self._consumer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._consumer_task
            self._consumer_task = None
        if self._redis:
            await self._redis.aclose()
            self._redis, self._connected, self._consuming = None, False, False
            logger.info("Disconnected from Redis")

    async def _ensure_consumer_group_exists(self) -> None:
        if not self._redis:
            return
        s, g = self._config.stream_name, self._config.consumer_group
        try:
            await self._redis.xgroup_create(name=s, groupname=g, id="0", mkstream=True)
            logger.info(f"Created consumer group '{g}' on stream '{s}'")
        except ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{g}' already exists")
            else:
                logger.error(f"Failed to create consumer group: {e}")
                raise

    async def get_stream_info(self) -> dict[str, Any]:
        if not self._connected or not self._redis:
            return {"connected": False}
        try:
            cfg = self._config
            info = await self._redis.xinfo_stream(cfg.stream_name)
            groups = await self._redis.xinfo_groups(cfg.stream_name)
            pending = await self._redis.xpending(cfg.stream_name, cfg.consumer_group)
            try:
                dlq_len = (await self._redis.xinfo_stream(cfg.dlq_stream_name)).get("length", 0)
            except Exception:
                dlq_len = 0
            c_groups = [
                {"name": g["name"], "consumers": g["consumers"], "pending": g["pending"]}
                for g in groups
            ]
            return {
                "connected": True,
                "stream": {
                    "name": cfg.stream_name,
                    "length": info.get("length", 0),
                    "first_entry_id": info.get("first-entry", [None])[0],
                    "last_entry_id": info.get("last-entry", [None])[0],
                },
                "consumer_groups": c_groups,
                "pending_messages": pending.get("pending", 0) if isinstance(pending, dict) else 0,
                "dlq_messages": dlq_len,
                "active_consumers": sum(g["consumers"] for g in groups),
                "stats": self.get_stats_dict(),
            }
        except Exception as e:
            logger.error(f"Failed to get stream info: {e}", exc_info=True)
            return {"connected": True, "error": str(e)}

    def get_stats_dict(self) -> dict[str, int]:
        s = self._stats
        return {
            "events_published": s.events_published,
            "events_consumed": s.events_consumed,
            "events_processed_success": s.events_processed_success,
            "events_processed_failed": s.events_processed_failed,
            "messages_recovered": s.messages_recovered,
            "messages_sent_to_dlq": s.messages_sent_to_dlq,
            "handler_errors": s.handler_errors,
            "reconnections": s.reconnections,
        }

    async def shutdown(self, timeout: float = 30.0) -> None:
        logger.info("Shutting down RedisEventBus")
        await self._drain_background(timeout)
        await self.stop_consuming()
        if self._consuming:
            await asyncio.sleep(min(timeout, 5.0))
        await self.disconnect()
        logger.info("RedisEventBus shutdown complete")


__all__ = ["RedisBusConnectionMixin"]
