"""Core lifecycle, connection, publish, and stats tests for RedisEventBus."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.redis.bus import (
    RedisEventBus,
    RedisEventBusConfig,
    RedisNotAvailableError,
    ResponseError,
)

from ._fixtures import SampleOrderCreated, bus, config, event_registry, mock_redis

__all__ = [
    "TestRedisEventBusConnection",
    "TestRedisEventBusErrorHandling",
    "TestRedisEventBusPublish",
    "TestRedisEventBusShutdown",
    "TestRedisEventBusStatistics",
    "TestRedisEventBusStreamInfo",
    "bus",
    "config",
    "event_registry",
    "mock_redis",
]


def _make_sample_order(order_number: str = "ORD-001") -> SampleOrderCreated:
    return SampleOrderCreated(aggregate_id=uuid4(), order_number=order_number, customer_id=uuid4())


class TestRedisEventBusConnection:
    """Tests for connection management."""

    async def test_connect(self, config: RedisEventBusConfig, mock_redis: AsyncMock) -> None:
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus_instance = RedisEventBus(config=config)
            await bus_instance.connect()

            assert bus_instance.is_connected is True
            mock_redis.ping.assert_called_once()
            mock_redis.xgroup_create.assert_called_once()
            await bus_instance.disconnect()

    async def test_disconnect(self, config: RedisEventBusConfig, mock_redis: AsyncMock) -> None:
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus_instance = RedisEventBus(config=config)
            await bus_instance.connect()
            await bus_instance.disconnect()

            assert bus_instance.is_connected is False
            mock_redis.aclose.assert_called_once()

    async def test_connect_already_connected(
        self, config: RedisEventBusConfig, mock_redis: AsyncMock
    ) -> None:
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus_instance = RedisEventBus(config=config)
            await bus_instance.connect()
            await bus_instance.connect()

            assert bus_instance.is_connected is True
            assert mock_redis.ping.call_count == 1
            await bus_instance.disconnect()

    async def test_consumer_group_already_exists(
        self, config: RedisEventBusConfig, mock_redis: AsyncMock
    ) -> None:
        mock_redis.xgroup_create = AsyncMock(
            side_effect=ResponseError("BUSYGROUP Consumer Group name already exists")
        )
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as mock_aioredis,
        ):
            mock_aioredis.from_url = AsyncMock(return_value=mock_redis)
            bus_instance = RedisEventBus(config=config)
            await bus_instance.connect()

            assert bus_instance.is_connected is True
            await bus_instance.disconnect()


class TestRedisEventBusPublish:
    """Tests for event publishing."""

    async def test_publish_single_event(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        await bus.publish([_make_sample_order()])
        mock_redis.xadd.assert_called_once()
        call_args = mock_redis.xadd.call_args
        assert call_args.kwargs["name"] == "test_events:stream"
        assert call_args.kwargs["fields"]["event_type"] == "SampleOrderCreated"

    async def test_publish_multiple_events_uses_pipeline(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        await bus.publish([_make_sample_order(f"ORD-{i}") for i in range(5)])
        mock_redis.pipeline.assert_called_once()
        assert mock_redis.xadd.call_count == 0

    async def test_publish_empty_list(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        await bus.publish([])
        mock_redis.xadd.assert_not_called()
        mock_redis.pipeline.assert_not_called()

    async def test_publish_updates_stats(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        initial_count = bus.stats.events_published
        await bus.publish([_make_sample_order()])
        assert bus.stats.events_published == initial_count + 1

    async def test_publish_serializes_event(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        aggregate_id = uuid4()
        customer_id = uuid4()
        event = SampleOrderCreated(
            aggregate_id=aggregate_id, order_number="ORD-001", customer_id=customer_id
        )
        await bus.publish([event])

        fields = mock_redis.xadd.call_args.kwargs["fields"]
        assert fields["event_id"] == str(event.event_id)
        assert fields["aggregate_id"] == str(aggregate_id)
        assert fields["event_type"] == "SampleOrderCreated"
        assert "payload" in fields

    async def test_background_publish_returns_before_pipeline_completes(
        self, bus: RedisEventBus, mock_redis: AsyncMock
    ) -> None:
        release = asyncio.Event()

        async def slow_xadd(*args: Any, **kwargs: Any) -> str:
            await release.wait()
            return "1-0"

        mock_redis.xadd = slow_xadd
        await asyncio.wait_for(bus.publish([_make_sample_order()], background=True), timeout=1.0)
        assert bus.get_background_task_count() == 1

        release.set()
        await bus._drain_background(timeout=5.0)
        assert bus.get_background_task_count() == 0


class TestRedisEventBusStreamInfo:
    """Tests for stream info retrieval."""

    async def test_get_stream_info(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        mock_redis.xinfo_stream.return_value = {
            "length": 100,
            "first-entry": ["1234567890-0", {}],
            "last-entry": ["1234567899-0", {}],
        }
        mock_redis.xinfo_groups.return_value = [
            {"name": "test_group", "consumers": 2, "pending": 5}
        ]
        mock_redis.xpending.return_value = {"pending": 5}

        info = await bus.get_stream_info()
        assert info["connected"] is True
        assert info["stream"]["length"] == 100
        assert info["pending_messages"] == 5
        assert len(info["consumer_groups"]) == 1
        assert info["active_consumers"] == 2

    async def test_get_stream_info_not_connected(self, config: RedisEventBusConfig) -> None:
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis"),
        ):
            bus_instance = RedisEventBus(config=config)
            info = await bus_instance.get_stream_info()
            assert info["connected"] is False


class TestRedisEventBusStatistics:
    """Tests for statistics."""

    async def test_get_stats_dict(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        await bus.publish([_make_sample_order()])
        stats = bus.get_stats_dict()
        for key in (
            "events_published",
            "events_consumed",
            "events_processed_success",
            "events_processed_failed",
        ):
            assert key in stats
        assert stats["events_published"] == 1


class TestRedisEventBusShutdown:
    """Tests for shutdown."""

    async def test_shutdown(self, bus: RedisEventBus, mock_redis: AsyncMock) -> None:
        await bus.shutdown()
        assert bus.is_connected is False
        assert bus.is_consuming is False


class TestRedisEventBusErrorHandling:
    """Tests for error handling."""

    def test_redis_not_available_error(self) -> None:
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", False),
            pytest.raises(RedisNotAvailableError),
        ):
            raise RedisNotAvailableError()

    def test_redis_not_available_error_message(self) -> None:
        error = RedisNotAvailableError()
        assert "redis package is not installed" in str(error).lower()
        assert "pip install" in str(error)
