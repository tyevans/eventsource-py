"""Tests for Redis connection management."""

from __future__ import annotations

from typing import Any

import pytest

from eventsource import (
    RedisEventBus,
    RedisEventBusConfig,
)
from tests.integration.conftest import skip_if_no_redis_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.redis,
    skip_if_no_redis_infra,
]


class TestRedisEventBusConnection:
    """Tests for Redis connection management."""

    async def test_connect_and_disconnect(
        self,
        redis_connection_url: str,
        clean_redis: None,
    ) -> None:
        """Test connecting and disconnecting from Redis."""
        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix="test_connect",
        )
        bus = RedisEventBus(config=config)

        assert not bus.is_connected

        await bus.connect()
        assert bus.is_connected

        await bus.disconnect()
        assert not bus.is_connected

    async def test_connect_creates_consumer_group(
        self,
        redis_connection_url: str,
        redis_client: Any,
        clean_redis: None,
    ) -> None:
        """Test that connecting creates the consumer group."""
        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix="test_group",
            consumer_group="test_consumers",
        )
        bus = RedisEventBus(config=config)

        await bus.connect()

        try:
            # Check that stream and consumer group exist
            groups = await redis_client.xinfo_groups(config.stream_name)
            group_names = [g["name"] for g in groups]
            assert "test_consumers" in group_names
        finally:
            await bus.disconnect()
