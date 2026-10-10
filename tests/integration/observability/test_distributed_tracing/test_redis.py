"""Redis distributed trace context propagation integration tests."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import pytest

from eventsource import REDIS_AVAILABLE, EventRegistry
from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig

from .conftest import TracingTestEvent, skip_if_no_otel

if TYPE_CHECKING:
    from collections.abc import Callable

pytestmark = [pytest.mark.integration]


@pytest.mark.redis
@skip_if_no_otel
class TestRedisDistributedTracing:
    """Test trace context propagation for Redis event bus.

    Note:
        Redis Streams do not natively support message headers like RabbitMQ
        or Kafka. Context propagation may need to be embedded in the message
        payload or handled differently.
    """

    @pytest.fixture
    def redis_url(self) -> str:
        """Get Redis connection URL for tests."""
        import os

        return os.environ.get("REDIS_URL", "redis://localhost:6379")

    @pytest.fixture
    async def check_redis_available(self, redis_url: str) -> bool:
        """Check if Redis is actually available."""
        try:
            import redis.asyncio as redis_lib

            client = redis_lib.from_url(redis_url, single_connection_client=True)
            await asyncio.wait_for(client.ping(), timeout=5.0)
            await client.aclose()
            return True
        except Exception:
            pytest.skip("Redis server not available")
            return False

    @pytest.mark.asyncio
    async def test_redis_tracing_creates_spans(
        self,
        trace_provider: Any,
        get_spans: Callable[[], list[Any]],
        redis_url: str,
        check_redis_available: bool,
    ) -> None:
        """Redis event bus creates proper tracing spans."""
        if not REDIS_AVAILABLE:
            pytest.skip("Redis not available")

        registry = EventRegistry()
        registry.register(TracingTestEvent)

        stream_prefix = f"test_tracing_{uuid4().hex[:8]}"

        config = RedisEventBusConfig(
            redis_url=redis_url,
            stream_prefix=stream_prefix,
            consumer_group="test_tracing_group",
            enable_tracing=True,
            single_connection_client=True,
        )

        bus = RedisEventBus(config=config, event_registry=registry)

        try:
            await bus.connect()

            # Publish an event
            event = TracingTestEvent(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Redis Tracing Test",
            )
            await bus.publish([event])

            spans = get_spans()

            # Should have at least one span for publishing
            publish_spans = [s for s in spans if "publish" in s.name.lower()]
            assert len(publish_spans) >= 1, "Should have at least one publish span"

        finally:
            await bus.shutdown()
