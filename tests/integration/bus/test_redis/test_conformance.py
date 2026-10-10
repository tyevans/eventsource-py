"""Redis EventBus conformance suite implementation."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID

import pytest

from eventsource import (
    DomainEvent,
    RedisEventBus,
    RedisEventBusConfig,
)
from eventsource.ports.bus import EventBus
from eventsource.testing.conformance import EventBusConformanceSuite
from tests.integration.conftest import (
    TestItemCreated,
    skip_if_no_redis_infra,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.redis,
    skip_if_no_redis_infra,
]


class TestRedisEventBusConformance(EventBusConformanceSuite):
    """Runs the shared EventBus contract against a real Redis.

    RedisEventBus only delivers to handlers via a running ``start_consuming()``
    loop, so each test gets one connected bus with consumption already running
    in the background (started here, not inside ``create_bus``, since the
    abstract factory method is sync and starting consumption requires
    ``await``).
    """

    @pytest.fixture(autouse=True)
    async def _redis_conformance_setup(
        self,
        redis_connection_url: str,
        clean_redis: None,
        request: pytest.FixtureRequest,
    ) -> AsyncGenerator[None]:
        # Deliberately does NOT use single_connection_client=True (unlike
        # redis_event_bus_factory): that mode shares one connection between
        # publish and the blocking start_consuming() XREADGROUP call, which
        # self-deadlocks when both run concurrently on the same bus.
        config = RedisEventBusConfig(
            redis_url=redis_connection_url,
            stream_prefix=f"conformance_{request.node.name}",
            consumer_group="conformance_group",
            stream_read_count=10,
            block_ms=200,
        )
        self._bus = RedisEventBus(config=config)
        await self._bus.connect()
        self._consume_task = asyncio.create_task(self._bus.start_consuming())

        yield

        await self._bus.stop_consuming()
        self._consume_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._consume_task
        if self._bus.is_connected:
            await self._bus.shutdown()

    def create_bus(self) -> EventBus:
        # Several base-suite tests assert on delivery immediately after
        # `await bus.publish(...)` with no call to `await_delivery` (that
        # hook is only used by tests written with pull-based backends in
        # mind). Redis only delivers via the background start_consuming()
        # loop, so we wrap publish() to wait for the consumer to catch up
        # before returning -- making delivery effectively synchronous from
        # the test's point of view, matching what the base suite assumes.
        bus = self._bus
        original_publish = bus.publish

        async def publish_and_await_delivery(
            events: list[DomainEvent], background: bool = False
        ) -> None:
            await original_publish(events, background=background)
            await self.await_delivery(bus)

        bus.publish = publish_and_await_delivery  # type: ignore[method-assign]
        return bus

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        return TestItemCreated(
            aggregate_id=aggregate_id,
            aggregate_version=1,
            name="conformance item",
            quantity=1,
        )

    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        class Subscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [TestItemCreated]

            async def handle(self, event: DomainEvent) -> None:
                received.append(event)

        return Subscriber()

    async def await_delivery(self, bus: EventBus) -> None:
        """Poll until the consumer loop has caught up, bounded to 10s."""
        assert isinstance(bus, RedisEventBus)
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 10.0

        # A background=True publish schedules a task that may not have run
        # (and thus not yet incremented events_published) the instant we get
        # here -- drain background tasks first so the stats comparison below
        # reflects the eventual state, not a stale snapshot.
        while bus.get_background_task_count() > 0:
            if loop.time() >= deadline:
                return
            await asyncio.sleep(0.05)

        while bus.stats.events_consumed < bus.stats.events_published:
            if loop.time() >= deadline:
                return
            await asyncio.sleep(0.05)
