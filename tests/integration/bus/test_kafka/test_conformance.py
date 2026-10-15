"""Conformance test suite for Kafka EventBus implementation."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from typing import Any
from uuid import UUID, uuid4

import pytest

from eventsource import (
    KAFKA_AVAILABLE,
    DomainEvent,
    EventRegistry,
    KafkaEventBus,
    KafkaEventBusConfig,
)
from eventsource.ports.bus import EventBus
from eventsource.testing.conformance_ports import EventBusConformanceSuite
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_kafka_infra

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    skip_if_no_kafka_infra,
]


class TestKafkaEventBusConformance(EventBusConformanceSuite):
    """Runs the shared EventBus contract against a real Kafka."""

    @pytest.fixture(autouse=True)
    async def _kafka_conformance_setup(
        self,
        kafka_bootstrap_servers: str,
        request: pytest.FixtureRequest,
    ) -> AsyncGenerator[None]:
        registry = EventRegistry()
        registry.register(TestItemCreated)
        unique_suffix = uuid4().hex[:8]
        config = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=f"conformance_{unique_suffix}",
            consumer_group=f"conformance_group_{unique_suffix}",
            enable_tracing=False,
            max_retries=0,
        )
        self._buses: list[KafkaEventBus] = []
        self._bus = KafkaEventBus(config=config, event_registry=registry)
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
        assert isinstance(bus, KafkaEventBus)
        loop = asyncio.get_event_loop()
        deadline = loop.time() + 10.0

        while bus.get_background_task_count() > 0:
            if loop.time() >= deadline:
                return
            await asyncio.sleep(0.05)

        while bus.stats.events_consumed < bus.stats.events_published:
            if loop.time() >= deadline:
                return
            await asyncio.sleep(0.05)
