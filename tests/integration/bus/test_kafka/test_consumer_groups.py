"""Integration tests for Kafka consumer groups."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource import (
    KAFKA_AVAILABLE,
    DomainEvent,
    EventRegistry,
    KafkaEventBus,
    KafkaEventBusConfig,
)
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_kafka_infra

if not KAFKA_AVAILABLE:
    pytest.skip("Kafka (aiokafka) not available", allow_module_level=True)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.kafka,
    skip_if_no_kafka_infra,
]


class TestKafkaEventBusConsumerGroups:
    """Tests for consumer group functionality."""

    @pytest.mark.slow
    async def test_same_consumer_group_shares_messages(self, kafka_bootstrap_servers: str) -> None:
        """Test that consumers in the same group share messages."""
        unique_suffix = uuid4().hex[:8]
        topic_prefix = f"test_shared_{unique_suffix}"
        consumer_group = f"shared_group_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        config1 = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            consumer_name=f"consumer-1-{unique_suffix}",
            enable_tracing=False,
        )
        config2 = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            consumer_name=f"consumer-2-{unique_suffix}",
            enable_tracing=False,
        )

        bus1 = KafkaEventBus(config=config1, event_registry=registry)
        bus2 = KafkaEventBus(config=config2, event_registry=registry)
        consumer1_events: list[DomainEvent] = []
        consumer2_events: list[DomainEvent] = []

        async def handler1(event: DomainEvent) -> None:
            consumer1_events.append(event)

        async def handler2(event: DomainEvent) -> None:
            consumer2_events.append(event)

        try:
            await bus1.connect()
            await bus2.connect()
            bus1.subscribe(TestItemCreated, handler1)
            bus2.subscribe(TestItemCreated, handler2)

            events: list[DomainEvent] = [
                TestItemCreated(
                    aggregate_id=uuid4(), aggregate_version=1, name=f"Shared Item {i}", quantity=i
                )
                for i in range(10)
            ]
            await bus1.publish(events)

            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())

            async def wait_for_all() -> None:
                while len(consumer1_events) + len(consumer2_events) < 10:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_all(), timeout=30.0)
            finally:
                await bus1.stop_consuming()
                await bus2.stop_consuming()
                for task in [task1, task2]:
                    try:
                        await asyncio.wait_for(task, timeout=5.0)
                    except TimeoutError:
                        task.cancel()

            assert len(consumer1_events) + len(consumer2_events) == 10
        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    @pytest.mark.slow
    async def test_different_consumer_groups_receive_all_messages(
        self, kafka_bootstrap_servers: str
    ) -> None:
        """Test that consumers in different groups each receive all messages."""
        unique_suffix = uuid4().hex[:8]
        topic_prefix = f"test_multi_{unique_suffix}"

        registry = EventRegistry()
        registry.register(TestItemCreated)

        config1 = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=f"group_a_{unique_suffix}",
            enable_tracing=False,
        )
        config2 = KafkaEventBusConfig(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=f"group_b_{unique_suffix}",
            enable_tracing=False,
        )

        bus1 = KafkaEventBus(config=config1, event_registry=registry)
        bus2 = KafkaEventBus(config=config2, event_registry=registry)
        group_a_events: list[DomainEvent] = []
        group_b_events: list[DomainEvent] = []

        async def handler_a(event: DomainEvent) -> None:
            group_a_events.append(event)

        async def handler_b(event: DomainEvent) -> None:
            group_b_events.append(event)

        try:
            await bus1.connect()
            await bus2.connect()
            bus1.subscribe(TestItemCreated, handler_a)
            bus2.subscribe(TestItemCreated, handler_b)

            events: list[DomainEvent] = [
                TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Multi Group Item {i}",
                    quantity=i,
                )
                for i in range(5)
            ]
            await bus1.publish(events)

            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())

            async def wait_for_both() -> None:
                while len(group_a_events) < 5 or len(group_b_events) < 5:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_both(), timeout=30.0)
            finally:
                await bus1.stop_consuming()
                await bus2.stop_consuming()
                for task in [task1, task2]:
                    try:
                        await asyncio.wait_for(task, timeout=5.0)
                    except TimeoutError:
                        task.cancel()

            assert len(group_a_events) == 5 and len(group_b_events) == 5
        finally:
            await bus1.disconnect()
            await bus2.disconnect()
