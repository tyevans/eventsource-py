"""Tests for multiple consumer groups receiving same events and load balancing."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import uuid4

import pytest

from eventsource import (
    DomainEvent,
)
from tests.integration.conftest import TestItemCreated

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rabbitmq,
    skip_if_no_rabbitmq_infra,
]


class TestRabbitMQMultipleConsumerGroups:
    """Tests for multiple consumer groups receiving same events."""

    async def test_multiple_consumer_groups_receive_same_event(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that multiple consumer groups each receive the same events."""
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_multi_consumer_{unique_suffix}"

        # Create two buses with different consumer groups but same exchange
        bus1 = rabbitmq_event_bus_factory(
            exchange_name=exchange_name,
            consumer_group=f"group_a_{unique_suffix}",
        )
        bus2 = rabbitmq_event_bus_factory(
            exchange_name=exchange_name,
            consumer_group=f"group_b_{unique_suffix}",
        )

        await bus1.connect()
        await bus2.connect()

        try:
            received_by_group_a: list[DomainEvent] = []
            received_by_group_b: list[DomainEvent] = []

            async def handler_a(event: DomainEvent) -> None:
                received_by_group_a.append(event)

            async def handler_b(event: DomainEvent) -> None:
                received_by_group_b.append(event)

            bus1.subscribe(TestItemCreated, handler_a)
            bus2.subscribe(TestItemCreated, handler_b)

            # Publish event through first bus
            event = TestItemCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                name="Multi Consumer Test",
                quantity=7,
            )
            await bus1.publish([event])

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())

            # Wait for both to receive the event
            async def wait_for_both() -> None:
                while len(received_by_group_a) < 1 or len(received_by_group_b) < 1:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_both(), timeout=15.0)
            finally:
                await bus1.stop_consuming()
                await bus2.stop_consuming()
                for task in [task1, task2]:
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except TimeoutError:
                        task.cancel()

            # Both groups should have received the event
            assert len(received_by_group_a) == 1
            assert len(received_by_group_b) == 1
            assert received_by_group_a[0].event_id == event.event_id
            assert received_by_group_b[0].event_id == event.event_id

        finally:
            await bus1.disconnect()
            await bus2.disconnect()

    @pytest.mark.slow
    async def test_consumer_group_load_balancing(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that messages are load-balanced within a consumer group."""
        unique_suffix = uuid4().hex[:8]
        exchange_name = f"test_load_balance_{unique_suffix}"
        consumer_group = f"shared_group_{unique_suffix}"

        # Create two buses with same consumer group
        bus1 = rabbitmq_event_bus_factory(
            exchange_name=exchange_name,
            consumer_group=consumer_group,
        )
        bus2 = rabbitmq_event_bus_factory(
            exchange_name=exchange_name,
            consumer_group=consumer_group,
        )

        await bus1.connect()
        await bus2.connect()

        try:
            received_by_bus1: list[DomainEvent] = []
            received_by_bus2: list[DomainEvent] = []

            async def handler1(event: DomainEvent) -> None:
                received_by_bus1.append(event)
                await asyncio.sleep(0.1)  # Simulate some work

            async def handler2(event: DomainEvent) -> None:
                received_by_bus2.append(event)
                await asyncio.sleep(0.1)  # Simulate some work

            bus1.subscribe(TestItemCreated, handler1)
            bus2.subscribe(TestItemCreated, handler2)

            # Publish multiple events
            event_count = 10
            for i in range(event_count):
                event = TestItemCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    name=f"Load Balance Item {i}",
                    quantity=i,
                )
                await bus1.publish([event])

            # Start both consumers
            task1 = asyncio.create_task(bus1.start_consuming())
            task2 = asyncio.create_task(bus2.start_consuming())

            # Wait for all events to be consumed
            async def wait_for_all() -> None:
                while len(received_by_bus1) + len(received_by_bus2) < event_count:
                    await asyncio.sleep(0.1)

            try:
                await asyncio.wait_for(wait_for_all(), timeout=30.0)
            finally:
                await bus1.stop_consuming()
                await bus2.stop_consuming()
                for task in [task1, task2]:
                    try:
                        await asyncio.wait_for(task, timeout=2.0)
                    except TimeoutError:
                        task.cancel()

            # Total should equal event_count
            total_received = len(received_by_bus1) + len(received_by_bus2)
            assert total_received == event_count

        finally:
            await bus1.disconnect()
            await bus2.disconnect()
