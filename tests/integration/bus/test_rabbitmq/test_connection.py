"""Tests for RabbitMQ connection management."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

import pytest

from eventsource import RabbitMQEventBus

from .conftest import skip_if_no_rabbitmq_infra

pytestmark = [
    pytest.mark.integration,
    pytest.mark.rabbitmq,
    skip_if_no_rabbitmq_infra,
]


class TestRabbitMQEventBusConnection:
    """Tests for RabbitMQ connection management."""

    async def test_connect_and_disconnect(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test connecting and disconnecting from RabbitMQ."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_connect_{uuid4().hex[:8]}",
        )

        assert not bus.is_connected

        await bus.connect()
        assert bus.is_connected

        await bus.disconnect()
        assert not bus.is_connected

    async def test_context_manager(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test using event bus as async context manager."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_ctx_{uuid4().hex[:8]}",
        )

        assert not bus.is_connected

        async with bus:
            assert bus.is_connected

        assert not bus.is_connected

    async def test_connect_creates_exchange_and_queue(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that connecting creates the exchange and consumer queue."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_topology_{uuid4().hex[:8]}",
        )

        await bus.connect()

        try:
            # Verify exchange was created by checking internal state
            assert bus._topology.exchange is not None
            assert bus._topology.consumer_queue is not None
        finally:
            await bus.disconnect()

    async def test_connect_creates_dlq_when_enabled(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that DLQ exchange and queue are created when DLQ is enabled."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_dlq_topology_{uuid4().hex[:8]}",
            enable_dlq=True,
        )

        await bus.connect()

        try:
            assert bus._topology.dlq_exchange is not None
            assert bus._topology.dlq_queue is not None
        finally:
            await bus.disconnect()

    async def test_connect_without_dlq(
        self,
        rabbitmq_event_bus_factory: Any,
    ) -> None:
        """Test that DLQ is not created when disabled."""
        bus = rabbitmq_event_bus_factory(
            exchange_name=f"test_no_dlq_{uuid4().hex[:8]}",
            enable_dlq=False,
        )

        await bus.connect()

        try:
            assert bus._topology.dlq_exchange is None
            assert bus._topology.dlq_queue is None
        finally:
            await bus.disconnect()

    async def test_double_connect_is_safe(
        self,
        rabbitmq_event_bus: RabbitMQEventBus,
    ) -> None:
        """Test that connecting when already connected is a no-op."""
        # Bus is already connected via fixture
        assert rabbitmq_event_bus.is_connected

        # Second connect should not raise
        await rabbitmq_event_bus.connect()
        assert rabbitmq_event_bus.is_connected
