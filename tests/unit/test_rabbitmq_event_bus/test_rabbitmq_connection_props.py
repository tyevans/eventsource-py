"""Unit tests extracted for test_rabbitmq_connection_props.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
)


class TestRabbitMQEventBusContextManager:
    """Tests for RabbitMQEventBus async context manager."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_connects_on_enter(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that async context manager connects on entry."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)

        async with bus as entered_bus:
            assert entered_bus is bus
            assert bus.is_connected is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_disconnects_on_exit(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that async context manager disconnects on exit."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)

        async with bus:
            pass

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_disconnects_on_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that async context manager disconnects even when exception occurs."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(ValueError, match="Test exception"):
            async with bus:
                raise ValueError("Test exception")

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert bus.is_connected is False


class TestRabbitMQEventBusProperties:
    """Tests for RabbitMQEventBus properties."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    def test_config_property(self, config: RabbitMQEventBusConfig) -> None:
        """Test config property returns configuration."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        assert bus.config is config

    def test_is_connected_property_when_disconnected(self, config: RabbitMQEventBusConfig) -> None:
        """Test is_connected returns False when disconnected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        assert bus.is_connected is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_is_connected_property_when_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test is_connected returns True when connected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        assert bus.is_connected is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_is_connected_returns_false_when_connection_closed(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test is_connected returns False when connection is closed."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Simulate connection closed externally
        mock_connection.is_closed = True

        assert bus.is_connected is False

    def test_is_consuming_property_when_not_consuming(self, config: RabbitMQEventBusConfig) -> None:
        """Test is_consuming returns False when not consuming."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        assert bus.is_consuming is False

    def test_stats_property(self, config: RabbitMQEventBusConfig) -> None:
        """Test stats property returns statistics."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        stats = bus.stats
        assert isinstance(stats, RabbitMQEventBusStats)
        assert stats.events_published == 0
