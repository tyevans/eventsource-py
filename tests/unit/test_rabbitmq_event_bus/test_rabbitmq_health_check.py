"""Unit tests extracted for test_rabbitmq_health_check.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    HealthCheckResult,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


def _setup_mock_broker(
    mock_aio_pika: MagicMock, *, setup_queue_and_exchange: bool = True
) -> tuple[AsyncMock, AsyncMock, AsyncMock]:
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_queue = AsyncMock()
    if setup_queue_and_exchange:
        mock_queue.declaration_result = MagicMock(message_count=0, consumer_count=0)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
    return mock_connection, mock_channel, mock_queue


class TestHealthCheck:
    """Tests for RabbitMQEventBus.health_check() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @pytest.fixture
    def config_no_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration without DLQ."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @pytest.mark.asyncio
    async def test_health_check_when_not_connected(self, config: RabbitMQEventBusConfig) -> None:
        """Test health_check returns unhealthy when not connected."""
        bus = RabbitMQEventBus(config=config)
        result = await bus.health_check()

        assert isinstance(result, HealthCheckResult)
        assert result.healthy is False
        assert result.connection_status == "disconnected"
        assert result.channel_status == "not_initialized"
        assert "Not connected to RabbitMQ" in (result.error or "")

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_healthy_state(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check returns healthy when all checks pass."""
        # Setup mock connection, channel, queue, and exchange
        _setup_mock_broker(mock_aio_pika)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        result = await bus.health_check()

        assert result.healthy is True
        assert result.connection_status == "connected"
        assert result.channel_status == "open"
        assert result.queue_status == "accessible"
        assert result.dlq_status == "accessible"
        assert result.error is None

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_includes_details(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check includes configuration details."""
        # Setup mock connection, channel, queue, and exchange
        _setup_mock_broker(mock_aio_pika)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        result = await bus.health_check()

        assert result.details is not None
        assert result.details["exchange"] == config.exchange_name
        assert result.details["queue"] == config.queue_name
        assert result.details["consumer_group"] == config.consumer_group
        assert result.details["consuming"] is False
        assert result.details["dlq_enabled"] is True
        assert result.details["dlq_queue"] == config.dlq_queue_name

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_includes_stats(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check includes stats in details."""
        # Setup mock connection, channel, queue, and exchange
        _setup_mock_broker(mock_aio_pika)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Set some stats
        bus._stats.events_published = 100
        bus._stats.events_consumed = 95

        result = await bus.health_check()

        assert result.details is not None
        assert "stats" in result.details
        assert result.details["stats"]["events_published"] == 100
        assert result.details["stats"]["events_consumed"] == 95

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_connection_closed(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check when connection is closed."""
        # Setup mock connection and channel
        mock_connection, _, _ = _setup_mock_broker(mock_aio_pika, setup_queue_and_exchange=False)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Simulate connection closed
        mock_connection.is_closed = True
        result = await bus.health_check()

        assert result.healthy is False
        assert result.connection_status == "closed"
        assert "RabbitMQ connection is closed" in (result.error or "")

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_channel_closed(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check when channel is closed."""
        # Setup mock connection and channel
        _, mock_channel, _ = _setup_mock_broker(mock_aio_pika, setup_queue_and_exchange=False)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Simulate channel closed
        mock_channel.is_closed = True
        result = await bus.health_check()

        assert result.healthy is False
        assert result.channel_status == "closed"
        assert "AMQP channel is closed" in (result.error or "")

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_queue_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check when queue check fails."""
        # Setup mock connection, channel, queue for connect, and exchange
        _, mock_channel, mock_queue_for_connect = _setup_mock_broker(mock_aio_pika)

        # connect() succeeds, but health_check passive declaration fails
        call_count = 0

        async def declare_queue_side_effect(**kwargs: dict) -> AsyncMock:
            nonlocal call_count
            call_count += 1
            # Fail on passive declaration (used by health_check)
            if kwargs.get("passive"):
                raise Exception("Queue not found")
            return mock_queue_for_connect

        mock_channel.declare_queue = AsyncMock(side_effect=declare_queue_side_effect)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        result = await bus.health_check()

        assert result.healthy is False
        assert "error:" in result.queue_status
        assert "Queue check failed" in (result.error or "")

    @pytest.mark.asyncio
    async def test_health_check_dlq_disabled(self, config_no_dlq: RabbitMQEventBusConfig) -> None:
        """Test health_check when DLQ is disabled."""
        bus = RabbitMQEventBus(config=config_no_dlq)
        result = await bus.health_check()
        assert result.dlq_status == "disabled"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_dlq_error_doesnt_fail_overall(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ check failure doesn't make overall health fail."""
        # Setup mock connection, channel, queue, and exchange
        _, mock_channel, mock_queue = _setup_mock_broker(mock_aio_pika)

        async def declare_queue_side_effect(**kwargs: dict) -> AsyncMock:
            name = kwargs.get("name", "")
            passive = kwargs.get("passive", False)
            # Fail on DLQ passive declaration in health_check
            if passive and "dlq" in name.lower():
                raise Exception("DLQ not found")
            return mock_queue

        mock_channel.declare_queue = AsyncMock(side_effect=declare_queue_side_effect)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        result = await bus.health_check()

        # Overall health should still be True (DLQ errors are warnings)
        assert result.healthy is True
        assert result.queue_status == "accessible"
        assert "error:" in (result.dlq_status or "")

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_multiple_errors(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check with multiple errors combines them."""
        bus = RabbitMQEventBus(config=config)
        # Not connected, so multiple errors expected
        result = await bus.health_check()

        assert result.healthy is False
        # Error should contain both connection and channel errors
        assert result.error is not None
        assert "Not connected to RabbitMQ" in result.error
        assert "Channel not initialized" in result.error
