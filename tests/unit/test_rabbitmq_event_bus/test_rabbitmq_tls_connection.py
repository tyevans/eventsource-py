"""TLS connection and socket setup unit tests."""

from __future__ import annotations

import logging
import ssl
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestTLSSupportConnection:
    """Tests for TLS/SSL connection execution support (P3-004).

    These tests verify connection behavior with TLS enabled, custom SSL contexts,
    and SSL error handling.
    """

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_with_amqps_url(self, mock_aio_pika: MagicMock) -> None:
        """Test connection with amqps:// URL passes ssl_context to connect_robust."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:pass@host:5671/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify connect_robust was called with ssl_context
        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert "ssl_context" in call_kwargs
        assert isinstance(call_kwargs["ssl_context"], ssl.SSLContext)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_with_custom_ssl_context(self, mock_aio_pika: MagicMock) -> None:
        """Test connection with custom SSL context."""
        import ssl as ssl_module

        custom_ctx = ssl_module.create_default_context()
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:pass@host:5671/",
            ssl_context=custom_ctx,
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify connect_robust was called with the custom ssl_context
        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert call_kwargs["ssl_context"] is custom_ctx

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_with_ssl_options(self, mock_aio_pika: MagicMock) -> None:
        """Test connection with additional ssl_options."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:pass@host:5671/",
            ssl_options={"server_hostname": "custom.hostname.com"},
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify ssl_options are passed to connect_robust
        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert "ssl_context" in call_kwargs
        assert call_kwargs.get("server_hostname") == "custom.hostname.com"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_without_tls(self, mock_aio_pika: MagicMock) -> None:
        """Test connection without TLS does not pass ssl_context."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://user:pass@host:5672/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify ssl_context is not in kwargs
        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert "ssl_context" not in call_kwargs

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_logs_tls_status(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that connection logs TLS status."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:pass@host:5671/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        with caplog.at_level(logging.INFO):
            bus = RabbitMQEventBus(config=config)
            await bus.connect()

        # Check that TLS is mentioned in connection log
        connect_logs = [r for r in caplog.records if "Connected to RabbitMQ" in r.message]
        assert len(connect_logs) >= 1
        assert "TLS" in connect_logs[0].message

        # Check log extras
        assert hasattr(connect_logs[0], "tls_enabled")
        assert connect_logs[0].tls_enabled is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_logs_plaintext_status(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that connection logs plaintext status when TLS is not used."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://user:pass@host:5672/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        with caplog.at_level(logging.INFO):
            bus = RabbitMQEventBus(config=config)
            await bus.connect()

        # Check that plaintext is mentioned in connection log
        connect_logs = [r for r in caplog.records if "Connected to RabbitMQ" in r.message]
        assert len(connect_logs) >= 1
        assert "plaintext" in connect_logs[0].message

        # Check log extras
        assert hasattr(connect_logs[0], "tls_enabled")
        assert connect_logs[0].tls_enabled is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_ssl_error_handling(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that SSL errors are caught and logged appropriately."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:pass@host:5671/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

        # Simulate SSL error
        mock_aio_pika.connect_robust = AsyncMock(
            side_effect=ssl.SSLError("certificate verify failed")
        )

        bus = RabbitMQEventBus(config=config)

        with caplog.at_level(logging.ERROR), pytest.raises(ssl.SSLError):
            await bus.connect()

        # Check that SSL error was logged
        error_logs = [r for r in caplog.records if "SSL error" in r.message]
        assert len(error_logs) >= 1
        assert "certificate verify failed" in str(error_logs[0].message)
