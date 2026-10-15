"""TLS connection and socket setup unit tests."""

from __future__ import annotations

import logging
import ssl
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig


def _setup_mock_aio_pika(mock_aio_pika: MagicMock) -> None:
    mock_connection = AsyncMock(is_closed=False)
    mock_channel = AsyncMock(is_closed=False)
    mock_channel.declare_exchange = AsyncMock(return_value=AsyncMock())
    mock_channel.declare_queue = AsyncMock(return_value=AsyncMock())
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)


def _config(url: str = "amqps://user:pass@host:5671/", **kwargs: Any) -> RabbitMQEventBusConfig:
    return RabbitMQEventBusConfig(
        rabbitmq_url=url,
        exchange_name="test-events",
        consumer_group="test-group",
        enable_dlq=False,
        **kwargs,
    )


class TestTLSSupportConnection:
    """Tests for TLS/SSL connection execution support (P3-004)."""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_with_amqps_url(self, mock_aio_pika: MagicMock) -> None:
        _setup_mock_aio_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=_config())
        await bus.connect()

        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert "ssl_context" in call_kwargs
        assert isinstance(call_kwargs["ssl_context"], ssl.SSLContext)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_with_custom_ssl_context(self, mock_aio_pika: MagicMock) -> None:
        _setup_mock_aio_pika(mock_aio_pika)
        custom_ctx = ssl.create_default_context()
        bus = RabbitMQEventBus(config=_config(ssl_context=custom_ctx))
        await bus.connect()

        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert call_kwargs["ssl_context"] is custom_ctx

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_with_ssl_options(self, mock_aio_pika: MagicMock) -> None:
        _setup_mock_aio_pika(mock_aio_pika)
        bus = RabbitMQEventBus(
            config=_config(ssl_options={"server_hostname": "custom.hostname.com"})
        )
        await bus.connect()

        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert "ssl_context" in call_kwargs
        assert call_kwargs.get("server_hostname") == "custom.hostname.com"

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_without_tls(self, mock_aio_pika: MagicMock) -> None:
        _setup_mock_aio_pika(mock_aio_pika)
        bus = RabbitMQEventBus(config=_config("amqp://user:pass@host:5672/"))
        await bus.connect()

        call_kwargs = mock_aio_pika.connect_robust.call_args.kwargs
        assert "ssl_context" not in call_kwargs

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_logs_tls_status(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        _setup_mock_aio_pika(mock_aio_pika)
        with caplog.at_level(logging.INFO):
            bus = RabbitMQEventBus(config=_config())
            await bus.connect()

        connect_logs = [r for r in caplog.records if "Connected to RabbitMQ" in r.message]
        assert len(connect_logs) >= 1
        assert "TLS" in connect_logs[0].message
        assert hasattr(connect_logs[0], "tls_enabled")
        assert connect_logs[0].tls_enabled is True

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_logs_plaintext_status(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        _setup_mock_aio_pika(mock_aio_pika)
        with caplog.at_level(logging.INFO):
            bus = RabbitMQEventBus(config=_config("amqp://user:pass@host:5672/"))
            await bus.connect()

        connect_logs = [r for r in caplog.records if "Connected to RabbitMQ" in r.message]
        assert len(connect_logs) >= 1
        assert "plaintext" in connect_logs[0].message
        assert hasattr(connect_logs[0], "tls_enabled")
        assert connect_logs[0].tls_enabled is False

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_ssl_error_handling(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_aio_pika.connect_robust = AsyncMock(
            side_effect=ssl.SSLError("certificate verify failed")
        )
        bus = RabbitMQEventBus(config=_config())

        with caplog.at_level(logging.ERROR), pytest.raises(ssl.SSLError):
            await bus.connect()

        error_logs = [r for r in caplog.records if "SSL error" in r.message]
        assert len(error_logs) >= 1
        assert "certificate verify failed" in str(error_logs[0].message)
