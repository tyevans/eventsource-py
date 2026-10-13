"""TLS configuration and context creation unit tests."""

from __future__ import annotations

import logging
import ssl
from unittest.mock import MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestTLSSupport:
    """Tests for TLS/SSL configuration support (P3-004).

    These tests verify the TLS configuration and SSL context creation
    functionality for secure RabbitMQ connections.
    """

    def test_tls_config_fields_exist(self) -> None:
        """Test that TLS configuration fields exist with correct defaults."""
        config = RabbitMQEventBusConfig()

        assert config.ssl_options is None
        assert config.ssl_context is None
        assert config.verify_ssl is True
        assert config.ca_file is None
        assert config.cert_file is None
        assert config.key_file is None

    def test_tls_config_fields_can_be_set(self) -> None:
        """Test that TLS configuration fields can be set."""
        import ssl as ssl_module

        ctx = ssl_module.create_default_context()
        config = RabbitMQEventBusConfig(
            ssl_options={"server_hostname": "rabbitmq.example.com"},
            ssl_context=ctx,
            verify_ssl=False,
            ca_file="/path/to/ca.crt",
            cert_file="/path/to/client.crt",
            key_file="/path/to/client.key",
        )

        assert config.ssl_options == {"server_hostname": "rabbitmq.example.com"}
        assert config.ssl_context is ctx
        assert config.verify_ssl is False
        assert config.ca_file == "/path/to/ca.crt"
        assert config.cert_file == "/path/to/client.crt"
        assert config.key_file == "/path/to/client.key"

    def test_amqp_url_no_tls(self) -> None:
        """Test that amqp:// URL without TLS config returns None SSL context."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://user:pass@host:5672/",
        )
        bus = RabbitMQEventBus(config=config)

        ctx = bus._create_ssl_context()
        assert ctx is None

    def test_amqps_url_triggers_tls(self) -> None:
        """Test that amqps:// URL automatically creates SSL context."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:pass@host:5671/",
        )
        bus = RabbitMQEventBus(config=config)

        ctx = bus._create_ssl_context()
        assert ctx is not None
        assert isinstance(ctx, ssl.SSLContext)

    def test_custom_ssl_context_used(self) -> None:
        """Test that provided ssl_context takes precedence."""
        import ssl as ssl_module

        custom_ctx = ssl_module.create_default_context()
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:pass@host:5671/",
            ssl_context=custom_ctx,
        )
        bus = RabbitMQEventBus(config=config)

        result = bus._create_ssl_context()
        assert result is custom_ctx

    def test_custom_ssl_context_with_amqp_url(self) -> None:
        """Test that ssl_context is used even with amqp:// URL."""
        import ssl as ssl_module

        custom_ctx = ssl_module.create_default_context()
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://user:pass@host:5672/",
            ssl_context=custom_ctx,
        )
        bus = RabbitMQEventBus(config=config)

        result = bus._create_ssl_context()
        assert result is custom_ctx

    def test_ca_file_triggers_tls(self) -> None:
        """Test that ca_file configuration triggers TLS context creation."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://user:pass@host:5672/",
            ca_file="/path/to/ca.crt",
        )
        bus = RabbitMQEventBus(config=config)

        # This will attempt to load the ca_file which doesn't exist
        # We need to mock the ssl module for this test
        with patch("ssl.create_default_context") as mock_create_ctx:
            mock_ctx = MagicMock(spec=ssl.SSLContext)
            mock_create_ctx.return_value = mock_ctx

            result = bus._create_ssl_context()

            assert result is mock_ctx
            mock_ctx.load_verify_locations.assert_called_once_with(cafile="/path/to/ca.crt")

    def test_cert_file_triggers_tls(self) -> None:
        """Test that cert_file configuration triggers TLS context creation."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://user:pass@host:5672/",
            cert_file="/path/to/client.crt",
            key_file="/path/to/client.key",
        )
        bus = RabbitMQEventBus(config=config)

        with patch("ssl.create_default_context") as mock_create_ctx:
            mock_ctx = MagicMock(spec=ssl.SSLContext)
            mock_create_ctx.return_value = mock_ctx

            result = bus._create_ssl_context()

            assert result is mock_ctx
            mock_ctx.load_cert_chain.assert_called_once_with(
                certfile="/path/to/client.crt",
                keyfile="/path/to/client.key",
            )

    def test_mtls_with_ca_and_client_cert(self) -> None:
        """Test mutual TLS with CA certificate and client certificates."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://host:5671/",
            ca_file="/path/to/ca.crt",
            cert_file="/path/to/client.crt",
            key_file="/path/to/client.key",
        )
        bus = RabbitMQEventBus(config=config)

        with patch("ssl.create_default_context") as mock_create_ctx:
            mock_ctx = MagicMock(spec=ssl.SSLContext)
            mock_create_ctx.return_value = mock_ctx

            result = bus._create_ssl_context()

            assert result is mock_ctx
            mock_ctx.load_verify_locations.assert_called_once_with(cafile="/path/to/ca.crt")
            mock_ctx.load_cert_chain.assert_called_once_with(
                certfile="/path/to/client.crt",
                keyfile="/path/to/client.key",
            )

    def test_verify_ssl_false_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that disabling SSL verification logs a warning."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://host:5671/",
            verify_ssl=False,
        )
        bus = RabbitMQEventBus(config=config)

        with caplog.at_level(logging.WARNING):
            ctx = bus._create_ssl_context()

        assert ctx is not None
        assert ctx.verify_mode == ssl.CERT_NONE
        assert ctx.check_hostname is False
        assert "NOT RECOMMENDED" in caplog.text

    def test_cert_file_without_key_file_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that providing cert_file without key_file logs a warning."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://host:5671/",
            cert_file="/path/to/client.crt",
        )
        bus = RabbitMQEventBus(config=config)

        with caplog.at_level(logging.WARNING):
            ctx = bus._create_ssl_context()

        assert ctx is not None
        assert "Both cert_file and key_file must be provided" in caplog.text

    def test_key_file_without_cert_file_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that providing key_file without cert_file logs a warning."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://host:5671/",
            key_file="/path/to/client.key",
        )
        bus = RabbitMQEventBus(config=config)

        with caplog.at_level(logging.WARNING):
            ctx = bus._create_ssl_context()

        assert ctx is not None
        assert "Both cert_file and key_file must be provided" in caplog.text

    def test_amqps_url_preserved_in_config(self) -> None:
        """Test that amqps:// URLs are stored correctly in config."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:password@rabbitmq.example.com:5671/myvhost"
        )

        assert config.rabbitmq_url == "amqps://user:password@rabbitmq.example.com:5671/myvhost"
        assert config.rabbitmq_url.startswith("amqps://")

    def test_full_tls_configuration(self) -> None:
        """Test creating a fully customized TLS configuration."""
        import ssl as ssl_module

        ctx = ssl_module.create_default_context()
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://admin:secret@mq.example.com:5671/production",
            ssl_context=ctx,
            ssl_options={"server_hostname": "mq.example.com"},
            verify_ssl=True,
            ca_file="/etc/ssl/ca-bundle.crt",
            cert_file="/etc/ssl/client.crt",
            key_file="/etc/ssl/client.key",
        )

        assert config.rabbitmq_url == "amqps://admin:secret@mq.example.com:5671/production"
        assert config.ssl_context is ctx
        assert config.ssl_options == {"server_hostname": "mq.example.com"}
        assert config.verify_ssl is True
        assert config.ca_file == "/etc/ssl/ca-bundle.crt"
        assert config.cert_file == "/etc/ssl/client.crt"
        assert config.key_file == "/etc/ssl/client.key"
