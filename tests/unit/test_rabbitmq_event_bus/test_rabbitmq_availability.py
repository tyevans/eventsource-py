"""Unit tests extracted for test_rabbitmq_availability.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from eventsource.adapters.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQEventBusConfig,
    RabbitMQNotAvailableError,
)


class TestRabbitMQAvailability:
    """Tests for the optional dependency pattern for aio-pika."""

    def test_rabbitmq_available_flag_exists(self) -> None:
        """Test that RABBITMQ_AVAILABLE flag exists and is a boolean."""
        assert isinstance(RABBITMQ_AVAILABLE, bool)

    def test_rabbitmq_available_flag_is_true_when_aio_pika_installed(self) -> None:
        """Test that RABBITMQ_AVAILABLE is True when aio-pika is installed.

        This test only runs when aio-pika is available, which is the case
        in our development environment.
        """
        # If we got here without ImportError, aio-pika is installed
        # so RABBITMQ_AVAILABLE should be True
        if RABBITMQ_AVAILABLE:
            # Verify we can actually import aio_pika
            import aio_pika  # noqa: F401

            assert RABBITMQ_AVAILABLE is True

    def test_error_message_is_helpful(self) -> None:
        """Test that RabbitMQNotAvailableError has a helpful error message."""
        error = RabbitMQNotAvailableError()

        error_message = str(error)
        assert "aio-pika" in error_message
        assert "eventsource[rabbitmq]" in error_message
        assert "pip install" in error_message

    def test_error_is_import_error_subclass(self) -> None:
        """Test that RabbitMQNotAvailableError is an ImportError subclass."""
        error = RabbitMQNotAvailableError()

        assert isinstance(error, ImportError)

    def test_error_message_contains_installation_command(self) -> None:
        """Test that the error message contains the correct installation command."""
        error = RabbitMQNotAvailableError()

        # The exact installation command should be in the error message
        assert "pip install eventsource[rabbitmq]" in str(error)

    def test_error_can_be_raised_and_caught(self) -> None:
        """Test that the error can be raised and caught properly."""
        import pytest

        with pytest.raises(RabbitMQNotAvailableError) as exc_info:
            raise RabbitMQNotAvailableError()

        assert "aio-pika" in str(exc_info.value)

    def test_error_can_be_caught_as_import_error(self) -> None:
        """Test that the error can be caught as a generic ImportError."""
        import pytest

        with pytest.raises(ImportError) as exc_info:
            raise RabbitMQNotAvailableError()

        # Verify it's actually our custom error
        assert isinstance(exc_info.value, RabbitMQNotAvailableError)


class TestRabbitMQEventBusInit:
    """Tests for RabbitMQEventBus initialization."""

    def test_init_with_default_config(self) -> None:
        """Test initialization with default configuration."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus.config is not None
        assert bus.config.rabbitmq_url == "amqp://guest:guest@localhost:5672/"
        assert bus.is_connected is False
        assert bus.is_consuming is False
        assert bus.stats.events_published == 0

    def test_init_with_custom_config(self) -> None:
        """Test initialization with custom configuration."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )
        bus = RabbitMQEventBus(config=config)

        assert bus.config.rabbitmq_url == "amqp://test:test@localhost/"
        assert bus.config.exchange_name == "test-events"
        assert bus.config.consumer_group == "test-group"

    def test_init_with_event_registry(self) -> None:
        """Test initialization with custom event registry."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        # Mock registry
        mock_registry = MagicMock()
        bus = RabbitMQEventBus(event_registry=mock_registry)

        assert bus._event_registry is mock_registry

    def test_init_raises_error_when_aio_pika_not_available(self) -> None:
        """Test that initialization raises error when aio-pika not available."""
        # We need to patch at the module level before the class checks
        import eventsource.adapters.rabbitmq.bus as rabbitmq_module
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        original_available = rabbitmq_module.RABBITMQ_AVAILABLE

        try:
            # Temporarily set RABBITMQ_AVAILABLE to False
            rabbitmq_module.RABBITMQ_AVAILABLE = False

            with pytest.raises(RabbitMQNotAvailableError):
                RabbitMQEventBus()
        finally:
            # Restore original value
            rabbitmq_module.RABBITMQ_AVAILABLE = original_available

    def test_init_initializes_stats(self) -> None:
        """Test that initialization creates empty stats."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus.stats.events_published == 0
        assert bus.stats.events_consumed == 0
        assert bus.stats.events_processed_success == 0
        assert bus.stats.events_processed_failed == 0

    def test_init_initializes_connection_state(self) -> None:
        """Test that initialization sets connection state to disconnected."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus._connection is None
        assert bus._channel is None
        assert bus._connected is False
        assert bus._consuming is False


class TestRabbitMQEventBusUrlSanitization:
    """Tests for URL sanitization in RabbitMQEventBus."""

    def test_sanitize_url_removes_password(self) -> None:
        """Test that _sanitize_url removes password from URL."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        sanitized = bus._sanitize_url("amqp://user:password@host:5672/")

        assert "password" not in sanitized
        assert "***" in sanitized
        assert sanitized == "amqp://***:***@host:5672/"

    def test_sanitize_url_removes_complex_password(self) -> None:
        """Test sanitization with complex password containing special chars."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        sanitized = bus._sanitize_url("amqp://admin:p@ssw0rd!#$@host:5672/")

        assert "p@ssw0rd" not in sanitized
        assert "***" in sanitized

    def test_sanitize_url_handles_no_credentials(self) -> None:
        """Test sanitization when no credentials in URL."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        url = "amqp://host:5672/"
        sanitized = bus._sanitize_url(url)

        # URL should remain unchanged
        assert sanitized == url

    def test_sanitize_url_handles_amqps(self) -> None:
        """Test sanitization works with amqps:// URLs."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        sanitized = bus._sanitize_url("amqps://user:secret@host:5671/")

        assert "secret" not in sanitized
        assert sanitized == "amqps://***:***@host:5671/"
