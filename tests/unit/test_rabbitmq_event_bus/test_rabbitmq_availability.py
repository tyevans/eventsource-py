"""Unit tests extracted for test_rabbitmq_availability.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import eventsource.adapters.rabbitmq.bus as rabbitmq_module
from eventsource.adapters.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQNotAvailableError,
)


class TestRabbitMQAvailability:
    def test_rabbitmq_available_flag_exists(self) -> None:
        assert isinstance(RABBITMQ_AVAILABLE, bool)

    def test_rabbitmq_available_flag_is_true_when_aio_pika_installed(self) -> None:
        if RABBITMQ_AVAILABLE:
            import aio_pika  # noqa: F401

            assert RABBITMQ_AVAILABLE is True

    def test_error_message_is_helpful(self) -> None:
        error_message = str(RabbitMQNotAvailableError())
        assert all(s in error_message for s in ("aio-pika", "eventsource[rabbitmq]", "pip install"))

    def test_error_is_import_error_subclass(self) -> None:
        assert isinstance(RabbitMQNotAvailableError(), ImportError)

    def test_error_message_contains_installation_command(self) -> None:
        assert "pip install eventsource[rabbitmq]" in str(RabbitMQNotAvailableError())

    def test_error_can_be_raised_and_caught(self) -> None:
        with pytest.raises(RabbitMQNotAvailableError) as exc_info:
            raise RabbitMQNotAvailableError()
        assert "aio-pika" in str(exc_info.value)

    def test_error_can_be_caught_as_import_error(self) -> None:
        with pytest.raises(ImportError) as exc_info:
            raise RabbitMQNotAvailableError()
        assert isinstance(exc_info.value, RabbitMQNotAvailableError)


class TestRabbitMQEventBusInit:
    def test_init_with_default_config(self) -> None:
        bus = RabbitMQEventBus()
        assert bus.config is not None
        assert bus.config.rabbitmq_url == "amqp://guest:guest@localhost:5672/"
        assert (bus.is_connected, bus.is_consuming, bus.stats.events_published) == (False, False, 0)

    def test_init_with_custom_config(self) -> None:
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )
        bus = RabbitMQEventBus(config=config)
        assert (bus.config.rabbitmq_url, bus.config.exchange_name, bus.config.consumer_group) == (
            "amqp://test:test@localhost/",
            "test-events",
            "test-group",
        )

    def test_init_with_event_registry(self) -> None:
        mock_registry = MagicMock()
        bus = RabbitMQEventBus(event_registry=mock_registry)
        assert bus._event_registry is mock_registry

    def test_init_raises_error_when_aio_pika_not_available(self) -> None:
        original_available = rabbitmq_module.RABBITMQ_AVAILABLE
        try:
            rabbitmq_module.RABBITMQ_AVAILABLE = False
            with pytest.raises(RabbitMQNotAvailableError):
                RabbitMQEventBus()
        finally:
            rabbitmq_module.RABBITMQ_AVAILABLE = original_available

    def test_init_initializes_stats(self) -> None:
        bus = RabbitMQEventBus()
        assert (
            bus.stats.events_published,
            bus.stats.events_consumed,
            bus.stats.events_processed_success,
            bus.stats.events_processed_failed,
        ) == (0, 0, 0, 0)

    def test_init_initializes_connection_state(self) -> None:
        bus = RabbitMQEventBus()
        assert (
            bus._connection_manager._connection,
            bus._connection_manager._channel,
            bus._connection_manager._connected,
            bus._consumer._consuming,
        ) == (None, None, False, False)


class TestRabbitMQEventBusUrlSanitization:
    def test_sanitize_url_removes_password(self) -> None:
        sanitized = RabbitMQEventBus()._sanitize_url("amqp://user:password@host:5672/")
        assert "password" not in sanitized and "***" in sanitized
        assert sanitized == "amqp://***:***@host:5672/"

    def test_sanitize_url_removes_complex_password(self) -> None:
        sanitized = RabbitMQEventBus()._sanitize_url("amqp://admin:p@ssw0rd!#$@host:5672/")
        assert "p@ssw0rd" not in sanitized and "***" in sanitized

    def test_sanitize_url_handles_no_credentials(self) -> None:
        url = "amqp://host:5672/"
        assert RabbitMQEventBus()._sanitize_url(url) == url

    def test_sanitize_url_handles_amqps(self) -> None:
        sanitized = RabbitMQEventBus()._sanitize_url("amqps://user:secret@host:5671/")
        assert "secret" not in sanitized and sanitized == "amqps://***:***@host:5671/"
