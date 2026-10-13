"""Unit tests extracted for test_rabbitmq_logging_exports.py."""

from __future__ import annotations

import asyncio


class TestRabbitMQStructuredLogging:
    """Tests for structured logging in RabbitMQEventBus (P2-008)."""

    def test_sanitize_url_in_connect_log(self) -> None:
        """Test that connect logs use sanitized URL."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

        config = RabbitMQEventBusConfig(rabbitmq_url="amqp://user:secret123@localhost:5672/")
        bus = RabbitMQEventBus(config=config)

        # Verify sanitize_url removes credentials
        sanitized = bus._sanitize_url(config.rabbitmq_url)
        assert "secret123" not in sanitized
        assert "user" not in sanitized
        assert "***:***" in sanitized
        assert "localhost:5672" in sanitized

    def test_sanitize_url_with_at_in_password(self) -> None:
        """Test sanitization handles @ symbol in password."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        # Password with @ symbol
        url = "amqp://user:p@ss@host:5672/"
        sanitized = bus._sanitize_url(url)

        # Should sanitize without exposing the password
        assert "p@ss" not in sanitized
        assert "***" in sanitized

    def test_sanitize_url_with_special_chars(self) -> None:
        """Test sanitization handles special characters."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        url = "amqp://admin:P@ssw0rd!#$%^&*()@rabbitmq.example.com:5672/"
        sanitized = bus._sanitize_url(url)

        # Password should be completely hidden
        assert "P@ssw0rd" not in sanitized
        assert "***" in sanitized
        assert "rabbitmq.example.com:5672" in sanitized

    def test_sanitize_url_preserves_vhost(self) -> None:
        """Test sanitization preserves virtual host."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        url = "amqp://user:password@localhost:5672/myvhost"
        sanitized = bus._sanitize_url(url)

        assert "password" not in sanitized
        assert "myvhost" in sanitized
        assert sanitized.endswith("/myvhost")

    def test_sanitize_url_with_no_path(self) -> None:
        """Test sanitization handles URL with no path."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        url = "amqp://user:password@localhost:5672"
        sanitized = bus._sanitize_url(url)

        assert "password" not in sanitized
        assert sanitized == "amqp://***:***@localhost:5672"

    def test_log_extra_fields_standard_names(self) -> None:
        """Test that standard field names are used in extra dicts.

        This is a documentation/reference test showing the expected field names.
        """
        # Standard field names for different operations
        connection_fields = ["rabbitmq_url", "exchange", "queue", "consumer_group"]
        publish_fields = [
            "event_id",
            "event_type",
            "aggregate_type",
            "aggregate_id",
            "routing_key",
        ]
        consume_fields = ["message_id", "event_type", "routing_key", "retry_count"]
        handler_fields = ["handler", "event_type", "event_id", "duration_ms"]
        error_fields = ["error", "error_type"]

        # Verify these are all valid Python identifiers (can be used as kwargs)
        for field in (
            connection_fields + publish_fields + consume_fields + handler_fields + error_fields
        ):
            assert field.isidentifier(), f"{field} is not a valid identifier"

    def test_duration_ms_field_is_numeric(self) -> None:
        """Test that duration_ms would be numeric (not a string)."""
        from datetime import UTC, datetime

        start = datetime.now(UTC)
        # Simulate some work
        end = datetime.now(UTC)
        duration = (end - start).total_seconds()
        duration_ms = duration * 1000

        # duration_ms should be a float
        assert isinstance(duration_ms, float)
        assert duration_ms >= 0


class TestRabbitMQLoggingIntegration:
    """Tests to verify logging is properly set up."""

    def test_logger_is_configured(self) -> None:
        """Test that the bus has a logger configured."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus._logger is not None
        assert bus._logger.name == "eventsource.adapters.rabbitmq"


class TestRabbitMQModuleAllExports:
    """Tests to verify __all__ exports in rabbitmq module."""

    def test_module_all_contains_expected_exports(self) -> None:
        """Test that __all__ contains all expected exports."""
        from eventsource.adapters import rabbitmq

        expected_exports = {
            "BatchPublishError",
            "DLQMessage",
            "HealthCheckResult",
            "OTEL_AVAILABLE",
            "QueueInfo",
            "RabbitMQEventBus",
            "RabbitMQEventBusConfig",
            "RabbitMQEventBusStats",
            "RabbitMQNotAvailableError",
            "RABBITMQ_AVAILABLE",
            "ShutdownError",
        }

        actual_exports = set(rabbitmq.__all__)
        assert expected_exports == actual_exports

    def test_all_exports_are_importable(self) -> None:
        """Test that all items in __all__ can be imported."""
        from eventsource.adapters import rabbitmq

        for name in rabbitmq.__all__:
            obj = getattr(rabbitmq, name)
            assert obj is not None, f"{name} is None"


class TestRabbitMQEventBusInterfaceCompliance:
    """Tests to verify RabbitMQEventBus implements EventBus interface."""

    def test_implements_event_bus_interface(self) -> None:
        """Test that RabbitMQEventBus implements EventBus protocol."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        # Check that required methods exist
        assert hasattr(bus, "publish")
        assert hasattr(bus, "subscribe")
        assert hasattr(bus, "unsubscribe")
        assert hasattr(bus, "subscribe_all")
        assert hasattr(bus, "subscribe_to_all_events")
        assert hasattr(bus, "unsubscribe_from_all_events")
        assert hasattr(bus, "get_subscriber_count")
        assert hasattr(bus, "get_wildcard_subscriber_count")
        assert hasattr(bus, "clear_subscribers")

        # Check that methods are callable
        assert callable(bus.publish)
        assert callable(bus.subscribe)
        assert callable(bus.unsubscribe)

    def test_has_async_context_manager_methods(self) -> None:
        """Test that RabbitMQEventBus has async context manager methods."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert hasattr(bus, "__aenter__")
        assert hasattr(bus, "__aexit__")
        assert asyncio.iscoroutinefunction(bus.__aenter__)
        assert asyncio.iscoroutinefunction(bus.__aexit__)
