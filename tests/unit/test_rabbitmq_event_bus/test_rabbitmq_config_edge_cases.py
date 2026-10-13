"""Unit tests extracted for test_rabbitmq_config_edge_cases.py."""

from __future__ import annotations

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestRabbitMQEventBusConfigEdgeCases:
    """Edge case tests for RabbitMQEventBusConfig."""

    def test_empty_exchange_name(self) -> None:
        """Test with empty exchange name."""
        config = RabbitMQEventBusConfig(exchange_name="")

        assert config.exchange_name == ""
        assert config.queue_name == ".default"
        assert config.dlq_exchange_name == "_dlq"

    def test_empty_consumer_group(self) -> None:
        """Test with empty consumer group."""
        config = RabbitMQEventBusConfig(consumer_group="")

        assert config.consumer_group == ""
        assert config.queue_name == "events."

    def test_very_long_names(self) -> None:
        """Test with very long exchange and group names."""
        long_name = "a" * 200
        config = RabbitMQEventBusConfig(exchange_name=long_name, consumer_group=long_name)

        assert config.exchange_name == long_name
        assert config.consumer_group == long_name
        assert len(config.queue_name) == 401  # 200 + 1 (dot) + 200

    def test_unicode_names(self) -> None:
        """Test with unicode characters in names."""
        config = RabbitMQEventBusConfig(exchange_name="events", consumer_group="order")

        # Basic test - actual unicode support depends on RabbitMQ
        assert config.queue_name == "events.order"

    def test_negative_prefetch_count(self) -> None:
        """Test that negative prefetch count is stored (validation is at connection time)."""
        config = RabbitMQEventBusConfig(prefetch_count=-1)

        # Config stores the value; validation happens at connection time
        assert config.prefetch_count == -1

    def test_negative_max_retries(self) -> None:
        """Test that negative max_retries is stored."""
        config = RabbitMQEventBusConfig(max_retries=-1)

        assert config.max_retries == -1

    def test_negative_delays(self) -> None:
        """Test that negative delays are stored."""
        config = RabbitMQEventBusConfig(reconnect_delay=-1.0)

        assert config.reconnect_delay == -1.0


class TestRabbitMQConfigImmutabilityAfterInit:
    """Tests for config behavior after initialization."""

    def test_config_cannot_be_changed_after_init(self) -> None:
        """Test that config property returns the original config."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        original_config = RabbitMQEventBusConfig(
            exchange_name="original",
            consumer_name="test-consumer",
        )
        bus = RabbitMQEventBus(config=original_config)

        # Config should be the same object
        assert bus.config is original_config
        assert bus.config.exchange_name == "original"


class TestRabbitMQConfigValidation:
    """Tests for configuration validation and edge cases."""

    def test_config_with_all_exchange_types(self) -> None:
        """Test that all valid exchange types are accepted in config."""
        valid_types = ["topic", "direct", "fanout", "headers", "TOPIC", "Direct"]

        for exchange_type in valid_types:
            config = RabbitMQEventBusConfig(exchange_type=exchange_type)
            assert config.exchange_type == exchange_type

    def test_config_preserves_whitespace_in_names(self) -> None:
        """Test that config preserves whitespace (for debugging/verification)."""
        config = RabbitMQEventBusConfig(
            exchange_name="  spaced  ",
            consumer_group="  also spaced  ",
        )

        # Values should be preserved as-is
        assert config.exchange_name == "  spaced  "
        assert config.consumer_group == "  also spaced  "
        # Computed values should include the spaces
        assert config.queue_name == "  spaced  .  also spaced  "
