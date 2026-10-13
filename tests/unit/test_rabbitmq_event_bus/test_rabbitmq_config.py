"""Unit tests extracted for test_rabbitmq_config.py."""

from __future__ import annotations

import socket
from unittest import mock

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBusConfig,
)


class TestRabbitMQEventBusConfig:
    """Tests for RabbitMQEventBusConfig dataclass."""

    def test_default_values(self) -> None:
        """Test that all default values are correctly set."""
        config = RabbitMQEventBusConfig()

        assert config.rabbitmq_url == "amqp://guest:guest@localhost:5672/"
        assert config.exchange_name == "events"
        assert config.exchange_type == "topic"
        assert config.consumer_group == "default"
        assert config.prefetch_count == 10
        assert config.max_retries == 3
        assert config.enable_dlq is True
        assert config.dlq_exchange_suffix == "_dlq"
        assert config.durable is True
        assert config.auto_delete is False
        assert config.reconnect_delay == 1.0
        assert config.heartbeat == 60
        assert config.enable_tracing is True
        assert config.ssl_options is None

    def test_consumer_name_auto_generated(self) -> None:
        """Test that consumer_name is auto-generated when not provided."""
        config = RabbitMQEventBusConfig()

        assert config.consumer_name is not None
        assert len(config.consumer_name) > 0
        # Should contain hostname and a UUID segment
        hostname = socket.gethostname()
        assert config.consumer_name.startswith(f"{hostname}-")
        # UUID portion should be 8 characters
        uuid_part = config.consumer_name.split("-")[-1]
        assert len(uuid_part) == 8

    def test_consumer_name_preserved_when_provided(self) -> None:
        """Test that explicit consumer_name is preserved."""
        config = RabbitMQEventBusConfig(consumer_name="my-consumer")

        assert config.consumer_name == "my-consumer"

    def test_consumer_name_auto_generated_unique(self) -> None:
        """Test that auto-generated consumer names are unique."""
        config1 = RabbitMQEventBusConfig()
        config2 = RabbitMQEventBusConfig()

        # Consumer names should be different due to UUID
        assert config1.consumer_name != config2.consumer_name

    def test_queue_name_property(self) -> None:
        """Test queue_name property returns correct format."""
        config = RabbitMQEventBusConfig(exchange_name="test-events", consumer_group="test-group")

        assert config.queue_name == "test-events.test-group"

    def test_queue_name_with_default_values(self) -> None:
        """Test queue_name property with default values."""
        config = RabbitMQEventBusConfig()

        assert config.queue_name == "events.default"

    def test_dlq_exchange_name_property(self) -> None:
        """Test dlq_exchange_name property returns correct format."""
        config = RabbitMQEventBusConfig(exchange_name="test-events")

        assert config.dlq_exchange_name == "test-events_dlq"

    def test_dlq_exchange_name_with_default_values(self) -> None:
        """Test dlq_exchange_name property with default values."""
        config = RabbitMQEventBusConfig()

        assert config.dlq_exchange_name == "events_dlq"

    def test_dlq_queue_name_property(self) -> None:
        """Test dlq_queue_name property returns correct format."""
        config = RabbitMQEventBusConfig(exchange_name="test-events", consumer_group="test-group")

        assert config.dlq_queue_name == "test-events.test-group.dlq"

    def test_dlq_queue_name_with_default_values(self) -> None:
        """Test dlq_queue_name property with default values."""
        config = RabbitMQEventBusConfig()

        assert config.dlq_queue_name == "events.default.dlq"

    def test_custom_dlq_suffix(self) -> None:
        """Test custom DLQ suffix is respected."""
        config = RabbitMQEventBusConfig(exchange_name="events", dlq_exchange_suffix=".dead")

        assert config.dlq_exchange_name == "events.dead"

    def test_custom_dlq_suffix_empty(self) -> None:
        """Test empty DLQ suffix produces exchange name only."""
        config = RabbitMQEventBusConfig(exchange_name="events", dlq_exchange_suffix="")

        assert config.dlq_exchange_name == "events"

    def test_custom_rabbitmq_url(self) -> None:
        """Test custom RabbitMQ URL."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://user:password@rabbitmq.example.com:5672/myvhost"
        )

        assert config.rabbitmq_url == "amqp://user:password@rabbitmq.example.com:5672/myvhost"

    def test_amqps_url(self) -> None:
        """Test AMQPS (TLS) URL is accepted."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://user:password@rabbitmq.example.com:5671/"
        )

        assert config.rabbitmq_url == "amqps://user:password@rabbitmq.example.com:5671/"

    def test_exchange_types(self) -> None:
        """Test various exchange types are accepted."""
        for exchange_type in ["topic", "direct", "fanout", "headers"]:
            config = RabbitMQEventBusConfig(exchange_type=exchange_type)
            assert config.exchange_type == exchange_type

    def test_prefetch_count_custom(self) -> None:
        """Test custom prefetch count."""
        config = RabbitMQEventBusConfig(prefetch_count=100)

        assert config.prefetch_count == 100

    def test_max_retries_custom(self) -> None:
        """Test custom max retries."""
        config = RabbitMQEventBusConfig(max_retries=5)

        assert config.max_retries == 5

    def test_max_retries_zero(self) -> None:
        """Test zero max retries (immediate DLQ)."""
        config = RabbitMQEventBusConfig(max_retries=0)

        assert config.max_retries == 0

    def test_disable_dlq(self) -> None:
        """Test DLQ can be disabled."""
        config = RabbitMQEventBusConfig(enable_dlq=False)

        assert config.enable_dlq is False
        # DLQ names should still be computed (may be used elsewhere)
        assert config.dlq_exchange_name == "events_dlq"
        assert config.dlq_queue_name == "events.default.dlq"

    def test_durable_false(self) -> None:
        """Test non-durable configuration."""
        config = RabbitMQEventBusConfig(durable=False)

        assert config.durable is False

    def test_auto_delete_true(self) -> None:
        """Test auto-delete configuration."""
        config = RabbitMQEventBusConfig(auto_delete=True)

        assert config.auto_delete is True

    def test_reconnect_delay_custom(self) -> None:
        """Test custom reconnect delay."""
        config = RabbitMQEventBusConfig(reconnect_delay=2.0)

        assert config.reconnect_delay == 2.0

    def test_heartbeat_custom(self) -> None:
        """Test custom heartbeat interval."""
        config = RabbitMQEventBusConfig(heartbeat=30)

        assert config.heartbeat == 30

    def test_heartbeat_zero_disables(self) -> None:
        """Test zero heartbeat (disabled)."""
        config = RabbitMQEventBusConfig(heartbeat=0)

        assert config.heartbeat == 0

    def test_disable_tracing(self) -> None:
        """Test tracing can be disabled."""
        config = RabbitMQEventBusConfig(enable_tracing=False)

        assert config.enable_tracing is False

    def test_ssl_options(self) -> None:
        """Test SSL options can be provided."""
        ssl_opts = {
            "certfile": "/path/to/cert.pem",
            "keyfile": "/path/to/key.pem",
            "cafile": "/path/to/ca.pem",
        }
        config = RabbitMQEventBusConfig(ssl_options=ssl_opts)

        assert config.ssl_options == ssl_opts

    def test_consumer_name_with_mocked_hostname(self) -> None:
        """Test consumer name generation with mocked hostname."""
        with mock.patch("socket.gethostname", return_value="test-host"):
            config = RabbitMQEventBusConfig()

            assert config.consumer_name is not None
            assert config.consumer_name.startswith("test-host-")

    def test_consumer_name_format(self) -> None:
        """Test consumer name follows expected format."""
        with (
            mock.patch("socket.gethostname", return_value="worker-01"),
            mock.patch("uuid.uuid4") as mock_uuid,
        ):
            mock_uuid.return_value = mock.Mock()
            mock_uuid.return_value.__str__ = mock.Mock(
                return_value="12345678-90ab-cdef-1234-567890abcdef"
            )

            config = RabbitMQEventBusConfig()

            assert config.consumer_name == "worker-01-12345678"

    def test_full_configuration(self) -> None:
        """Test creating a fully customized configuration."""
        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqps://admin:secret@mq.example.com:5671/production",
            exchange_name="myapp.events",
            exchange_type="topic",
            consumer_group="order-projection",
            consumer_name="worker-1",
            prefetch_count=50,
            max_retries=5,
            enable_dlq=True,
            dlq_exchange_suffix=".dlx",
            durable=True,
            auto_delete=False,
            reconnect_delay=2.0,
            heartbeat=30,
            enable_tracing=True,
            ssl_options={"verify": True},
        )

        assert config.rabbitmq_url == "amqps://admin:secret@mq.example.com:5671/production"
        assert config.exchange_name == "myapp.events"
        assert config.exchange_type == "topic"
        assert config.consumer_group == "order-projection"
        assert config.consumer_name == "worker-1"
        assert config.prefetch_count == 50
        assert config.max_retries == 5
        assert config.enable_dlq is True
        assert config.dlq_exchange_suffix == ".dlx"
        assert config.durable is True
        assert config.auto_delete is False
        assert config.reconnect_delay == 2.0
        assert config.heartbeat == 30
        assert config.enable_tracing is True
        assert config.ssl_options == {"verify": True}

        # Computed properties
        assert config.queue_name == "myapp.events.order-projection"
        assert config.dlq_exchange_name == "myapp.events.dlx"
        assert config.dlq_queue_name == "myapp.events.order-projection.dlq"

    def test_config_is_dataclass(self) -> None:
        """Test that config is a proper dataclass."""
        from dataclasses import is_dataclass

        assert is_dataclass(RabbitMQEventBusConfig)
        assert is_dataclass(RabbitMQEventBusConfig())

    def test_config_equality(self) -> None:
        """Test dataclass equality comparison."""
        config1 = RabbitMQEventBusConfig(
            exchange_name="test",
            consumer_group="group",
            consumer_name="fixed-name",  # Fix consumer name to ensure equality
        )
        config2 = RabbitMQEventBusConfig(
            exchange_name="test", consumer_group="group", consumer_name="fixed-name"
        )

        assert config1 == config2

    def test_config_inequality(self) -> None:
        """Test dataclass inequality comparison."""
        config1 = RabbitMQEventBusConfig(exchange_name="test1", consumer_name="name1")
        config2 = RabbitMQEventBusConfig(exchange_name="test2", consumer_name="name1")

        assert config1 != config2

    def test_special_characters_in_names(self) -> None:
        """Test handling of special characters in names."""
        config = RabbitMQEventBusConfig(
            exchange_name="my-app.events_v2", consumer_group="order-service.projections"
        )

        assert config.queue_name == "my-app.events_v2.order-service.projections"
        assert config.dlq_exchange_name == "my-app.events_v2_dlq"
        assert config.dlq_queue_name == "my-app.events_v2.order-service.projections.dlq"
