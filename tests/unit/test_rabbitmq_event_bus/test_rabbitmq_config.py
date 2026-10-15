"""Unit tests extracted for test_rabbitmq_config.py."""

from __future__ import annotations

import socket
from dataclasses import is_dataclass
from unittest import mock

from eventsource.adapters.rabbitmq import RabbitMQEventBusConfig


class TestRabbitMQEventBusConfig:
    """Tests for RabbitMQEventBusConfig dataclass."""

    def test_default_values(self) -> None:
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
        config = RabbitMQEventBusConfig()
        assert config.consumer_name is not None and len(config.consumer_name) > 0
        assert config.consumer_name.startswith(f"{socket.gethostname()}-")
        assert len(config.consumer_name.split("-")[-1]) == 8

    def test_consumer_name_preserved_when_provided(self) -> None:
        assert RabbitMQEventBusConfig(consumer_name="my-consumer").consumer_name == "my-consumer"

    def test_consumer_name_auto_generated_unique(self) -> None:
        assert RabbitMQEventBusConfig().consumer_name != RabbitMQEventBusConfig().consumer_name

    def test_queue_name_property(self) -> None:
        config = RabbitMQEventBusConfig(exchange_name="test-events", consumer_group="test-group")
        assert config.queue_name == "test-events.test-group"

    def test_queue_name_with_default_values(self) -> None:
        assert RabbitMQEventBusConfig().queue_name == "events.default"

    def test_dlq_exchange_name_property(self) -> None:
        config = RabbitMQEventBusConfig(exchange_name="test-events")
        assert config.dlq_exchange_name == "test-events_dlq"

    def test_dlq_exchange_name_with_default_values(self) -> None:
        assert RabbitMQEventBusConfig().dlq_exchange_name == "events_dlq"

    def test_dlq_queue_name_property(self) -> None:
        config = RabbitMQEventBusConfig(exchange_name="test-events", consumer_group="test-group")
        assert config.dlq_queue_name == "test-events.test-group.dlq"

    def test_dlq_queue_name_with_default_values(self) -> None:
        assert RabbitMQEventBusConfig().dlq_queue_name == "events.default.dlq"

    def test_custom_dlq_suffix(self) -> None:
        config = RabbitMQEventBusConfig(exchange_name="events", dlq_exchange_suffix=".dead")
        assert config.dlq_exchange_name == "events.dead"

    def test_custom_dlq_suffix_empty(self) -> None:
        config = RabbitMQEventBusConfig(exchange_name="events", dlq_exchange_suffix="")
        assert config.dlq_exchange_name == "events"

    def test_custom_rabbitmq_url(self) -> None:
        url = "amqp://user:password@rabbitmq.example.com:5672/myvhost"
        assert RabbitMQEventBusConfig(rabbitmq_url=url).rabbitmq_url == url

    def test_amqps_url(self) -> None:
        url = "amqps://user:password@rabbitmq.example.com:5671/"
        assert RabbitMQEventBusConfig(rabbitmq_url=url).rabbitmq_url == url

    def test_exchange_types(self) -> None:
        for exchange_type in ["topic", "direct", "fanout", "headers"]:
            assert (
                RabbitMQEventBusConfig(exchange_type=exchange_type).exchange_type == exchange_type
            )

    def test_prefetch_count_custom(self) -> None:
        assert RabbitMQEventBusConfig(prefetch_count=100).prefetch_count == 100

    def test_max_retries_custom(self) -> None:
        assert RabbitMQEventBusConfig(max_retries=5).max_retries == 5

    def test_max_retries_zero(self) -> None:
        assert RabbitMQEventBusConfig(max_retries=0).max_retries == 0

    def test_disable_dlq(self) -> None:
        config = RabbitMQEventBusConfig(enable_dlq=False)
        assert config.enable_dlq is False
        assert config.dlq_exchange_name == "events_dlq"
        assert config.dlq_queue_name == "events.default.dlq"

    def test_durable_false(self) -> None:
        assert RabbitMQEventBusConfig(durable=False).durable is False

    def test_auto_delete_true(self) -> None:
        assert RabbitMQEventBusConfig(auto_delete=True).auto_delete is True

    def test_reconnect_delay_custom(self) -> None:
        assert RabbitMQEventBusConfig(reconnect_delay=2.0).reconnect_delay == 2.0

    def test_heartbeat_custom(self) -> None:
        assert RabbitMQEventBusConfig(heartbeat=30).heartbeat == 30

    def test_heartbeat_zero_disables(self) -> None:
        assert RabbitMQEventBusConfig(heartbeat=0).heartbeat == 0

    def test_disable_tracing(self) -> None:
        assert RabbitMQEventBusConfig(enable_tracing=False).enable_tracing is False

    def test_ssl_options(self) -> None:
        ssl_opts = {
            "certfile": "/path/to/cert.pem",
            "keyfile": "/path/to/key.pem",
            "cafile": "/path/to/ca.pem",
        }
        assert RabbitMQEventBusConfig(ssl_options=ssl_opts).ssl_options == ssl_opts

    def test_consumer_name_with_mocked_hostname(self) -> None:
        with mock.patch("socket.gethostname", return_value="test-host"):
            config = RabbitMQEventBusConfig()
            assert config.consumer_name is not None and config.consumer_name.startswith(
                "test-host-"
            )

    def test_consumer_name_format(self) -> None:
        with (
            mock.patch("socket.gethostname", return_value="worker-01"),
            mock.patch("uuid.uuid4") as mock_uuid,
        ):
            mock_uuid.return_value = mock.Mock(
                __str__=mock.Mock(return_value="12345678-90ab-cdef-1234-567890abcdef")
            )
            assert RabbitMQEventBusConfig().consumer_name == "worker-01-12345678"

    def test_full_configuration(self) -> None:
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
        assert config.queue_name == "myapp.events.order-projection"
        assert config.dlq_exchange_name == "myapp.events.dlx"
        assert config.dlq_queue_name == "myapp.events.order-projection.dlq"

    def test_config_is_dataclass(self) -> None:
        assert is_dataclass(RabbitMQEventBusConfig)
        assert is_dataclass(RabbitMQEventBusConfig())

    def test_config_equality(self) -> None:
        c1 = RabbitMQEventBusConfig(
            exchange_name="test", consumer_group="group", consumer_name="fixed-name"
        )
        c2 = RabbitMQEventBusConfig(
            exchange_name="test", consumer_group="group", consumer_name="fixed-name"
        )
        assert c1 == c2

    def test_config_inequality(self) -> None:
        c1 = RabbitMQEventBusConfig(exchange_name="test1", consumer_name="name1")
        c2 = RabbitMQEventBusConfig(exchange_name="test2", consumer_name="name1")
        assert c1 != c2

    def test_special_characters_in_names(self) -> None:
        config = RabbitMQEventBusConfig(
            exchange_name="my-app.events_v2", consumer_group="order-service.projections"
        )
        assert config.queue_name == "my-app.events_v2.order-service.projections"
        assert config.dlq_exchange_name == "my-app.events_v2_dlq"
        assert config.dlq_queue_name == "my-app.events_v2.order-service.projections.dlq"
