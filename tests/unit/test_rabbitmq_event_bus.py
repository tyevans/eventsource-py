"""Unit tests for RabbitMQ event bus configuration and statistics.

Tests for RabbitMQEventBusConfig and RabbitMQEventBusStats dataclasses including
default values, computed properties, auto-generation of consumer names, and
statistics tracking fields.

Also includes tests for the optional dependency pattern (RABBITMQ_AVAILABLE flag
and RabbitMQNotAvailableError exception).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import socket
import ssl
from datetime import UTC, datetime
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.bus.rabbitmq import (
    RABBITMQ_AVAILABLE,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
)
from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry


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
        assert config.max_reconnect_delay == 60.0
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
        """Test custom reconnect delays."""
        config = RabbitMQEventBusConfig(reconnect_delay=2.0, max_reconnect_delay=120.0)

        assert config.reconnect_delay == 2.0
        assert config.max_reconnect_delay == 120.0

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
            max_reconnect_delay=120.0,
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
        assert config.max_reconnect_delay == 120.0
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
        config = RabbitMQEventBusConfig(reconnect_delay=-1.0, max_reconnect_delay=-1.0)

        assert config.reconnect_delay == -1.0
        assert config.max_reconnect_delay == -1.0


class TestRabbitMQEventBusStats:
    """Tests for RabbitMQEventBusStats dataclass."""

    def test_default_values(self) -> None:
        """Test that all fields default to zero."""
        stats = RabbitMQEventBusStats()

        assert stats.events_published == 0
        assert stats.events_consumed == 0
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.publish_confirms == 0
        assert stats.publish_returns == 0

    def test_field_modification(self) -> None:
        """Test that fields can be modified after creation."""
        stats = RabbitMQEventBusStats()

        stats.events_published = 100
        stats.events_consumed = 95
        stats.events_processed_success = 90
        stats.events_processed_failed = 5
        stats.messages_sent_to_dlq = 2
        stats.handler_errors = 7
        stats.reconnections = 3
        stats.publish_confirms = 98
        stats.publish_returns = 2

        assert stats.events_published == 100
        assert stats.events_consumed == 95
        assert stats.events_processed_success == 90
        assert stats.events_processed_failed == 5
        assert stats.messages_sent_to_dlq == 2
        assert stats.handler_errors == 7
        assert stats.reconnections == 3
        assert stats.publish_confirms == 98
        assert stats.publish_returns == 2

    def test_dataclass_equality(self) -> None:
        """Test dataclass equality comparison."""
        stats1 = RabbitMQEventBusStats(events_published=10)
        stats2 = RabbitMQEventBusStats(events_published=10)

        assert stats1 == stats2

    def test_dataclass_inequality(self) -> None:
        """Test dataclass inequality comparison."""
        stats1 = RabbitMQEventBusStats(events_published=10)
        stats2 = RabbitMQEventBusStats(events_published=20)

        assert stats1 != stats2

    def test_partial_initialization(self) -> None:
        """Test creating stats with only some fields specified."""
        stats = RabbitMQEventBusStats(
            events_published=50,
            events_consumed=45,
        )

        assert stats.events_published == 50
        assert stats.events_consumed == 45
        # Other fields should be default
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.publish_confirms == 0
        assert stats.publish_returns == 0

    def test_full_initialization(self) -> None:
        """Test creating stats with all fields specified."""
        stats = RabbitMQEventBusStats(
            events_published=1000,
            events_consumed=950,
            events_processed_success=900,
            events_processed_failed=50,
            messages_sent_to_dlq=10,
            handler_errors=55,
            reconnections=5,
            publish_confirms=980,
            publish_returns=20,
        )

        assert stats.events_published == 1000
        assert stats.events_consumed == 950
        assert stats.events_processed_success == 900
        assert stats.events_processed_failed == 50
        assert stats.messages_sent_to_dlq == 10
        assert stats.handler_errors == 55
        assert stats.reconnections == 5
        assert stats.publish_confirms == 980
        assert stats.publish_returns == 20

    def test_is_dataclass(self) -> None:
        """Test that stats is a proper dataclass."""
        from dataclasses import is_dataclass

        assert is_dataclass(RabbitMQEventBusStats)
        assert is_dataclass(RabbitMQEventBusStats())

    def test_increment_pattern(self) -> None:
        """Test that fields can be incremented (common usage pattern)."""
        stats = RabbitMQEventBusStats()

        # Simulate incrementing during operation
        stats.events_published += 1
        stats.events_published += 1
        stats.events_consumed += 1
        stats.events_processed_success += 1

        assert stats.events_published == 2
        assert stats.events_consumed == 1
        assert stats.events_processed_success == 1

    def test_rabbitmq_specific_fields(self) -> None:
        """Test RabbitMQ-specific fields that differ from Redis."""
        stats = RabbitMQEventBusStats()

        # RabbitMQ-specific: publish_confirms and publish_returns
        stats.publish_confirms = 100
        stats.publish_returns = 5

        assert stats.publish_confirms == 100
        assert stats.publish_returns == 5

        # Verify these fields exist (RabbitMQ-specific)
        assert hasattr(stats, "publish_confirms")
        assert hasattr(stats, "publish_returns")

    def test_common_fields_with_redis(self) -> None:
        """Test that common fields match those in RedisEventBusStats."""
        stats = RabbitMQEventBusStats()

        # These fields should exist in both Redis and RabbitMQ stats
        common_fields = [
            "events_published",
            "events_consumed",
            "events_processed_success",
            "events_processed_failed",
            "messages_sent_to_dlq",
            "handler_errors",
            "reconnections",
        ]

        for field in common_fields:
            assert hasattr(stats, field), f"Missing common field: {field}"

    def test_large_values(self) -> None:
        """Test that stats can handle large values."""
        stats = RabbitMQEventBusStats()

        large_value = 10**12  # 1 trillion
        stats.events_published = large_value
        stats.events_consumed = large_value

        assert stats.events_published == large_value
        assert stats.events_consumed == large_value

    def test_repr(self) -> None:
        """Test string representation of stats."""
        stats = RabbitMQEventBusStats(events_published=10, events_consumed=8)

        repr_str = repr(stats)
        assert "RabbitMQEventBusStats" in repr_str
        assert "events_published=10" in repr_str
        assert "events_consumed=8" in repr_str


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


# ============================================================================
# RabbitMQEventBus Connection Management Tests (P1-005)
# ============================================================================

# Note: Tests use mocks and don't require actual RabbitMQ, so we don't skip.
# RABBITMQ_AVAILABLE needs to be True for the RabbitMQEventBus class to be importable.
# If aio-pika is not installed, the RabbitMQEventBus class won't exist and tests
# will fail at import time, which is the expected behavior.


class TestRabbitMQEventBusInit:
    """Tests for RabbitMQEventBus initialization."""

    def test_init_with_default_config(self) -> None:
        """Test initialization with default configuration."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus.config is not None
        assert bus.config.rabbitmq_url == "amqp://guest:guest@localhost:5672/"
        assert bus.is_connected is False
        assert bus.is_consuming is False
        assert bus.stats.events_published == 0

    def test_init_with_custom_config(self) -> None:
        """Test initialization with custom configuration."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Mock registry
        mock_registry = MagicMock()
        bus = RabbitMQEventBus(event_registry=mock_registry)

        assert bus._event_registry is mock_registry

    def test_init_raises_error_when_aio_pika_not_available(self) -> None:
        """Test that initialization raises error when aio-pika not available."""
        # We need to patch at the module level before the class checks
        import eventsource.bus.rabbitmq as rabbitmq_module
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus.stats.events_published == 0
        assert bus.stats.events_consumed == 0
        assert bus.stats.events_processed_success == 0
        assert bus.stats.events_processed_failed == 0

    def test_init_initializes_connection_state(self) -> None:
        """Test that initialization sets connection state to disconnected."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus._connection is None
        assert bus._channel is None
        assert bus._connected is False
        assert bus._consuming is False


class TestRabbitMQEventBusConnection:
    """Tests for RabbitMQEventBus connect and disconnect methods."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_creates_connection(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect creates a robust connection."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_aio_pika.connect_robust.assert_called_once_with(
            config.rabbitmq_url,
            heartbeat=config.heartbeat,
            reconnect_interval=config.reconnect_delay,
        )
        assert bus.is_connected is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_creates_channel(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect creates a channel."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_connection.channel.assert_called_once()
        assert bus._channel is mock_channel

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_sets_prefetch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect sets prefetch count on channel."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_channel.set_qos.assert_called_once_with(prefetch_count=config.prefetch_count)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_when_already_connected_logs_warning(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that calling connect when already connected logs warning."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Reset mock to track second call
        mock_aio_pika.connect_robust.reset_mock()

        # Connect again
        await bus.connect()

        # Should not call connect_robust again
        mock_aio_pika.connect_robust.assert_not_called()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_handles_connection_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect handles connection errors properly."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_aio_pika.connect_robust = AsyncMock(side_effect=Exception("Connection refused"))

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(Exception, match="Connection refused"):
            await bus.connect()

        assert bus.is_connected is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_closes_connection(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that disconnect closes the connection."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.disconnect()

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert bus.is_connected is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_clears_references(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that disconnect clears all references."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.disconnect()

        assert bus._connection is None
        assert bus._channel is None
        assert bus._exchange is None
        assert bus._dlq_exchange is None
        assert bus._consumer_queue is None
        assert bus._dlq_queue is None

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_when_not_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that disconnect works when not connected."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        # Should not raise
        await bus.disconnect()

        assert bus.is_connected is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that disconnect cancels consumer task if running."""
        import asyncio

        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Simulate a running consumer task
        async def mock_consumer() -> None:
            await asyncio.sleep(100)

        bus._consumer_task = asyncio.create_task(mock_consumer())

        await bus.disconnect()

        assert bus._consumer_task is None


class TestRabbitMQEventBusContextManager:
    """Tests for RabbitMQEventBus async context manager."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_connects_on_enter(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that async context manager connects on entry."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)

        async with bus as entered_bus:
            assert entered_bus is bus
            assert bus.is_connected is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_disconnects_on_exit(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that async context manager disconnects on exit."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)

        async with bus:
            pass

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert bus.is_connected is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_disconnects_on_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that async context manager disconnects even when exception occurs."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(ValueError, match="Test exception"):
            async with bus:
                raise ValueError("Test exception")

        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()
        assert bus.is_connected is False


class TestRabbitMQEventBusProperties:
    """Tests for RabbitMQEventBus properties."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    def test_config_property(self, config: RabbitMQEventBusConfig) -> None:
        """Test config property returns configuration."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        assert bus.config is config

    def test_is_connected_property_when_disconnected(self, config: RabbitMQEventBusConfig) -> None:
        """Test is_connected returns False when disconnected."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        assert bus.is_connected is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_is_connected_property_when_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test is_connected returns True when connected."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        assert bus.is_connected is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_is_connected_returns_false_when_connection_closed(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test is_connected returns False when connection is closed."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Simulate connection closed externally
        mock_connection.is_closed = True

        assert bus.is_connected is False

    def test_is_consuming_property_when_not_consuming(self, config: RabbitMQEventBusConfig) -> None:
        """Test is_consuming returns False when not consuming."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        assert bus.is_consuming is False

    def test_stats_property(self, config: RabbitMQEventBusConfig) -> None:
        """Test stats property returns statistics."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        stats = bus.stats
        assert isinstance(stats, RabbitMQEventBusStats)
        assert stats.events_published == 0


class TestRabbitMQEventBusUrlSanitization:
    """Tests for URL sanitization in RabbitMQEventBus."""

    def test_sanitize_url_removes_password(self) -> None:
        """Test that _sanitize_url removes password from URL."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        sanitized = bus._sanitize_url("amqp://user:password@host:5672/")

        assert "password" not in sanitized
        assert "***" in sanitized
        assert sanitized == "amqp://***:***@host:5672/"

    def test_sanitize_url_removes_complex_password(self) -> None:
        """Test sanitization with complex password containing special chars."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        sanitized = bus._sanitize_url("amqp://admin:p@ssw0rd!#$@host:5672/")

        assert "p@ssw0rd" not in sanitized
        assert "***" in sanitized

    def test_sanitize_url_handles_no_credentials(self) -> None:
        """Test sanitization when no credentials in URL."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        url = "amqp://host:5672/"
        sanitized = bus._sanitize_url(url)

        # URL should remain unchanged
        assert sanitized == url

    def test_sanitize_url_handles_amqps(self) -> None:
        """Test sanitization works with amqps:// URLs."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        sanitized = bus._sanitize_url("amqps://user:secret@host:5671/")

        assert "secret" not in sanitized
        assert sanitized == "amqps://***:***@host:5671/"


# ============================================================================
# Structured Logging Tests (P2-008)
# ============================================================================


class TestRabbitMQStructuredLogging:
    """Tests for structured logging in RabbitMQEventBus (P2-008)."""

    def test_sanitize_url_in_connect_log(self) -> None:
        """Test that connect logs use sanitized URL."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig

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
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        # Password with @ symbol
        url = "amqp://user:p@ss@host:5672/"
        sanitized = bus._sanitize_url(url)

        # Should sanitize without exposing the password
        assert "p@ss" not in sanitized
        assert "***" in sanitized

    def test_sanitize_url_with_special_chars(self) -> None:
        """Test sanitization handles special characters."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        url = "amqp://admin:P@ssw0rd!#$%^&*()@rabbitmq.example.com:5672/"
        sanitized = bus._sanitize_url(url)

        # Password should be completely hidden
        assert "P@ssw0rd" not in sanitized
        assert "***" in sanitized
        assert "rabbitmq.example.com:5672" in sanitized

    def test_sanitize_url_preserves_vhost(self) -> None:
        """Test sanitization preserves virtual host."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        url = "amqp://user:password@localhost:5672/myvhost"
        sanitized = bus._sanitize_url(url)

        assert "password" not in sanitized
        assert "myvhost" in sanitized
        assert sanitized.endswith("/myvhost")

    def test_sanitize_url_with_no_path(self) -> None:
        """Test sanitization handles URL with no path."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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


# ============================================================================
# RabbitMQEventBus Subscription Management Tests (P1-009)
# ============================================================================


class TestRabbitMQSubscriptionManagement:
    """Tests for subscription management in RabbitMQEventBus (P1-009)."""

    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        """Create a bus instance for testing."""
        return RabbitMQEventBus()

    def test_subscribe_registers_handler(self, bus: RabbitMQEventBus) -> None:
        """Test that subscribe registers a handler for an event type."""
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_subscribe_multiple_handlers(self, bus: RabbitMQEventBus) -> None:
        """Test subscribing multiple handlers to the same event type."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)
        assert bus.get_subscriber_count(DomainEvent) == 2

    def test_subscribe_same_handler_twice(self, bus: RabbitMQEventBus) -> None:
        """Test that subscribing the same handler twice adds it twice."""
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        bus.subscribe(DomainEvent, handler)
        # Same handler can be added multiple times
        assert bus.get_subscriber_count(DomainEvent) == 2

    def test_subscribe_to_different_event_types(self, bus: RabbitMQEventBus) -> None:
        """Test subscribing handlers to different event types."""

        # Create a test event class
        class TestEvent(DomainEvent):
            pass

        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(TestEvent, handler2)
        assert bus.get_subscriber_count(DomainEvent) == 1
        assert bus.get_subscriber_count(TestEvent) == 1
        assert bus.get_subscriber_count() == 2

    def test_unsubscribe_removes_handler(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe removes a handler."""
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        result = bus.unsubscribe(DomainEvent, handler)
        assert result is True
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_unsubscribe_returns_false_if_not_found(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe returns False if handler not found."""
        handler = MagicMock()
        result = bus.unsubscribe(DomainEvent, handler)
        assert result is False

    def test_unsubscribe_only_removes_correct_handler(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe only removes the specified handler."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)
        bus.unsubscribe(DomainEvent, handler1)
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_unsubscribe_removes_only_first_occurrence(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe removes only the first occurrence of a handler."""
        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)
        bus.subscribe(DomainEvent, handler)
        assert bus.get_subscriber_count(DomainEvent) == 2
        bus.unsubscribe(DomainEvent, handler)
        # Only one instance should be removed
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_subscribe_all(self, bus: RabbitMQEventBus) -> None:
        """Test subscribe_all subscribes to all declared event types."""

        class TestEvent1(DomainEvent):
            pass

        class TestEvent2(DomainEvent):
            pass

        class TestSubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [TestEvent1, TestEvent2]

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = TestSubscriber()
        bus.subscribe_all(subscriber)
        assert bus.get_subscriber_count(TestEvent1) == 1
        assert bus.get_subscriber_count(TestEvent2) == 1

    def test_subscribe_all_with_empty_subscribed_to(self, bus: RabbitMQEventBus) -> None:
        """Test subscribe_all with subscriber that returns empty list."""

        class EmptySubscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return []

            async def handle(self, event: DomainEvent) -> None:
                pass

        subscriber = EmptySubscriber()
        bus.subscribe_all(subscriber)
        assert bus.get_subscriber_count() == 0

    def test_subscribe_to_all_events(self, bus: RabbitMQEventBus) -> None:
        """Test subscribe_to_all_events adds to wildcard handlers."""
        handler = MagicMock()
        bus.subscribe_to_all_events(handler)
        assert bus.get_wildcard_subscriber_count() == 1

    def test_subscribe_to_all_events_multiple(self, bus: RabbitMQEventBus) -> None:
        """Test subscribing multiple wildcard handlers."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe_to_all_events(handler1)
        bus.subscribe_to_all_events(handler2)
        assert bus.get_wildcard_subscriber_count() == 2

    def test_unsubscribe_from_all_events(self, bus: RabbitMQEventBus) -> None:
        """Test unsubscribe_from_all_events removes wildcard handler."""
        handler = MagicMock()
        bus.subscribe_to_all_events(handler)
        result = bus.unsubscribe_from_all_events(handler)
        assert result is True
        assert bus.get_wildcard_subscriber_count() == 0

    def test_unsubscribe_from_all_events_returns_false_if_not_found(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test unsubscribe_from_all_events returns False if not found."""
        handler = MagicMock()
        result = bus.unsubscribe_from_all_events(handler)
        assert result is False

    def test_unsubscribe_from_all_events_only_removes_correct_handler(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test unsubscribe_from_all_events only removes specified handler."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe_to_all_events(handler1)
        bus.subscribe_to_all_events(handler2)
        bus.unsubscribe_from_all_events(handler1)
        assert bus.get_wildcard_subscriber_count() == 1

    def test_clear_subscribers(self, bus: RabbitMQEventBus) -> None:
        """Test clear_subscribers removes all subscriptions."""
        handler1 = MagicMock()
        handler2 = MagicMock()
        bus.subscribe(DomainEvent, handler1)
        bus.subscribe_to_all_events(handler2)
        bus.clear_subscribers()
        assert bus.get_subscriber_count() == 0
        assert bus.get_wildcard_subscriber_count() == 0

    def test_get_subscriber_count_no_event_type(self, bus: RabbitMQEventBus) -> None:
        """Test get_subscriber_count returns total when no event_type provided."""

        class TestEvent1(DomainEvent):
            pass

        class TestEvent2(DomainEvent):
            pass

        bus.subscribe(TestEvent1, MagicMock())
        bus.subscribe(TestEvent1, MagicMock())
        bus.subscribe(TestEvent2, MagicMock())
        assert bus.get_subscriber_count() == 3

    def test_get_subscriber_count_with_event_type(self, bus: RabbitMQEventBus) -> None:
        """Test get_subscriber_count returns count for specific event type."""

        class TestEvent(DomainEvent):
            pass

        bus.subscribe(TestEvent, MagicMock())
        bus.subscribe(DomainEvent, MagicMock())
        assert bus.get_subscriber_count(TestEvent) == 1
        assert bus.get_subscriber_count(DomainEvent) == 1

    def test_get_subscriber_count_empty(self, bus: RabbitMQEventBus) -> None:
        """Test get_subscriber_count returns 0 when no subscribers."""
        assert bus.get_subscriber_count() == 0
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_get_wildcard_subscriber_count_empty(self, bus: RabbitMQEventBus) -> None:
        """Test get_wildcard_subscriber_count returns 0 when no wildcard handlers."""
        assert bus.get_wildcard_subscriber_count() == 0


class TestRabbitMQHandlerNormalization:
    """Tests for handler normalization via HandlerAdapter (P1-009).

    Note: The normalization logic was extracted to HandlerAdapter as part of
    SOLID refactoring. These tests now validate the adapter directly and
    verify that RabbitMQ bus correctly uses it.
    """

    def test_normalize_async_function(self) -> None:
        """Test normalizing an async function handler via HandlerAdapter."""
        import asyncio

        from eventsource.handlers.adapter import HandlerAdapter

        async def async_handler(event: DomainEvent) -> None:
            pass

        adapter = HandlerAdapter(async_handler)
        assert adapter._original is async_handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_sync_function(self) -> None:
        """Test normalizing a sync function handler via HandlerAdapter."""
        import asyncio

        from eventsource.handlers.adapter import HandlerAdapter

        def sync_handler(event: DomainEvent) -> None:
            pass

        adapter = HandlerAdapter(sync_handler)
        assert adapter._original is sync_handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_async_handler_class(self) -> None:
        """Test normalizing a handler class with async handle method."""
        import asyncio

        from eventsource.handlers.adapter import HandlerAdapter

        class AsyncHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = AsyncHandler()
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_sync_handler_class(self) -> None:
        """Test normalizing a handler class with sync handle method."""
        import asyncio

        from eventsource.handlers.adapter import HandlerAdapter

        class SyncHandler:
            def handle(self, event: DomainEvent) -> None:
                pass

        handler = SyncHandler()
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_normalize_invalid_handler_raises_type_error(self) -> None:
        """Test that normalizing an invalid handler raises TypeError."""
        from eventsource.handlers.adapter import HandlerAdapter

        with pytest.raises(TypeError, match="Handler must have a handle\\(\\) method"):
            HandlerAdapter("not a handler")  # type: ignore[arg-type]

    def test_normalize_invalid_handler_with_number(self) -> None:
        """Test that normalizing a number raises TypeError."""
        from eventsource.handlers.adapter import HandlerAdapter

        with pytest.raises(TypeError, match="Handler must have a handle\\(\\) method"):
            HandlerAdapter(42)  # type: ignore[arg-type]

    def test_normalize_lambda(self) -> None:
        """Test normalizing a lambda handler."""
        import asyncio

        from eventsource.handlers.adapter import HandlerAdapter

        handler = lambda event: None  # noqa: E731
        adapter = HandlerAdapter(handler)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    @pytest.mark.asyncio
    async def test_normalized_sync_handler_invocation(self) -> None:
        """Test that normalized sync handler can be invoked."""
        from eventsource.handlers.adapter import HandlerAdapter

        results: list[DomainEvent] = []

        def sync_handler(event: DomainEvent) -> None:
            results.append(event)

        adapter = HandlerAdapter(sync_handler)

        # Create a test event - need a concrete subclass with event_type
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Invoke the adapter
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_normalized_async_handler_invocation(self) -> None:
        """Test that normalized async handler can be invoked."""
        from eventsource.handlers.adapter import HandlerAdapter

        results: list[DomainEvent] = []

        async def async_handler(event: DomainEvent) -> None:
            results.append(event)

        adapter = HandlerAdapter(async_handler)

        # Create a test event - need a concrete subclass with event_type
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Invoke the adapter
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_normalized_handler_class_invocation(self) -> None:
        """Test that normalized handler class can be invoked."""
        from eventsource.handlers.adapter import HandlerAdapter

        results: list[DomainEvent] = []

        class TestHandler:
            async def handle(self, event: DomainEvent) -> None:
                results.append(event)

        handler = TestHandler()
        adapter = HandlerAdapter(handler)

        # Create a test event - need a concrete subclass with event_type
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Invoke the adapter
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event


class TestRabbitMQGetHandlerName:
    """Tests for HandlerAdapter.name property (formerly _get_handler_name method) (P1-009).

    Note: The handler naming logic was extracted to HandlerAdapter as part of
    SOLID refactoring. These tests now validate the adapter directly.
    """

    def test_get_handler_name_from_class(self) -> None:
        """Test getting name from a class instance."""
        from eventsource.handlers.adapter import HandlerAdapter

        class MyHandler:
            async def handle(self, event: DomainEvent) -> None:
                pass

        handler = MyHandler()
        adapter = HandlerAdapter(handler)
        assert adapter.name == "MyHandler"

    def test_get_handler_name_from_function(self) -> None:
        """Test getting name from a function."""
        from eventsource.handlers.adapter import HandlerAdapter

        def my_handler_func(event: DomainEvent) -> None:
            pass

        adapter = HandlerAdapter(my_handler_func)
        assert adapter.name == "my_handler_func"

    def test_get_handler_name_from_lambda(self) -> None:
        """Test getting name from a lambda."""
        from eventsource.handlers.adapter import HandlerAdapter

        handler = lambda event: None  # noqa: E731
        adapter = HandlerAdapter(handler)
        assert adapter.name == "<lambda>"

    def test_get_handler_name_from_mock(self) -> None:
        """Test getting name from a MagicMock."""
        from eventsource.handlers.adapter import HandlerAdapter

        handler = MagicMock()
        adapter = HandlerAdapter(handler)
        assert "MagicMock" in adapter.name


class TestRabbitMQSubscriptionIntegration:
    """Integration tests for subscription management with HandlerAdapter (P1-009)."""

    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        """Create a bus instance for testing."""
        return RabbitMQEventBus()

    def test_subscribe_creates_valid_adapter(self, bus: RabbitMQEventBus) -> None:
        """Test that subscribe creates a valid HandlerAdapter."""
        import asyncio

        from eventsource.handlers.adapter import HandlerAdapter

        handler = MagicMock()
        bus.subscribe(DomainEvent, handler)

        # Access the internal subscribers dict to verify adapter structure
        adapters = bus._subscribers[DomainEvent]
        assert len(adapters) == 1

        adapter = adapters[0]
        assert isinstance(adapter, HandlerAdapter)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_wildcard_subscribe_creates_valid_adapter(self, bus: RabbitMQEventBus) -> None:
        """Test that subscribe_to_all_events creates a valid HandlerAdapter."""
        import asyncio

        from eventsource.handlers.adapter import HandlerAdapter

        handler = MagicMock()
        bus.subscribe_to_all_events(handler)

        # Access the internal all_event_handlers list
        adapters = bus._all_event_handlers
        assert len(adapters) == 1

        adapter = adapters[0]
        assert isinstance(adapter, HandlerAdapter)
        assert adapter._original is handler
        assert asyncio.iscoroutinefunction(adapter.handle)

    def test_unsubscribe_uses_identity_comparison(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe uses identity comparison via HandlerAdapter equality."""
        # Two different mock objects that might be equal
        handler1 = MagicMock()
        handler2 = MagicMock()
        # MagicMock objects are not equal by default

        bus.subscribe(DomainEvent, handler1)
        bus.subscribe(DomainEvent, handler2)

        # Unsubscribing handler1 should only remove handler1
        result = bus.unsubscribe(DomainEvent, handler1)
        assert result is True
        assert bus.get_subscriber_count(DomainEvent) == 1

        # Handler2 should still be subscribed
        result = bus.unsubscribe(DomainEvent, handler2)
        assert result is True
        assert bus.get_subscriber_count(DomainEvent) == 0

    def test_wildcard_unsubscribe_uses_identity_comparison(self, bus: RabbitMQEventBus) -> None:
        """Test that unsubscribe_from_all_events uses identity comparison."""
        handler1 = MagicMock()
        handler2 = MagicMock()

        bus.subscribe_to_all_events(handler1)
        bus.subscribe_to_all_events(handler2)

        result = bus.unsubscribe_from_all_events(handler1)
        assert result is True
        assert bus.get_wildcard_subscriber_count() == 1

        result = bus.unsubscribe_from_all_events(handler2)
        assert result is True
        assert bus.get_wildcard_subscriber_count() == 0


# ============================================================================
# RabbitMQEventBus Exchange and Queue Declaration Tests (P1-006)
# ============================================================================


class TestRabbitMQExchangeDeclaration:
    """Tests for exchange declaration in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            exchange_type="topic",
            consumer_group="test-group",
            enable_dlq=False,  # Disable DLQ for basic exchange tests
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_topic(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect declares a topic exchange."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        mock_channel.declare_exchange.assert_called_once()
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["name"] == config.exchange_name
        assert call_kwargs["durable"] == config.durable
        assert call_kwargs["auto_delete"] == config.auto_delete

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_direct(self, mock_aio_pika: MagicMock) -> None:
        """Test that connect declares a direct exchange when configured."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="direct-events",
            exchange_type="direct",
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

        # Need to import ExchangeType from the mock's perspective
        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.DIRECT

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_fanout(self, mock_aio_pika: MagicMock) -> None:
        """Test that connect declares a fanout exchange when configured."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="fanout-events",
            exchange_type="fanout",
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

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.FANOUT

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_headers(self, mock_aio_pika: MagicMock) -> None:
        """Test that connect declares a headers exchange when configured."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="headers-events",
            exchange_type="headers",
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

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.HEADERS

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_unknown_type_defaults_to_topic(
        self, mock_aio_pika: MagicMock
    ) -> None:
        """Test that unknown exchange type defaults to topic."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="events",
            exchange_type="unknown_type",
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

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.TOPIC

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_case_insensitive(self, mock_aio_pika: MagicMock) -> None:
        """Test that exchange type is case-insensitive."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="events",
            exchange_type="TOPIC",  # uppercase
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

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.TOPIC

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_durable_setting(self, mock_aio_pika: MagicMock) -> None:
        """Test that exchange respects durable setting."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="events",
            durable=False,
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

        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["durable"] is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_exchange_auto_delete_setting(self, mock_aio_pika: MagicMock) -> None:
        """Test that exchange respects auto_delete setting."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="events",
            auto_delete=True,
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

        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["auto_delete"] is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_exchange_reference_stored(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that exchange reference is stored after declaration."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        assert bus._exchange is mock_exchange


class TestRabbitMQQueueDeclaration:
    """Tests for queue declaration in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_queue_basic(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect declares a queue."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        mock_channel.declare_queue.assert_called_once()
        call_kwargs = mock_channel.declare_queue.call_args.kwargs
        assert call_kwargs["name"] == config.queue_name
        assert call_kwargs["durable"] == config.durable
        assert call_kwargs["auto_delete"] == config.auto_delete

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_queue_with_dlq_arguments(self, mock_aio_pika: MagicMock) -> None:
        """Test that queue includes DLQ arguments when enabled."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
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

        # Find the call for the main queue (not DLQ)
        main_queue_call = None
        for call in mock_channel.declare_queue.call_args_list:
            if call.kwargs["name"] == config.queue_name:
                main_queue_call = call
                break

        assert main_queue_call is not None
        call_kwargs = main_queue_call.kwargs
        assert "arguments" in call_kwargs
        assert call_kwargs["arguments"]["x-dead-letter-exchange"] == config.dlq_exchange_name
        assert call_kwargs["arguments"]["x-dead-letter-routing-key"] == config.queue_name

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_declare_queue_without_dlq_arguments(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that queue has no DLQ arguments when DLQ disabled."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        call_kwargs = mock_channel.declare_queue.call_args.kwargs
        assert call_kwargs["arguments"] is None

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_reference_stored(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that queue reference is stored after declaration."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        assert bus._consumer_queue is mock_queue


class TestRabbitMQQueueBinding:
    """Tests for queue binding in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_queue_to_exchange(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that queue is bound to exchange."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        mock_queue.bind.assert_called_once()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_queue_with_wildcard_routing_key(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that queue is bound with '#' routing key."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "#"

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_queue_to_correct_exchange(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that queue is bound to the correct exchange."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["exchange"] is mock_exchange


class TestRabbitMQDLQDeclaration:
    """Tests for dead letter queue declaration in RabbitMQEventBus."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_exchange_declared_when_enabled(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ exchange is declared when DLQ is enabled."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_dlq_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Return different mocks for different exchanges
        mock_channel.declare_exchange = AsyncMock(side_effect=[mock_dlq_exchange, mock_exchange])
        mock_channel.declare_queue = AsyncMock(side_effect=[mock_dlq_queue, mock_queue])

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        # Should have 2 exchange declarations (DLQ + main)
        assert mock_channel.declare_exchange.call_count == 2

        # First call should be DLQ exchange
        first_call_kwargs = mock_channel.declare_exchange.call_args_list[0].kwargs
        assert first_call_kwargs["name"] == config_with_dlq.dlq_exchange_name

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_exchange_is_direct_type(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ exchange is of direct type."""
        from aio_pika import ExchangeType

        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        # First call should be DLQ exchange with DIRECT type
        first_call_kwargs = mock_channel.declare_exchange.call_args_list[0].kwargs
        assert first_call_kwargs["type"] == ExchangeType.DIRECT

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_when_enabled(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue is declared when DLQ is enabled."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        # Should have 2 queue declarations (DLQ + main)
        assert mock_channel.declare_queue.call_count == 2

        # First call should be DLQ queue
        first_call_kwargs = mock_channel.declare_queue.call_args_list[0].kwargs
        assert first_call_kwargs["name"] == config_with_dlq.dlq_queue_name

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_bound_to_dlq_exchange(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue is bound to DLQ exchange."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_dlq_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(side_effect=[mock_dlq_exchange, mock_exchange])
        mock_channel.declare_queue = AsyncMock(side_effect=[mock_dlq_queue, mock_queue])

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        # DLQ queue should be bound to DLQ exchange
        mock_dlq_queue.bind.assert_called_once()
        dlq_bind_kwargs = mock_dlq_queue.bind.call_args.kwargs
        assert dlq_bind_kwargs["exchange"] is mock_dlq_exchange
        assert dlq_bind_kwargs["routing_key"] == config_with_dlq.queue_name

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_references_stored(
        self, mock_aio_pika: MagicMock, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ references are stored after declaration."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_dlq_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(side_effect=[mock_dlq_exchange, mock_exchange])
        mock_channel.declare_queue = AsyncMock(side_effect=[mock_dlq_queue, mock_queue])

        bus = RabbitMQEventBus(config=config_with_dlq)
        await bus.connect()

        assert bus._dlq_exchange is mock_dlq_exchange
        assert bus._dlq_queue is mock_dlq_queue

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_no_dlq_when_disabled(self, mock_aio_pika: MagicMock) -> None:
        """Test that no DLQ is declared when DLQ is disabled."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
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

        # Should have only 1 exchange declaration (main only)
        assert mock_channel.declare_exchange.call_count == 1

        # Should have only 1 queue declaration (main only)
        assert mock_channel.declare_queue.call_count == 1

        # DLQ references should be None
        assert bus._dlq_exchange is None
        assert bus._dlq_queue is None


class TestRabbitMQConnectionWithTopology:
    """Tests for the complete connection flow with topology setup."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_sets_up_complete_topology(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect sets up complete topology."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        # Verify all components are set up
        assert bus._connection is not None
        assert bus._channel is not None
        assert bus._exchange is not None
        assert bus._consumer_queue is not None
        assert bus._dlq_exchange is not None
        assert bus._dlq_queue is not None
        assert bus.is_connected is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_cleanup_on_failure(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect cleans up on failure."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        # Fail during exchange declaration
        mock_channel.declare_exchange = AsyncMock(
            side_effect=Exception("Exchange declaration failed")
        )

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(Exception, match="Exchange declaration failed"):
            await bus.connect()

        # Verify cleanup was attempted
        assert bus.is_connected is False
        assert bus._exchange is None
        assert bus._consumer_queue is None

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_order_dlq_before_main(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ is declared before main queue."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        call_order: list[str] = []

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        async def track_exchange(name: str, **kwargs: object) -> AsyncMock:
            call_order.append(f"exchange:{name}")
            return mock_exchange

        async def track_queue(name: str, **kwargs: object) -> AsyncMock:
            call_order.append(f"queue:{name}")
            return mock_queue

        mock_channel.declare_exchange = AsyncMock(side_effect=track_exchange)
        mock_channel.declare_queue = AsyncMock(side_effect=track_queue)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # DLQ exchange should be declared before main exchange
        dlq_exchange_idx = call_order.index(f"exchange:{config.dlq_exchange_name}")
        main_exchange_idx = call_order.index(f"exchange:{config.exchange_name}")
        assert dlq_exchange_idx < main_exchange_idx

        # DLQ queue should be declared before main queue
        dlq_queue_idx = call_order.index(f"queue:{config.dlq_queue_name}")
        main_queue_idx = call_order.index(f"queue:{config.queue_name}")
        assert dlq_queue_idx < main_queue_idx


class TestRabbitMQDeclarationErrors:
    """Tests for error handling during declaration."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @pytest.mark.asyncio
    async def test_declare_exchange_without_channel_raises(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _declare_exchange raises if channel not initialized."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Channel not initialized"):
            await bus._declare_exchange()

    @pytest.mark.asyncio
    async def test_declare_queue_without_channel_raises(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _declare_queue raises if channel not initialized."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Channel not initialized"):
            await bus._declare_queue()

    @pytest.mark.asyncio
    async def test_bind_queue_without_queue_raises(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _bind_queue raises if queue not initialized."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Queue or exchange not initialized"):
            await bus._bind_queue()

    @pytest.mark.asyncio
    async def test_declare_dlq_without_channel_raises(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _declare_dlq raises if channel not initialized."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Channel not initialized"):
            await bus._declare_dlq()


# ============================================================================
# RabbitMQEventBus Event Serialization Tests (P1-007)
# ============================================================================


class SerializationTestEvent(DomainEvent):
    """Test event for serialization tests."""

    event_type: str = "SerializationTestEvent"
    aggregate_type: str = "TestAggregate"
    data: str = "test"


class SerializationOrderCreatedEvent(DomainEvent):
    """Order created event for serialization tests."""

    event_type: str = "SerializationOrderCreated"
    aggregate_type: str = "Order"
    order_number: str
    customer_id: str


class TestRabbitMQRoutingKey:
    """Tests for routing key generation in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        return RabbitMQEventBus(config=config)

    def test_routing_key_format(self, bus: RabbitMQEventBus) -> None:
        """Test routing key follows {aggregate_type}.{event_type} format."""
        event = SerializationTestEvent(aggregate_id=uuid4())

        routing_key = bus._get_routing_key(event)

        assert routing_key == "TestAggregate.SerializationTestEvent"

    def test_routing_key_with_order_event(self, bus: RabbitMQEventBus) -> None:
        """Test routing key for order event."""
        event = SerializationOrderCreatedEvent(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id="customer-123",
        )

        routing_key = bus._get_routing_key(event)

        assert routing_key == "Order.SerializationOrderCreated"

    def test_routing_key_with_custom_aggregate_type(self, bus: RabbitMQEventBus) -> None:
        """Test routing key with custom aggregate type."""

        class CustomEvent(DomainEvent):
            event_type: str = "CustomEvent"
            aggregate_type: str = "CustomAggregate"

        event = CustomEvent(aggregate_id=uuid4())

        routing_key = bus._get_routing_key(event)

        assert routing_key == "CustomAggregate.CustomEvent"


class TestRabbitMQEventSerialization:
    """Tests for event serialization in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def sample_event(self) -> SerializationTestEvent:
        """Create a sample event for testing."""
        return SerializationTestEvent(
            aggregate_id=uuid4(),
            data="test-data",
        )

    def test_serialize_event_returns_tuple(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that _serialize_event returns tuple of (bytes, dict)."""
        result = bus._serialize_event(sample_event)

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], bytes)
        assert isinstance(result[1], dict)

    def test_serialize_event_body_is_valid_json(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that serialized body is valid JSON."""
        body, _ = bus._serialize_event(sample_event)

        # Should not raise
        parsed = json.loads(body.decode("utf-8"))
        assert isinstance(parsed, dict)

    def test_serialize_event_body_contains_event_data(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that serialized body contains event data."""
        body, _ = bus._serialize_event(sample_event)

        parsed = json.loads(body.decode("utf-8"))
        assert parsed["data"] == "test-data"
        assert parsed["aggregate_type"] == "TestAggregate"
        assert parsed["event_type"] == "SerializationTestEvent"

    def test_serialize_event_headers_contain_event_type(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain event_type."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["event_type"] == "SerializationTestEvent"

    def test_serialize_event_headers_contain_aggregate_type(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain aggregate_type."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["aggregate_type"] == "TestAggregate"

    def test_serialize_event_headers_contain_aggregate_id(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain aggregate_id as string."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["aggregate_id"] == str(sample_event.aggregate_id)

    def test_serialize_event_headers_contain_aggregate_version(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain aggregate_version."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["aggregate_version"] == sample_event.aggregate_version

    def test_serialize_event_headers_contain_retry_count(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers contain x-retry-count initialized to 0."""
        _, headers = bus._serialize_event(sample_event)

        assert headers["x-retry-count"] == 0

    def test_serialize_event_headers_contain_correlation_id(self, bus: RabbitMQEventBus) -> None:
        """Test that headers contain correlation_id when present."""
        correlation_id = uuid4()
        event = SerializationTestEvent(
            aggregate_id=uuid4(),
            correlation_id=correlation_id,
        )

        _, headers = bus._serialize_event(event)

        assert headers["correlation_id"] == str(correlation_id)

    def test_serialize_event_headers_contain_causation_id(self, bus: RabbitMQEventBus) -> None:
        """Test that headers contain causation_id when present."""
        causation_id = uuid4()
        event = SerializationTestEvent(
            aggregate_id=uuid4(),
            causation_id=causation_id,
        )

        _, headers = bus._serialize_event(event)

        assert headers["causation_id"] == str(causation_id)

    def test_serialize_event_headers_contain_tenant_id(self, bus: RabbitMQEventBus) -> None:
        """Test that headers contain tenant_id when present."""
        tenant_id = uuid4()
        event = SerializationTestEvent(
            aggregate_id=uuid4(),
            tenant_id=tenant_id,
        )

        _, headers = bus._serialize_event(event)

        assert headers["tenant_id"] == str(tenant_id)

    def test_serialize_event_headers_omit_optional_when_none(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that optional headers are omitted when None."""
        # sample_event has causation_id=None and tenant_id=None
        _, headers = bus._serialize_event(sample_event)

        assert "causation_id" not in headers
        assert "tenant_id" not in headers
        # But correlation_id should always be present (has default_factory)
        assert "correlation_id" in headers


class TestRabbitMQCreateMessage:
    """Tests for message creation in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def sample_event(self) -> SerializationTestEvent:
        """Create a sample event for testing."""
        return SerializationTestEvent(
            aggregate_id=uuid4(),
            data="test-data",
        )

    def test_create_message_returns_message(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that _create_message returns an aio_pika.Message."""
        from aio_pika import Message

        message = bus._create_message(sample_event)

        assert isinstance(message, Message)

    def test_create_message_content_type(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that message has correct content_type."""
        message = bus._create_message(sample_event)

        assert message.content_type == "application/json"

    def test_create_message_content_encoding(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that message has correct content_encoding."""
        message = bus._create_message(sample_event)

        assert message.content_encoding == "utf-8"

    def test_create_message_delivery_mode_persistent(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that message uses persistent delivery mode."""
        from aio_pika import DeliveryMode

        message = bus._create_message(sample_event)

        assert message.delivery_mode == DeliveryMode.PERSISTENT

    def test_create_message_message_id(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that message_id is set to event_id."""
        message = bus._create_message(sample_event)

        assert message.message_id == str(sample_event.event_id)

    def test_create_message_timestamp(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that timestamp is set to occurred_at."""
        message = bus._create_message(sample_event)

        assert message.timestamp == sample_event.occurred_at

    def test_create_message_headers(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that headers are set correctly."""
        message = bus._create_message(sample_event)

        assert message.headers is not None
        assert message.headers["event_type"] == "SerializationTestEvent"
        assert message.headers["aggregate_type"] == "TestAggregate"
        assert message.headers["x-retry-count"] == 0

    def test_create_message_body_is_json(
        self, bus: RabbitMQEventBus, sample_event: SerializationTestEvent
    ) -> None:
        """Test that body is valid JSON."""
        message = bus._create_message(sample_event)

        parsed = json.loads(message.body.decode("utf-8"))
        assert parsed["data"] == "test-data"


class TestRabbitMQEventDeserialization:
    """Tests for event deserialization in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create event registry with test events."""
        registry = EventRegistry()
        registry.register(SerializationTestEvent)
        registry.register(SerializationOrderCreatedEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        """Create bus instance with event registry."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        return RabbitMQEventBus(config=config, event_registry=registry)

    @pytest.fixture
    def mock_message(self) -> MagicMock:
        """Create a mock incoming message."""
        message = MagicMock()
        message.message_id = "test-message-id"
        return message

    def test_deserialize_event_success(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test successful event deserialization."""
        aggregate_id = uuid4()
        event_data = {
            "event_id": str(uuid4()),
            "event_type": "SerializationTestEvent",
            "aggregate_type": "TestAggregate",
            "aggregate_id": str(aggregate_id),
            "aggregate_version": 1,
            "data": "test-data",
            "occurred_at": datetime.now(UTC).isoformat(),
            "correlation_id": str(uuid4()),
        }
        mock_message.headers = {"event_type": "SerializationTestEvent"}
        mock_message.body = json.dumps(event_data).encode("utf-8")

        event = bus._deserialize_event(mock_message)

        assert event is not None
        assert isinstance(event, SerializationTestEvent)
        assert event.data == "test-data"
        assert event.aggregate_id == aggregate_id

    def test_deserialize_event_missing_header_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that missing event_type header returns None."""
        mock_message.headers = {}  # No event_type
        mock_message.body = b"{}"

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_event_none_headers_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that None headers returns None."""
        mock_message.headers = None
        mock_message.body = b"{}"

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_event_unknown_type_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that unknown event type returns None."""
        mock_message.headers = {"event_type": "UnknownEventType"}
        mock_message.body = b"{}"

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_event_invalid_json_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that invalid JSON returns None."""
        mock_message.headers = {"event_type": "SerializationTestEvent"}
        mock_message.body = b"not valid json"

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_event_validation_error_returns_none(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that Pydantic validation error returns None."""
        mock_message.headers = {"event_type": "SerializationTestEvent"}
        # Missing required fields
        mock_message.body = json.dumps({"data": "test"}).encode("utf-8")

        event = bus._deserialize_event(mock_message)

        assert event is None

    def test_deserialize_order_created_event(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test deserializing a more complex event."""
        aggregate_id = uuid4()
        event_data = {
            "event_id": str(uuid4()),
            "event_type": "SerializationOrderCreated",
            "aggregate_type": "Order",
            "aggregate_id": str(aggregate_id),
            "aggregate_version": 1,
            "order_number": "ORD-001",
            "customer_id": "customer-123",
            "occurred_at": datetime.now(UTC).isoformat(),
            "correlation_id": str(uuid4()),
        }
        mock_message.headers = {"event_type": "SerializationOrderCreated"}
        mock_message.body = json.dumps(event_data).encode("utf-8")

        event = bus._deserialize_event(mock_message)

        assert event is not None
        assert isinstance(event, SerializationOrderCreatedEvent)
        assert event.order_number == "ORD-001"
        assert event.customer_id == "customer-123"


class TestRabbitMQGetEventClass:
    """Tests for event class lookup in RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    def test_get_event_class_from_explicit_registry(self, config: RabbitMQEventBusConfig) -> None:
        """Test that event class is found in explicit registry."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        registry = EventRegistry()
        registry.register(SerializationTestEvent)
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        event_class = bus._get_event_class("SerializationTestEvent")

        assert event_class is SerializationTestEvent

    def test_get_event_class_not_in_registry_returns_none(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that unknown event type returns None."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        registry = EventRegistry()
        bus = RabbitMQEventBus(config=config, event_registry=registry)

        event_class = bus._get_event_class("UnknownEvent")

        assert event_class is None

    def test_get_event_class_falls_back_to_default_registry(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that None registry falls back to default registry."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus
        from eventsource.events.registry import default_registry

        # Register in default registry
        class DefaultRegistryEvent(DomainEvent):
            event_type: str = "DefaultRegistryEvent"
            aggregate_type: str = "Test"

        default_registry.register(DefaultRegistryEvent)

        try:
            # Bus with no explicit registry
            bus = RabbitMQEventBus(config=config)

            event_class = bus._get_event_class("DefaultRegistryEvent")

            assert event_class is DefaultRegistryEvent
        finally:
            # Clean up default registry
            default_registry.unregister("DefaultRegistryEvent")


class TestRabbitMQSerializationRoundTrip:
    """Tests for serialization and deserialization round-trip."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create event registry with test events."""
        registry = EventRegistry()
        registry.register(SerializationTestEvent)
        registry.register(SerializationOrderCreatedEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        """Create bus instance with event registry."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        return RabbitMQEventBus(config=config, event_registry=registry)

    def test_round_trip_preserves_event_data(self, bus: RabbitMQEventBus) -> None:
        """Test that serialize -> deserialize preserves event data."""
        original_event = SerializationTestEvent(
            aggregate_id=uuid4(),
            data="round-trip-test",
        )

        # Serialize
        message = bus._create_message(original_event)

        # Create mock incoming message from serialized data
        mock_message = MagicMock()
        mock_message.message_id = message.message_id
        mock_message.headers = message.headers
        mock_message.body = message.body

        # Deserialize
        deserialized_event = bus._deserialize_event(mock_message)

        assert deserialized_event is not None
        assert deserialized_event.event_id == original_event.event_id
        assert deserialized_event.event_type == original_event.event_type
        assert deserialized_event.aggregate_id == original_event.aggregate_id
        assert deserialized_event.data == original_event.data

    def test_round_trip_preserves_all_fields(self, bus: RabbitMQEventBus) -> None:
        """Test that all fields are preserved in round-trip."""
        tenant_id = uuid4()
        causation_id = uuid4()
        correlation_id = uuid4()

        original_event = SerializationOrderCreatedEvent(
            aggregate_id=uuid4(),
            order_number="ORD-999",
            customer_id="customer-abc",
            tenant_id=tenant_id,
            causation_id=causation_id,
            correlation_id=correlation_id,
            aggregate_version=5,
        )

        # Serialize and deserialize
        message = bus._create_message(original_event)
        mock_message = MagicMock()
        mock_message.message_id = message.message_id
        mock_message.headers = message.headers
        mock_message.body = message.body

        deserialized = bus._deserialize_event(mock_message)

        assert deserialized is not None
        assert deserialized.order_number == "ORD-999"
        assert deserialized.customer_id == "customer-abc"
        assert deserialized.tenant_id == tenant_id
        assert deserialized.causation_id == causation_id
        assert deserialized.correlation_id == correlation_id
        assert deserialized.aggregate_version == 5


# ============================================================================
# RabbitMQEventBus Publish Tests (P1-008)
# ============================================================================


class PublishTestEvent(DomainEvent):
    """Test event for publish tests."""

    event_type: str = "PublishTestEvent"
    aggregate_type: str = "TestAggregate"
    data: str = "test"


class TestRabbitMQPublish:
    """Tests for RabbitMQEventBus.publish() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.fixture
    def sample_events(self) -> list[PublishTestEvent]:
        """Create sample events for testing."""
        return [
            PublishTestEvent(aggregate_id=uuid4(), data="event-1"),
            PublishTestEvent(aggregate_id=uuid4(), data="event-2"),
        ]

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_empty_list_returns_immediately(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publishing empty list does nothing and doesn't raise."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        # Should not raise or attempt connection
        await bus.publish([])

        # Should not have tried to connect
        mock_aio_pika.connect_robust.assert_not_called()
        assert bus.is_connected is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_auto_connects_if_not_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish auto-connects if not already connected."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)

        assert bus.is_connected is False

        await bus.publish([event])

        assert bus.is_connected is True
        mock_aio_pika.connect_robust.assert_called_once()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_calls_exchange_publish(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        sample_events: list[PublishTestEvent],
    ) -> None:
        """Test that publish calls exchange.publish for each event."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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
        await bus.publish(sample_events)

        # Should have called publish for each event
        assert mock_exchange.publish.call_count == len(sample_events)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_increments_events_published_stat(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        sample_events: list[PublishTestEvent],
    ) -> None:
        """Test that events_published stat is incremented for each event."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        assert bus.stats.events_published == 0

        await bus.publish(sample_events)

        assert bus.stats.events_published == len(sample_events)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_increments_publish_confirms_stat(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        sample_events: list[PublishTestEvent],
    ) -> None:
        """Test that publish_confirms stat is incremented when background=False."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        assert bus.stats.publish_confirms == 0

        await bus.publish(sample_events, background=False)

        assert bus.stats.publish_confirms == len(sample_events)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_background_does_not_increment_confirms(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        sample_events: list[PublishTestEvent],
    ) -> None:
        """Test that publish_confirms stat is NOT incremented when background=True."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        await bus.publish(sample_events, background=True)

        # events_published should still be incremented
        assert bus.stats.events_published == len(sample_events)
        # But publish_confirms should NOT be incremented
        assert bus.stats.publish_confirms == 0

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_uses_correct_routing_key(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish uses the correct routing key for each event."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event])

        # Verify routing key was passed correctly
        call_kwargs = mock_exchange.publish.call_args.kwargs
        assert call_kwargs["routing_key"] == "TestAggregate.PublishTestEvent"

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_creates_message_using_create_message(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish creates AMQP message using _create_message."""
        from aio_pika import Message

        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event])

        # Verify a Message was passed to publish
        call_args = mock_exchange.publish.call_args
        published_message = call_args.args[0] if call_args.args else call_args.kwargs.get("message")
        assert isinstance(published_message, Message)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_event(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publishing a single event."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event])

        assert mock_exchange.publish.call_count == 1
        assert bus.stats.events_published == 1

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_raises_runtime_error_when_exchange_none(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish raises RuntimeError if exchange is None after connect."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)

        # Manually set connected but leave exchange as None
        bus._connected = True
        bus._exchange = None

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await bus.publish([event])

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_reraises_exception_on_failure(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish re-raises exceptions from exchange.publish."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_exchange.publish = AsyncMock(side_effect=Exception("Connection lost"))
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with pytest.raises(Exception, match="Connection lost"):
            await bus.publish([event])

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_does_not_increment_stats_on_failure(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that stats are not incremented when publish fails."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_exchange.publish = AsyncMock(side_effect=Exception("Publish failed"))
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        initial_published = bus.stats.events_published
        initial_confirms = bus.stats.publish_confirms

        with pytest.raises(Exception, match="Publish failed"):
            await bus.publish([event])

        # Stats should not have changed
        assert bus.stats.events_published == initial_published
        assert bus.stats.publish_confirms == initial_confirms


class TestRabbitMQPublishSingle:
    """Tests for RabbitMQEventBus._publish_single() internal method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_raises_when_exchange_none(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _publish_single raises RuntimeError if exchange is None."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        bus._exchange = None

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await bus._publish_single(event)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_wait_for_confirm_true(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test _publish_single with wait_for_confirm=True increments confirms."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus._publish_single(event, wait_for_confirm=True)

        assert bus.stats.events_published == 1
        assert bus.stats.publish_confirms == 1

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_wait_for_confirm_false(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test _publish_single with wait_for_confirm=False does not increment confirms."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        event = PublishTestEvent(aggregate_id=uuid4())
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus._publish_single(event, wait_for_confirm=False)

        assert bus.stats.events_published == 1
        assert bus.stats.publish_confirms == 0


class TestRabbitMQPublishMultipleEvents:
    """Tests for publishing multiple events with RabbitMQEventBus."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_multiple_events_sequential(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that multiple events are published sequentially."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish(events)

        assert mock_exchange.publish.call_count == 5
        assert bus.stats.events_published == 5
        assert bus.stats.publish_confirms == 5

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_handles_partial_failure_in_batch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that batch publishing handles partial failures.

        With batch publishing (concurrent), all events are attempted even if
        some fail. The first error encountered is re-raised after all events
        are processed.
        """
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        # First call succeeds, second fails, third succeeds
        mock_exchange.publish = AsyncMock(side_effect=[None, Exception("Connection lost"), None])
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(3)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with pytest.raises(Exception, match="Connection lost"):
            await bus.publish(events)

        # With batch publishing, all events are attempted concurrently
        # So 2 events succeed (first and third), 1 fails (second)
        assert bus.stats.events_published == 2
        assert mock_exchange.publish.call_count == 3  # All 3 attempted
        assert bus.stats.batch_publishes == 1
        assert bus.stats.batch_partial_failures == 1


class TestRabbitMQPublishWithDifferentEventTypes:
    """Tests for publishing different event types."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_mixed_event_types(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publishing events of different types uses correct routing keys."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        # Create different event types
        event1 = PublishTestEvent(aggregate_id=uuid4())
        event2 = SerializationOrderCreatedEvent(
            aggregate_id=uuid4(),
            order_number="ORD-001",
            customer_id="customer-123",
        )

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event1, event2])

        # Verify different routing keys were used
        calls = mock_exchange.publish.call_args_list
        assert len(calls) == 2
        assert calls[0].kwargs["routing_key"] == "TestAggregate.PublishTestEvent"
        assert calls[1].kwargs["routing_key"] == "Order.SerializationOrderCreated"


# ============================================================================
# RabbitMQEventBus Consumer Loop Tests (P1-010)
# ============================================================================


class ConsumerTestEvent(DomainEvent):
    """Test event for consumer loop testing."""

    event_type: str = "ConsumerTestEvent"
    aggregate_type: str = "ConsumerTest"
    data: str = "test"


class TestRabbitMQStartConsuming:
    """Tests for start_consuming() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            consumer_name="test-consumer",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_auto_connects(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming auto-connects if not connected."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        # Create a mock async iterator that yields no messages
        async def empty_iterator():
            if False:  # Never yields
                yield

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=empty_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        # Start consuming - should auto-connect
        task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(0.1)
        await bus.stop_consuming()
        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        mock_aio_pika.connect_robust.assert_called_once()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_raises_if_queue_not_initialized(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming raises RuntimeError if queue not initialized."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)
        bus._connected = True  # Pretend connected but no queue

        with pytest.raises(RuntimeError, match="Consumer queue not initialized"):
            await bus.start_consuming()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_sets_consuming_flag(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming sets is_consuming to True."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        consuming_flag_during_loop = []

        async def async_iterator():
            consuming_flag_during_loop.append(bus.is_consuming)
            if False:
                yield

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=async_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(0.1)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

        # During the loop, consuming should have been True
        if consuming_flag_during_loop:
            assert consuming_flag_during_loop[0] is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_handles_cancellation(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming handles CancelledError gracefully."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        async def infinite_iterator():
            while True:
                await asyncio.sleep(0.1)
                yield MagicMock()

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=infinite_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        task = asyncio.create_task(bus.start_consuming())
        await asyncio.sleep(0.05)
        task.cancel()

        # The consumer loop catches CancelledError and handles it gracefully
        # It should not propagate the exception
        with contextlib.suppress(asyncio.CancelledError):
            await task

        # Consuming flag should be reset in any case
        assert bus.is_consuming is False


class TestRabbitMQStopConsuming:
    """Tests for stop_consuming() method."""

    def test_stop_consuming_sets_flag(self) -> None:
        """Test that stop_consuming sets consuming flag to False."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        bus._consuming = True

        asyncio.get_event_loop().run_until_complete(bus.stop_consuming())

        assert bus._consuming is False

    @pytest.mark.asyncio
    async def test_stop_consuming_when_already_stopped(self) -> None:
        """Test that stop_consuming works even when not consuming."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()
        assert bus._consuming is False

        await bus.stop_consuming()

        assert bus._consuming is False


class TestRabbitMQStartConsumingInBackground:
    """Tests for start_consuming_in_background() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            consumer_name="bg-consumer",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_in_background_returns_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming_in_background returns an asyncio Task."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        async def empty_iterator():
            if False:
                yield

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=empty_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        task = bus.start_consuming_in_background()

        assert isinstance(task, asyncio.Task)
        assert task.get_name() == f"rabbitmq-consumer-{config.consumer_name}"

        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_start_consuming_in_background_raises_if_already_running(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that start_consuming_in_background raises if already running."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        async def empty_iterator():
            if False:
                yield

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=empty_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        task1 = bus.start_consuming_in_background()
        await asyncio.sleep(0.05)

        with pytest.raises(RuntimeError, match="Consumer already running"):
            bus.start_consuming_in_background()

        task1.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task1


class TestRabbitMQProcessMessage:
    """Tests for _process_message() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create event registry with test event."""
        registry = EventRegistry()
        registry.register(ConsumerTestEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        bus = RabbitMQEventBus(config=config, event_registry=registry)
        bus._connected = True
        return bus

    @pytest.mark.asyncio
    async def test_process_message_acks_on_success(self, bus: RabbitMQEventBus) -> None:
        """Test that _process_message acknowledges message on successful processing."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        event_json = event.model_dump_json()

        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent"}
        mock_message.body = event_json.encode("utf-8")
        mock_message.message_id = "test-msg-id"
        mock_message.routing_key = "ConsumerTest.ConsumerTestEvent"

        received_events = []

        async def test_handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(ConsumerTestEvent, test_handler)

        await bus._process_message(mock_message)

        mock_message.ack.assert_called_once()
        mock_message.reject.assert_not_called()
        assert len(received_events) == 1
        assert bus.stats.events_consumed == 1
        assert bus.stats.events_processed_success == 1

    @pytest.mark.asyncio
    async def test_process_message_acks_unknown_event_type(self, bus: RabbitMQEventBus) -> None:
        """Test that _process_message acknowledges unknown event types to prevent blocking."""
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "UnknownEvent"}
        mock_message.body = b'{"aggregate_id": "test-id"}'
        mock_message.message_id = "test-msg-id"
        mock_message.routing_key = "Unknown.UnknownEvent"

        await bus._process_message(mock_message)

        mock_message.ack.assert_called_once()
        mock_message.reject.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_message_retries_on_handler_error(self, bus: RabbitMQEventBus) -> None:
        """Test that _process_message republishes message for retry when handler raises."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        event_json = event.model_dump_json()

        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent", "x-retry-count": 0}
        mock_message.body = event_json.encode("utf-8")
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-msg-id"
        mock_message.routing_key = "ConsumerTest.ConsumerTestEvent"

        # Set up exchange for retry republish
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler failed")

        bus.subscribe(ConsumerTestEvent, failing_handler)

        await bus._process_message(mock_message)

        # Message should be acked (original removed from queue) and republished
        mock_message.ack.assert_called_once()
        mock_exchange.publish.assert_called_once()
        assert bus.stats.events_processed_failed == 1

        # Verify retry count was incremented
        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]
        assert new_message.headers["x-retry-count"] == 1

    @pytest.mark.asyncio
    async def test_process_message_extracts_event_type_from_headers(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that _process_message extracts event type from headers."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        event_json = event.model_dump_json()

        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent", "other": "header"}
        mock_message.body = event_json.encode("utf-8")
        mock_message.message_id = "test-msg-id"
        mock_message.routing_key = "test"

        received_events = []

        async def test_handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(ConsumerTestEvent, test_handler)

        await bus._process_message(mock_message)

        assert len(received_events) == 1
        assert received_events[0].event_type == "ConsumerTestEvent"

    @pytest.mark.asyncio
    async def test_process_message_handles_missing_headers(self, bus: RabbitMQEventBus) -> None:
        """Test that _process_message handles missing headers gracefully."""
        mock_message = AsyncMock()
        mock_message.headers = None
        mock_message.body = b'{"aggregate_id": "test-id"}'
        mock_message.message_id = "test-msg-id"
        mock_message.routing_key = "test"

        await bus._process_message(mock_message)

        # Should ack because event couldn't be deserialized (unknown type)
        mock_message.ack.assert_called_once()


class TestRabbitMQDispatchEvent:
    """Tests for _dispatch_event() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        bus = RabbitMQEventBus(config=config)
        bus._connected = True
        return bus

    @pytest.mark.asyncio
    async def test_dispatch_to_specific_handlers(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event calls specific event handlers."""
        received = []

        async def handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe(ConsumerTestEvent, handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        assert len(received) == 1
        assert received[0] is event

    @pytest.mark.asyncio
    async def test_dispatch_to_wildcard_handlers(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event calls wildcard handlers."""
        received = []

        async def wildcard_handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe_to_all_events(wildcard_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        assert len(received) == 1
        assert received[0] is event

    @pytest.mark.asyncio
    async def test_dispatch_to_both_specific_and_wildcard(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event calls both specific and wildcard handlers."""
        specific_received = []
        wildcard_received = []

        async def specific_handler(event: DomainEvent) -> None:
            specific_received.append(event)

        async def wildcard_handler(event: DomainEvent) -> None:
            wildcard_received.append(event)

        bus.subscribe(ConsumerTestEvent, specific_handler)
        bus.subscribe_to_all_events(wildcard_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        assert len(specific_received) == 1
        assert len(wildcard_received) == 1
        assert specific_received[0] is event
        assert wildcard_received[0] is event

    @pytest.mark.asyncio
    async def test_dispatch_with_no_handlers(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event handles no handlers gracefully."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        # Should not raise
        await bus._dispatch_event(event, mock_message)

    @pytest.mark.asyncio
    async def test_dispatch_raises_on_handler_error(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event re-raises handler exceptions."""

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Handler error")

        bus.subscribe(ConsumerTestEvent, failing_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        with pytest.raises(ValueError, match="Handler error"):
            await bus._dispatch_event(event, mock_message)

        assert bus.stats.handler_errors == 1

    @pytest.mark.asyncio
    async def test_dispatch_stops_on_first_handler_error(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event stops processing on first handler error."""
        call_order = []

        async def first_handler(event: DomainEvent) -> None:
            call_order.append("first")
            raise ValueError("First handler error")

        async def second_handler(event: DomainEvent) -> None:
            call_order.append("second")

        bus.subscribe(ConsumerTestEvent, first_handler)
        bus.subscribe(ConsumerTestEvent, second_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        with pytest.raises(ValueError):
            await bus._dispatch_event(event, mock_message)

        assert call_order == ["first"]

    @pytest.mark.asyncio
    async def test_dispatch_calls_handlers_sequentially(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event calls handlers in order."""
        call_order = []

        async def handler1(event: DomainEvent) -> None:
            call_order.append("handler1")

        async def handler2(event: DomainEvent) -> None:
            call_order.append("handler2")

        async def wildcard_handler(event: DomainEvent) -> None:
            call_order.append("wildcard")

        bus.subscribe(ConsumerTestEvent, handler1)
        bus.subscribe(ConsumerTestEvent, handler2)
        bus.subscribe_to_all_events(wildcard_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        # Specific handlers first, then wildcard
        assert call_order == ["handler1", "handler2", "wildcard"]

    @pytest.mark.asyncio
    async def test_dispatch_with_sync_handler(self, bus: RabbitMQEventBus) -> None:
        """Test that _dispatch_event handles synchronous handlers."""
        received = []

        def sync_handler(event: DomainEvent) -> None:
            received.append(event)

        bus.subscribe(ConsumerTestEvent, sync_handler)

        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        await bus._dispatch_event(event, mock_message)

        assert len(received) == 1


class TestRabbitMQConsumerStatistics:
    """Tests for consumer loop statistics tracking."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create event registry with test event."""
        registry = EventRegistry()
        registry.register(ConsumerTestEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        bus = RabbitMQEventBus(config=config, event_registry=registry)
        bus._connected = True
        return bus

    @pytest.mark.asyncio
    async def test_events_consumed_incremented_on_success(self, bus: RabbitMQEventBus) -> None:
        """Test that events_consumed is incremented on successful processing."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent"}
        mock_message.body = event.model_dump_json().encode("utf-8")
        mock_message.message_id = "test-id"
        mock_message.routing_key = "test"

        async def handler(e: DomainEvent) -> None:
            pass

        bus.subscribe(ConsumerTestEvent, handler)

        initial_consumed = bus.stats.events_consumed

        await bus._process_message(mock_message)

        assert bus.stats.events_consumed == initial_consumed + 1

    @pytest.mark.asyncio
    async def test_events_processed_success_incremented(self, bus: RabbitMQEventBus) -> None:
        """Test that events_processed_success is incremented."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent"}
        mock_message.body = event.model_dump_json().encode("utf-8")
        mock_message.message_id = "test-id"
        mock_message.routing_key = "test"

        async def handler(e: DomainEvent) -> None:
            pass

        bus.subscribe(ConsumerTestEvent, handler)

        initial_success = bus.stats.events_processed_success

        await bus._process_message(mock_message)

        assert bus.stats.events_processed_success == initial_success + 1

    @pytest.mark.asyncio
    async def test_events_processed_failed_incremented_on_error(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that events_processed_failed is incremented on handler error."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "ConsumerTestEvent", "x-retry-count": 0}
        mock_message.body = event.model_dump_json().encode("utf-8")
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "test"

        # Set up exchange for retry republish
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Error")

        bus.subscribe(ConsumerTestEvent, failing_handler)

        initial_failed = bus.stats.events_processed_failed

        await bus._process_message(mock_message)

        assert bus.stats.events_processed_failed == initial_failed + 1

    @pytest.mark.asyncio
    async def test_handler_errors_incremented(self, bus: RabbitMQEventBus) -> None:
        """Test that handler_errors is incremented on handler exception."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test")
        mock_message = MagicMock()
        mock_message.message_id = "test-id"

        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Error")

        bus.subscribe(ConsumerTestEvent, failing_handler)

        initial_errors = bus.stats.handler_errors

        with pytest.raises(ValueError):
            await bus._dispatch_event(event, mock_message)

        assert bus.stats.handler_errors == initial_errors + 1

    @pytest.mark.asyncio
    async def test_stats_not_incremented_for_unknown_event(self, bus: RabbitMQEventBus) -> None:
        """Test that stats are not incremented for unknown event types."""
        mock_message = AsyncMock()
        mock_message.headers = {"event_type": "UnknownEvent"}
        mock_message.body = b'{"aggregate_id": "test-id"}'
        mock_message.message_id = "test-id"
        mock_message.routing_key = "test"

        initial_consumed = bus.stats.events_consumed
        initial_success = bus.stats.events_processed_success

        await bus._process_message(mock_message)

        # Stats should not change for unknown events
        assert bus.stats.events_consumed == initial_consumed
        assert bus.stats.events_processed_success == initial_success


# ============================================================================
# Additional Coverage Tests
# ============================================================================


class TestRabbitMQHandlerNormalizationEdgeCases:
    """Additional edge case tests for handler normalization via HandlerAdapter.

    Note: The normalization logic was extracted to HandlerAdapter as part of
    SOLID refactoring. These tests now validate the adapter directly.
    """

    @pytest.mark.asyncio
    async def test_normalize_sync_method_returning_coroutine(self) -> None:
        """Test normalizing a sync method that returns a coroutine.

        This tests the `if asyncio.iscoroutine(result)` branch in the sync wrapper.
        """
        from eventsource.handlers.adapter import HandlerAdapter

        results: list[DomainEvent] = []

        class HybridHandler:
            def handle(self, event: DomainEvent) -> object:
                # This sync method returns a coroutine (unusual but possible)
                async def coro() -> None:
                    results.append(event)

                return coro()

        handler = HybridHandler()
        adapter = HandlerAdapter(handler)

        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event

    @pytest.mark.asyncio
    async def test_normalize_sync_callable_returning_coroutine(self) -> None:
        """Test normalizing a sync callable that returns a coroutine.

        This tests the `if asyncio.iscoroutine(result)` branch in the callable wrapper.
        """
        from eventsource.handlers.adapter import HandlerAdapter

        results: list[DomainEvent] = []

        def hybrid_handler(event: DomainEvent) -> object:
            async def coro() -> None:
                results.append(event)

            return coro()

        adapter = HandlerAdapter(hybrid_handler)

        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())
        await adapter.handle(event)

        assert len(results) == 1
        assert results[0] is event


class TestRabbitMQGetHandlerNameEdgeCases:
    """Additional edge case tests for HandlerAdapter.name property (formerly _get_handler_name).

    Note: The handler naming logic was extracted to HandlerAdapter as part of
    SOLID refactoring. These tests now validate the adapter directly.
    """

    def test_get_handler_name_from_object_without_class(self) -> None:
        """Test getting name from an object without __class__ attribute.

        Note: In practice, everything in Python has __class__, but we test
        the fallback path by using an object that has __name__ instead.
        """
        from eventsource.handlers.adapter import HandlerAdapter

        # Create an object that pretends to not have __class__ by overriding
        # hasattr check - this is tricky since everything has __class__
        # Instead, test the __name__ branch with a module-like object
        class MockWithName:
            __name__ = "MockHandlerName"

            def handle(self, event: DomainEvent) -> None:
                pass

        handler = MockWithName()
        adapter = HandlerAdapter(handler)
        # Since MockWithName has __class__, it will use __class__.__name__
        assert adapter.name == "MockWithName"

    def test_get_handler_name_repr_fallback(self) -> None:
        """Test that HandlerAdapter.name works with mocks.

        Note: This is hard to test since almost everything has __class__,
        but we can verify the behavior with mocks.
        """
        from eventsource.handlers.adapter import HandlerAdapter

        # Test with a MagicMock to verify it handles objects properly
        handler = MagicMock(spec=[])  # Empty spec removes __class__ from mock
        handler.__class__ = MagicMock
        handler.__class__.__name__ = "TestMock"

        adapter = HandlerAdapter(handler)
        # Should still work since mock has __class__
        assert "Mock" in adapter.name


class TestRabbitMQConsumerLoopErrorHandling:
    """Tests for consumer loop error handling and edge cases."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            consumer_name="error-test-consumer",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_consumer_loop_handles_general_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that consumer loop handles and re-raises general exceptions."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        async def failing_iterator() -> None:
            raise RuntimeError("Queue iteration failed")

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(side_effect=RuntimeError("Queue iteration failed"))
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Queue iteration failed"):
            await bus.start_consuming()

        # Consuming flag should be reset after exception
        assert bus.is_consuming is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_consumer_stops_when_consuming_flag_cleared(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that consumer loop stops when is_consuming is set to False."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()

        message_count = 0

        async def controlled_iterator():
            nonlocal message_count
            while message_count < 5:
                message_count += 1
                mock_msg = AsyncMock()
                mock_msg.headers = {"event_type": "UnknownEvent"}
                mock_msg.body = b'{"data": "test"}'
                mock_msg.message_id = f"msg-{message_count}"
                mock_msg.routing_key = "test"
                yield mock_msg
                if message_count >= 2:
                    # Simulate stop being called
                    bus._consuming = False

        mock_queue_iter = AsyncMock()
        mock_queue_iter.__aenter__ = AsyncMock(return_value=controlled_iterator())
        mock_queue_iter.__aexit__ = AsyncMock(return_value=None)
        mock_queue.iterator = MagicMock(return_value=mock_queue_iter)

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        bus = RabbitMQEventBus(config=config)

        await bus.start_consuming()

        # Should have processed some messages then stopped
        assert message_count >= 2
        assert bus.is_consuming is False


class TestRabbitMQModuleAllExports:
    """Tests to verify __all__ exports in rabbitmq module."""

    def test_module_all_contains_expected_exports(self) -> None:
        """Test that __all__ contains all expected exports."""
        from eventsource.bus import rabbitmq

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
        from eventsource.bus import rabbitmq

        for name in rabbitmq.__all__:
            obj = getattr(rabbitmq, name)
            assert obj is not None, f"{name} is None"


class TestRabbitMQEventBusInterfaceCompliance:
    """Tests to verify RabbitMQEventBus implements EventBus interface."""

    def test_implements_event_bus_interface(self) -> None:
        """Test that RabbitMQEventBus implements EventBus protocol."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert hasattr(bus, "__aenter__")
        assert hasattr(bus, "__aexit__")
        assert asyncio.iscoroutinefunction(bus.__aenter__)
        assert asyncio.iscoroutinefunction(bus.__aexit__)


class TestRabbitMQStatsResetBehavior:
    """Tests for stats behavior and edge cases."""

    def test_stats_are_not_shared_between_instances(self) -> None:
        """Test that each bus instance has its own stats object."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus1 = RabbitMQEventBus()
        bus2 = RabbitMQEventBus()

        bus1.stats.events_published = 100

        assert bus2.stats.events_published == 0
        assert bus1.stats is not bus2.stats


class TestRabbitMQConfigImmutabilityAfterInit:
    """Tests for config behavior after initialization."""

    def test_config_cannot_be_changed_after_init(self) -> None:
        """Test that config property returns the original config."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        original_config = RabbitMQEventBusConfig(
            exchange_name="original",
            consumer_name="test-consumer",
        )
        bus = RabbitMQEventBus(config=original_config)

        # Config should be the same object
        assert bus.config is original_config
        assert bus.config.exchange_name == "original"


class TestRabbitMQConnectionStateTransitions:
    """Tests for connection state transitions."""

    def test_initial_state_is_disconnected(self) -> None:
        """Test that a new bus starts in disconnected state."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus.is_connected is False
        assert bus.is_consuming is False
        assert bus._connection is None
        assert bus._channel is None
        assert bus._exchange is None
        assert bus._consumer_queue is None

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connection_state_after_connect_then_disconnect(
        self, mock_aio_pika: MagicMock
    ) -> None:
        """Test state transitions through connect/disconnect cycle."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        bus = RabbitMQEventBus()

        # Initial state
        assert bus.is_connected is False

        # After connect
        await bus.connect()
        assert bus.is_connected is True
        assert bus._connection is not None
        assert bus._channel is not None

        # After disconnect
        await bus.disconnect()
        assert bus.is_connected is False
        assert bus._connection is None
        assert bus._channel is None
        assert bus._exchange is None


class TestRabbitMQLoggingIntegration:
    """Tests to verify logging is properly set up."""

    def test_logger_is_configured(self) -> None:
        """Test that the bus has a logger configured."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus()

        assert bus._logger is not None
        assert bus._logger.name == "eventsource.bus.rabbitmq"


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


# ============================================================================
# P2-001: Enhanced DLQ Configuration Tests
# ============================================================================


class TestRabbitMQDLQConfigOptions:
    """Tests for enhanced DLQ configuration options (P2-001)."""

    def test_dlq_message_ttl_default_is_none(self) -> None:
        """Test that dlq_message_ttl defaults to None."""
        config = RabbitMQEventBusConfig()
        assert config.dlq_message_ttl is None

    def test_dlq_max_length_default_is_none(self) -> None:
        """Test that dlq_max_length defaults to None."""
        config = RabbitMQEventBusConfig()
        assert config.dlq_max_length is None

    def test_dlq_message_ttl_can_be_set(self) -> None:
        """Test that dlq_message_ttl can be configured."""
        config = RabbitMQEventBusConfig(dlq_message_ttl=3600000)  # 1 hour in ms
        assert config.dlq_message_ttl == 3600000

    def test_dlq_max_length_can_be_set(self) -> None:
        """Test that dlq_max_length can be configured."""
        config = RabbitMQEventBusConfig(dlq_max_length=10000)
        assert config.dlq_max_length == 10000

    def test_dlq_message_ttl_zero_is_valid(self) -> None:
        """Test that dlq_message_ttl of 0 is valid (immediate expiration)."""
        config = RabbitMQEventBusConfig(dlq_message_ttl=0)
        assert config.dlq_message_ttl == 0

    def test_dlq_max_length_one_is_valid(self) -> None:
        """Test that dlq_max_length of 1 is valid (single message)."""
        config = RabbitMQEventBusConfig(dlq_max_length=1)
        assert config.dlq_max_length == 1

    def test_combined_dlq_options(self) -> None:
        """Test that both DLQ options can be set together."""
        config = RabbitMQEventBusConfig(
            dlq_message_ttl=86400000,  # 24 hours
            dlq_max_length=5000,
        )
        assert config.dlq_message_ttl == 86400000
        assert config.dlq_max_length == 5000


class TestRabbitMQDLQQueueDeclarationWithOptions:
    """Tests for DLQ queue declaration with TTL and max_length (P2-001)."""

    @pytest.fixture
    def config_with_dlq_options(self) -> RabbitMQEventBusConfig:
        """Create test config with DLQ options."""
        return RabbitMQEventBusConfig(
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
            dlq_message_ttl=3600000,
            dlq_max_length=1000,
        )

    @pytest.fixture
    def config_without_dlq_options(self) -> RabbitMQEventBusConfig:
        """Create test config without DLQ options."""
        return RabbitMQEventBusConfig(
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_with_ttl(
        self, mock_aio_pika: MagicMock, config_with_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue is declared with x-message-ttl when configured."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        bus = RabbitMQEventBus(config=config_with_dlq_options)
        await bus.connect()

        # Find DLQ queue declaration call (first call)
        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        assert dlq_queue_call.kwargs["name"] == config_with_dlq_options.dlq_queue_name
        assert "arguments" in dlq_queue_call.kwargs
        assert dlq_queue_call.kwargs["arguments"]["x-message-ttl"] == 3600000

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_with_max_length(
        self, mock_aio_pika: MagicMock, config_with_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue is declared with x-max-length when configured."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        bus = RabbitMQEventBus(config=config_with_dlq_options)
        await bus.connect()

        # Find DLQ queue declaration call (first call)
        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        assert dlq_queue_call.kwargs["arguments"]["x-max-length"] == 1000

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dlq_queue_declared_without_arguments_when_options_not_set(
        self, mock_aio_pika: MagicMock, config_without_dlq_options: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ queue has no arguments when options are not set."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        bus = RabbitMQEventBus(config=config_without_dlq_options)
        await bus.connect()

        # Find DLQ queue declaration call (first call)
        dlq_queue_call = mock_channel.declare_queue.call_args_list[0]
        # arguments should be None when no options are set
        assert dlq_queue_call.kwargs.get("arguments") is None


# ============================================================================
# P2-001: DLQ Helper Methods Tests
# ============================================================================


class TestRabbitMQDLQHelperMethods:
    """Tests for DLQ helper methods (P2-001)."""

    @pytest.fixture
    def mock_message_no_death(self) -> MagicMock:
        """Create mock message without x-death headers."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
        }
        return message

    @pytest.fixture
    def mock_message_with_death(self) -> MagicMock:
        """Create mock message with x-death headers (dead-lettered)."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-death": [
                {
                    "count": 1,
                    "reason": "rejected",
                    "queue": "events.default",
                    "exchange": "events",
                    "routing-keys": ["TestAggregate.TestEvent"],
                    "time": 1699999999,
                }
            ],
            "x-first-death-queue": "events.default",
            "x-first-death-reason": "rejected",
            "x-first-death-exchange": "events",
        }
        return message

    @pytest.fixture
    def mock_message_multiple_deaths(self) -> MagicMock:
        """Create mock message with multiple death records."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-death": [
                {
                    "count": 3,
                    "reason": "rejected",
                    "queue": "events.default",
                    "exchange": "events",
                    "routing-keys": ["TestAggregate.TestEvent"],
                },
                {
                    "count": 2,
                    "reason": "expired",
                    "queue": "events.retry",
                    "exchange": "events_retry",
                    "routing-keys": ["events.default"],
                },
            ],
            "x-first-death-queue": "events.default",
            "x-first-death-reason": "rejected",
            "x-first-death-exchange": "events",
        }
        return message

    @pytest.fixture
    def mock_message_empty_headers(self) -> MagicMock:
        """Create mock message with empty headers."""
        message = MagicMock()
        message.headers = {}
        return message

    @pytest.fixture
    def mock_message_none_headers(self) -> MagicMock:
        """Create mock message with None headers."""
        message = MagicMock()
        message.headers = None
        return message

    def test_get_death_count_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_death_count returns 0 when no x-death header."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_no_death)
        assert count == 0

    def test_get_death_count_single_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_death_count returns correct count for single death."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_with_death)
        assert count == 1

    def test_get_death_count_multiple_deaths(self, mock_message_multiple_deaths: MagicMock) -> None:
        """Test get_death_count sums counts from multiple death records."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_multiple_deaths)
        assert count == 5  # 3 + 2

    def test_get_death_count_empty_headers(self, mock_message_empty_headers: MagicMock) -> None:
        """Test get_death_count returns 0 for empty headers."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_empty_headers)
        assert count == 0

    def test_get_death_count_none_headers(self, mock_message_none_headers: MagicMock) -> None:
        """Test get_death_count returns 0 for None headers."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_none_headers)
        assert count == 0

    def test_get_first_death_queue_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_first_death_queue returns None when not dead-lettered."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        queue = RabbitMQEventBus.get_first_death_queue(mock_message_no_death)
        assert queue is None

    def test_get_first_death_queue_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_first_death_queue returns correct queue name."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        queue = RabbitMQEventBus.get_first_death_queue(mock_message_with_death)
        assert queue == "events.default"

    def test_get_first_death_reason_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_first_death_reason returns None when not dead-lettered."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        reason = RabbitMQEventBus.get_first_death_reason(mock_message_no_death)
        assert reason is None

    def test_get_first_death_reason_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_first_death_reason returns correct reason."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        reason = RabbitMQEventBus.get_first_death_reason(mock_message_with_death)
        assert reason == "rejected"

    def test_get_first_death_exchange_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_first_death_exchange returns None when not dead-lettered."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        exchange = RabbitMQEventBus.get_first_death_exchange(mock_message_no_death)
        assert exchange is None

    def test_get_first_death_exchange_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_first_death_exchange returns correct exchange name."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        exchange = RabbitMQEventBus.get_first_death_exchange(mock_message_with_death)
        assert exchange == "events"

    def test_get_original_routing_key_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_original_routing_key returns None when not dead-lettered."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        routing_key = RabbitMQEventBus.get_original_routing_key(mock_message_no_death)
        assert routing_key is None

    def test_get_original_routing_key_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_original_routing_key returns correct routing key."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        routing_key = RabbitMQEventBus.get_original_routing_key(mock_message_with_death)
        assert routing_key == "TestAggregate.TestEvent"

    def test_is_from_dlq_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test is_from_dlq returns False when not dead-lettered."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        result = RabbitMQEventBus.is_from_dlq(mock_message_no_death)
        assert result is False

    def test_is_from_dlq_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test is_from_dlq returns True when dead-lettered."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        result = RabbitMQEventBus.is_from_dlq(mock_message_with_death)
        assert result is True

    def test_is_from_dlq_empty_headers(self, mock_message_empty_headers: MagicMock) -> None:
        """Test is_from_dlq returns False for empty headers."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        result = RabbitMQEventBus.is_from_dlq(mock_message_empty_headers)
        assert result is False

    def test_is_from_dlq_none_headers(self, mock_message_none_headers: MagicMock) -> None:
        """Test is_from_dlq returns False for None headers."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        result = RabbitMQEventBus.is_from_dlq(mock_message_none_headers)
        assert result is False

    def test_is_from_dlq_empty_x_death_list(self) -> None:
        """Test is_from_dlq returns False for empty x-death list."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {"x-death": []}
        result = RabbitMQEventBus.is_from_dlq(message)
        assert result is False

    def test_get_death_info_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_death_info returns complete dict for non-dead-lettered message."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        info = RabbitMQEventBus.get_death_info(mock_message_no_death)

        assert info["is_dead_lettered"] is False
        assert info["death_count"] == 0
        assert info["first_death_queue"] is None
        assert info["first_death_reason"] is None
        assert info["first_death_exchange"] is None
        assert info["original_routing_key"] is None
        assert info["x_death"] is None

    def test_get_death_info_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_death_info returns complete dict for dead-lettered message."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        info = RabbitMQEventBus.get_death_info(mock_message_with_death)

        assert info["is_dead_lettered"] is True
        assert info["death_count"] == 1
        assert info["first_death_queue"] == "events.default"
        assert info["first_death_reason"] == "rejected"
        assert info["first_death_exchange"] == "events"
        assert info["original_routing_key"] == "TestAggregate.TestEvent"
        assert info["x_death"] is not None
        assert len(info["x_death"]) == 1


class TestRabbitMQDLQHelperEdgeCases:
    """Edge case tests for DLQ helper methods."""

    def test_get_death_count_invalid_count_type(self) -> None:
        """Test get_death_count handles non-integer count values."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                {"count": "not-a-number"},
                {"count": 5},
            ]
        }

        # Should only count valid integer values
        count = RabbitMQEventBus.get_death_count(message)
        assert count == 5

    def test_get_death_count_missing_count_field(self) -> None:
        """Test get_death_count handles death records without count field."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                {"reason": "rejected"},  # No count field
                {"count": 3},
            ]
        }

        count = RabbitMQEventBus.get_death_count(message)
        assert count == 3

    def test_get_original_routing_key_empty_routing_keys(self) -> None:
        """Test get_original_routing_key handles empty routing-keys list."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                {"routing-keys": []},
            ]
        }

        routing_key = RabbitMQEventBus.get_original_routing_key(message)
        assert routing_key is None

    def test_get_original_routing_key_no_routing_keys(self) -> None:
        """Test get_original_routing_key handles missing routing-keys field."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                {"count": 1, "reason": "rejected"},
            ]
        }

        routing_key = RabbitMQEventBus.get_original_routing_key(message)
        assert routing_key is None

    def test_x_death_non_list_type(self) -> None:
        """Test helper methods handle non-list x-death header."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {"x-death": "not-a-list"}

        assert RabbitMQEventBus.get_death_count(message) == 0
        assert RabbitMQEventBus.is_from_dlq(message) is False
        assert RabbitMQEventBus.get_original_routing_key(message) is None

    def test_x_death_non_dict_entries(self) -> None:
        """Test get_death_count handles non-dict entries in x-death list."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {
            "x-death": [
                "not-a-dict",
                {"count": 2},
                None,
            ]
        }

        count = RabbitMQEventBus.get_death_count(message)
        assert count == 2


class TestRabbitMQProcessMessageWithDLQTracking:
    """Tests for _process_message with DLQ tracking (P2-001)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(enable_dlq=True)

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance for testing."""
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_normal_message(self) -> MagicMock:
        """Create mock message without death headers."""
        message = AsyncMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
        }
        message.message_id = "test-message-id"
        message.routing_key = "TestAggregate.TestEvent"
        message.body = b'{"event_type": "TestEvent", "aggregate_type": "TestAggregate"}'
        return message

    @pytest.fixture
    def mock_dlq_message(self) -> MagicMock:
        """Create mock message from DLQ."""
        message = AsyncMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-death": [
                {
                    "count": 1,
                    "reason": "rejected",
                    "queue": "events.default",
                    "exchange": "events",
                    "routing-keys": ["TestAggregate.TestEvent"],
                }
            ],
            "x-first-death-queue": "events.default",
            "x-first-death-reason": "rejected",
            "x-first-death-exchange": "events",
        }
        message.message_id = "test-message-id"
        message.routing_key = "events.default"
        message.body = b'{"event_type": "TestEvent", "aggregate_type": "TestAggregate"}'
        return message

    @pytest.mark.asyncio
    async def test_process_message_increments_dlq_stats_on_max_retries(
        self, bus: RabbitMQEventBus, mock_normal_message: MagicMock
    ) -> None:
        """Test that messages_sent_to_dlq is incremented when max retries exceeded."""
        # Set retry count to max_retries so it goes to DLQ
        mock_normal_message.headers["x-retry-count"] = bus._config.max_retries

        # Set up bus to have a failing wildcard handler
        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        # Subscribe as wildcard handler (receives all events)
        from eventsource.handlers.adapter import HandlerAdapter

        bus._all_event_handlers.append(HandlerAdapter(failing_handler))

        # Mock deserialize to return an event
        mock_event = MagicMock(spec=DomainEvent)
        mock_event.event_id = uuid4()
        mock_event.event_type = "TestEvent"

        # Set up DLQ exchange mock
        mock_dlq_exchange = AsyncMock()
        bus._dlq_exchange = mock_dlq_exchange

        initial_dlq_count = bus.stats.messages_sent_to_dlq

        with patch.object(bus, "_deserialize_event", return_value=mock_event):
            await bus._process_message(mock_normal_message)

        assert bus.stats.messages_sent_to_dlq == initial_dlq_count + 1
        mock_dlq_exchange.publish.assert_called_once()
        mock_normal_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_message_no_dlq_stats_when_dlq_disabled(
        self, mock_normal_message: MagicMock
    ) -> None:
        """Test that messages_sent_to_dlq is not incremented when DLQ disabled."""
        config = RabbitMQEventBusConfig(enable_dlq=False, max_retries=0)
        bus = RabbitMQEventBus(config=config)

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        # Subscribe as wildcard handler (receives all events)
        from eventsource.handlers.adapter import HandlerAdapter

        bus._all_event_handlers.append(HandlerAdapter(failing_handler))

        mock_event = MagicMock(spec=DomainEvent)
        mock_event.event_id = uuid4()
        mock_event.event_type = "TestEvent"

        initial_dlq_count = bus.stats.messages_sent_to_dlq

        with patch.object(bus, "_deserialize_event", return_value=mock_event):
            await bus._process_message(mock_normal_message)

        # DLQ stats should not be incremented when DLQ is disabled
        assert bus.stats.messages_sent_to_dlq == initial_dlq_count


# ============================================================================
# P2-002: Retry Logic Tests
# ============================================================================


class TestRetryConfiguration:
    """Tests for retry-related configuration options."""

    def test_default_retry_config_values(self) -> None:
        """Test that default retry config values are set."""
        config = RabbitMQEventBusConfig()

        assert config.retry_base_delay == 1.0
        assert config.retry_max_delay == 60.0
        assert config.retry_jitter == 0.1

    def test_custom_retry_config_values(self) -> None:
        """Test that custom retry config values are accepted."""
        config = RabbitMQEventBusConfig(
            retry_base_delay=2.0,
            retry_max_delay=120.0,
            retry_jitter=0.2,
        )

        assert config.retry_base_delay == 2.0
        assert config.retry_max_delay == 120.0
        assert config.retry_jitter == 0.2

    def test_zero_jitter_config(self) -> None:
        """Test that zero jitter is accepted (no random delay)."""
        config = RabbitMQEventBusConfig(retry_jitter=0.0)

        assert config.retry_jitter == 0.0


class TestCalculateRetryDelay:
    """Tests for the _calculate_retry_delay method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test config with predictable values."""
        return RabbitMQEventBusConfig(
            retry_base_delay=1.0,
            retry_max_delay=60.0,
            retry_jitter=0.0,  # No jitter for deterministic tests
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config)

    def test_exponential_backoff_retry_0(self, bus: RabbitMQEventBus) -> None:
        """Test delay for retry count 0 (first retry)."""
        delay = bus._calculate_retry_delay(0)
        assert delay == 1.0  # 1.0 * (2 ** 0) = 1.0

    def test_exponential_backoff_retry_1(self, bus: RabbitMQEventBus) -> None:
        """Test delay for retry count 1."""
        delay = bus._calculate_retry_delay(1)
        assert delay == 2.0  # 1.0 * (2 ** 1) = 2.0

    def test_exponential_backoff_retry_2(self, bus: RabbitMQEventBus) -> None:
        """Test delay for retry count 2."""
        delay = bus._calculate_retry_delay(2)
        assert delay == 4.0  # 1.0 * (2 ** 2) = 4.0

    def test_exponential_backoff_retry_3(self, bus: RabbitMQEventBus) -> None:
        """Test delay for retry count 3."""
        delay = bus._calculate_retry_delay(3)
        assert delay == 8.0  # 1.0 * (2 ** 3) = 8.0

    def test_delay_capped_at_max(self, bus: RabbitMQEventBus) -> None:
        """Test that delay is capped at max_delay."""
        delay = bus._calculate_retry_delay(10)
        assert delay == 60.0  # Would be 1024.0 but capped at 60.0

    def test_custom_base_delay(self) -> None:
        """Test with custom base delay."""
        config = RabbitMQEventBusConfig(
            retry_base_delay=0.5,
            retry_max_delay=60.0,
            retry_jitter=0.0,
        )
        bus = RabbitMQEventBus(config=config)

        assert bus._calculate_retry_delay(0) == 0.5
        assert bus._calculate_retry_delay(1) == 1.0
        assert bus._calculate_retry_delay(2) == 2.0

    def test_jitter_adds_randomness(self) -> None:
        """Test that jitter adds randomness to delay."""
        config = RabbitMQEventBusConfig(
            retry_base_delay=1.0,
            retry_max_delay=60.0,
            retry_jitter=0.5,  # 50% jitter
        )
        bus = RabbitMQEventBus(config=config)

        # Collect multiple delay values
        delays = [bus._calculate_retry_delay(1) for _ in range(20)]

        # With 50% jitter on base delay of 2.0, values should range from 1.0 to 3.0
        # Check that we have some variance (not all the same)
        assert len(set(delays)) > 1, "Jitter should cause variance in delays"

        # Check all values are within expected range
        for delay in delays:
            assert 1.0 <= delay <= 3.0


class TestRepublishForRetry:
    """Tests for the _republish_for_retry method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_message(self) -> MagicMock:
        """Create mock incoming message."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-retry-count": 0,
        }
        message.body = b'{"test": "data"}'
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "TestAggregate.TestEvent"
        return message

    @pytest.mark.asyncio
    async def test_republish_increments_retry_count(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that retry count is incremented in republished message."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 1)

        # Verify publish was called
        mock_exchange.publish.assert_called_once()

        # Get the published message
        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        # Verify header was updated
        assert new_message.headers["x-retry-count"] == 1

    @pytest.mark.asyncio
    async def test_republish_adds_timestamp_header(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that x-last-retry-at timestamp is added."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 1)

        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        assert "x-last-retry-at" in new_message.headers
        # Verify it looks like an ISO timestamp
        timestamp = new_message.headers["x-last-retry-at"]
        assert "T" in timestamp  # ISO format includes T separator

    @pytest.mark.asyncio
    async def test_republish_preserves_original_headers(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that original headers are preserved."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 2)

        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        assert new_message.headers["event_type"] == "TestEvent"
        assert new_message.headers["aggregate_type"] == "TestAggregate"

    @pytest.mark.asyncio
    async def test_republish_preserves_body(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that message body is preserved."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 1)

        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        assert new_message.body == b'{"test": "data"}'

    @pytest.mark.asyncio
    async def test_republish_uses_original_routing_key(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that original routing key is used."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 1)

        call_args = mock_exchange.publish.call_args
        routing_key = call_args[1]["routing_key"]

        assert routing_key == "TestAggregate.TestEvent"

    @pytest.mark.asyncio
    async def test_republish_raises_if_exchange_not_initialized(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that RuntimeError is raised if exchange not initialized."""
        bus._exchange = None

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await bus._republish_for_retry(mock_message, 1)


class TestSendToDLQ:
    """Tests for the _send_to_dlq method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(enable_dlq=True)

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_message(self) -> MagicMock:
        """Create mock incoming message."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-retry-count": 3,
        }
        message.body = b'{"test": "data"}'
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "TestAggregate.TestEvent"
        return message

    @pytest.mark.asyncio
    async def test_send_to_dlq_adds_failure_metadata(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that failure metadata is added to DLQ message."""
        mock_dlq_exchange = AsyncMock()
        bus._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        call_args = mock_dlq_exchange.publish.call_args
        dlq_message = call_args[0][0]

        assert dlq_message.headers["x-dlq-reason"] == "Test error"
        assert dlq_message.headers["x-dlq-error-type"] == "ValueError"
        assert dlq_message.headers["x-dlq-retry-count"] == 3
        assert "x-dlq-timestamp" in dlq_message.headers
        assert dlq_message.headers["x-original-routing-key"] == "TestAggregate.TestEvent"

    @pytest.mark.asyncio
    async def test_send_to_dlq_preserves_original_headers(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that original headers are preserved in DLQ message."""
        mock_dlq_exchange = AsyncMock()
        bus._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        call_args = mock_dlq_exchange.publish.call_args
        dlq_message = call_args[0][0]

        assert dlq_message.headers["event_type"] == "TestEvent"
        assert dlq_message.headers["aggregate_type"] == "TestAggregate"

    @pytest.mark.asyncio
    async def test_send_to_dlq_increments_stats(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that messages_sent_to_dlq stat is incremented."""
        mock_dlq_exchange = AsyncMock()
        bus._dlq_exchange = mock_dlq_exchange

        initial_count = bus.stats.messages_sent_to_dlq

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        assert bus.stats.messages_sent_to_dlq == initial_count + 1

    @pytest.mark.asyncio
    async def test_send_to_dlq_uses_queue_name_as_routing_key(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that queue name is used as routing key for DLQ."""
        mock_dlq_exchange = AsyncMock()
        bus._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        call_args = mock_dlq_exchange.publish.call_args
        routing_key = call_args[1]["routing_key"]

        assert routing_key == bus._config.queue_name

    @pytest.mark.asyncio
    async def test_send_to_dlq_does_nothing_when_disabled(self, mock_message: MagicMock) -> None:
        """Test that nothing happens when DLQ is disabled."""
        config = RabbitMQEventBusConfig(enable_dlq=False)
        bus = RabbitMQEventBus(config=config)

        mock_dlq_exchange = AsyncMock()
        bus._dlq_exchange = mock_dlq_exchange

        initial_count = bus.stats.messages_sent_to_dlq

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        # Should not publish or increment stats
        mock_dlq_exchange.publish.assert_not_called()
        assert bus.stats.messages_sent_to_dlq == initial_count

    @pytest.mark.asyncio
    async def test_send_to_dlq_does_nothing_when_exchange_not_initialized(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that nothing happens when DLQ exchange not initialized."""
        bus._dlq_exchange = None

        initial_count = bus.stats.messages_sent_to_dlq

        error = ValueError("Test error")
        await bus._send_to_dlq(mock_message, error, 3)

        assert bus.stats.messages_sent_to_dlq == initial_count


class TestHandleFailedMessage:
    """Tests for the _handle_failed_message method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            max_retries=3,
            retry_base_delay=0.01,  # Very short for testing
            retry_max_delay=1.0,
            retry_jitter=0.0,
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_message(self) -> AsyncMock:
        """Create mock incoming message."""
        message = AsyncMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-retry-count": 0,
        }
        message.body = b'{"test": "data"}'
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "TestAggregate.TestEvent"
        return message

    @pytest.mark.asyncio
    async def test_retry_when_under_max_retries(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that message is republished when under max_retries."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        error = ValueError("Test error")
        await bus._handle_failed_message(mock_message, error, retry_count=0)

        # Should republish, not send to DLQ
        mock_exchange.publish.assert_called_once()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_dlq_when_at_max_retries(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that message is sent to DLQ at max_retries."""
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        bus._exchange = mock_exchange
        bus._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._handle_failed_message(mock_message, error, retry_count=3)

        # Should send to DLQ, not republish to main exchange
        mock_dlq_exchange.publish.assert_called_once()
        mock_exchange.publish.assert_not_called()
        mock_message.ack.assert_called_once()

    @pytest.mark.asyncio
    async def test_dlq_when_over_max_retries(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that message is sent to DLQ when over max_retries."""
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        bus._exchange = mock_exchange
        bus._dlq_exchange = mock_dlq_exchange

        error = ValueError("Test error")
        await bus._handle_failed_message(mock_message, error, retry_count=5)

        # Should send to DLQ
        mock_dlq_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_retry_increments_count(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that retry count is incremented in republished message."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        error = ValueError("Test error")
        await bus._handle_failed_message(mock_message, error, retry_count=1)

        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        assert new_message.headers["x-retry-count"] == 2


class TestProcessMessageWithRetry:
    """Tests for _process_message with retry handling."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            max_retries=3,
            retry_base_delay=0.01,
            retry_jitter=0.0,
        )

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create event registry with test event."""
        registry = EventRegistry()
        registry.register(ConsumerTestEvent)
        return registry

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig, registry: EventRegistry) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config, event_registry=registry)

    @pytest.fixture
    def mock_message(self) -> AsyncMock:
        """Create mock incoming message with proper event JSON."""
        event = ConsumerTestEvent(aggregate_id=uuid4(), data="test-data")
        event_json = event.model_dump_json()

        message = AsyncMock()
        message.headers = {
            "event_type": "ConsumerTestEvent",
            "aggregate_type": "ConsumerTest",
            "x-retry-count": 0,
        }
        message.body = event_json.encode("utf-8")
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "ConsumerTest.ConsumerTestEvent"
        return message

    @pytest.mark.asyncio
    async def test_reads_retry_count_from_header(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that retry count is read from x-retry-count header."""
        mock_message.headers["x-retry-count"] = 2

        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        # Create a failing handler
        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        from eventsource.handlers.adapter import HandlerAdapter

        bus._all_event_handlers.append(HandlerAdapter(failing_handler))

        await bus._process_message(mock_message)

        # Should have republished with retry_count + 1 = 3
        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]
        assert new_message.headers["x-retry-count"] == 3

    @pytest.mark.asyncio
    async def test_default_retry_count_is_zero(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that default retry count is 0 when header is missing."""
        del mock_message.headers["x-retry-count"]

        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        from eventsource.handlers.adapter import HandlerAdapter

        bus._all_event_handlers.append(HandlerAdapter(failing_handler))

        await bus._process_message(mock_message)

        # Should have republished with retry_count = 1
        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]
        assert new_message.headers["x-retry-count"] == 1

    @pytest.mark.asyncio
    async def test_success_does_not_trigger_retry(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that successful processing does not trigger retry logic."""
        from eventsource.handlers.adapter import HandlerAdapter

        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        # Create a successful handler
        async def success_handler(event: DomainEvent) -> None:
            pass

        bus._all_event_handlers.append(HandlerAdapter(success_handler))

        await bus._process_message(mock_message)

        # Should just ack, not republish
        mock_exchange.publish.assert_not_called()
        mock_message.ack.assert_called_once()
        assert bus.stats.events_processed_success == 1

    @pytest.mark.asyncio
    async def test_failure_increments_failed_stats(
        self, bus: RabbitMQEventBus, mock_message: AsyncMock
    ) -> None:
        """Test that failure increments events_processed_failed stat."""
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Intentional failure")

        from eventsource.handlers.adapter import HandlerAdapter

        bus._all_event_handlers.append(HandlerAdapter(failing_handler))

        await bus._process_message(mock_message)

        assert bus.stats.events_processed_failed == 1


# ============================================================================
# RabbitMQEventBus DLQ Operations Tests (P2-003)
# ============================================================================


class TestDLQMessageDataclass:
    """Tests for DLQMessage dataclass."""

    def test_dlq_message_with_all_fields(self) -> None:
        """Test DLQMessage with all fields populated."""
        from eventsource.bus.rabbitmq import DLQMessage

        msg = DLQMessage(
            message_id="test-id-123",
            routing_key="Order.OrderCreated",
            body='{"event_type": "OrderCreated"}',
            headers={"event_type": "OrderCreated", "x-dlq-reason": "Error"},
            event_type="OrderCreated",
            dlq_reason="Processing failed",
            dlq_error_type="ValueError",
            dlq_retry_count=3,
            dlq_timestamp="2024-01-15T10:30:00Z",
            original_routing_key="Order.OrderCreated",
        )

        assert msg.message_id == "test-id-123"
        assert msg.routing_key == "Order.OrderCreated"
        assert msg.body == '{"event_type": "OrderCreated"}'
        assert msg.headers == {"event_type": "OrderCreated", "x-dlq-reason": "Error"}
        assert msg.event_type == "OrderCreated"
        assert msg.dlq_reason == "Processing failed"
        assert msg.dlq_error_type == "ValueError"
        assert msg.dlq_retry_count == 3
        assert msg.dlq_timestamp == "2024-01-15T10:30:00Z"
        assert msg.original_routing_key == "Order.OrderCreated"

    def test_dlq_message_with_minimal_fields(self) -> None:
        """Test DLQMessage with only required fields."""
        from eventsource.bus.rabbitmq import DLQMessage

        msg = DLQMessage(
            message_id=None,
            routing_key=None,
            body="{}",
            headers={},
        )

        assert msg.message_id is None
        assert msg.routing_key is None
        assert msg.body == "{}"
        assert msg.headers == {}
        assert msg.event_type is None
        assert msg.dlq_reason is None
        assert msg.dlq_error_type is None
        assert msg.dlq_retry_count is None
        assert msg.dlq_timestamp is None
        assert msg.original_routing_key is None

    def test_dlq_message_default_values(self) -> None:
        """Test DLQMessage default values for optional fields."""
        from eventsource.bus.rabbitmq import DLQMessage

        msg = DLQMessage(
            message_id="id",
            routing_key="key",
            body="body",
            headers={"x": "y"},
        )

        # Default values should be None
        assert msg.event_type is None
        assert msg.dlq_reason is None
        assert msg.dlq_error_type is None
        assert msg.dlq_retry_count is None
        assert msg.dlq_timestamp is None
        assert msg.original_routing_key is None


class TestGetDLQMessages:
    """Tests for get_dlq_messages method."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @pytest.fixture
    def config_without_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ disabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @pytest.mark.asyncio
    async def test_returns_empty_when_not_connected(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_messages returns empty list when not connected."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        assert bus._connected is False

        result = await bus.get_dlq_messages(limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_messages returns empty list when DLQ disabled."""
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connected = True

        result = await bus.get_dlq_messages(limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_messages returns empty list when channel not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True
        bus._channel = None

        result = await bus.get_dlq_messages(limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_dlq_messages(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test get_dlq_messages returns list of DLQMessage objects."""
        from eventsource.bus.rabbitmq import DLQMessage

        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "test-msg-id"
        mock_message.routing_key = "Test.TestEvent"
        mock_message.body = b'{"data": "test"}'
        mock_message.headers = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Handler error",
            "x-dlq-error-type": "ValueError",
            "x-dlq-retry-count": 3,
            "x-dlq-timestamp": "2024-01-15T10:00:00Z",
            "x-original-routing-key": "Test.TestEvent",
        }

        # First call returns message, second call returns None
        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel

        result = await bus.get_dlq_messages(limit=10)

        assert len(result) == 1
        assert isinstance(result[0], DLQMessage)
        assert result[0].message_id == "test-msg-id"
        assert result[0].routing_key == "Test.TestEvent"
        assert result[0].body == '{"data": "test"}'
        assert result[0].event_type == "TestEvent"
        assert result[0].dlq_reason == "Handler error"
        assert result[0].dlq_error_type == "ValueError"
        assert result[0].dlq_retry_count == 3
        assert result[0].dlq_timestamp == "2024-01-15T10:00:00Z"
        assert result[0].original_routing_key == "Test.TestEvent"

    @pytest.mark.asyncio
    async def test_rejects_messages_with_requeue(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that messages are rejected with requeue to preserve them."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "test-id"
        mock_message.routing_key = "Test.Event"
        mock_message.body = b"{}"
        mock_message.headers = {}

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel

        await bus.get_dlq_messages(limit=10)

        # Verify message was rejected with requeue=True
        mock_message.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_respects_limit_parameter(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that limit parameter is respected."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()

        messages = []
        for i in range(5):
            msg = AsyncMock()
            msg.message_id = f"msg-{i}"
            msg.routing_key = "Test.Event"
            msg.body = b"{}"
            msg.headers = {}
            messages.append(msg)

        mock_queue.get.side_effect = messages + [None]
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel

        result = await bus.get_dlq_messages(limit=3)

        # Should only get 3 messages even though 5 are available
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that exceptions are handled gracefully."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue not found")
        bus._channel = mock_channel

        result = await bus.get_dlq_messages(limit=10)

        assert result == []

    @pytest.mark.asyncio
    async def test_handles_retry_count_as_string(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test that string retry count is converted to int."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "test-id"
        mock_message.routing_key = "Test.Event"
        mock_message.body = b"{}"
        mock_message.headers = {"x-dlq-retry-count": "5"}  # String instead of int

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel

        result = await bus.get_dlq_messages(limit=10)

        assert result[0].dlq_retry_count == 5


class TestGetDLQMessageCount:
    """Tests for get_dlq_message_count method."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @pytest.fixture
    def config_without_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ disabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @pytest.mark.asyncio
    async def test_returns_zero_when_not_connected(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_message_count returns 0 when not connected."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        assert bus._connected is False

        result = await bus.get_dlq_message_count()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_message_count returns 0 when DLQ disabled."""
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connected = True

        result = await bus.get_dlq_message_count()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test get_dlq_message_count returns 0 when channel not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True
        bus._channel = None

        result = await bus.get_dlq_message_count()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_message_count(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test get_dlq_message_count returns correct count."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_queue_info = AsyncMock()
        mock_queue_info.declaration_result.message_count = 42
        mock_channel.declare_queue.return_value = mock_queue_info
        bus._channel = mock_channel

        result = await bus.get_dlq_message_count()

        assert result == 42
        mock_channel.declare_queue.assert_called_once_with(
            name=config_with_dlq.dlq_queue_name,
            passive=True,
        )

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that exceptions are handled gracefully."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_channel.declare_queue.side_effect = Exception("Queue not found")
        bus._channel = mock_channel

        result = await bus.get_dlq_message_count()

        assert result == 0


class TestReplayDLQMessage:
    """Tests for replay_dlq_message method."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @pytest.fixture
    def config_without_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ disabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @pytest.mark.asyncio
    async def test_returns_false_when_not_connected(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when not connected."""
        bus = RabbitMQEventBus(config=config_with_dlq)

        result = await bus.replay_dlq_message("test-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_exchange_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when exchange not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True
        bus._exchange = None

        result = await bus.replay_dlq_message("test-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when DLQ disabled."""
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connected = True
        bus._exchange = AsyncMock()
        bus._channel = AsyncMock()

        result = await bus.replay_dlq_message("test-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_returns_false_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when channel not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True
        bus._exchange = AsyncMock()
        bus._channel = None

        result = await bus.replay_dlq_message("test-id")

        assert result is False

    @pytest.mark.asyncio
    async def test_replays_message_successfully(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test successful message replay."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "target-id"
        mock_message.routing_key = "dlq-routing-key"
        mock_message.body = b'{"data": "test"}'
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.headers = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Error",
            "x-dlq-error-type": "ValueError",
            "x-dlq-retry-count": 3,
            "x-dlq-timestamp": "2024-01-15T10:00:00Z",
            "x-original-routing-key": "Test.TestEvent",
        }

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel
        bus._exchange = mock_exchange

        result = await bus.replay_dlq_message("target-id")

        assert result is True
        mock_message.ack.assert_called_once()
        mock_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_false_when_message_not_found(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test replay_dlq_message returns False when message not found."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_message = AsyncMock()
        mock_message.message_id = "other-id"
        mock_message.routing_key = "key"
        mock_message.body = b"{}"
        mock_message.headers = {}

        mock_queue.get.side_effect = [mock_message, None]
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel
        bus._exchange = mock_exchange

        result = await bus.replay_dlq_message("non-existent-id")

        assert result is False
        # The non-matching message should be rejected with requeue
        mock_message.reject.assert_called_once_with(requeue=True)

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that exceptions are handled gracefully."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_exchange = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue error")
        bus._channel = mock_channel
        bus._exchange = mock_exchange

        result = await bus.replay_dlq_message("test-id")

        assert result is False


class TestReplayMessage:
    """Tests for _replay_message helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.mark.asyncio
    async def test_raises_when_exchange_not_initialized(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test _replay_message raises when exchange not initialized."""
        bus = RabbitMQEventBus(config=config)
        bus._exchange = None

        mock_message = AsyncMock()

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await bus._replay_message(mock_message)

    @pytest.mark.asyncio
    async def test_removes_dlq_headers(self, config: RabbitMQEventBusConfig) -> None:
        """Test that DLQ headers are removed from replayed message."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b'{"data": "test"}'
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "dlq-key"
        mock_message.headers = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Error",
            "x-dlq-error-type": "ValueError",
            "x-dlq-retry-count": 3,
            "x-dlq-timestamp": "2024-01-15T10:00:00Z",
            "x-original-routing-key": "Test.TestEvent",
            "x-death": [{"queue": "test"}],
        }

        await bus._replay_message(mock_message)

        # Verify the published message has DLQ headers removed
        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert "x-dlq-reason" not in replay_msg.headers
        assert "x-dlq-error-type" not in replay_msg.headers
        assert "x-dlq-retry-count" not in replay_msg.headers
        assert "x-dlq-timestamp" not in replay_msg.headers
        assert "x-original-routing-key" not in replay_msg.headers
        assert "x-death" not in replay_msg.headers

    @pytest.mark.asyncio
    async def test_resets_retry_count(self, config: RabbitMQEventBusConfig) -> None:
        """Test that retry count is reset to 0."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "key"
        mock_message.headers = {"x-retry-count": 5}

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert replay_msg.headers["x-retry-count"] == 0

    @pytest.mark.asyncio
    async def test_adds_replay_timestamp(self, config: RabbitMQEventBusConfig) -> None:
        """Test that x-replayed-from-dlq timestamp is added."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "key"
        mock_message.headers = {}

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert "x-replayed-from-dlq" in replay_msg.headers
        # Verify it looks like an ISO timestamp
        assert "T" in replay_msg.headers["x-replayed-from-dlq"]

    @pytest.mark.asyncio
    async def test_uses_original_routing_key(self, config: RabbitMQEventBusConfig) -> None:
        """Test that original routing key is used for republishing."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "dlq-routing-key"
        mock_message.headers = {
            "x-original-routing-key": "Original.RoutingKey",
        }

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        routing_key = publish_call.kwargs["routing_key"]

        assert routing_key == "Original.RoutingKey"

    @pytest.mark.asyncio
    async def test_falls_back_to_message_routing_key(self, config: RabbitMQEventBusConfig) -> None:
        """Test fallback to message routing key when original not in headers."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "fallback-routing-key"
        mock_message.headers = {}

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        routing_key = publish_call.kwargs["routing_key"]

        assert routing_key == "fallback-routing-key"

    @pytest.mark.asyncio
    async def test_preserves_non_dlq_headers(self, config: RabbitMQEventBusConfig) -> None:
        """Test that non-DLQ headers are preserved."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "key"
        mock_message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "correlation_id": "corr-123",
            "x-dlq-reason": "Error",  # Should be removed
        }

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert replay_msg.headers["event_type"] == "TestEvent"
        assert replay_msg.headers["aggregate_type"] == "TestAggregate"
        assert replay_msg.headers["correlation_id"] == "corr-123"


class TestPurgeDLQ:
    """Tests for purge_dlq method."""

    @pytest.fixture
    def config_with_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=True,
        )

    @pytest.fixture
    def config_without_dlq(self) -> RabbitMQEventBusConfig:
        """Create test configuration with DLQ disabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_dlq=False,
        )

    @pytest.mark.asyncio
    async def test_returns_zero_when_not_connected(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test purge_dlq returns 0 when not connected."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        assert bus._connected is False

        result = await bus.purge_dlq()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_dlq_disabled(
        self, config_without_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test purge_dlq returns 0 when DLQ disabled."""
        bus = RabbitMQEventBus(config=config_without_dlq)
        bus._connected = True

        result = await bus.purge_dlq()

        assert result == 0

    @pytest.mark.asyncio
    async def test_returns_zero_when_channel_not_initialized(
        self, config_with_dlq: RabbitMQEventBusConfig
    ) -> None:
        """Test purge_dlq returns 0 when channel not initialized."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True
        bus._channel = None

        result = await bus.purge_dlq()

        assert result == 0

    @pytest.mark.asyncio
    async def test_purges_dlq_successfully(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test successful DLQ purge."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_queue = AsyncMock()
        # purge() returns PurgeOk which has message_count attribute
        mock_purge_result = AsyncMock()
        mock_purge_result.message_count = 15
        mock_queue.purge.return_value = mock_purge_result
        mock_channel.get_queue.return_value = mock_queue
        bus._channel = mock_channel

        result = await bus.purge_dlq()

        assert result == 15
        mock_channel.get_queue.assert_called_once_with(config_with_dlq.dlq_queue_name)
        mock_queue.purge.assert_called_once()

    @pytest.mark.asyncio
    async def test_handles_exception(self, config_with_dlq: RabbitMQEventBusConfig) -> None:
        """Test that exceptions are handled gracefully."""
        bus = RabbitMQEventBus(config=config_with_dlq)
        bus._connected = True

        mock_channel = AsyncMock()
        mock_channel.get_queue.side_effect = Exception("Queue error")
        bus._channel = mock_channel

        result = await bus.purge_dlq()

        assert result == 0


class TestDLQMessageExport:
    """Tests for DLQMessage export in __all__."""

    def test_dlq_message_exported(self) -> None:
        """Test that DLQMessage is exported from rabbitmq module."""
        from eventsource.bus.rabbitmq import DLQMessage

        assert DLQMessage is not None

    def test_dlq_message_in_all(self) -> None:
        """Test that DLQMessage is in __all__."""
        from eventsource.bus import rabbitmq

        assert "DLQMessage" in rabbitmq.__all__


# =============================================================================
# P2-004: Reconnection Handling Tests
# =============================================================================


class TestRabbitMQReconnectionCallbacks:
    """Tests for reconnection callback methods (P2-004)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create a test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            reconnect_delay=1.0,
            max_reconnect_delay=60.0,
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.mark.asyncio
    async def test_on_reconnect_increments_reconnection_stats(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect increments the reconnections counter."""
        bus._connected = False
        bus._stats.reconnections = 0

        # Create mock connection with channel
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Mock topology declaration methods
        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        assert bus._stats.reconnections == 1

    @pytest.mark.asyncio
    async def test_on_reconnect_multiple_times_increments_counter(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that multiple reconnections increment the counter correctly."""
        bus._connected = False
        bus._stats.reconnections = 0

        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)
        await bus._on_reconnect(mock_connection)
        await bus._on_reconnect(mock_connection)

        assert bus._stats.reconnections == 3

    @pytest.mark.asyncio
    async def test_on_reconnect_recreates_channel(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect recreates the channel."""
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        mock_connection.channel.assert_called_once()
        assert bus._channel is mock_channel

    @pytest.mark.asyncio
    async def test_on_reconnect_sets_prefetch_count(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect sets QoS on the new channel."""
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        mock_channel.set_qos.assert_called_once_with(prefetch_count=bus._config.prefetch_count)

    @pytest.mark.asyncio
    async def test_on_reconnect_redeclares_dlq_when_enabled(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect redeclares DLQ when enabled."""
        bus._config.enable_dlq = True

        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        bus._declare_dlq.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_reconnect_skips_dlq_when_disabled(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect skips DLQ declaration when disabled."""
        bus._config.enable_dlq = False

        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        bus._declare_dlq.assert_not_called()

    @pytest.mark.asyncio
    async def test_on_reconnect_redeclares_exchange(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect redeclares the exchange."""
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        bus._declare_exchange.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_reconnect_redeclares_queue(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect redeclares the queue."""
        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        bus._declare_queue.assert_called_once()
        bus._bind_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_on_reconnect_sets_connected_true_on_success(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect sets _connected to True on success."""
        bus._connected = False

        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        assert bus._connected is True

    @pytest.mark.asyncio
    async def test_on_reconnect_sets_connected_false_on_failure(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that _on_reconnect sets _connected to False on failure."""
        bus._connected = False

        mock_connection = AsyncMock()
        mock_connection.channel = AsyncMock(side_effect=Exception("Channel error"))

        await bus._on_reconnect(mock_connection)

        assert bus._connected is False

    @pytest.mark.asyncio
    async def test_on_reconnect_sets_reconnecting_flag(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect manages the _reconnecting flag."""
        bus._reconnecting = False

        mock_connection = AsyncMock()
        mock_channel = AsyncMock()
        mock_channel.close_callbacks = MagicMock()
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus._declare_dlq = AsyncMock()
        bus._declare_exchange = AsyncMock()
        bus._declare_queue = AsyncMock()
        bus._bind_queue = AsyncMock()

        await bus._on_reconnect(mock_connection)

        # After successful reconnection, reconnecting should be False
        assert bus._reconnecting is False

    @pytest.mark.asyncio
    async def test_on_reconnect_clears_reconnecting_on_failure(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_reconnect clears _reconnecting on failure."""
        mock_connection = AsyncMock()
        mock_connection.channel = AsyncMock(side_effect=Exception("Channel error"))

        await bus._on_reconnect(mock_connection)

        assert bus._reconnecting is False


class TestRabbitMQConnectionCloseCallback:
    """Tests for connection close callback (P2-004)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create a test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    def test_connection_close_updates_connected_state(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_connection_close sets _connected to False."""
        bus._connected = True

        bus._on_connection_close(None, None)

        assert bus._connected is False

    def test_connection_close_tracks_was_consuming_when_consuming(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that _on_connection_close tracks consuming state."""
        bus._connected = True
        bus._consuming = True
        bus._was_consuming = False

        bus._on_connection_close(None, None)

        assert bus._was_consuming is True

    def test_connection_close_does_not_set_was_consuming_when_not_consuming(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that _on_connection_close doesn't set _was_consuming when not consuming."""
        bus._connected = True
        bus._consuming = False
        bus._was_consuming = False

        bus._on_connection_close(None, None)

        assert bus._was_consuming is False

    def test_connection_close_with_exception(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_connection_close handles exceptions."""
        bus._connected = True

        exception = ValueError("Connection lost")
        bus._on_connection_close(MagicMock(), exception)

        assert bus._connected is False

    def test_connection_close_without_exception(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_connection_close handles graceful close."""
        bus._connected = True

        bus._on_connection_close(MagicMock(), None)

        assert bus._connected is False


class TestRabbitMQChannelCloseCallback:
    """Tests for channel close callback (P2-004)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create a test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    def test_channel_close_with_exception(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_channel_close handles exception case."""
        # This should not raise - just log the warning
        exception = ValueError("Channel closed unexpectedly")
        bus._on_channel_close(MagicMock(), exception)

    def test_channel_close_without_exception(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_channel_close handles normal close."""
        # This should not raise - just log debug message
        bus._on_channel_close(MagicMock(), None)

    def test_channel_close_with_none_channel(self, bus: RabbitMQEventBus) -> None:
        """Test that _on_channel_close handles None channel."""
        bus._on_channel_close(None, None)


class TestRabbitMQConnectWithReconnectionCallbacks:
    """Tests for connect() method registering reconnection callbacks (P2-004)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create a test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            reconnect_delay=2.0,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_uses_reconnect_interval(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect passes reconnect_interval to connect_robust."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.reconnect_callbacks = MagicMock()
        mock_connection.close_callbacks = MagicMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close_callbacks = MagicMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_aio_pika.connect_robust.assert_called_once_with(
            config.rabbitmq_url,
            heartbeat=config.heartbeat,
            reconnect_interval=config.reconnect_delay,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_registers_reconnect_callback(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect registers the reconnect callback."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.reconnect_callbacks = MagicMock()
        mock_connection.close_callbacks = MagicMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close_callbacks = MagicMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_connection.reconnect_callbacks.add.assert_called_once_with(bus._on_reconnect)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_registers_close_callback(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect registers the close callback."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.reconnect_callbacks = MagicMock()
        mock_connection.close_callbacks = MagicMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close_callbacks = MagicMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_connection.close_callbacks.add.assert_called_once_with(bus._on_connection_close)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_registers_channel_close_callback(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that connect registers the channel close callback."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.reconnect_callbacks = MagicMock()
        mock_connection.close_callbacks = MagicMock()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close_callbacks = MagicMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        mock_channel.close_callbacks.add.assert_called_once_with(bus._on_channel_close)


class TestRabbitMQReconnectionStateTracking:
    """Tests for reconnection state tracking (P2-004)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create a test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    def test_initial_was_consuming_is_false(self, bus: RabbitMQEventBus) -> None:
        """Test that _was_consuming is initialized to False."""
        assert bus._was_consuming is False

    def test_initial_reconnecting_is_false(self, bus: RabbitMQEventBus) -> None:
        """Test that _reconnecting is initialized to False."""
        assert bus._reconnecting is False

    def test_was_consuming_preserved_after_connection_close(self, bus: RabbitMQEventBus) -> None:
        """Test that _was_consuming is preserved after multiple connection closes."""
        bus._consuming = True
        bus._connected = True

        # First disconnect
        bus._on_connection_close(None, None)
        assert bus._was_consuming is True

        # Simulate reconnection success
        bus._consuming = False  # Consumer not restarted yet
        bus._connected = True

        # Second disconnect (without active consumption)
        bus._on_connection_close(None, None)
        # _was_consuming should still be True from first disconnect
        assert bus._was_consuming is True

    def test_reconnections_counter_persists_across_reconnections(
        self, bus: RabbitMQEventBus
    ) -> None:
        """Test that reconnections counter accumulates correctly."""
        assert bus._stats.reconnections == 0

        # Simulate multiple disconnects/reconnects by directly incrementing
        bus._stats.reconnections = 5
        assert bus._stats.reconnections == 5

        bus._stats.reconnections += 1
        assert bus._stats.reconnections == 6


# ============================================================================
# Graceful Shutdown Tests (P2-005)
# ============================================================================


class TestGracefulShutdown:
    """Tests for graceful shutdown functionality (P2-005)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            shutdown_timeout=30.0,
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    def test_shutdown_timeout_config_default(self) -> None:
        """Test that shutdown_timeout has a default value."""
        config = RabbitMQEventBusConfig()
        assert config.shutdown_timeout == 30.0

    def test_shutdown_timeout_config_custom(self) -> None:
        """Test custom shutdown_timeout configuration."""
        config = RabbitMQEventBusConfig(shutdown_timeout=60.0)
        assert config.shutdown_timeout == 60.0

    def test_is_shutdown_initial_state(self, bus: RabbitMQEventBus) -> None:
        """Test that is_shutdown is False initially."""
        assert bus.is_shutdown is False

    def test_shutdown_initiated_flag_initial(self, bus: RabbitMQEventBus) -> None:
        """Test that _shutdown_initiated is False initially."""
        assert bus._shutdown_initiated is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_sets_shutdown_flag(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown sets the shutdown flag."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.shutdown(timeout=5.0)

        assert bus.is_shutdown is True
        assert bus._shutdown_initiated is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_stops_consuming(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown stops consuming."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        bus._consuming = True

        await bus.shutdown(timeout=5.0)

        assert bus._consuming is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_disconnects(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown calls disconnect."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.shutdown(timeout=5.0)

        assert bus.is_connected is False
        mock_channel.close.assert_called_once()
        mock_connection.close.assert_called_once()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_is_idempotent(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that calling shutdown multiple times is safe."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # First shutdown
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True

        # Second shutdown should not raise
        await bus.shutdown(timeout=5.0)
        assert bus.is_shutdown is True

        # Channel close should only be called once (during first shutdown)
        mock_channel.close.assert_called_once()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown cancels the consumer task."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Create a mock consumer task
        async def long_running() -> None:
            await asyncio.sleep(100)

        bus._consumer_task = asyncio.create_task(long_running())
        bus._consuming = True

        await bus.shutdown(timeout=2.0)

        assert bus._consumer_task is None

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_when_not_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown works when not connected."""
        bus = RabbitMQEventBus(config=config)

        # Should not raise
        await bus.shutdown(timeout=5.0)

        assert bus.is_shutdown is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_when_not_consuming(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that shutdown works when not consuming."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        assert bus._consuming is False

        await bus.shutdown(timeout=5.0)

        assert bus.is_shutdown is True
        assert bus.is_connected is False


class TestGracefulShutdownContextManager:
    """Tests for context manager graceful shutdown (P2-005)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            shutdown_timeout=5.0,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_uses_shutdown(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that context manager calls shutdown instead of disconnect."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        async with RabbitMQEventBus(config=config) as bus:
            assert bus.is_connected is True

        assert bus.is_shutdown is True
        assert bus.is_connected is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_uses_config_timeout(self, mock_aio_pika: MagicMock) -> None:
        """Test that context manager uses shutdown_timeout from config."""
        config = RabbitMQEventBusConfig(shutdown_timeout=10.0)

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        async with RabbitMQEventBus(config=config) as bus:
            pass

        assert bus.is_shutdown is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_context_manager_shutdown_on_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that context manager performs shutdown even on exception."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(ValueError, match="Test exception"):
            async with bus:
                raise ValueError("Test exception")

        assert bus.is_shutdown is True
        assert bus.is_connected is False


class TestStopConsumingGracefully:
    """Tests for _stop_consuming_gracefully helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.mark.asyncio
    async def test_stop_consuming_when_not_consuming(self, bus: RabbitMQEventBus) -> None:
        """Test _stop_consuming_gracefully when not consuming does nothing."""
        bus._consuming = False

        await bus._stop_consuming_gracefully(timeout=5.0)

        # Should complete without error

    @pytest.mark.asyncio
    async def test_stop_consuming_sets_flag(self, bus: RabbitMQEventBus) -> None:
        """Test that _stop_consuming_gracefully sets consuming flag to False."""
        bus._consuming = True

        await bus._stop_consuming_gracefully(timeout=5.0)

        assert bus._consuming is False

    @pytest.mark.asyncio
    async def test_stop_consuming_cancels_slow_task(self, bus: RabbitMQEventBus) -> None:
        """Test that slow consumer task is cancelled if it doesn't stop in time."""
        bus._consuming = True

        # Create a task that won't respond to _consuming flag
        async def slow_consumer() -> None:
            await asyncio.sleep(100)

        bus._consumer_task = asyncio.create_task(slow_consumer())

        await bus._stop_consuming_gracefully(timeout=0.1)

        assert bus._consumer_task is None

    @pytest.mark.asyncio
    async def test_stop_consuming_waits_for_graceful_stop(self, bus: RabbitMQEventBus) -> None:
        """Test that _stop_consuming_gracefully waits for task to complete."""
        bus._consuming = True
        completed = False

        async def quick_consumer() -> None:
            nonlocal completed
            # Simulate quick completion
            await asyncio.sleep(0.01)
            completed = True

        bus._consumer_task = asyncio.create_task(quick_consumer())

        await bus._stop_consuming_gracefully(timeout=5.0)

        # Allow task to complete
        await asyncio.sleep(0.02)
        assert bus._consumer_task is None


class TestDrainInFlight:
    """Tests for _drain_in_flight helper method."""

    @pytest.fixture
    def bus(self) -> RabbitMQEventBus:
        """Create a test event bus instance."""
        return RabbitMQEventBus()

    @pytest.mark.asyncio
    async def test_drain_waits_for_portion_of_timeout(self, bus: RabbitMQEventBus) -> None:
        """Test that _drain_in_flight waits for a portion of the timeout."""
        import time

        start = time.monotonic()
        await bus._drain_in_flight(timeout=4.0)  # Should wait 1.0s (4.0/4)
        elapsed = time.monotonic() - start

        # Should wait roughly 1.0 seconds (timeout / 4)
        assert 0.9 <= elapsed <= 1.5

    @pytest.mark.asyncio
    async def test_drain_caps_at_5_seconds(self, bus: RabbitMQEventBus) -> None:
        """Test that drain time is capped at 5 seconds."""
        import time

        start = time.monotonic()
        await bus._drain_in_flight(timeout=100.0)  # Would be 25s without cap
        elapsed = time.monotonic() - start

        # Should wait at most 5 seconds
        assert 4.5 <= elapsed <= 5.5


class TestForceDisconnect:
    """Tests for _force_disconnect helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_clears_all_state(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _force_disconnect clears all connection state."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus._force_disconnect()

        assert bus._connection is None
        assert bus._channel is None
        assert bus._exchange is None
        assert bus._dlq_exchange is None
        assert bus._consumer_queue is None
        assert bus._dlq_queue is None
        assert bus._connected is False
        assert bus._consuming is False
        assert bus._shutdown_initiated is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_cancels_consumer_task(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _force_disconnect cancels consumer task immediately."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        async def long_running() -> None:
            await asyncio.sleep(100)

        bus._consumer_task = asyncio.create_task(long_running())

        await bus._force_disconnect()

        assert bus._consumer_task is None

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_force_disconnect_handles_close_errors(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _force_disconnect handles errors during close gracefully."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_connection.close = AsyncMock(side_effect=Exception("Connection error"))
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_channel.close = AsyncMock(side_effect=Exception("Channel error"))
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Should not raise even with errors
        await bus._force_disconnect()

        assert bus._connected is False
        assert bus._channel is None
        assert bus._connection is None

    @pytest.mark.asyncio
    async def test_force_disconnect_when_not_connected(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _force_disconnect works when not connected."""
        bus = RabbitMQEventBus(config=config)

        # Should not raise
        await bus._force_disconnect()

        assert bus._shutdown_initiated is True


class TestShutdownError:
    """Tests for ShutdownError exception."""

    def test_shutdown_error_default_message(self) -> None:
        """Test that ShutdownError has a default message."""
        from eventsource.bus.rabbitmq import ShutdownError

        error = ShutdownError()
        assert str(error) == "Event bus has been shut down"

    def test_shutdown_error_custom_message(self) -> None:
        """Test that ShutdownError accepts custom message."""
        from eventsource.bus.rabbitmq import ShutdownError

        error = ShutdownError("Custom shutdown message")
        assert str(error) == "Custom shutdown message"

    def test_shutdown_error_is_exception(self) -> None:
        """Test that ShutdownError is an Exception subclass."""
        from eventsource.bus.rabbitmq import ShutdownError

        error = ShutdownError()
        assert isinstance(error, Exception)

    def test_shutdown_error_can_be_raised_and_caught(self) -> None:
        """Test that ShutdownError can be raised and caught."""
        from eventsource.bus.rabbitmq import ShutdownError

        with pytest.raises(ShutdownError) as exc_info:
            raise ShutdownError("Test shutdown")

        assert "Test shutdown" in str(exc_info.value)


class TestShutdownLogging:
    """Tests for shutdown logging."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_logs_initiation(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that shutdown logs initiation."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with caplog.at_level(logging.INFO):
            await bus.shutdown(timeout=5.0)

        assert "Initiating graceful shutdown" in caplog.text

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_shutdown_logs_completion(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """Test that shutdown logs completion with duration."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with caplog.at_level(logging.INFO):
            await bus.shutdown(timeout=5.0)

        assert "Graceful shutdown completed" in caplog.text


class TestShutdownConfigDocumentation:
    """Tests for shutdown_timeout config documentation."""

    def test_config_has_shutdown_timeout(self) -> None:
        """Test that RabbitMQEventBusConfig has shutdown_timeout attribute."""
        config = RabbitMQEventBusConfig()
        assert hasattr(config, "shutdown_timeout")

    def test_config_shutdown_timeout_type(self) -> None:
        """Test that shutdown_timeout is a float."""
        config = RabbitMQEventBusConfig()
        assert isinstance(config.shutdown_timeout, float)

    def test_config_shutdown_timeout_zero(self) -> None:
        """Test that shutdown_timeout can be set to zero."""
        config = RabbitMQEventBusConfig(shutdown_timeout=0.0)
        assert config.shutdown_timeout == 0.0

    def test_config_shutdown_timeout_large_value(self) -> None:
        """Test that shutdown_timeout can be set to a large value."""
        config = RabbitMQEventBusConfig(shutdown_timeout=3600.0)
        assert config.shutdown_timeout == 3600.0


# ============================================================================
# Statistics Methods Tests (P2-006)
# ============================================================================


class TestStatisticsTimingFields:
    """Tests for timing fields in RabbitMQEventBusStats."""

    def test_timing_fields_default_to_none(self) -> None:
        """Test that timing fields default to None."""
        stats = RabbitMQEventBusStats()

        assert stats.last_publish_at is None
        assert stats.last_consume_at is None
        assert stats.last_error_at is None
        assert stats.connected_at is None

    def test_timing_fields_can_be_set(self) -> None:
        """Test that timing fields can be set to datetime values."""
        now = datetime.now(UTC)
        stats = RabbitMQEventBusStats(
            last_publish_at=now,
            last_consume_at=now,
            last_error_at=now,
            connected_at=now,
        )

        assert stats.last_publish_at == now
        assert stats.last_consume_at == now
        assert stats.last_error_at == now
        assert stats.connected_at == now

    def test_timing_fields_independent(self) -> None:
        """Test that timing fields can be set independently."""
        publish_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
        consume_time = datetime(2024, 1, 1, 11, 0, 0, tzinfo=UTC)

        stats = RabbitMQEventBusStats(
            last_publish_at=publish_time,
            last_consume_at=consume_time,
        )

        assert stats.last_publish_at == publish_time
        assert stats.last_consume_at == consume_time
        assert stats.last_error_at is None
        assert stats.connected_at is None


class TestGetStatsMethod:
    """Tests for RabbitMQEventBus.get_stats() method."""

    def test_get_stats_returns_stats_object(self) -> None:
        """Test that get_stats() returns a RabbitMQEventBusStats object."""
        bus = RabbitMQEventBus()
        stats = bus.get_stats()

        assert isinstance(stats, RabbitMQEventBusStats)

    def test_get_stats_returns_same_as_property(self) -> None:
        """Test that get_stats() returns the same object as stats property."""
        bus = RabbitMQEventBus()

        assert bus.get_stats() is bus.stats

    def test_get_stats_reflects_modifications(self) -> None:
        """Test that get_stats() reflects modifications to stats."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 42

        stats = bus.get_stats()
        assert stats.events_published == 42


class TestGetStatsDictMethod:
    """Tests for RabbitMQEventBus.get_stats_dict() method."""

    def test_get_stats_dict_returns_dict(self) -> None:
        """Test that get_stats_dict() returns a dictionary."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        assert isinstance(stats_dict, dict)

    def test_get_stats_dict_contains_all_counter_fields(self) -> None:
        """Test that get_stats_dict() contains all counter fields."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        expected_fields = [
            "events_published",
            "events_consumed",
            "events_processed_success",
            "events_processed_failed",
            "messages_sent_to_dlq",
            "handler_errors",
            "reconnections",
            "publish_confirms",
            "publish_returns",
        ]

        for field in expected_fields:
            assert field in stats_dict, f"Missing counter field: {field}"

    def test_get_stats_dict_contains_timing_fields(self) -> None:
        """Test that get_stats_dict() contains timing fields."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        expected_fields = [
            "last_publish_at",
            "last_consume_at",
            "last_error_at",
            "connected_at",
        ]

        for field in expected_fields:
            assert field in stats_dict, f"Missing timing field: {field}"

    def test_get_stats_dict_contains_connection_state(self) -> None:
        """Test that get_stats_dict() contains connection state fields."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        assert "is_connected" in stats_dict
        assert "is_consuming" in stats_dict
        assert "uptime_seconds" in stats_dict

    def test_get_stats_dict_uptime_none_when_not_connected(self) -> None:
        """Test that uptime_seconds is None when not connected."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        assert stats_dict["uptime_seconds"] is None
        assert stats_dict["is_connected"] is False

    def test_get_stats_dict_uptime_calculated_when_connected(self) -> None:
        """Test that uptime_seconds is calculated when connected_at is set."""
        bus = RabbitMQEventBus()
        # Simulate being connected 60 seconds ago
        bus._stats.connected_at = datetime.now(UTC)
        bus._connected = True
        bus._connection = MagicMock()
        bus._connection.is_closed = False

        stats_dict = bus.get_stats_dict()

        assert stats_dict["uptime_seconds"] is not None
        assert stats_dict["uptime_seconds"] >= 0
        # Should be very close to 0 since we just set connected_at
        assert stats_dict["uptime_seconds"] < 1.0

    def test_get_stats_dict_timing_fields_as_iso_strings(self) -> None:
        """Test that timing fields are converted to ISO format strings."""
        bus = RabbitMQEventBus()
        now = datetime(2024, 6, 15, 12, 30, 45, tzinfo=UTC)
        bus._stats.last_publish_at = now
        bus._stats.last_consume_at = now

        stats_dict = bus.get_stats_dict()

        # Check ISO format string
        assert stats_dict["last_publish_at"] == now.isoformat()
        assert stats_dict["last_consume_at"] == now.isoformat()

    def test_get_stats_dict_timing_fields_none_when_not_set(self) -> None:
        """Test that timing fields are None when not set."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        assert stats_dict["last_publish_at"] is None
        assert stats_dict["last_consume_at"] is None
        assert stats_dict["last_error_at"] is None
        assert stats_dict["connected_at"] is None

    def test_get_stats_dict_is_json_serializable(self) -> None:
        """Test that get_stats_dict() returns JSON serializable data."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100
        bus._stats.last_publish_at = datetime.now(UTC)
        stats_dict = bus.get_stats_dict()

        # Should not raise any exceptions
        json_str = json.dumps(stats_dict)
        assert isinstance(json_str, str)

        # Round-trip should work
        parsed = json.loads(json_str)
        assert parsed["events_published"] == 100

    def test_get_stats_dict_reflects_current_values(self) -> None:
        """Test that get_stats_dict() returns current values."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 10
        bus._stats.events_consumed = 8
        bus._stats.handler_errors = 2

        stats_dict = bus.get_stats_dict()

        assert stats_dict["events_published"] == 10
        assert stats_dict["events_consumed"] == 8
        assert stats_dict["handler_errors"] == 2


class TestResetStatsMethod:
    """Tests for RabbitMQEventBus.reset_stats() method."""

    def test_reset_stats_clears_counters(self) -> None:
        """Test that reset_stats() resets all counters to zero."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100
        bus._stats.events_consumed = 95
        bus._stats.events_processed_success = 90
        bus._stats.events_processed_failed = 5
        bus._stats.handler_errors = 3
        bus._stats.reconnections = 2

        bus.reset_stats()

        assert bus.stats.events_published == 0
        assert bus.stats.events_consumed == 0
        assert bus.stats.events_processed_success == 0
        assert bus.stats.events_processed_failed == 0
        assert bus.stats.handler_errors == 0
        assert bus.stats.reconnections == 0

    def test_reset_stats_clears_timing_when_disconnected(self) -> None:
        """Test that reset_stats() clears timing fields when disconnected."""
        bus = RabbitMQEventBus()
        now = datetime.now(UTC)
        bus._stats.last_publish_at = now
        bus._stats.last_consume_at = now
        bus._stats.last_error_at = now

        bus.reset_stats()

        assert bus.stats.last_publish_at is None
        assert bus.stats.last_consume_at is None
        assert bus.stats.last_error_at is None
        assert bus.stats.connected_at is None

    def test_reset_stats_preserves_connected_at_when_connected(self) -> None:
        """Test that reset_stats() preserves connected_at when connected."""
        bus = RabbitMQEventBus()
        connect_time = datetime.now(UTC)
        bus._stats.connected_at = connect_time
        bus._connected = True
        bus._stats.events_published = 100

        bus.reset_stats()

        # connected_at should be preserved
        assert bus.stats.connected_at == connect_time
        # But counters should be reset
        assert bus.stats.events_published == 0

    def test_reset_stats_clears_connected_at_when_disconnected(self) -> None:
        """Test that reset_stats() clears connected_at when disconnected."""
        bus = RabbitMQEventBus()
        bus._stats.connected_at = datetime.now(UTC)
        bus._connected = False  # Not connected

        bus.reset_stats()

        assert bus.stats.connected_at is None

    def test_reset_stats_logs_message(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that reset_stats() logs a message."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100

        with caplog.at_level(logging.INFO):
            bus.reset_stats()

        assert "Statistics reset" in caplog.text

    def test_reset_stats_creates_new_stats_object(self) -> None:
        """Test that reset_stats() creates a new stats object."""
        bus = RabbitMQEventBus()
        original_stats = bus._stats
        original_stats.events_published = 100

        bus.reset_stats()

        # Should be a new object
        assert bus._stats is not original_stats


class TestStatisticsIntegration:
    """Integration tests for statistics tracking."""

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_sets_connected_at(self, mock_aio_pika: MagicMock) -> None:
        """Test that connect() sets connected_at timestamp."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)

        assert bus.stats.connected_at is None

        await bus.connect()

        assert bus.stats.connected_at is not None
        assert isinstance(bus.stats.connected_at, datetime)

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_clears_connected_at(self, mock_aio_pika: MagicMock) -> None:
        """Test that disconnect() clears connected_at timestamp."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        assert bus.stats.connected_at is not None

        await bus.disconnect()

        assert bus.stats.connected_at is None

    def test_initial_stats_are_zero(self) -> None:
        """Test that initial statistics are all zero."""
        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)
        stats = bus.get_stats()

        assert stats.events_published == 0
        assert stats.events_consumed == 0
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.publish_confirms == 0
        assert stats.publish_returns == 0

    def test_stats_dict_contains_all_fields(self) -> None:
        """Test that get_stats_dict() contains all expected fields."""
        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)
        stats_dict = bus.get_stats_dict()

        # All counter fields
        assert "events_published" in stats_dict
        assert "events_consumed" in stats_dict
        assert "events_processed_success" in stats_dict
        assert "events_processed_failed" in stats_dict
        assert "messages_sent_to_dlq" in stats_dict
        assert "handler_errors" in stats_dict
        assert "reconnections" in stats_dict
        assert "publish_confirms" in stats_dict
        assert "publish_returns" in stats_dict

        # Timing fields
        assert "last_publish_at" in stats_dict
        assert "last_consume_at" in stats_dict
        assert "last_error_at" in stats_dict
        assert "connected_at" in stats_dict

        # Connection state
        assert "is_connected" in stats_dict
        assert "is_consuming" in stats_dict
        assert "uptime_seconds" in stats_dict

    def test_reset_stats_then_get_stats(self) -> None:
        """Test that get_stats() works correctly after reset_stats()."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100

        bus.reset_stats()
        stats = bus.get_stats()

        assert stats.events_published == 0

    def test_reset_stats_then_get_stats_dict(self) -> None:
        """Test that get_stats_dict() works correctly after reset_stats()."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100

        bus.reset_stats()
        stats_dict = bus.get_stats_dict()

        assert stats_dict["events_published"] == 0


# ============================================================================
# QueueInfo and HealthCheckResult Dataclass Tests (P2-007)
# ============================================================================


class TestQueueInfo:
    """Tests for QueueInfo dataclass."""

    def test_default_values(self) -> None:
        """Test that QueueInfo has correct default values."""
        from eventsource.bus.rabbitmq import QueueInfo

        info = QueueInfo(
            name="test-queue",
            message_count=10,
            consumer_count=2,
        )

        assert info.name == "test-queue"
        assert info.message_count == 10
        assert info.consumer_count == 2
        assert info.state == "running"
        assert info.error is None

    def test_with_error_state(self) -> None:
        """Test QueueInfo with error state."""
        from eventsource.bus.rabbitmq import QueueInfo

        info = QueueInfo(
            name="test-queue",
            message_count=0,
            consumer_count=0,
            state="error",
            error="Not connected to RabbitMQ",
        )

        assert info.state == "error"
        assert info.error == "Not connected to RabbitMQ"

    def test_idle_state(self) -> None:
        """Test QueueInfo with idle state."""
        from eventsource.bus.rabbitmq import QueueInfo

        info = QueueInfo(
            name="test-queue",
            message_count=5,
            consumer_count=0,
            state="idle",
        )

        assert info.state == "idle"
        assert info.consumer_count == 0

    def test_is_dataclass(self) -> None:
        """Test that QueueInfo is a proper dataclass."""
        from dataclasses import is_dataclass

        from eventsource.bus.rabbitmq import QueueInfo

        assert is_dataclass(QueueInfo)

    def test_equality(self) -> None:
        """Test QueueInfo equality comparison."""
        from eventsource.bus.rabbitmq import QueueInfo

        info1 = QueueInfo(name="q1", message_count=10, consumer_count=2)
        info2 = QueueInfo(name="q1", message_count=10, consumer_count=2)
        info3 = QueueInfo(name="q2", message_count=10, consumer_count=2)

        assert info1 == info2
        assert info1 != info3


class TestHealthCheckResult:
    """Tests for HealthCheckResult dataclass."""

    def test_healthy_result(self) -> None:
        """Test HealthCheckResult for healthy state."""
        from eventsource.bus.rabbitmq import HealthCheckResult

        result = HealthCheckResult(
            healthy=True,
            connection_status="connected",
            channel_status="open",
            queue_status="accessible",
        )

        assert result.healthy is True
        assert result.connection_status == "connected"
        assert result.channel_status == "open"
        assert result.queue_status == "accessible"
        assert result.error is None

    def test_unhealthy_result(self) -> None:
        """Test HealthCheckResult for unhealthy state."""
        from eventsource.bus.rabbitmq import HealthCheckResult

        result = HealthCheckResult(
            healthy=False,
            connection_status="disconnected",
            channel_status="not_initialized",
            queue_status="not_initialized",
            error="Not connected to RabbitMQ",
        )

        assert result.healthy is False
        assert result.error == "Not connected to RabbitMQ"

    def test_with_dlq_status(self) -> None:
        """Test HealthCheckResult with DLQ status."""
        from eventsource.bus.rabbitmq import HealthCheckResult

        result = HealthCheckResult(
            healthy=True,
            connection_status="connected",
            channel_status="open",
            queue_status="accessible",
            dlq_status="accessible",
        )

        assert result.dlq_status == "accessible"

    def test_with_dlq_disabled(self) -> None:
        """Test HealthCheckResult with DLQ disabled."""
        from eventsource.bus.rabbitmq import HealthCheckResult

        result = HealthCheckResult(
            healthy=True,
            connection_status="connected",
            channel_status="open",
            queue_status="accessible",
            dlq_status="disabled",
        )

        assert result.dlq_status == "disabled"

    def test_with_details(self) -> None:
        """Test HealthCheckResult with details dictionary."""
        from eventsource.bus.rabbitmq import HealthCheckResult

        details = {
            "exchange": "test-exchange",
            "queue": "test-queue",
            "consumer_group": "test-group",
            "consuming": True,
        }

        result = HealthCheckResult(
            healthy=True,
            connection_status="connected",
            channel_status="open",
            queue_status="accessible",
            details=details,
        )

        assert result.details == details
        assert result.details["exchange"] == "test-exchange"

    def test_is_dataclass(self) -> None:
        """Test that HealthCheckResult is a proper dataclass."""
        from dataclasses import is_dataclass

        from eventsource.bus.rabbitmq import HealthCheckResult

        assert is_dataclass(HealthCheckResult)


# ============================================================================
# get_queue_info() Method Tests (P2-007)
# ============================================================================


class TestGetQueueInfo:
    """Tests for RabbitMQEventBus.get_queue_info() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.mark.asyncio
    async def test_queue_info_when_not_connected(self, config: RabbitMQEventBusConfig) -> None:
        """Test get_queue_info returns error state when not connected."""
        from eventsource.bus.rabbitmq import QueueInfo, RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        info = await bus.get_queue_info()

        assert isinstance(info, QueueInfo)
        assert info.name == config.queue_name
        assert info.message_count == 0
        assert info.consumer_count == 0
        assert info.state == "error"
        assert info.error == "Not connected to RabbitMQ"

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_info_when_connected(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test get_queue_info returns queue statistics when connected."""
        from eventsource.bus.rabbitmq import QueueInfo, RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue declaration result with bind as AsyncMock
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 42
        mock_queue.declaration_result.consumer_count = 3
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        info = await bus.get_queue_info()

        assert isinstance(info, QueueInfo)
        assert info.name == config.queue_name
        assert info.message_count == 42
        assert info.consumer_count == 3
        assert info.state == "running"
        assert info.error is None

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_info_idle_state_when_no_consumers(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test get_queue_info returns idle state when no consumers."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue with zero consumers (use AsyncMock for bind method)
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 10
        mock_queue.declaration_result.consumer_count = 0
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        info = await bus.get_queue_info()

        assert info.state == "idle"
        assert info.consumer_count == 0
        assert info.message_count == 10

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_info_handles_exception(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test get_queue_info handles exceptions gracefully."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue for connect (succeeds)
        mock_queue_for_connect = AsyncMock()
        mock_queue_for_connect.declaration_result = MagicMock()
        mock_queue_for_connect.declaration_result.message_count = 0
        mock_queue_for_connect.declaration_result.consumer_count = 0

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        # First calls during connect succeed, then get_queue_info call fails
        call_count = 0

        async def declare_queue_side_effect(**kwargs: dict) -> AsyncMock:
            nonlocal call_count
            call_count += 1
            if kwargs.get("passive"):
                raise Exception("Queue not found")
            return mock_queue_for_connect

        mock_channel.declare_queue = AsyncMock(side_effect=declare_queue_side_effect)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        info = await bus.get_queue_info()

        assert info.state == "error"
        assert info.error == "Queue not found"
        assert info.message_count == 0
        assert info.consumer_count == 0

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_queue_info_uses_passive_declaration(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test get_queue_info uses passive queue declaration."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 5
        mock_queue.declaration_result.consumer_count = 1
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.get_queue_info()

        # Verify passive=True was passed in the last call (for get_queue_info)
        last_call = mock_channel.declare_queue.call_args_list[-1]
        assert last_call.kwargs.get("passive") is True
        assert last_call.kwargs.get("name") == config.queue_name


# ============================================================================
# health_check() Method Tests (P2-007)
# ============================================================================


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
        from eventsource.bus.rabbitmq import HealthCheckResult, RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)

        result = await bus.health_check()

        assert isinstance(result, HealthCheckResult)
        assert result.healthy is False
        assert result.connection_status == "disconnected"
        assert result.channel_status == "not_initialized"
        assert "Not connected to RabbitMQ" in (result.error or "")

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_healthy_state(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check returns healthy when all checks pass."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue (use AsyncMock so bind() can be awaited)
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.health_check()

        assert result.healthy is True
        assert result.connection_status == "connected"
        assert result.channel_status == "open"
        assert result.queue_status == "accessible"
        assert result.dlq_status == "accessible"
        assert result.error is None

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_includes_details(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check includes configuration details."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue (use AsyncMock so bind() can be awaited)
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

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

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_includes_stats(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check includes stats in details."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue (use AsyncMock so bind() can be awaited)
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

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

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_connection_closed(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check when connection is closed."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Simulate connection closed
        mock_connection.is_closed = True

        result = await bus.health_check()

        assert result.healthy is False
        assert result.connection_status == "closed"
        assert "RabbitMQ connection is closed" in (result.error or "")

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_channel_closed(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check when channel is closed."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Simulate channel closed
        mock_channel.is_closed = True

        result = await bus.health_check()

        assert result.healthy is False
        assert result.channel_status == "closed"
        assert "AMQP channel is closed" in (result.error or "")

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_queue_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check when queue check fails."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue for connect (use AsyncMock for bind)
        mock_queue_for_connect = AsyncMock()
        mock_queue_for_connect.declaration_result = MagicMock()
        mock_queue_for_connect.declaration_result.message_count = 0
        mock_queue_for_connect.declaration_result.consumer_count = 0

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

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
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config_no_dlq)

        result = await bus.health_check()

        assert result.dlq_status == "disabled"

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_dlq_error_doesnt_fail_overall(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that DLQ check failure doesn't make overall health fail."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Setup mock connection and channel
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        # Setup mock queue (use AsyncMock for bind)
        mock_queue = AsyncMock()
        mock_queue.declaration_result = MagicMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        # Setup mock exchange
        mock_exchange = AsyncMock()
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)

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

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_health_check_multiple_errors(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test health_check with multiple errors combines them."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)
        # Not connected, so multiple errors expected

        result = await bus.health_check()

        assert result.healthy is False
        # Error should contain both connection and channel errors
        assert result.error is not None
        assert "Not connected to RabbitMQ" in result.error
        assert "Channel not initialized" in result.error


# =============================================================================
# OpenTelemetry Tracing Tests (P3-001)
# =============================================================================


class TestOpenTelemetryTracing:
    """Tests for OpenTelemetry tracing integration (P3-001)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration with tracing enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=True,
        )

    @pytest.fixture
    def config_no_tracing(self) -> RabbitMQEventBusConfig:
        """Create test configuration with tracing disabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=False,
        )

    def test_otel_available_exported(self) -> None:
        """Test that OTEL_AVAILABLE is properly exported."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE

        # OTEL_AVAILABLE should be a boolean
        assert isinstance(OTEL_AVAILABLE, bool)

    def test_tracer_is_none_when_tracing_disabled(
        self, config_no_tracing: RabbitMQEventBusConfig
    ) -> None:
        """Test that _tracer is disabled when tracing is disabled."""
        bus = RabbitMQEventBus(config=config_no_tracing)

        # With composition-based tracing, _tracer is always set but disabled
        assert bus._tracer is not None
        assert bus._tracer.enabled is False
        assert bus._enable_tracing is False

    def test_tracer_set_when_enabled_and_available(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _tracer is set when enabled and available."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE

        bus = RabbitMQEventBus(config=config)

        # With composition-based tracing, _tracer is always set
        assert bus._tracer is not None
        if OTEL_AVAILABLE:
            assert bus._enable_tracing is True
            assert bus._tracer.enabled is True
        else:
            assert bus._tracer.enabled is False

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_creates_span_when_tracing_enabled(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish creates a span when tracing is enabled."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._tracer = mock_tracer

        await bus.publish([event])

        # Verify span was created and ended
        mock_tracer.start_span.assert_called()
        mock_span.end.assert_called()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_span_has_correct_attributes(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish span includes correct attributes."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus
        from eventsource.observability.attributes import (
            ATTR_EVENT_TYPE,
            ATTR_MESSAGING_DESTINATION,
            ATTR_MESSAGING_SYSTEM,
        )

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._tracer = mock_tracer

        await bus.publish([event])

        # Verify span was created with correct name pattern (now uses eventsource.event_bus.publish)
        call_args = mock_tracer.start_span.call_args
        span_name = call_args[0][0]
        assert "eventsource.event_bus.publish" in span_name

        # Verify attributes were set using standard constants
        attributes = call_args[1]["attributes"]
        assert attributes[ATTR_MESSAGING_SYSTEM] == "rabbitmq"
        assert attributes[ATTR_MESSAGING_DESTINATION] == config.exchange_name
        assert attributes[ATTR_EVENT_TYPE] == "TestEvent"
        assert attributes["aggregate.type"] == "TestAggregate"

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_no_span_when_tracing_disabled(
        self, mock_aio_pika: MagicMock, config_no_tracing: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish does not create span when tracing is disabled."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config_no_tracing)
        await bus.connect()

        # With composition-based tracing, _tracer is always set but disabled
        assert bus._tracer is not None
        assert bus._tracer.enabled is False
        assert bus._enable_tracing is False

        # Publish should still work without tracing
        await bus.publish([event])
        mock_exchange.publish.assert_called_once()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_span_records_exception_on_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish span records exception when publish fails."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        # Make publish fail
        publish_error = Exception("Publish failed")
        mock_exchange.publish = AsyncMock(side_effect=publish_error)

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._tracer = mock_tracer

        with pytest.raises(Exception, match="Publish failed"):
            await bus.publish([event])

        # Verify exception was recorded on span
        mock_span.record_exception.assert_called_once()
        mock_span.end.assert_called()

    def test_create_message_with_tracing_injects_context(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _create_message_with_tracing injects trace context."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)

        # Mock inject to verify it gets called
        with patch("eventsource.bus.rabbitmq.inject") as mock_inject:
            mock_span = MagicMock()
            message = bus._create_message_with_tracing(event, mock_span)

            # Verify inject was called
            mock_inject.assert_called_once()
            # Message should be created
            assert message is not None

    def test_create_message_with_tracing_without_span(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _create_message_with_tracing works without span."""

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)

        # Without span, inject should not be called
        with patch("eventsource.bus.rabbitmq.inject") as mock_inject:
            message = bus._create_message_with_tracing(event, None)

            # Verify inject was not called
            mock_inject.assert_not_called()
            # Message should still be created
            assert message is not None


class TestOpenTelemetryConsumerTracing:
    """Tests for OpenTelemetry consumer tracing (P3-001)."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration with tracing enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=True,
        )

    @pytest.fixture
    def registry(self) -> EventRegistry:
        """Create an event registry with test events."""
        registry = EventRegistry()

        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        registry.register(TestEvent)
        return registry

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_process_message_creates_consumer_span(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        registry: EventRegistry,
    ) -> None:
        """Test that _process_message creates a consumer span."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create mock message
        mock_message = MagicMock()
        mock_message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-retry-count": 0,
        }
        mock_message.routing_key = "TestAggregate.TestEvent"
        mock_message.message_id = "test-message-id"
        mock_message.body = b'{"event_type": "TestEvent", "aggregate_type": "TestAggregate", "aggregate_id": "12345678-1234-1234-1234-123456789012", "aggregate_version": 1}'
        mock_message.ack = AsyncMock()

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        # Register a handler
        handler_called = []

        async def test_handler(event: DomainEvent) -> None:
            handler_called.append(event)

        bus.subscribe(registry.get("TestEvent"), test_handler)

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span
        bus._tracer = mock_tracer

        # Also mock extract for context propagation
        with patch("eventsource.bus.rabbitmq.extract") as mock_extract:
            mock_extract.return_value = None

            await bus._process_message(mock_message)

            # Verify span was created
            mock_tracer.start_span.assert_called()
            # Verify extract was called for context propagation
            mock_extract.assert_called()
            # Verify span was ended
            mock_span.end.assert_called()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dispatch_event_creates_handler_spans(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        registry: EventRegistry,
    ) -> None:
        """Test that _dispatch_event creates handler spans."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Create mock message
        mock_message = MagicMock()
        mock_message.message_id = "test-message-id"

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        # Register a handler
        async def test_handler(e: DomainEvent) -> None:
            pass

        bus.subscribe(TestEvent, test_handler)

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_parent_span = MagicMock()
        mock_handler_span = MagicMock()
        mock_tracer.start_span.return_value = mock_handler_span
        bus._tracer = mock_tracer

        await bus._dispatch_event(event, mock_message, mock_parent_span)

        # Verify handler span was created
        mock_tracer.start_span.assert_called()
        call_args = mock_tracer.start_span.call_args
        span_name = call_args[0][0]
        # Updated to use new span naming convention
        assert "eventsource.event_bus.handle" in span_name

        # Verify handler span was ended
        mock_handler_span.end.assert_called()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_dispatch_event_no_handler_spans_without_parent(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        registry: EventRegistry,
    ) -> None:
        """Test that _dispatch_event does not create handler spans without parent span."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Create mock message
        mock_message = MagicMock()
        mock_message.message_id = "test-message-id"

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        # Register a handler
        handler_called = []

        async def test_handler(e: DomainEvent) -> None:
            handler_called.append(e)

        bus.subscribe(TestEvent, test_handler)

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        bus._tracer = mock_tracer

        # Without parent_span, handler spans should not be created
        await bus._dispatch_event(event, mock_message, parent_span=None)

        # Handler should still be called
        assert len(handler_called) == 1
        # But no spans should be created (because parent_span is None)
        mock_tracer.start_span.assert_not_called()

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_handler_span_records_exception_on_error(
        self,
        mock_aio_pika: MagicMock,
        config: RabbitMQEventBusConfig,
        registry: EventRegistry,
    ) -> None:
        """Test that handler span records exception when handler fails."""
        from eventsource.bus.rabbitmq import OTEL_AVAILABLE, RabbitMQEventBus

        if not OTEL_AVAILABLE:
            pytest.skip("OpenTelemetry not installed")

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        # Create mock message
        mock_message = MagicMock()
        mock_message.message_id = "test-message-id"

        bus = RabbitMQEventBus(config=config, event_registry=registry)

        # Register a failing handler
        async def failing_handler(e: DomainEvent) -> None:
            raise ValueError("Handler failed")

        bus.subscribe(TestEvent, failing_handler)

        # Mock the tracer via TracingMixin's _tracer attribute
        mock_tracer = MagicMock()
        mock_parent_span = MagicMock()
        mock_handler_span = MagicMock()
        mock_tracer.start_span.return_value = mock_handler_span
        bus._tracer = mock_tracer

        with pytest.raises(ValueError, match="Handler failed"):
            await bus._dispatch_event(event, mock_message, mock_parent_span)

        # Verify exception was recorded on handler span
        mock_handler_span.record_exception.assert_called_once()
        mock_handler_span.end.assert_called()


class TestOpenTelemetryGracefulDegradation:
    """Tests for graceful degradation when OpenTelemetry is not available."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration with tracing enabled."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            enable_tracing=True,
        )

    def test_tracer_none_when_otel_unavailable(self, config: RabbitMQEventBusConfig) -> None:
        """Test that _tracer is disabled gracefully when OTEL unavailable."""
        # With composition-based tracing, _tracer is always set but may be disabled
        bus = RabbitMQEventBus(config=config)

        # _tracer is always set with composition-based tracing
        assert bus._tracer is not None

        from eventsource.bus.rabbitmq import OTEL_AVAILABLE

        if not OTEL_AVAILABLE:
            # When OTEL is not available, _tracer should be a NullTracer (disabled)
            assert bus._tracer.enabled is False
        else:
            # When OTEL is available, tracer should be enabled
            assert bus._tracer.enabled is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_works_without_otel(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish works correctly when OTEL is not available."""
        # Set up mocks
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Make OTEL unavailable
        with patch("eventsource.bus.rabbitmq.OTEL_AVAILABLE", False):
            # Should not raise any exceptions
            await bus.publish([event])

            # Verify publish was called
            mock_exchange.publish.assert_called_once()

    def test_create_message_with_tracing_works_without_otel(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that _create_message_with_tracing works when OTEL is unavailable."""

        # Create test event
        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "TestAggregate"

        event = TestEvent(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)

        # Make OTEL unavailable
        with patch("eventsource.bus.rabbitmq.OTEL_AVAILABLE", False):
            mock_span = MagicMock()
            # Should not raise, even with a span provided
            message = bus._create_message_with_tracing(event, mock_span)
            assert message is not None


# =============================================================================
# Direct Exchange Tests (P3-002)
# =============================================================================


class TestDirectExchangeConfig:
    """Tests for direct exchange configuration options."""

    def test_routing_key_pattern_default_is_none(self) -> None:
        """Test that routing_key_pattern is None by default."""
        config = RabbitMQEventBusConfig()
        assert config.routing_key_pattern is None

    def test_routing_key_pattern_can_be_set(self) -> None:
        """Test that routing_key_pattern can be explicitly set."""
        config = RabbitMQEventBusConfig(routing_key_pattern="custom.key")
        assert config.routing_key_pattern == "custom.key"

    def test_get_effective_routing_key_topic_default(self) -> None:
        """Test effective routing key for topic exchange without explicit pattern."""
        config = RabbitMQEventBusConfig(exchange_type="topic")
        assert config.get_effective_routing_key() == "#"

    def test_get_effective_routing_key_direct_default(self) -> None:
        """Test effective routing key for direct exchange without explicit pattern."""
        config = RabbitMQEventBusConfig(
            exchange_name="events",
            exchange_type="direct",
            consumer_group="workers",
        )
        # Direct exchange defaults to queue_name for work queue pattern
        assert config.get_effective_routing_key() == "events.workers"

    def test_get_effective_routing_key_fanout_default(self) -> None:
        """Test effective routing key for fanout exchange."""
        config = RabbitMQEventBusConfig(exchange_type="fanout")
        # Fanout ignores routing key
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_headers_default(self) -> None:
        """Test effective routing key for headers exchange."""
        config = RabbitMQEventBusConfig(exchange_type="headers")
        # Headers ignores routing key
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_explicit_pattern_overrides(self) -> None:
        """Test that explicit routing_key_pattern overrides defaults."""
        config = RabbitMQEventBusConfig(
            exchange_type="direct",
            routing_key_pattern="my.custom.pattern",
        )
        assert config.get_effective_routing_key() == "my.custom.pattern"

    def test_get_effective_routing_key_empty_pattern_allowed(self) -> None:
        """Test that empty string routing_key_pattern is allowed."""
        config = RabbitMQEventBusConfig(
            exchange_type="topic",
            routing_key_pattern="",  # Explicitly empty
        )
        assert config.get_effective_routing_key() == ""

    def test_get_effective_routing_key_unknown_exchange_type(self) -> None:
        """Test behavior with unknown exchange type defaults to topic behavior."""
        config = RabbitMQEventBusConfig(exchange_type="unknown")
        assert config.get_effective_routing_key() == "#"

    def test_get_effective_routing_key_case_insensitive(self) -> None:
        """Test that exchange type comparison is case insensitive."""
        config1 = RabbitMQEventBusConfig(exchange_type="DIRECT", consumer_group="test")
        config2 = RabbitMQEventBusConfig(exchange_type="Direct", consumer_group="test")
        config3 = RabbitMQEventBusConfig(exchange_type="direct", consumer_group="test")

        expected = "events.test"  # queue_name = exchange_name.consumer_group
        assert config1.get_effective_routing_key() == expected
        assert config2.get_effective_routing_key() == expected
        assert config3.get_effective_routing_key() == expected


class TestDirectExchangeBinding:
    """Tests for direct exchange binding behavior."""

    @pytest.fixture
    def direct_exchange_config(self) -> RabbitMQEventBusConfig:
        """Create configuration for direct exchange testing."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            exchange_type="direct",
            consumer_group="test-workers",
            enable_dlq=False,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_direct_exchange_uses_queue_name_as_routing_key(
        self, mock_aio_pika: MagicMock, direct_exchange_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that direct exchange binds with queue_name as routing key."""
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

        bus = RabbitMQEventBus(config=direct_exchange_config)
        await bus.connect()

        # Verify queue was bound with queue_name as routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == direct_exchange_config.queue_name

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_topic_exchange_uses_wildcard_routing_key(self, mock_aio_pika: MagicMock) -> None:
        """Test that topic exchange binds with '#' wildcard routing key."""
        config = RabbitMQEventBusConfig(
            exchange_type="topic",
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

        # Verify queue was bound with '#' routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "#"

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_exchange_uses_empty_routing_key(self, mock_aio_pika: MagicMock) -> None:
        """Test that fanout exchange binds with empty routing key."""
        config = RabbitMQEventBusConfig(
            exchange_type="fanout",
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

        # Verify queue was bound with empty routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == ""

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_custom_routing_key_pattern_overrides_default(
        self, mock_aio_pika: MagicMock
    ) -> None:
        """Test that custom routing_key_pattern overrides exchange type default."""
        config = RabbitMQEventBusConfig(
            exchange_type="direct",
            routing_key_pattern="custom.routing.key",
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

        # Verify queue was bound with custom routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "custom.routing.key"


class TestBindEventType:
    """Tests for bind_event_type helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            exchange_type="direct",
            consumer_group="test-workers",
            enable_dlq=False,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_event_type_creates_binding(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that bind_event_type creates a binding for the event type."""
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

        class OrderCreated(DomainEvent):
            event_type: str = "OrderCreated"
            aggregate_type: str = "Order"

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Reset mock to clear initial bind call
        mock_queue.bind.reset_mock()

        # Bind for specific event type
        await bus.bind_event_type(OrderCreated)

        # Verify binding was created with correct routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "Order.OrderCreated"

    @pytest.mark.asyncio
    async def test_bind_event_type_raises_when_not_connected(self) -> None:
        """Test that bind_event_type raises RuntimeError when not connected."""
        config = RabbitMQEventBusConfig()

        class TestEvent(DomainEvent):
            event_type: str = "TestEvent"
            aggregate_type: str = "Test"

        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Not connected"):
            await bus.bind_event_type(TestEvent)


class TestBindRoutingKey:
    """Tests for bind_routing_key helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            exchange_type="direct",
            consumer_group="test-workers",
            enable_dlq=False,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_routing_key_creates_binding(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that bind_routing_key creates a binding with the specified key."""
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

        # Reset mock to clear initial bind call
        mock_queue.bind.reset_mock()

        # Bind with custom routing key
        await bus.bind_routing_key("Order.OrderCreated")

        # Verify binding was created
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "Order.OrderCreated"

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_bind_routing_key_supports_wildcards_for_topic(
        self, mock_aio_pika: MagicMock
    ) -> None:
        """Test that bind_routing_key works with wildcards for topic exchange."""
        config = RabbitMQEventBusConfig(
            exchange_type="topic",
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

        # Reset mock to clear initial bind call
        mock_queue.bind.reset_mock()

        # Bind with wildcard pattern
        await bus.bind_routing_key("Order.*")

        # Verify binding was created with wildcard
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == "Order.*"

    @pytest.mark.asyncio
    async def test_bind_routing_key_raises_when_not_connected(self) -> None:
        """Test that bind_routing_key raises RuntimeError when not connected."""
        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)

        with pytest.raises(RuntimeError, match="Not connected"):
            await bus.bind_routing_key("some.key")


class TestDirectExchangeWorkQueuePattern:
    """Tests for work queue pattern with direct exchange.

    Work queue pattern: Multiple consumers share the same queue and compete
    for messages. RabbitMQ load-balances messages between consumers.
    """

    @pytest.fixture
    def work_queue_config(self) -> RabbitMQEventBusConfig:
        """Create configuration for work queue pattern."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",  # All workers share this queue
            enable_dlq=False,
        )

    def test_work_queue_uses_consistent_queue_name(
        self, work_queue_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that work queue config produces consistent queue name."""
        # Multiple workers with same config should use same queue
        config1 = RabbitMQEventBusConfig(
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",
            consumer_name="worker-1",
        )
        config2 = RabbitMQEventBusConfig(
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",
            consumer_name="worker-2",
        )

        # Queue names should be identical (based on exchange + consumer_group)
        assert config1.queue_name == config2.queue_name
        assert config1.queue_name == "tasks.workers"

    def test_work_queue_routing_key_matches_queue_name(
        self, work_queue_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that work queue routing key matches queue name."""
        # This ensures published messages with queue_name as routing key
        # are delivered to the correct queue
        routing_key = work_queue_config.get_effective_routing_key()
        assert routing_key == work_queue_config.queue_name

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_work_queue_multiple_consumers_same_binding(
        self, mock_aio_pika: MagicMock, work_queue_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that multiple workers create same binding for work queue pattern."""
        # Set up mocks
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

        # Create two workers
        worker1_config = RabbitMQEventBusConfig(
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",
            consumer_name="worker-1",
            enable_dlq=False,
        )
        worker2_config = RabbitMQEventBusConfig(
            exchange_name="tasks",
            exchange_type="direct",
            consumer_group="workers",
            consumer_name="worker-2",
            enable_dlq=False,
        )

        bus1 = RabbitMQEventBus(config=worker1_config)
        bus2 = RabbitMQEventBus(config=worker2_config)

        await bus1.connect()
        first_bind_call = mock_queue.bind.call_args

        mock_queue.bind.reset_mock()
        await bus2.connect()
        second_bind_call = mock_queue.bind.call_args

        # Both should bind with the same routing key
        assert first_bind_call.kwargs["routing_key"] == "tasks.workers"
        assert second_bind_call.kwargs["routing_key"] == "tasks.workers"


class TestDirectExchangeRoutingKeyGeneration:
    """Tests for routing key generation in publish for direct exchange."""

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_uses_aggregate_event_routing_key(self, mock_aio_pika: MagicMock) -> None:
        """Test that publish uses {aggregate_type}.{event_type} as routing key."""
        config = RabbitMQEventBusConfig(
            exchange_type="direct",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        class OrderCreated(DomainEvent):
            event_type: str = "OrderCreated"
            aggregate_type: str = "Order"

        event = OrderCreated(aggregate_id=uuid4())

        bus = RabbitMQEventBus(config=config)
        await bus.connect()
        await bus.publish([event])

        # Verify publish was called with correct routing key
        mock_exchange.publish.assert_called_once()
        call_kwargs = mock_exchange.publish.call_args.kwargs
        assert call_kwargs["routing_key"] == "Order.OrderCreated"


# =============================================================================
# Fanout Exchange Comprehensive Tests (P3-003)
# =============================================================================


class TestFanoutExchangeBroadcastBehavior:
    """Comprehensive tests for fanout exchange broadcast behavior.

    Fanout exchanges broadcast all messages to all bound queues, regardless
    of routing key. These tests validate:
    - Routing key is ignored for fanout exchanges
    - Multiple consumer groups can receive the same broadcast
    - Configuration is correct for fanout use cases
    """

    @pytest.fixture
    def fanout_config(self) -> RabbitMQEventBusConfig:
        """Create configuration for fanout exchange testing."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="broadcast-events",
            exchange_type="fanout",
            consumer_group="notifications",
            enable_dlq=False,
        )

    def test_fanout_config_returns_empty_routing_key(
        self, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that fanout config correctly returns empty routing key."""
        assert fanout_config.get_effective_routing_key() == ""
        assert fanout_config.exchange_type == "fanout"

    def test_fanout_config_routing_key_pattern_override(self) -> None:
        """Test that explicit routing_key_pattern still works for fanout.

        Even though fanout ignores routing keys, explicit pattern should
        still be used if configured (for logging/tracing purposes).
        """
        config = RabbitMQEventBusConfig(
            exchange_type="fanout",
            routing_key_pattern="custom.pattern",
        )
        # Explicit pattern takes precedence
        assert config.get_effective_routing_key() == "custom.pattern"

    def test_fanout_config_case_insensitive(self) -> None:
        """Test that 'FANOUT' and 'Fanout' work the same as 'fanout'."""
        config_upper = RabbitMQEventBusConfig(exchange_type="FANOUT")
        config_mixed = RabbitMQEventBusConfig(exchange_type="Fanout")
        config_lower = RabbitMQEventBusConfig(exchange_type="fanout")

        assert config_upper.get_effective_routing_key() == ""
        assert config_mixed.get_effective_routing_key() == ""
        assert config_lower.get_effective_routing_key() == ""

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_exchange_declared_with_correct_type(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that fanout exchange is declared with ExchangeType.FANOUT."""
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

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()

        # Verify exchange was declared with FANOUT type
        mock_channel.declare_exchange.assert_called()
        call_kwargs = mock_channel.declare_exchange.call_args.kwargs
        assert call_kwargs["type"] == ExchangeType.FANOUT
        assert call_kwargs["name"] == "broadcast-events"
        assert call_kwargs["durable"] is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_queue_bound_with_empty_routing_key(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that queue is bound with empty routing key for fanout."""
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

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()

        # Verify queue was bound with empty routing key
        mock_queue.bind.assert_called_once()
        call_kwargs = mock_queue.bind.call_args.kwargs
        assert call_kwargs["routing_key"] == ""
        assert call_kwargs["exchange"] == mock_exchange

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_publish_includes_routing_key_for_logging(
        self, mock_aio_pika: MagicMock, fanout_config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publish still generates routing key for logging/tracing.

        Even though fanout exchanges ignore routing keys, we still generate
        them for observability purposes (logging, tracing, debugging).
        """
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        class NotificationSent(DomainEvent):
            event_type: str = "NotificationSent"
            aggregate_type: str = "Notification"
            message: str = "test"

        event = NotificationSent(aggregate_id=uuid4(), message="Hello!")

        bus = RabbitMQEventBus(config=fanout_config)
        await bus.connect()
        await bus.publish([event])

        # Routing key is still generated and passed (for logging)
        # Fanout exchange ignores it, but it's included for observability
        mock_exchange.publish.assert_called_once()
        call_kwargs = mock_exchange.publish.call_args.kwargs
        assert call_kwargs["routing_key"] == "Notification.NotificationSent"


class TestFanoutExchangeMultipleConsumerGroups:
    """Tests for multiple consumer groups with fanout exchange.

    With fanout exchanges, each consumer group gets its own queue, and all
    queues bound to the exchange receive all messages (broadcast pattern).
    """

    def test_different_consumer_groups_have_different_queue_names(self) -> None:
        """Test that different consumer groups create different queue names."""
        config1 = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="notifications",
        )
        config2 = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="audit-log",
        )
        config3 = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="cache-invalidation",
        )

        # Same exchange, different queues
        assert config1.exchange_name == config2.exchange_name == config3.exchange_name
        assert config1.queue_name == "broadcast.notifications"
        assert config2.queue_name == "broadcast.audit-log"
        assert config3.queue_name == "broadcast.cache-invalidation"

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_multiple_buses_same_fanout_exchange(self, mock_aio_pika: MagicMock) -> None:
        """Test that multiple buses can connect to the same fanout exchange.

        This simulates the pattern where multiple consumer groups (services)
        each receive all broadcast messages independently.
        """
        # Create shared mock connection infrastructure
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel1 = AsyncMock()
        mock_channel1.is_closed = False
        mock_channel2 = AsyncMock()
        mock_channel2.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue1 = AsyncMock()
        mock_queue2 = AsyncMock()

        # Track which channel/queue is being used
        call_count = {"channel": 0, "queue": 0}

        def get_channel() -> AsyncMock:
            call_count["channel"] += 1
            return mock_channel1 if call_count["channel"] == 1 else mock_channel2

        def get_queue(name: str, **kwargs) -> AsyncMock:
            call_count["queue"] += 1
            return mock_queue1 if call_count["queue"] == 1 else mock_queue2

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(side_effect=get_channel)
        mock_channel1.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel2.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel1.declare_queue = AsyncMock(side_effect=get_queue)
        mock_channel2.declare_queue = AsyncMock(side_effect=get_queue)

        # Configure two consumer groups on the same fanout exchange
        config1 = RabbitMQEventBusConfig(
            exchange_name="system-events",
            exchange_type="fanout",
            consumer_group="email-service",
            enable_dlq=False,
        )
        config2 = RabbitMQEventBusConfig(
            exchange_name="system-events",
            exchange_type="fanout",
            consumer_group="sms-service",
            enable_dlq=False,
        )

        bus1 = RabbitMQEventBus(config=config1)
        bus2 = RabbitMQEventBus(config=config2)

        await bus1.connect()
        await bus2.connect()

        # Both should connect successfully
        assert bus1._connected
        assert bus2._connected

        # Verify different queue names were declared
        queue_declare_calls = [
            mock_channel1.declare_queue.call_args,
            mock_channel2.declare_queue.call_args,
        ]
        queue_names = {call.kwargs.get("name") for call in queue_declare_calls if call}
        assert "system-events.email-service" in queue_names
        assert "system-events.sms-service" in queue_names


class TestFanoutExchangeUseCases:
    """Tests demonstrating fanout exchange use cases.

    Fanout exchanges are ideal for:
    1. Notifications - broadcast to all notification handlers
    2. Logging/Audit - all services receive all events
    3. Cache Invalidation - broadcast cache clear commands
    4. Development/Debug - monitor all events during development
    """

    def test_notification_broadcast_config(self) -> None:
        """Test configuration for notification broadcast use case."""
        config = RabbitMQEventBusConfig(
            exchange_name="notifications",
            exchange_type="fanout",
            consumer_group="email-handler",
            durable=True,  # Survive restarts
            auto_delete=False,  # Don't delete on disconnect
        )

        assert config.exchange_type == "fanout"
        assert config.get_effective_routing_key() == ""
        assert config.queue_name == "notifications.email-handler"

    def test_audit_log_broadcast_config(self) -> None:
        """Test configuration for audit logging use case."""
        config = RabbitMQEventBusConfig(
            exchange_name="domain-events",
            exchange_type="fanout",
            consumer_group="audit-logger",
            durable=True,
            prefetch_count=50,  # Higher throughput for logging
        )

        assert config.exchange_type == "fanout"
        assert config.queue_name == "domain-events.audit-logger"

    def test_cache_invalidation_broadcast_config(self) -> None:
        """Test configuration for cache invalidation use case."""
        config = RabbitMQEventBusConfig(
            exchange_name="cache-invalidation",
            exchange_type="fanout",
            consumer_group="frontend-cache",
            durable=False,  # Cache invalidation can be transient
            auto_delete=True,  # OK to lose messages if consumer disconnects
        )

        assert config.exchange_type == "fanout"
        assert config.durable is False
        assert config.auto_delete is True

    def test_development_monitoring_config(self) -> None:
        """Test configuration for development event monitoring."""
        config = RabbitMQEventBusConfig(
            exchange_name="all-events",
            exchange_type="fanout",
            consumer_group="dev-monitor",
            durable=False,  # Don't need durability for dev
            auto_delete=True,  # Clean up when dev session ends
            enable_dlq=False,  # No need for DLQ in dev
        )

        assert config.exchange_type == "fanout"
        assert config.enable_dlq is False
        assert config.auto_delete is True


class TestFanoutExchangeWithDLQ:
    """Tests for fanout exchange with dead letter queue."""

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_with_dlq_enabled(self, mock_aio_pika: MagicMock) -> None:
        """Test that fanout exchange works correctly with DLQ enabled."""
        config = RabbitMQEventBusConfig(
            exchange_name="broadcast-events",
            exchange_type="fanout",
            consumer_group="handlers",
            enable_dlq=True,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_dlq_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_dlq_queue = AsyncMock()

        declare_exchange_calls = []

        def mock_declare_exchange(name: str, **kwargs) -> AsyncMock:
            declare_exchange_calls.append({"name": name, **kwargs})
            return mock_dlq_exchange if "_dlq" in name else mock_exchange

        declare_queue_calls = []

        def mock_declare_queue(name: str, **kwargs) -> AsyncMock:
            declare_queue_calls.append({"name": name, **kwargs})
            return mock_dlq_queue if ".dlq" in name else mock_queue

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(side_effect=mock_declare_exchange)
        mock_channel.declare_queue = AsyncMock(side_effect=mock_declare_queue)

        from aio_pika import ExchangeType

        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        # Verify both main and DLQ exchanges were declared
        exchange_names = [call["name"] for call in declare_exchange_calls]
        assert "broadcast-events_dlq" in exchange_names
        assert "broadcast-events" in exchange_names

        # Verify main exchange is fanout type
        main_exchange_call = next(
            call for call in declare_exchange_calls if call["name"] == "broadcast-events"
        )
        assert main_exchange_call["type"] == ExchangeType.FANOUT


class TestFanoutExchangeLogging:
    """Tests for fanout exchange logging behavior."""

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_binding_logged_correctly(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that fanout binding is logged with appropriate information."""
        config = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
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

        # Verify fanout-specific binding log message
        binding_logs = [r for r in caplog.records if "Bound queue" in r.message]
        assert len(binding_logs) >= 1

        # Check log message includes fanout-specific broadcast mode info
        binding_log = binding_logs[0]
        assert "fanout" in binding_log.message.lower()
        assert "broadcast" in binding_log.message.lower()

        # Check log extras contain fanout-specific fields
        assert hasattr(binding_log, "broadcast_mode")
        assert binding_log.broadcast_mode is True
        assert hasattr(binding_log, "routing_key_ignored")
        assert binding_log.routing_key_ignored is True

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_fanout_publish_logged_with_routing_key(
        self, mock_aio_pika: MagicMock, caplog: pytest.LogCaptureFixture
    ) -> None:
        """Test that fanout publish logs include routing key for debugging."""
        config = RabbitMQEventBusConfig(
            exchange_name="broadcast",
            exchange_type="fanout",
            consumer_group="test-group",
            enable_dlq=False,
        )

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_queue.declaration_result.message_count = 0
        mock_queue.declaration_result.consumer_count = 0

        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)
        mock_exchange.publish = AsyncMock()

        class BroadcastEvent(DomainEvent):
            event_type: str = "BroadcastEvent"
            aggregate_type: str = "System"

        event = BroadcastEvent(aggregate_id=uuid4())

        with caplog.at_level(logging.DEBUG):
            bus = RabbitMQEventBus(config=config)
            await bus.connect()
            await bus.publish([event])

        # Verify publish log includes routing key
        publish_logs = [r for r in caplog.records if "Published" in r.message]
        assert len(publish_logs) >= 1


class TestTLSSupport:
    """Tests for TLS/SSL connection support (P3-004).

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

    @patch("eventsource.bus.rabbitmq.aio_pika")
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

    @patch("eventsource.bus.rabbitmq.aio_pika")
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

    @patch("eventsource.bus.rabbitmq.aio_pika")
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

    @patch("eventsource.bus.rabbitmq.aio_pika")
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

    @patch("eventsource.bus.rabbitmq.aio_pika")
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

    @patch("eventsource.bus.rabbitmq.aio_pika")
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

    @patch("eventsource.bus.rabbitmq.aio_pika")
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


# =============================================================================
# Batch Publishing Tests (P3-005)
# =============================================================================


class TestBatchPublishingConfig:
    """Tests for batch publishing configuration options."""

    def test_default_batch_size(self) -> None:
        """Test default batch_size is 100."""
        config = RabbitMQEventBusConfig()
        assert config.batch_size == 100

    def test_custom_batch_size(self) -> None:
        """Test custom batch_size configuration."""
        config = RabbitMQEventBusConfig(batch_size=50)
        assert config.batch_size == 50

    def test_default_max_concurrent_publishes(self) -> None:
        """Test default max_concurrent_publishes is 10."""
        config = RabbitMQEventBusConfig()
        assert config.max_concurrent_publishes == 10

    def test_custom_max_concurrent_publishes(self) -> None:
        """Test custom max_concurrent_publishes configuration."""
        config = RabbitMQEventBusConfig(max_concurrent_publishes=20)
        assert config.max_concurrent_publishes == 20


class TestBatchPublishingStats:
    """Tests for batch publishing statistics fields."""

    def test_stats_have_batch_fields(self) -> None:
        """Test that stats include batch publishing fields."""
        stats = RabbitMQEventBusStats()
        assert hasattr(stats, "batch_publishes")
        assert hasattr(stats, "batch_events_published")
        assert hasattr(stats, "batch_partial_failures")

    def test_batch_stats_default_to_zero(self) -> None:
        """Test that batch stats default to zero."""
        stats = RabbitMQEventBusStats()
        assert stats.batch_publishes == 0
        assert stats.batch_events_published == 0
        assert stats.batch_partial_failures == 0


class TestBatchPublishError:
    """Tests for BatchPublishError exception."""

    def test_batch_publish_error_can_be_raised(self) -> None:
        """Test that BatchPublishError can be raised and caught."""
        from eventsource.bus.rabbitmq import BatchPublishError

        with pytest.raises(BatchPublishError):
            raise BatchPublishError("Test error")

    def test_batch_publish_error_has_results(self) -> None:
        """Test that BatchPublishError contains results dict."""
        from eventsource.bus.rabbitmq import BatchPublishError

        results = {"total": 10, "published": 8, "failed": 2, "chunks": 1}
        error = BatchPublishError("Partial failure", results=results)

        assert error.results == results
        assert error.results["total"] == 10
        assert error.results["published"] == 8
        assert error.results["failed"] == 2

    def test_batch_publish_error_has_errors_list(self) -> None:
        """Test that BatchPublishError contains list of errors."""
        from eventsource.bus.rabbitmq import BatchPublishError

        errors = [Exception("Error 1"), Exception("Error 2")]
        error = BatchPublishError("Multiple failures", errors=errors)

        assert len(error.errors) == 2
        assert str(error.errors[0]) == "Error 1"
        assert str(error.errors[1]) == "Error 2"

    def test_batch_publish_error_defaults(self) -> None:
        """Test that BatchPublishError has sensible defaults."""
        from eventsource.bus.rabbitmq import BatchPublishError

        error = BatchPublishError("Test")

        assert error.results == {"total": 0, "published": 0, "failed": 0, "chunks": 0}
        assert error.errors == []


class TestPublishBatchMethod:
    """Tests for the publish_batch() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration with batch settings."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            batch_size=10,
            max_concurrent_publishes=5,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_empty_list(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch with empty list returns zero stats."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        bus = RabbitMQEventBus(config=config)
        result = await bus.publish_batch([])

        assert result == {"total": 0, "published": 0, "failed": 0, "chunks": 0}

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_success(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch successfully publishes all events."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events)

        assert result["total"] == 5
        assert result["published"] == 5
        assert result["failed"] == 0
        assert result["chunks"] == 1
        assert bus.stats.batch_publishes == 1
        assert bus.stats.batch_events_published == 5

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_with_chunking(self, mock_aio_pika: MagicMock) -> None:
        """Test publish_batch splits large batches into chunks."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

        config = RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            batch_size=5,  # Small batch size to force chunking
            max_concurrent_publishes=3,
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

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(12)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events)

        assert result["total"] == 12
        assert result["published"] == 12
        assert result["chunks"] == 3  # 12 events / 5 batch_size = 3 chunks

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_preserve_order(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch with preserve_order=True publishes sequentially."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        result = await bus.publish_batch(events, preserve_order=True)

        assert result["total"] == 5
        assert result["published"] == 5
        assert result["chunks"] == 1  # Sequential is always 1 chunk
        assert mock_exchange.publish.call_count == 5

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_partial_failure_raises_error(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch raises BatchPublishError on partial failure."""
        from eventsource.bus.rabbitmq import BatchPublishError, RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        # Second call fails
        mock_exchange.publish = AsyncMock(
            side_effect=[None, Exception("Publish failed"), None, None, None]
        )
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        with pytest.raises(BatchPublishError) as exc_info:
            await bus.publish_batch(events)

        assert exc_info.value.results["total"] == 5
        assert exc_info.value.results["published"] == 4
        assert exc_info.value.results["failed"] == 1
        assert len(exc_info.value.errors) == 1
        assert bus.stats.batch_partial_failures == 1

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_batch_auto_connects(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test publish_batch auto-connects if not connected."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        events = [PublishTestEvent(aggregate_id=uuid4(), data="test")]
        bus = RabbitMQEventBus(config=config)

        # Not connected yet
        assert not bus.is_connected

        result = await bus.publish_batch(events)

        # Should have auto-connected
        assert bus.is_connected
        assert result["published"] == 1


class TestPublishMethodBatchOptimization:
    """Tests for batch optimization in the publish() method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
            batch_size=10,
            max_concurrent_publishes=5,
        )

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_single_event_no_batch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publishing a single event does not use batch optimization."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        event = PublishTestEvent(aggregate_id=uuid4(), data="single")
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.publish([event])

        # Single event should not increment batch_publishes
        assert bus.stats.events_published == 1
        assert bus.stats.batch_publishes == 0

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_publish_multiple_events_uses_batch(
        self, mock_aio_pika: MagicMock, config: RabbitMQEventBusConfig
    ) -> None:
        """Test that publishing multiple events uses batch optimization."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        await bus.publish(events)

        # Multiple events should use batch optimization
        assert bus.stats.events_published == 5
        assert bus.stats.batch_publishes == 1
        assert bus.stats.batch_events_published == 5


class TestBatchPublishingStatsDict:
    """Tests for batch stats in get_stats_dict()."""

    @patch("eventsource.bus.rabbitmq.aio_pika")
    @pytest.mark.asyncio
    async def test_stats_dict_includes_batch_fields(self, mock_aio_pika: MagicMock) -> None:
        """Test that get_stats_dict includes batch publishing fields."""
        from eventsource.bus.rabbitmq import RabbitMQEventBus

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

        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        await bus.publish(events)

        stats_dict = bus.get_stats_dict()

        assert "batch_publishes" in stats_dict
        assert "batch_events_published" in stats_dict
        assert "batch_partial_failures" in stats_dict
        assert stats_dict["batch_publishes"] == 1
        assert stats_dict["batch_events_published"] == 5
        assert stats_dict["batch_partial_failures"] == 0
