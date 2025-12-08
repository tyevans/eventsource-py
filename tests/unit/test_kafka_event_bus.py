"""Unit tests for KafkaEventBus implementation.

These tests use mocks to test the KafkaEventBus without requiring
a real Kafka cluster. Focus areas include:
- Security configuration validation
- SSL/TLS context creation
- Credential sanitization for logging
- Handler subscription patterns
"""

import ssl
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch
from uuid import UUID, uuid4

import pytest

from eventsource.bus.kafka import (
    KAFKA_AVAILABLE,
    KafkaEventBus,
    KafkaEventBusConfig,
    KafkaEventBusStats,
    KafkaNotAvailableError,
)
from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry

# --- Test Events ---
# Prefixed with Sample to avoid pytest collection warnings


class SampleOrderCreated(DomainEvent):
    """Test event for order creation."""

    event_type: str = "SampleOrderCreated"
    aggregate_type: str = "Order"
    order_number: str
    customer_id: UUID


class SampleOrderShipped(DomainEvent):
    """Test event for order shipping."""

    event_type: str = "SampleOrderShipped"
    aggregate_type: str = "Order"
    tracking_number: str


class SamplePaymentReceived(DomainEvent):
    """Test event for payment."""

    event_type: str = "SamplePaymentReceived"
    aggregate_type: str = "Payment"
    amount: float


# --- Test Handlers ---


class OrderHandler:
    """Test handler for order events."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class SyncHandler:
    """Synchronous handler for testing."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


class SampleSubscriber:
    """Test subscriber that subscribes to multiple event types."""

    def __init__(self) -> None:
        self.handled_events: list[DomainEvent] = []

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SampleOrderCreated, SampleOrderShipped]

    async def handle(self, event: DomainEvent) -> None:
        self.handled_events.append(event)


# --- Fixtures ---


@pytest.fixture
def event_registry() -> EventRegistry:
    """Create an event registry with test events."""
    registry = EventRegistry()
    registry.register(SampleOrderCreated)
    registry.register(SampleOrderShipped)
    registry.register(SamplePaymentReceived)
    return registry


@pytest.fixture
def sample_event() -> SampleOrderCreated:
    """Create a sample event for testing."""
    return SampleOrderCreated(
        aggregate_id=uuid4(),
        aggregate_version=1,
        order_number="ORD-001",
        customer_id=uuid4(),
    )


# =============================================================================
# KafkaEventBusConfig Tests
# =============================================================================


class TestKafkaEventBusConfig:
    """Tests for KafkaEventBusConfig dataclass."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig()

        assert config.bootstrap_servers == "localhost:9092"
        assert config.topic_prefix == "events"
        assert config.consumer_group == "default"
        assert config.consumer_name is not None  # Auto-generated
        assert config.acks == "all"
        assert config.compression_type == "gzip"
        assert config.batch_size == 16384
        assert config.linger_ms == 5
        assert config.auto_offset_reset == "earliest"
        assert config.security_protocol == "PLAINTEXT"
        assert config.enable_dlq is True
        assert config.enable_tracing is True

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(
                bootstrap_servers="kafka1:9092,kafka2:9092",
                topic_prefix="myapp.events",
                consumer_group="projections",
                consumer_name="worker-1",
                acks="1",
                compression_type="snappy",
            )

        assert config.bootstrap_servers == "kafka1:9092,kafka2:9092"
        assert config.topic_prefix == "myapp.events"
        assert config.consumer_group == "projections"
        assert config.consumer_name == "worker-1"
        assert config.acks == "1"
        assert config.compression_type == "snappy"

    def test_topic_name_property(self) -> None:
        """Test topic_name derived property."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(topic_prefix="orders")

        assert config.topic_name == "orders.stream"

    def test_dlq_topic_name_property(self) -> None:
        """Test dlq_topic_name derived property."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(
                topic_prefix="orders",
                dlq_topic_suffix=".dead-letters",
            )

        assert config.dlq_topic_name == "orders.stream.dead-letters"

    def test_auto_generated_consumer_name(self) -> None:
        """Test consumer_name is auto-generated when not provided."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig()

        assert config.consumer_name is not None
        # Should contain hostname and UUID
        assert "-" in config.consumer_name


# =============================================================================
# Security Configuration Tests
# =============================================================================


class TestKafkaEventBusConfigSecurity:
    """Tests for security configuration validation."""

    def test_valid_security_protocols(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test all valid security protocols are accepted."""
        for protocol in ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]:
            # Reset caplog for each protocol
            caplog.clear()

            if protocol.startswith("SASL_"):
                config = KafkaEventBusConfig(
                    security_protocol=protocol,
                    sasl_mechanism="PLAIN",
                    sasl_username="user",
                    sasl_password="pass",
                )
            else:
                config = KafkaEventBusConfig(security_protocol=protocol)

            assert config.security_protocol == protocol

    def test_invalid_security_protocol_raises(self) -> None:
        """Test invalid security_protocol raises ValueError."""
        with pytest.raises(ValueError, match="Invalid security_protocol"):
            KafkaEventBusConfig(security_protocol="INVALID")

    def test_sasl_requires_mechanism(self) -> None:
        """Test SASL protocol requires sasl_mechanism."""
        with pytest.raises(ValueError, match="sasl_mechanism required"):
            KafkaEventBusConfig(
                security_protocol="SASL_SSL",
                sasl_mechanism=None,
            )

    def test_sasl_requires_credentials(self) -> None:
        """Test SASL authentication requires username and password."""
        with pytest.raises(ValueError, match="sasl_username and sasl_password"):
            KafkaEventBusConfig(
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_username="user",
                sasl_password=None,
            )

        with pytest.raises(ValueError, match="sasl_username and sasl_password"):
            KafkaEventBusConfig(
                security_protocol="SASL_SSL",
                sasl_mechanism="PLAIN",
                sasl_username=None,
                sasl_password="pass",
            )

    def test_invalid_sasl_mechanism_raises(self) -> None:
        """Test invalid SASL mechanism raises ValueError."""
        with pytest.raises(ValueError, match="Invalid sasl_mechanism"):
            KafkaEventBusConfig(
                security_protocol="SASL_SSL",
                sasl_mechanism="GSSAPI",  # Kerberos not supported
                sasl_username="user",
                sasl_password="pass",
            )

    @pytest.mark.parametrize("mechanism", ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    def test_valid_sasl_mechanisms(self, mechanism: str) -> None:
        """Test all valid SASL mechanisms are accepted."""
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism=mechanism,
            sasl_username="user",
            sasl_password="pass",
        )
        assert config.sasl_mechanism == mechanism

    def test_mtls_requires_both_cert_and_key(self) -> None:
        """Test mTLS requires both certificate and key files."""
        with pytest.raises(ValueError, match="ssl_keyfile required"):
            KafkaEventBusConfig(
                security_protocol="SSL",
                ssl_certfile="/path/to/cert",
                ssl_keyfile=None,
            )

        with pytest.raises(ValueError, match="ssl_certfile required"):
            KafkaEventBusConfig(
                security_protocol="SSL",
                ssl_certfile=None,
                ssl_keyfile="/path/to/key",
            )

    def test_mtls_with_both_files_succeeds(self) -> None:
        """Test mTLS configuration with both cert and key."""
        config = KafkaEventBusConfig(
            security_protocol="SSL",
            ssl_certfile="/path/to/cert.pem",
            ssl_keyfile="/path/to/key.pem",
        )
        assert config.ssl_certfile == "/path/to/cert.pem"
        assert config.ssl_keyfile == "/path/to/key.pem"

    def test_plaintext_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test PLAINTEXT protocol logs security warning."""
        with caplog.at_level("WARNING"):
            KafkaEventBusConfig(security_protocol="PLAINTEXT")

        assert "not recommended for production" in caplog.text

    def test_sasl_plaintext_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test SASL_PLAINTEXT protocol logs security warning."""
        with caplog.at_level("WARNING"):
            KafkaEventBusConfig(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_username="user",
                sasl_password="pass",
            )

        assert "credentials sent in plain text" in caplog.text

    def test_disabled_hostname_check_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test disabled hostname verification logs security warning."""
        with caplog.at_level("WARNING"):
            KafkaEventBusConfig(
                security_protocol="SSL",
                ssl_check_hostname=False,
            )

        assert "vulnerable to MITM" in caplog.text


# =============================================================================
# SSL Context Tests
# =============================================================================


class TestKafkaEventBusConfigSSLContext:
    """Tests for SSL context creation."""

    def test_ssl_context_created_for_ssl_protocol(self) -> None:
        """Test SSL context is created when using SSL protocol."""
        config = KafkaEventBusConfig(security_protocol="SSL")
        context = config.create_ssl_context()

        assert context is not None
        assert isinstance(context, ssl.SSLContext)

    def test_ssl_context_created_for_sasl_ssl_protocol(self) -> None:
        """Test SSL context is created when using SASL_SSL protocol."""
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",
        )
        context = config.create_ssl_context()

        assert context is not None
        assert isinstance(context, ssl.SSLContext)

    def test_ssl_context_none_for_plaintext(self) -> None:
        """Test SSL context is None for PLAINTEXT protocol."""
        config = KafkaEventBusConfig(security_protocol="PLAINTEXT")
        context = config.create_ssl_context()

        assert context is None

    def test_ssl_context_none_for_sasl_plaintext(self) -> None:
        """Test SSL context is None for SASL_PLAINTEXT protocol."""
        config = KafkaEventBusConfig(
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",
        )
        context = config.create_ssl_context()

        assert context is None

    def test_ssl_context_hostname_verification_enabled(self) -> None:
        """Test SSL context has hostname verification enabled by default."""
        config = KafkaEventBusConfig(
            security_protocol="SSL",
            ssl_check_hostname=True,
        )
        context = config.create_ssl_context()

        assert context is not None
        assert context.check_hostname is True
        assert context.verify_mode == ssl.CERT_REQUIRED

    def test_ssl_context_hostname_verification_disabled(self) -> None:
        """Test SSL context can disable hostname verification."""
        config = KafkaEventBusConfig(
            security_protocol="SSL",
            ssl_check_hostname=False,
        )
        context = config.create_ssl_context()

        assert context is not None
        assert context.check_hostname is False
        assert context.verify_mode == ssl.CERT_NONE

    def test_ssl_context_loads_ca_file(self, tmp_path: Any) -> None:
        """Test SSL context attempts to load CA file."""
        # Create a dummy CA file (not a real cert, just for path testing)
        ca_file = tmp_path / "ca.crt"
        ca_file.write_text("dummy")

        config = KafkaEventBusConfig(
            security_protocol="SSL",
            ssl_cafile=str(ca_file),
        )

        # This will fail with invalid cert, but tests the path is used
        with pytest.raises(ssl.SSLError):
            config.create_ssl_context()


# =============================================================================
# Credential Sanitization Tests
# =============================================================================


class TestKafkaEventBusConfigSanitization:
    """Tests for credential sanitization in logging."""

    def test_sanitized_config_redacts_password(self) -> None:
        """Test sasl_password is redacted in sanitized config."""
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="myuser",
            sasl_password="supersecret",
        )

        sanitized = config.get_sanitized_config()

        assert sanitized["sasl_password"] == "***"
        assert sanitized["sasl_username"] == "myuser"  # Username not redacted
        assert sanitized["bootstrap_servers"] == "kafka:9092"

    def test_sanitized_config_redacts_ssl_keyfile(self) -> None:
        """Test ssl_keyfile path is redacted in sanitized config."""
        config = KafkaEventBusConfig(
            security_protocol="SSL",
            ssl_certfile="/path/to/cert.pem",
            ssl_keyfile="/path/to/private.key",
        )

        sanitized = config.get_sanitized_config()

        assert sanitized["ssl_keyfile"] == "***"
        assert sanitized["ssl_certfile"] == "/path/to/cert.pem"  # Cert not redacted

    def test_sanitized_config_handles_none_values(self) -> None:
        """Test sanitized config handles None values properly."""
        config = KafkaEventBusConfig(security_protocol="PLAINTEXT")
        sanitized = config.get_sanitized_config()

        assert sanitized["sasl_password"] is None
        assert sanitized["ssl_keyfile"] is None
        assert sanitized["sasl_username"] is None
        assert sanitized["sasl_mechanism"] is None

    def test_sanitized_config_includes_all_fields(self) -> None:
        """Test sanitized config includes all expected fields."""
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            topic_prefix="events",
            consumer_group="my-group",
            security_protocol="SSL",
            ssl_cafile="/path/to/ca.crt",
            enable_dlq=True,
            enable_tracing=False,
        )

        sanitized = config.get_sanitized_config()

        assert "bootstrap_servers" in sanitized
        assert "topic_prefix" in sanitized
        assert "consumer_group" in sanitized
        assert "consumer_name" in sanitized
        assert "security_protocol" in sanitized
        assert "sasl_mechanism" in sanitized
        assert "sasl_username" in sanitized
        assert "sasl_password" in sanitized
        assert "ssl_cafile" in sanitized
        assert "ssl_certfile" in sanitized
        assert "ssl_keyfile" in sanitized
        assert "ssl_check_hostname" in sanitized
        assert "enable_dlq" in sanitized
        assert "enable_tracing" in sanitized


# =============================================================================
# Producer/Consumer Config Tests
# =============================================================================


class TestKafkaEventBusConfigProducerConsumer:
    """Tests for producer and consumer configuration generation."""

    def test_get_producer_config_basic(self) -> None:
        """Test basic producer configuration."""
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            acks="all",
            compression_type="gzip",
            batch_size=32768,
            linger_ms=10,
            security_protocol="PLAINTEXT",
        )

        producer_config = config.get_producer_config()

        assert producer_config["bootstrap_servers"] == "kafka:9092"
        assert producer_config["acks"] == "all"
        assert producer_config["compression_type"] == "gzip"
        assert producer_config["max_batch_size"] == 32768
        assert producer_config["linger_ms"] == 10
        assert producer_config["security_protocol"] == "PLAINTEXT"

    def test_get_producer_config_with_security(self) -> None:
        """Test producer configuration with security settings."""
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="user",
            sasl_password="pass",
            ssl_cafile="/path/to/ca.crt",
        )

        producer_config = config.get_producer_config()

        assert producer_config["security_protocol"] == "SASL_SSL"
        assert producer_config["sasl_mechanism"] == "SCRAM-SHA-512"
        assert producer_config["sasl_plain_username"] == "user"
        assert producer_config["sasl_plain_password"] == "pass"
        assert producer_config["ssl_cafile"] == "/path/to/ca.crt"
        assert producer_config["ssl_check_hostname"] is True

    def test_get_consumer_config_basic(self) -> None:
        """Test basic consumer configuration."""
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            consumer_group="my-group",
            consumer_name="worker-1",
            auto_offset_reset="earliest",
            security_protocol="PLAINTEXT",
        )

        consumer_config = config.get_consumer_config()

        assert consumer_config["bootstrap_servers"] == "kafka:9092"
        assert consumer_config["group_id"] == "my-group"
        assert consumer_config["client_id"] == "worker-1"
        assert consumer_config["auto_offset_reset"] == "earliest"
        assert consumer_config["enable_auto_commit"] is False

    def test_get_consumer_config_with_security(self) -> None:
        """Test consumer configuration with security settings."""
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",
            ssl_check_hostname=False,
        )

        consumer_config = config.get_consumer_config()

        assert consumer_config["security_protocol"] == "SASL_SSL"
        assert consumer_config["sasl_mechanism"] == "PLAIN"
        assert consumer_config["sasl_plain_username"] == "user"
        assert consumer_config["sasl_plain_password"] == "pass"
        assert consumer_config["ssl_check_hostname"] is False

    def test_ssl_check_hostname_not_included_for_plaintext(self) -> None:
        """Test ssl_check_hostname is not included for PLAINTEXT."""
        config = KafkaEventBusConfig(security_protocol="PLAINTEXT")

        producer_config = config.get_producer_config()
        consumer_config = config.get_consumer_config()

        assert "ssl_check_hostname" not in producer_config
        assert "ssl_check_hostname" not in consumer_config


# =============================================================================
# KafkaEventBusStats Tests
# =============================================================================


class TestKafkaEventBusStats:
    """Tests for KafkaEventBusStats dataclass."""

    def test_default_values(self) -> None:
        """Test default statistics values."""
        stats = KafkaEventBusStats()

        assert stats.events_published == 0
        assert stats.events_consumed == 0
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.rebalance_count == 0
        assert stats.last_publish_at is None
        assert stats.last_consume_at is None
        assert stats.last_error_at is None
        assert stats.connected_at is None

    def test_get_stats_dict(self) -> None:
        """Test stats dictionary serialization."""
        now = datetime.now(UTC)
        stats = KafkaEventBusStats(
            events_published=10,
            events_consumed=8,
            events_processed_success=7,
            events_processed_failed=1,
            last_publish_at=now,
            connected_at=now,
        )

        stats_dict = stats.get_stats_dict()

        assert stats_dict["events_published"] == 10
        assert stats_dict["events_consumed"] == 8
        assert stats_dict["events_processed_success"] == 7
        assert stats_dict["events_processed_failed"] == 1
        assert stats_dict["last_publish_at"] == now.isoformat()
        assert stats_dict["connected_at"] == now.isoformat()
        assert stats_dict["last_consume_at"] is None


# =============================================================================
# KafkaEventBus Tests (with mocks)
# =============================================================================


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBus:
    """Tests for KafkaEventBus class."""

    def test_initialization(self, event_registry: EventRegistry) -> None:
        """Test basic initialization."""
        config = KafkaEventBusConfig(security_protocol="PLAINTEXT")
        bus = KafkaEventBus(config=config, event_registry=event_registry)

        assert bus.config is config
        assert bus.is_connected is False
        assert bus.is_consuming is False
        assert bus.get_subscriber_count() == 0

    def test_initialization_with_defaults(self, event_registry: EventRegistry) -> None:
        """Test initialization with default config."""
        bus = KafkaEventBus(event_registry=event_registry)

        assert bus.config is not None
        assert bus.config.bootstrap_servers == "localhost:9092"

    def test_subscribe(self, event_registry: EventRegistry) -> None:
        """Test subscribing a handler."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1

    def test_subscribe_multiple_handlers(self, event_registry: EventRegistry) -> None:
        """Test subscribing multiple handlers to the same event."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler1 = OrderHandler()
        handler2 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe(SampleOrderCreated, handler2)

        assert bus.get_subscriber_count(SampleOrderCreated) == 2

    def test_unsubscribe(self, event_registry: EventRegistry) -> None:
        """Test unsubscribing a handler."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler)
        assert bus.get_subscriber_count(SampleOrderCreated) == 1

        result = bus.unsubscribe(SampleOrderCreated, handler)
        assert result is True
        assert bus.get_subscriber_count(SampleOrderCreated) == 0

    def test_unsubscribe_not_found(self, event_registry: EventRegistry) -> None:
        """Test unsubscribing a handler that was not subscribed."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        result = bus.unsubscribe(SampleOrderCreated, handler)
        assert result is False

    def test_subscribe_all(self, event_registry: EventRegistry) -> None:
        """Test subscribe_all with a subscriber."""
        bus = KafkaEventBus(event_registry=event_registry)
        subscriber = SampleSubscriber()

        bus.subscribe_all(subscriber)

        assert bus.get_subscriber_count(SampleOrderCreated) == 1
        assert bus.get_subscriber_count(SampleOrderShipped) == 1

    def test_subscribe_to_all_events(self, event_registry: EventRegistry) -> None:
        """Test wildcard subscription."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        bus.subscribe_to_all_events(handler)

        assert bus.get_wildcard_subscriber_count() == 1

    def test_unsubscribe_from_all_events(self, event_registry: EventRegistry) -> None:
        """Test unsubscribing from wildcard subscription."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler = OrderHandler()

        bus.subscribe_to_all_events(handler)
        assert bus.get_wildcard_subscriber_count() == 1

        result = bus.unsubscribe_from_all_events(handler)
        assert result is True
        assert bus.get_wildcard_subscriber_count() == 0

    def test_clear_subscribers(self, event_registry: EventRegistry) -> None:
        """Test clearing all subscribers."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler1 = OrderHandler()
        handler2 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe_to_all_events(handler2)

        bus.clear_subscribers()

        assert bus.get_subscriber_count() == 0
        assert bus.get_wildcard_subscriber_count() == 0

    def test_get_handlers_for_event(self, event_registry: EventRegistry) -> None:
        """Test getting handlers for an event type."""
        bus = KafkaEventBus(event_registry=event_registry)
        handler1 = OrderHandler()
        handler2 = OrderHandler()

        bus.subscribe(SampleOrderCreated, handler1)
        bus.subscribe_to_all_events(handler2)

        handlers = bus.get_handlers_for_event("SampleOrderCreated")
        assert len(handlers) == 2

    @pytest.mark.asyncio
    async def test_publish_without_connection_raises(
        self, event_registry: EventRegistry, sample_event: SampleOrderCreated
    ) -> None:
        """Test publishing without connection raises error."""
        bus = KafkaEventBus(event_registry=event_registry)

        with pytest.raises(RuntimeError, match="Not connected"):
            await bus.publish([sample_event])

    @pytest.mark.asyncio
    async def test_connect_disconnect(self, event_registry: EventRegistry) -> None:
        """Test connect and disconnect lifecycle."""
        bus = KafkaEventBus(event_registry=event_registry)

        with (
            patch("eventsource.bus.kafka.AIOKafkaProducer") as mock_producer_class,
            patch("eventsource.bus.kafka.AIOKafkaConsumer") as mock_consumer_class,
        ):
            mock_producer = AsyncMock()
            mock_consumer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            mock_consumer_class.return_value = mock_consumer

            await bus.connect()
            assert bus.is_connected is True

            await bus.disconnect()
            assert bus.is_connected is False

            mock_producer.start.assert_called_once()
            mock_producer.stop.assert_called_once()
            mock_consumer.start.assert_called_once()
            mock_consumer.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_context_manager(self, event_registry: EventRegistry) -> None:
        """Test async context manager usage."""
        with (
            patch("eventsource.bus.kafka.AIOKafkaProducer") as mock_producer_class,
            patch("eventsource.bus.kafka.AIOKafkaConsumer") as mock_consumer_class,
        ):
            mock_producer = AsyncMock()
            mock_consumer = AsyncMock()
            mock_producer_class.return_value = mock_producer
            mock_consumer_class.return_value = mock_consumer

            async with KafkaEventBus(event_registry=event_registry) as bus:
                assert bus.is_connected is True

            # After exiting context, should be disconnected
            assert bus.is_connected is False


class TestKafkaNotAvailableError:
    """Tests for KafkaNotAvailableError."""

    def test_error_message(self) -> None:
        """Test error message is informative."""
        error = KafkaNotAvailableError()
        assert "aiokafka" in str(error)
        assert "pip install" in str(error)
