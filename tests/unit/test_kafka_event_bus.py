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


# =============================================================================
# OpenTelemetry Metrics Tests
# =============================================================================

# Check for OpenTelemetry SDK availability
try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.sdk.metrics import MeterProvider
    from opentelemetry.sdk.metrics.export import InMemoryMetricReader

    OTEL_METRICS_AVAILABLE = True
except ImportError:
    otel_metrics = None  # type: ignore[assignment]
    MeterProvider = None  # type: ignore[assignment, misc]
    InMemoryMetricReader = None  # type: ignore[assignment, misc]
    OTEL_METRICS_AVAILABLE = False

skip_if_no_otel_metrics = pytest.mark.skipif(
    not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed"
)


# =============================================================================
# Helper Functions for Metric Extraction
# =============================================================================


def _get_metric_value(metrics_data: Any, metric_name: str) -> int:
    """Get the sum of a counter metric.

    Args:
        metrics_data: MetricsData from InMemoryMetricReader.get_metrics_data()
        metric_name: Name of the metric to find

    Returns:
        Sum of all data point values for the metric, or 0 if not found.
    """
    if not metrics_data or not metrics_data.resource_metrics:
        return 0

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if metric.name == metric_name:
                    # Sum all data points
                    total = 0
                    if hasattr(metric, "data") and hasattr(metric.data, "data_points"):
                        for dp in metric.data.data_points:
                            total += dp.value
                    return total
    return 0


def _get_metric_attributes(metrics_data: Any, metric_name: str) -> dict[str, Any]:
    """Get attributes from the first data point of a metric.

    Args:
        metrics_data: MetricsData from InMemoryMetricReader.get_metrics_data()
        metric_name: Name of the metric to find

    Returns:
        Dictionary of attributes from the first data point, or empty dict.
    """
    if not metrics_data or not metrics_data.resource_metrics:
        return {}

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if (
                    metric.name == metric_name
                    and hasattr(metric, "data")
                    and hasattr(metric.data, "data_points")
                ):
                    for dp in metric.data.data_points:
                        return dict(dp.attributes)
    return {}


def _has_histogram_data(metrics_data: Any, metric_name: str) -> bool:
    """Check if a histogram has recorded data.

    Args:
        metrics_data: MetricsData from InMemoryMetricReader.get_metrics_data()
        metric_name: Name of the metric to find

    Returns:
        True if the histogram has at least one data point.
    """
    if not metrics_data or not metrics_data.resource_metrics:
        return False

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if (
                    metric.name == metric_name
                    and hasattr(metric, "data")
                    and hasattr(metric.data, "data_points")
                ):
                    return len(metric.data.data_points) > 0
    return False


def _get_histogram_sum(metrics_data: Any, metric_name: str) -> float:
    """Get the sum value from a histogram metric.

    Args:
        metrics_data: MetricsData from InMemoryMetricReader.get_metrics_data()
        metric_name: Name of the metric to find

    Returns:
        Sum value from the histogram, or 0.0 if not found.
    """
    if not metrics_data or not metrics_data.resource_metrics:
        return 0.0

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if (
                    metric.name == metric_name
                    and hasattr(metric, "data")
                    and hasattr(metric.data, "data_points")
                ):
                    for dp in metric.data.data_points:
                        return dp.sum
    return 0.0


def _get_gauge_value(metrics_data: Any, metric_name: str) -> float | None:
    """Get the value from a gauge metric.

    Args:
        metrics_data: MetricsData from InMemoryMetricReader.get_metrics_data()
        metric_name: Name of the metric to find

    Returns:
        Value from the gauge, or None if not found.
    """
    if not metrics_data or not metrics_data.resource_metrics:
        return None

    for resource_metric in metrics_data.resource_metrics:
        for scope_metric in resource_metric.scope_metrics:
            for metric in scope_metric.metrics:
                if (
                    metric.name == metric_name
                    and hasattr(metric, "data")
                    and hasattr(metric.data, "data_points")
                ):
                    for dp in metric.data.data_points:
                        return dp.value
    return None


# =============================================================================
# Metrics Infrastructure Tests
# =============================================================================


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsInfrastructure:
    """Tests for metrics infrastructure."""

    @pytest.fixture(autouse=True)
    def reset_meter(self) -> None:
        """Reset cached meter before each test."""
        import eventsource.bus.kafka as kafka_module

        kafka_module._meter = None
        yield
        kafka_module._meter = None

    @skip_if_no_otel_metrics
    def test_get_meter_returns_meter(self) -> None:
        """Test _get_meter returns a meter instance."""
        from eventsource.bus.kafka import _get_meter

        meter = _get_meter()
        assert meter is not None

    @skip_if_no_otel_metrics
    def test_get_meter_cached(self) -> None:
        """Test _get_meter returns cached meter on subsequent calls."""
        from eventsource.bus.kafka import _get_meter

        meter1 = _get_meter()
        meter2 = _get_meter()
        assert meter1 is meter2

    def test_get_meter_none_when_otel_unavailable(self) -> None:
        """Test _get_meter returns None when OpenTelemetry not installed."""
        import eventsource.bus.kafka as kafka_module

        kafka_module._meter = None

        with patch.object(kafka_module, "OTEL_AVAILABLE", False):
            from eventsource.bus.kafka import _get_meter

            meter = _get_meter()
            assert meter is None

    @skip_if_no_otel_metrics
    def test_metrics_class_creates_all_counters(self) -> None:
        """Test KafkaEventBusMetrics creates all counter instruments."""
        from eventsource.bus.kafka import KafkaEventBusMetrics, _get_meter

        meter = _get_meter()
        metrics = KafkaEventBusMetrics(meter)

        # Verify all 9 counters exist
        assert hasattr(metrics, "messages_published")
        assert hasattr(metrics, "messages_consumed")
        assert hasattr(metrics, "handler_invocations")
        assert hasattr(metrics, "handler_errors")
        assert hasattr(metrics, "dlq_messages")
        assert hasattr(metrics, "connection_errors")
        assert hasattr(metrics, "reconnections")
        assert hasattr(metrics, "rebalances")
        assert hasattr(metrics, "publish_errors")

    @skip_if_no_otel_metrics
    def test_metrics_class_creates_all_histograms(self) -> None:
        """Test KafkaEventBusMetrics creates all histogram instruments."""
        from eventsource.bus.kafka import KafkaEventBusMetrics, _get_meter

        meter = _get_meter()
        metrics = KafkaEventBusMetrics(meter)

        # Verify all 4 histograms exist
        assert hasattr(metrics, "publish_duration")
        assert hasattr(metrics, "consume_duration")
        assert hasattr(metrics, "handler_duration")
        assert hasattr(metrics, "batch_size")


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsConfig:
    """Tests for metrics configuration."""

    @pytest.fixture(autouse=True)
    def reset_meter(self) -> None:
        """Reset cached meter before each test."""
        import eventsource.bus.kafka as kafka_module

        kafka_module._meter = None
        yield
        kafka_module._meter = None

    @skip_if_no_otel_metrics
    def test_metrics_enabled_by_default(self) -> None:
        """Test metrics are enabled by default."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig()
            bus = KafkaEventBus(config=config)

        assert bus._metrics is not None

    @skip_if_no_otel_metrics
    def test_metrics_disabled_when_config_false(self) -> None:
        """Test metrics disabled when enable_metrics=False."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(enable_metrics=False)
            bus = KafkaEventBus(config=config)

        assert bus._metrics is None

    def test_metrics_none_when_otel_unavailable(self) -> None:
        """Test metrics is None when OpenTelemetry not installed."""
        import eventsource.bus.kafka as kafka_module

        with (
            patch.object(kafka_module, "OTEL_AVAILABLE", False),
            patch.object(KafkaEventBusConfig, "_validate_security_config"),
        ):
            kafka_module._meter = None
            config = KafkaEventBusConfig(enable_metrics=True)
            bus = KafkaEventBus(config=config)

        assert bus._metrics is None


# =============================================================================
# Counter Metrics Tests
# =============================================================================


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusCounterMetrics:
    """Tests for counter metrics."""

    @pytest.fixture(autouse=True)
    def setup_metrics(self) -> Any:
        """Set up metrics for each test."""
        import eventsource.bus.kafka as kafka_module

        kafka_module._meter = None

        # Set up fresh metric reader and provider FIRST
        self.reader = InMemoryMetricReader()
        self.provider = MeterProvider(metric_readers=[self.reader])
        otel_metrics.set_meter_provider(self.provider)

        # Create a meter from the new provider
        self.meter = self.provider.get_meter("test.kafka.eventbus")

        yield

        # Reset
        kafka_module._meter = None

    def _create_bus_with_test_metrics(self) -> "KafkaEventBus":
        """Create a bus and set up metrics with our test provider."""
        from eventsource.bus.kafka import KafkaEventBusMetrics

        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus()

        # Replace the bus's metrics with one using our test meter
        bus._metrics = KafkaEventBusMetrics(self.meter)
        bus._meter = self.meter

        return bus

    @pytest.mark.asyncio
    async def test_handler_invocation_counter(self) -> None:
        """Test handler invocation increments counter."""
        handler = OrderHandler()
        bus = self._create_bus_with_test_metrics()

        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        # Simulate handler dispatch
        handlers = bus.get_handlers_for_event("SampleOrderCreated")
        await bus._dispatch_to_handlers(event, handlers)

        metrics_data = self.reader.get_metrics_data()
        invocation_count = _get_metric_value(metrics_data, "kafka.eventbus.handler.invocations")
        assert invocation_count == 1

    @pytest.mark.asyncio
    async def test_handler_error_counter(self) -> None:
        """Test handler error increments error counter."""

        async def failing_handler(event: DomainEvent) -> None:
            raise ValueError("Test error")

        bus = self._create_bus_with_test_metrics()

        bus.subscribe(SampleOrderCreated, failing_handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        handlers = bus.get_handlers_for_event("SampleOrderCreated")

        with pytest.raises(ValueError):
            await bus._dispatch_to_handlers(event, handlers)

        metrics_data = self.reader.get_metrics_data()
        error_count = _get_metric_value(metrics_data, "kafka.eventbus.handler.errors")
        assert error_count == 1

        # Verify error.type attribute
        attributes = _get_metric_attributes(metrics_data, "kafka.eventbus.handler.errors")
        assert attributes.get("error.type") == "ValueError"

    @pytest.mark.asyncio
    async def test_reconnection_counter(self) -> None:
        """Test reconnection counter via record_reconnection method."""
        bus = self._create_bus_with_test_metrics()

        # Record a reconnection
        bus.record_reconnection()
        bus.record_reconnection()

        metrics_data = self.reader.get_metrics_data()
        reconnection_count = _get_metric_value(metrics_data, "kafka.eventbus.reconnections")
        assert reconnection_count == 2

    @pytest.mark.asyncio
    async def test_rebalance_counter(self) -> None:
        """Test rebalance counter via record_rebalance method."""
        bus = self._create_bus_with_test_metrics()

        # Record a rebalance
        bus.record_rebalance()

        metrics_data = self.reader.get_metrics_data()
        rebalance_count = _get_metric_value(metrics_data, "kafka.eventbus.rebalances")
        assert rebalance_count == 1

    @pytest.mark.asyncio
    async def test_connection_error_counter(self) -> None:
        """Test connection error counter on connect failure."""
        mock_producer = AsyncMock()
        mock_producer.start.side_effect = Exception("Connection failed")

        bus = self._create_bus_with_test_metrics()

        with (
            patch("eventsource.bus.kafka.AIOKafkaProducer", return_value=mock_producer),
            patch("eventsource.bus.kafka.AIOKafkaConsumer", return_value=AsyncMock()),
        ):
            with pytest.raises(Exception, match="Connection failed"):
                await bus.connect()

            metrics_data = self.reader.get_metrics_data()
            error_count = _get_metric_value(metrics_data, "kafka.eventbus.connection.errors")
            assert error_count == 1


# =============================================================================
# Histogram Metrics Tests
# =============================================================================


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusHistogramMetrics:
    """Tests for histogram metrics."""

    @pytest.fixture(autouse=True)
    def setup_metrics(self) -> Any:
        """Set up metrics for each test."""
        import eventsource.bus.kafka as kafka_module

        kafka_module._meter = None

        # Set up fresh metric reader and provider
        self.reader = InMemoryMetricReader()
        self.provider = MeterProvider(metric_readers=[self.reader])
        otel_metrics.set_meter_provider(self.provider)

        # Create a meter from the new provider
        self.meter = self.provider.get_meter("test.kafka.eventbus")

        yield

        # Reset
        kafka_module._meter = None

    def _create_bus_with_test_metrics(self) -> "KafkaEventBus":
        """Create a bus and set up metrics with our test provider."""
        from eventsource.bus.kafka import KafkaEventBusMetrics

        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus()

        # Replace the bus's metrics with one using our test meter
        bus._metrics = KafkaEventBusMetrics(self.meter)
        bus._meter = self.meter

        return bus

    @pytest.mark.asyncio
    async def test_handler_duration_recorded(self) -> None:
        """Test handler duration is recorded."""
        import asyncio

        async def slow_handler(event: DomainEvent) -> None:
            await asyncio.sleep(0.01)  # 10ms

        bus = self._create_bus_with_test_metrics()

        bus.subscribe(SampleOrderCreated, slow_handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        handlers = bus.get_handlers_for_event("SampleOrderCreated")
        await bus._dispatch_to_handlers(event, handlers)

        metrics_data = self.reader.get_metrics_data()
        duration_recorded = _has_histogram_data(metrics_data, "kafka.eventbus.handler.duration")
        assert duration_recorded

        # Verify duration is reasonable (>= 10ms)
        duration_sum = _get_histogram_sum(metrics_data, "kafka.eventbus.handler.duration")
        assert duration_sum >= 10  # At least 10ms


# =============================================================================
# Observable Gauge Tests
# =============================================================================


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
@pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="opentelemetry-sdk not installed")
class TestKafkaEventBusGaugeMetrics:
    """Tests for observable gauge metrics."""

    @pytest.fixture(autouse=True)
    def setup_metrics(self) -> Any:
        """Set up metrics for each test."""
        import eventsource.bus.kafka as kafka_module

        kafka_module._meter = None

        # Set up fresh metric reader and provider
        self.reader = InMemoryMetricReader()
        self.provider = MeterProvider(metric_readers=[self.reader])
        otel_metrics.set_meter_provider(self.provider)

        # Create a meter from the new provider
        self.meter = self.provider.get_meter("test.kafka.eventbus")

        yield

        # Reset
        kafka_module._meter = None

    def _create_bus_with_test_metrics(self) -> "KafkaEventBus":
        """Create a bus and set up metrics with our test provider."""
        from eventsource.bus.kafka import KafkaEventBusMetrics

        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            bus = KafkaEventBus()

        # Replace the bus's metrics with one using our test meter
        bus._metrics = KafkaEventBusMetrics(self.meter)
        bus._meter = self.meter

        return bus

    @pytest.mark.asyncio
    async def test_connection_gauge_registered_on_connect(self) -> None:
        """Test connection gauge is registered when connected."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        bus = self._create_bus_with_test_metrics()

        with (
            patch("eventsource.bus.kafka.AIOKafkaProducer", return_value=mock_producer),
            patch("eventsource.bus.kafka.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            await bus.connect()

            # Verify gauge registered flag is set
            assert bus._connection_gauge_registered is True

            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_connection_gauge_reports_connected(self) -> None:
        """Test connection gauge reports 1 when connected."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        bus = self._create_bus_with_test_metrics()

        with (
            patch("eventsource.bus.kafka.AIOKafkaProducer", return_value=mock_producer),
            patch("eventsource.bus.kafka.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            await bus.connect()

            # Force metric collection
            metrics_data = self.reader.get_metrics_data()

            connection_status = _get_gauge_value(metrics_data, "kafka.eventbus.connections.active")
            assert connection_status == 1

            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_connection_gauge_reports_disconnected(self) -> None:
        """Test connection gauge reports 0 when disconnected."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        bus = self._create_bus_with_test_metrics()

        with (
            patch("eventsource.bus.kafka.AIOKafkaProducer", return_value=mock_producer),
            patch("eventsource.bus.kafka.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            await bus.connect()
            await bus.disconnect()

            # Force metric collection
            metrics_data = self.reader.get_metrics_data()

            connection_status = _get_gauge_value(metrics_data, "kafka.eventbus.connections.active")
            assert connection_status == 0

    @pytest.mark.asyncio
    async def test_lag_gauge_registered_on_start_consuming(self) -> None:
        """Test consumer lag gauge is registered when consuming starts."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()
        mock_consumer.__aiter__ = AsyncMock(return_value=iter([]))

        bus = self._create_bus_with_test_metrics()

        with (
            patch("eventsource.bus.kafka.AIOKafkaProducer", return_value=mock_producer),
            patch("eventsource.bus.kafka.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            await bus.connect()

            # Manually set consuming to simulate start
            bus._consuming = True
            bus._register_consumer_lag_gauge()

            # Verify gauge registered flag is set
            assert bus._lag_gauge_registered is True

            await bus.disconnect()

    @pytest.mark.asyncio
    async def test_gauge_registration_idempotent(self) -> None:
        """Test gauge registration is idempotent (safe to call multiple times)."""
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        bus = self._create_bus_with_test_metrics()

        with (
            patch("eventsource.bus.kafka.AIOKafkaProducer", return_value=mock_producer),
            patch("eventsource.bus.kafka.AIOKafkaConsumer", return_value=mock_consumer),
        ):
            await bus.connect()

            # Try to register again (should be a no-op)
            bus._register_connection_gauge()
            bus._register_connection_gauge()

            # Should still be registered only once (no error)
            assert bus._connection_gauge_registered is True

            await bus.disconnect()


# =============================================================================
# Edge Case Tests
# =============================================================================


@pytest.mark.skipif(not KAFKA_AVAILABLE, reason="aiokafka not installed")
class TestKafkaEventBusMetricsEdgeCases:
    """Tests for edge cases in metrics handling."""

    @pytest.fixture(autouse=True)
    def reset_meter(self) -> None:
        """Reset cached meter before each test."""
        import eventsource.bus.kafka as kafka_module

        kafka_module._meter = None
        yield
        kafka_module._meter = None

    @pytest.mark.asyncio
    async def test_no_error_when_metrics_disabled(self) -> None:
        """Test no errors occur when metrics are disabled."""
        with patch.object(KafkaEventBusConfig, "_validate_security_config"):
            config = KafkaEventBusConfig(enable_metrics=False)
            bus = KafkaEventBus(config=config)

        # All operations should work without errors
        # (No need to connect/publish since we're testing that metrics being None doesn't crash)

        # Record methods should not raise
        bus.record_reconnection()
        bus.record_rebalance()

        # Dispatch should work without error even with no metrics
        handler = OrderHandler()
        bus.subscribe(SampleOrderCreated, handler)

        event = SampleOrderCreated(
            aggregate_id=uuid4(),
            aggregate_version=1,
            order_number="ORD-001",
            customer_id=uuid4(),
        )

        handlers = bus.get_handlers_for_event("SampleOrderCreated")
        await bus._dispatch_to_handlers(event, handlers)

    @pytest.mark.asyncio
    async def test_no_error_when_otel_unavailable(self) -> None:
        """Test no errors occur when OpenTelemetry not installed."""
        import eventsource.bus.kafka as kafka_module

        with (
            patch.object(kafka_module, "OTEL_AVAILABLE", False),
            patch.object(KafkaEventBusConfig, "_validate_security_config"),
        ):
            kafka_module._meter = None
            config = KafkaEventBusConfig(enable_metrics=True)
            bus = KafkaEventBus(config=config)

        # Bus should be created successfully, just without metrics
        assert bus._metrics is None

        # Operations should not raise
        bus.record_reconnection()
        bus.record_rebalance()

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_handler_metrics_on_error_still_records_duration(self) -> None:
        """Test handler duration is recorded even when handler raises."""
        import eventsource.bus.kafka as kafka_module
        from eventsource.bus.kafka import KafkaEventBusMetrics

        kafka_module._meter = None

        # Set up fresh metric reader
        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        otel_metrics.set_meter_provider(provider)
        meter = provider.get_meter("test.kafka.eventbus")

        try:

            async def failing_handler(event: DomainEvent) -> None:
                raise ValueError("Test error")

            with patch.object(KafkaEventBusConfig, "_validate_security_config"):
                bus = KafkaEventBus()

            # Set up test metrics
            bus._metrics = KafkaEventBusMetrics(meter)
            bus._meter = meter

            bus.subscribe(SampleOrderCreated, failing_handler)

            event = SampleOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                order_number="ORD-001",
                customer_id=uuid4(),
            )

            handlers = bus.get_handlers_for_event("SampleOrderCreated")

            with pytest.raises(ValueError):
                await bus._dispatch_to_handlers(event, handlers)

            metrics_data = reader.get_metrics_data()

            # Duration should still be recorded on error path
            duration_recorded = _has_histogram_data(metrics_data, "kafka.eventbus.handler.duration")
            assert duration_recorded

        finally:
            kafka_module._meter = None

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_multiple_handlers_increment_counter_correctly(self) -> None:
        """Test that multiple handlers each increment the counter."""
        import eventsource.bus.kafka as kafka_module
        from eventsource.bus.kafka import KafkaEventBusMetrics

        kafka_module._meter = None

        # Set up fresh metric reader
        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        otel_metrics.set_meter_provider(provider)
        meter = provider.get_meter("test.kafka.eventbus")

        try:
            handler1 = OrderHandler()
            handler2 = OrderHandler()
            handler3 = OrderHandler()

            with patch.object(KafkaEventBusConfig, "_validate_security_config"):
                bus = KafkaEventBus()

            # Set up test metrics
            bus._metrics = KafkaEventBusMetrics(meter)
            bus._meter = meter

            bus.subscribe(SampleOrderCreated, handler1)
            bus.subscribe(SampleOrderCreated, handler2)
            bus.subscribe(SampleOrderCreated, handler3)

            event = SampleOrderCreated(
                aggregate_id=uuid4(),
                aggregate_version=1,
                order_number="ORD-001",
                customer_id=uuid4(),
            )

            handlers = bus.get_handlers_for_event("SampleOrderCreated")
            await bus._dispatch_to_handlers(event, handlers)

            metrics_data = reader.get_metrics_data()
            invocation_count = _get_metric_value(metrics_data, "kafka.eventbus.handler.invocations")
            assert invocation_count == 3

        finally:
            kafka_module._meter = None

    @skip_if_no_otel_metrics
    @pytest.mark.asyncio
    async def test_publish_error_counter(self) -> None:
        """Test publish error increments publish_errors counter."""
        import eventsource.bus.kafka as kafka_module
        from eventsource.bus.kafka import KafkaEventBusMetrics

        kafka_module._meter = None

        # Set up fresh metric reader
        reader = InMemoryMetricReader()
        provider = MeterProvider(metric_readers=[reader])
        otel_metrics.set_meter_provider(provider)
        meter = provider.get_meter("test.kafka.eventbus")

        try:
            mock_producer = AsyncMock()
            mock_producer.send.side_effect = Exception("Publish failed")

            with patch.object(KafkaEventBusConfig, "_validate_security_config"):
                bus = KafkaEventBus()

            # Set up test metrics
            bus._metrics = KafkaEventBusMetrics(meter)
            bus._meter = meter

            with (
                patch("eventsource.bus.kafka.AIOKafkaProducer", return_value=mock_producer),
                patch("eventsource.bus.kafka.AIOKafkaConsumer", return_value=AsyncMock()),
            ):
                await bus.connect()

                event = SampleOrderCreated(
                    aggregate_id=uuid4(),
                    aggregate_version=1,
                    order_number="ORD-001",
                    customer_id=uuid4(),
                )

                with pytest.raises(Exception, match="Publish failed"):
                    await bus.publish([event])

                metrics_data = reader.get_metrics_data()
                error_count = _get_metric_value(metrics_data, "kafka.eventbus.publish.errors")
                assert error_count == 1

                await bus.disconnect()

        finally:
            kafka_module._meter = None
