"""Kafka event bus implementation using aiokafka.

This module provides a distributed event bus implementation using Apache Kafka
for high-throughput event distribution across multiple processes and servers.

Features:
- High-throughput event publishing
- Consumer groups for horizontal scaling
- At-least-once delivery guarantees
- Partition-based ordering by aggregate_id
- Dead letter queue for unrecoverable failures
- Configurable retry policies with non-blocking republish
- Consumer rebalance handling for safe horizontal scaling
- Optional OpenTelemetry tracing
- Optional OpenTelemetry metrics

Delivery Guarantees:
    This implementation provides **at-least-once** delivery semantics. Events may
    be processed multiple times in failure scenarios (consumer crash, rebalance).
    Handlers should be idempotent to handle duplicate deliveries gracefully.

    **Exactly-once semantics are NOT supported.** The event bus does not coordinate
    transactionally with the event store. This means:
    - If an event store write succeeds but Kafka publish fails, consumers miss events
    - If Kafka publish succeeds but event store write fails, consumers see phantom events

    For use cases requiring stronger consistency guarantees, consider:
    1. Transactional Outbox Pattern: Store events in an outbox table atomically with
       domain changes, then relay to Kafka separately
    2. Kafka Transactions: Use idempotent producers with transactional semantics
       (requires Kafka 0.11+)
    3. Saga/Compensation patterns: Design for eventual consistency with compensating
       actions for failures

OpenTelemetry Metrics:
    When ``enable_metrics=True`` (default) and the OpenTelemetry SDK is installed,
    the Kafka event bus emits comprehensive metrics for monitoring throughput,
    latency, and health of your event processing pipeline.

    Counter Metrics:
        - ``kafka.eventbus.messages.published``: Total messages published
        - ``kafka.eventbus.messages.consumed``: Total messages consumed
        - ``kafka.eventbus.handler.invocations``: Handler invocation count
        - ``kafka.eventbus.handler.errors``: Handler error count
        - ``kafka.eventbus.messages.dlq``: Messages sent to dead letter queue
        - ``kafka.eventbus.connection.errors``: Connection error count
        - ``kafka.eventbus.reconnections``: Reconnection attempt count
        - ``kafka.eventbus.rebalances``: Consumer rebalance count
        - ``kafka.eventbus.publish.errors``: Publish error count

    Histogram Metrics:
        - ``kafka.eventbus.publish.duration``: Publish latency in milliseconds
        - ``kafka.eventbus.consume.duration``: Message processing latency in ms
        - ``kafka.eventbus.handler.duration``: Handler execution time in ms
        - ``kafka.eventbus.batch.size``: Publish batch sizes

    Observable Gauge Metrics:
        - ``kafka.eventbus.connections.active``: Connection status (1=connected, 0=disconnected)
        - ``kafka.eventbus.consumer.lag``: Messages behind per partition

    See the Kafka Metrics Guide (docs/guides/kafka-metrics.md) for PromQL
    queries, alerting recommendations, and Grafana dashboard examples.

Example:
    >>> from eventsource.bus.kafka import KafkaEventBus, KafkaEventBusConfig
    >>>
    >>> config = KafkaEventBusConfig(
    ...     bootstrap_servers="localhost:9092",
    ...     topic_prefix="events",
    ...     consumer_group="projections",
    ...     enable_metrics=True,  # Enable OpenTelemetry metrics (default)
    ... )
    >>> bus = KafkaEventBus(config=config, event_registry=my_registry)
    >>> await bus.connect()
    >>> await bus.publish([MyEvent(...)])
    >>> await bus.start_consuming()
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import random
import socket
import ssl
import time
import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import TYPE_CHECKING, Any

from eventsource.bus.interface import (
    EventBus,
    EventHandlerFunc,
)
from eventsource.events.base import DomainEvent
from eventsource.handlers.adapter import HandlerAdapter
from eventsource.observability import OTEL_AVAILABLE, SpanKindEnum, Tracer, create_tracer
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_HANDLER_NAME,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_OPERATION,
    ATTR_MESSAGING_SYSTEM,
)
from eventsource.protocols import (
    FlexibleEventHandler,
    FlexibleEventSubscriber,
)

if TYPE_CHECKING:
    from eventsource.events.registry import EventRegistry

# Optional aiokafka import - fail gracefully if not installed
try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener as _ConsumerRebalanceListener
    from aiokafka.errors import IllegalStateError, KafkaError

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None
    AIOKafkaConsumer = None
    TopicPartition = None
    _ConsumerRebalanceListener = object
    IllegalStateError = Exception
    KafkaError = Exception

# OpenTelemetry metrics and propagation imports - kept separate from TracingMixin
# These are NOT in TracingMixin and must be imported directly for metrics and inject/extract
try:
    from opentelemetry import metrics as otel_metrics
    from opentelemetry.metrics import CallbackOptions, Observation
    from opentelemetry.propagate import extract, inject
    from opentelemetry.trace import SpanKind, Status, StatusCode

    PROPAGATION_AVAILABLE = OTEL_AVAILABLE
except ImportError:
    otel_metrics = None  # type: ignore[assignment]
    CallbackOptions = None  # type: ignore[assignment, misc]
    Observation = None  # type: ignore[assignment, misc]
    extract = None  # type: ignore[assignment]
    inject = None  # type: ignore[assignment]
    SpanKind = None  # type: ignore[assignment, misc]
    Status = None  # type: ignore[assignment, misc]
    StatusCode = None  # type: ignore[assignment, misc]
    PROPAGATION_AVAILABLE = False

logger = logging.getLogger(__name__)

# Module-level meter cache for lazy initialization
_meter: Any = None


def _get_meter() -> Any:
    """Get or create the OpenTelemetry meter.

    Returns a meter instance for creating metric instruments. The meter is
    lazily initialized on first use and cached for subsequent calls.

    Returns:
        The meter instance, or None if OpenTelemetry is not available.
    """
    global _meter
    if not OTEL_AVAILABLE:
        return None
    if _meter is None:
        _meter = otel_metrics.get_meter("eventsource.bus.kafka")
    return _meter


class KafkaEventBusMetrics:
    """Container for Kafka event bus OpenTelemetry metric instruments.

    This class creates and holds all metric instruments used by the
    KafkaEventBus for observability. Instruments are created once at
    initialization and reused throughout the bus lifecycle.

    The metrics follow OpenTelemetry semantic conventions for messaging systems
    and use consistent attribute names across all instruments.

    Attributes:
        messages_published: Counter for messages published to Kafka.
            Attributes: messaging.system, messaging.destination, event.type
        messages_consumed: Counter for messages consumed from Kafka.
            Attributes: messaging.system, messaging.destination,
            messaging.kafka.partition, event.type
        handler_invocations: Counter for handler invocations.
            Attributes: handler.name, event.type
        handler_errors: Counter for handler errors.
            Attributes: handler.name, event.type, error.type
        dlq_messages: Counter for messages sent to dead letter queue.
            Attributes: dlq.reason, error.type
        connection_errors: Counter for connection errors.
            Attributes: error.type
        reconnections: Counter for reconnection attempts. No attributes.
        rebalances: Counter for consumer rebalances.
            Attributes: messaging.kafka.consumer_group
        publish_errors: Counter for publish errors.
            Attributes: messaging.system, messaging.destination,
            event.type, error.type
        publish_duration: Histogram for publish latency in milliseconds.
            Attributes: messaging.destination
        consume_duration: Histogram for consume/process latency in ms.
            Attributes: messaging.destination
        handler_duration: Histogram for handler execution time in ms.
            Attributes: handler.name, event.type
        batch_size: Histogram for publish batch sizes. No attributes.

    Note:
        Observable gauges (connections.active, consumer.lag) are registered
        separately on the KafkaEventBus instance via callback functions,
        not stored in this class.

    Example:
        >>> meter = _get_meter()
        >>> if meter:
        ...     metrics = KafkaEventBusMetrics(meter)
        ...     metrics.messages_published.add(1, {"event.type": "OrderCreated"})
    """

    def __init__(self, meter: Any) -> None:
        """Initialize metric instruments.

        Args:
            meter: OpenTelemetry meter instance for creating instruments.
        """
        # Counters
        self.messages_published = meter.create_counter(
            name="kafka.eventbus.messages.published",
            description="Total messages published to Kafka",
            unit="messages",
        )
        self.messages_consumed = meter.create_counter(
            name="kafka.eventbus.messages.consumed",
            description="Total messages consumed from Kafka",
            unit="messages",
        )
        self.handler_invocations = meter.create_counter(
            name="kafka.eventbus.handler.invocations",
            description="Total handler invocations",
            unit="invocations",
        )
        self.handler_errors = meter.create_counter(
            name="kafka.eventbus.handler.errors",
            description="Total handler errors",
            unit="errors",
        )
        self.dlq_messages = meter.create_counter(
            name="kafka.eventbus.messages.dlq",
            description="Total messages sent to dead letter queue",
            unit="messages",
        )
        self.connection_errors = meter.create_counter(
            name="kafka.eventbus.connection.errors",
            description="Total connection errors",
            unit="errors",
        )
        self.reconnections = meter.create_counter(
            name="kafka.eventbus.reconnections",
            description="Total reconnection attempts",
            unit="reconnections",
        )
        self.rebalances = meter.create_counter(
            name="kafka.eventbus.rebalances",
            description="Total consumer rebalances",
            unit="rebalances",
        )
        self.publish_errors = meter.create_counter(
            name="kafka.eventbus.publish.errors",
            description="Total publish errors",
            unit="errors",
        )

        # Histograms
        self.publish_duration = meter.create_histogram(
            name="kafka.eventbus.publish.duration",
            description="Time to publish messages to Kafka",
            unit="ms",
        )
        self.consume_duration = meter.create_histogram(
            name="kafka.eventbus.consume.duration",
            description="Time to process consumed messages",
            unit="ms",
        )
        self.handler_duration = meter.create_histogram(
            name="kafka.eventbus.handler.duration",
            description="Handler execution time",
            unit="ms",
        )
        self.batch_size = meter.create_histogram(
            name="kafka.eventbus.batch.size",
            description="Publish batch size",
            unit="messages",
        )


class KafkaNotAvailableError(ImportError):
    """Raised when aiokafka package is not installed.

    This exception is raised when attempting to use Kafka functionality
    without having the aiokafka package installed. The error message includes
    the installation command to help users resolve the issue.

    Example:
        >>> from eventsource.bus.kafka import KafkaEventBus
        >>> bus = KafkaEventBus()  # Raises if aiokafka not installed
        KafkaNotAvailableError: aiokafka package is not installed. ...
    """

    def __init__(self) -> None:
        """Initialize the error with a helpful installation message."""
        super().__init__(
            "aiokafka package is not installed. Install it with: pip install eventsource[kafka]"
        )


class DeserializationError(ValueError):
    """Raised when a message cannot be deserialized.

    This exception is raised when a Kafka message cannot be deserialized
    into a valid DomainEvent. These errors are unrecoverable and the message
    should be sent directly to the DLQ without retry attempts.

    Attributes:
        message: Description of the deserialization failure.
    """

    pass


class EventSerializer:
    """Base class for event serializers.

    This class defines the interface for serializing and deserializing events.
    Subclass this to implement custom serialization formats (Avro, Protobuf, etc.)
    or to integrate with schema registries.

    The default implementation uses JSON serialization via Pydantic.

    Example - Custom Avro Serializer:
        >>> class AvroEventSerializer(EventSerializer):
        ...     def __init__(self, schema_registry_url: str):
        ...         self._registry = SchemaRegistryClient(schema_registry_url)
        ...
        ...     def serialize(self, event: DomainEvent) -> bytes:
        ...         schema = self._get_schema(event.event_type)
        ...         return self._avro_serialize(event, schema)
        ...
        ...     def deserialize(
        ...         self,
        ...         data: bytes,
        ...         event_type: str,
        ...         event_class: type[DomainEvent],
        ...     ) -> DomainEvent:
        ...         schema = self._get_schema(event_type)
        ...         return self._avro_deserialize(data, schema, event_class)
    """

    def serialize(self, event: DomainEvent) -> bytes:
        """Serialize a domain event to bytes.

        Args:
            event: The event to serialize.

        Returns:
            The serialized event as bytes.

        Raises:
            ValueError: If serialization fails.
        """
        return event.model_dump_json().encode("utf-8")

    def deserialize(
        self,
        data: bytes,
        event_type: str,
        event_class: type[DomainEvent],
    ) -> DomainEvent:
        """Deserialize bytes to a domain event.

        Args:
            data: The serialized event data.
            event_type: The event type name (for logging/debugging).
            event_class: The event class to deserialize into.

        Returns:
            The deserialized DomainEvent.

        Raises:
            DeserializationError: If deserialization fails.
        """
        try:
            return event_class.model_validate_json(data)
        except Exception as e:
            raise DeserializationError(f"Failed to deserialize {event_type}: {e}") from e

    def content_type(self) -> str:
        """Get the content type for this serializer.

        Returns:
            The MIME content type string.
        """
        return "application/json"


@dataclass
class KafkaEventBusConfig:
    """Configuration for Kafka event bus.

    This configuration class provides all settings needed for connecting to
    Apache Kafka and managing producer/consumer behavior. It follows the same
    patterns as RabbitMQEventBusConfig for consistency across the eventsource
    library.

    Attributes:
        bootstrap_servers: Kafka broker addresses (comma-separated).
            Format: host1:port1,host2:port2
        topic_prefix: Prefix for topic names. The main topic will be
            {topic_prefix}.stream and DLQ will be {topic_prefix}.stream.dlq
        consumer_group: Consumer group ID for coordinated consumption.
            Multiple consumers in the same group share partitions.
        consumer_name: Unique name for this consumer instance. Auto-generated
            from hostname and UUID if not provided.
        acks: Producer acknowledgment level. Options:
            - "0": No acknowledgment (fire and forget)
            - "1": Leader acknowledgment only
            - "all": All in-sync replicas (default, most durable)
        compression_type: Compression for messages. Options:
            - None: No compression
            - "gzip": Good compression ratio (default)
            - "snappy": Fast compression
            - "lz4": Balanced compression
            - "zstd": Best compression ratio
        batch_size: Maximum size in bytes for batching messages.
        linger_ms: Time to wait for additional messages before sending batch.
        auto_offset_reset: Where to start consuming when no offset exists:
            - "earliest": Start from beginning (default)
            - "latest": Start from end
        session_timeout_ms: Consumer session timeout in milliseconds.
        heartbeat_interval_ms: Consumer heartbeat interval in milliseconds.
        max_poll_interval_ms: Maximum time between poll calls before consumer
            is considered failed.
        max_retries: Maximum retry attempts before sending to DLQ.
        retry_base_delay: Base delay in seconds for exponential backoff.
        retry_max_delay: Maximum delay in seconds between retries.
        retry_jitter: Fraction of delay to randomize (0.0 to 1.0).
        enable_dlq: Whether to enable dead letter queue.
        dlq_topic_suffix: Suffix for DLQ topic name.
        security_protocol: Security protocol for broker connection:
            - "PLAINTEXT": No security (development only)
            - "SSL": TLS encryption
            - "SASL_PLAINTEXT": SASL auth without encryption
            - "SASL_SSL": SASL auth with TLS (production recommended)
        sasl_mechanism: SASL mechanism when using SASL_* protocol:
            - "PLAIN": Simple username/password
            - "SCRAM-SHA-256": SCRAM with SHA-256
            - "SCRAM-SHA-512": SCRAM with SHA-512
        sasl_username: Username for SASL authentication.
        sasl_password: Password for SASL authentication.
        ssl_cafile: Path to CA certificate file.
        ssl_certfile: Path to client certificate for mTLS.
        ssl_keyfile: Path to client private key for mTLS.
        ssl_check_hostname: Whether to verify server hostname.
        enable_tracing: Enable OpenTelemetry tracing if available.
        enable_metrics: Enable OpenTelemetry metrics if available.
        shutdown_timeout: Timeout in seconds for graceful shutdown.

    Security Configuration Examples:

        Development (no security - NOT for production):

        >>> config = KafkaEventBusConfig(
        ...     bootstrap_servers="localhost:9092",
        ...     security_protocol="PLAINTEXT",
        ... )

        TLS only (encryption, no authentication):

        >>> config = KafkaEventBusConfig(
        ...     bootstrap_servers="kafka:9093",
        ...     security_protocol="SSL",
        ...     ssl_cafile="/path/to/ca.crt",
        ... )

        SASL/PLAIN with TLS (recommended for production):

        >>> config = KafkaEventBusConfig(
        ...     bootstrap_servers="kafka:9093",
        ...     security_protocol="SASL_SSL",
        ...     sasl_mechanism="PLAIN",
        ...     sasl_username="myuser",
        ...     sasl_password="mypassword",
        ...     ssl_cafile="/path/to/ca.crt",
        ... )

        SASL/SCRAM-SHA-512 with TLS (most secure):

        >>> config = KafkaEventBusConfig(
        ...     bootstrap_servers="kafka:9093",
        ...     security_protocol="SASL_SSL",
        ...     sasl_mechanism="SCRAM-SHA-512",
        ...     sasl_username="myuser",
        ...     sasl_password="mypassword",
        ...     ssl_cafile="/path/to/ca.crt",
        ... )

        Mutual TLS (mTLS) - client certificate authentication:

        >>> config = KafkaEventBusConfig(
        ...     bootstrap_servers="kafka:9093",
        ...     security_protocol="SSL",
        ...     ssl_cafile="/path/to/ca.crt",
        ...     ssl_certfile="/path/to/client.crt",
        ...     ssl_keyfile="/path/to/client.key",
        ... )

    Raises:
        ValueError: If security configuration is invalid (e.g., SASL protocol
            without credentials, mismatched SSL cert/key files).
    """

    # Connection
    bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "events"
    consumer_group: str = "default"
    consumer_name: str | None = None

    # Producer settings
    acks: str = "all"
    compression_type: str | None = "gzip"
    batch_size: int = 16384
    linger_ms: int = 5

    # Consumer settings
    auto_offset_reset: str = "earliest"
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 10000
    max_poll_interval_ms: int = 300000

    # Error handling
    max_retries: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    retry_jitter: float = 0.1

    # DLQ settings
    enable_dlq: bool = True
    dlq_topic_suffix: str = ".dlq"
    dlq_consumer_group: str | None = None  # Consumer group for DLQ processing
    dlq_max_replay_attempts: int = 3  # Maximum times a message can be replayed

    # Security
    security_protocol: str = "PLAINTEXT"
    sasl_mechanism: str | None = None
    sasl_username: str | None = None
    sasl_password: str | None = None
    ssl_cafile: str | None = None
    ssl_certfile: str | None = None
    ssl_keyfile: str | None = None
    ssl_check_hostname: bool = True

    # Observability
    enable_tracing: bool = True
    enable_metrics: bool = True

    # Shutdown
    shutdown_timeout: float = 30.0

    def __post_init__(self) -> None:
        """Auto-generate consumer_name if not provided and validate configuration.

        Raises:
            ValueError: If security configuration is invalid.
        """
        if self.consumer_name is None:
            hostname = socket.gethostname()
            unique_id = uuid.uuid4().hex[:8]
            self.consumer_name = f"{hostname}-{unique_id}"

        # Validate security configuration
        self._validate_security_config()

    def _validate_security_config(self) -> None:
        """Validate security configuration consistency.

        Validates that the security protocol, SASL mechanism, and SSL/TLS
        configuration are consistent and complete. Logs warnings for insecure
        configurations that may be acceptable for development but not production.

        Raises:
            ValueError: If security configuration is invalid.
        """
        valid_protocols = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
        if self.security_protocol not in valid_protocols:
            raise ValueError(
                f"Invalid security_protocol: {self.security_protocol}. "
                f"Must be one of: {valid_protocols}"
            )

        # SASL validation
        if self.security_protocol.startswith("SASL_"):
            valid_mechanisms = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}
            if not self.sasl_mechanism:
                raise ValueError(f"sasl_mechanism required for {self.security_protocol}")
            if self.sasl_mechanism not in valid_mechanisms:
                raise ValueError(
                    f"Invalid sasl_mechanism: {self.sasl_mechanism}. "
                    f"Must be one of: {valid_mechanisms}"
                )
            if not self.sasl_username or not self.sasl_password:
                raise ValueError("sasl_username and sasl_password required for SASL authentication")

        # SSL file validation for mTLS
        if self.security_protocol in ("SSL", "SASL_SSL"):
            if self.ssl_certfile and not self.ssl_keyfile:
                raise ValueError("ssl_keyfile required when ssl_certfile is provided (mTLS)")
            if self.ssl_keyfile and not self.ssl_certfile:
                raise ValueError("ssl_certfile required when ssl_keyfile is provided (mTLS)")

        # Warn about insecure configurations
        if self.security_protocol == "PLAINTEXT":
            logger.warning("Using PLAINTEXT security protocol - not recommended for production")
        if self.security_protocol == "SASL_PLAINTEXT":
            logger.warning("Using SASL without SSL - credentials sent in plain text")
        if "SSL" in self.security_protocol and not self.ssl_check_hostname:
            logger.warning("SSL hostname verification disabled - vulnerable to MITM attacks")

    @property
    def topic_name(self) -> str:
        """Get the main topic name."""
        return f"{self.topic_prefix}.stream"

    @property
    def dlq_topic_name(self) -> str:
        """Get the dead letter queue topic name."""
        return f"{self.topic_name}{self.dlq_topic_suffix}"

    def get_producer_config(self) -> dict[str, Any]:
        """Get aiokafka producer configuration dict.

        Returns:
            Dictionary of producer configuration for AIOKafkaProducer.
        """
        config: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "acks": self.acks,
            "compression_type": self.compression_type,
            "max_batch_size": self.batch_size,
            "linger_ms": self.linger_ms,
        }
        self._add_security_config(config)
        return config

    def get_consumer_config(self) -> dict[str, Any]:
        """Get aiokafka consumer configuration dict.

        Returns:
            Dictionary of consumer configuration for AIOKafkaConsumer.
        """
        config: dict[str, Any] = {
            "bootstrap_servers": self.bootstrap_servers,
            "group_id": self.consumer_group,
            "client_id": self.consumer_name,
            "auto_offset_reset": self.auto_offset_reset,
            "session_timeout_ms": self.session_timeout_ms,
            "heartbeat_interval_ms": self.heartbeat_interval_ms,
            "max_poll_interval_ms": self.max_poll_interval_ms,
            "enable_auto_commit": False,  # Manual commit for at-least-once
        }
        self._add_security_config(config)
        return config

    def _add_security_config(self, config: dict[str, Any]) -> None:
        """Add security configuration to a config dict.

        Args:
            config: The configuration dictionary to add security settings to.
        """
        config["security_protocol"] = self.security_protocol

        if self.sasl_mechanism:
            config["sasl_mechanism"] = self.sasl_mechanism
        if self.sasl_username:
            config["sasl_plain_username"] = self.sasl_username
        if self.sasl_password:
            config["sasl_plain_password"] = self.sasl_password
        if self.ssl_cafile:
            config["ssl_cafile"] = self.ssl_cafile
        if self.ssl_certfile:
            config["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            config["ssl_keyfile"] = self.ssl_keyfile

        # Only set ssl_check_hostname if using SSL
        if "SSL" in self.security_protocol:
            config["ssl_check_hostname"] = self.ssl_check_hostname

    def create_ssl_context(self) -> ssl.SSLContext | None:
        """Create an SSL context from configuration.

        Creates and configures an SSLContext based on the security settings.
        This is useful when you need to customize the SSL context beyond what
        aiokafka provides by default, or for testing SSL configurations.

        Returns:
            Configured SSLContext if using SSL/TLS, None otherwise.

        Raises:
            ssl.SSLError: If certificate files are invalid or cannot be loaded.
            FileNotFoundError: If specified certificate files do not exist.

        Example:
            >>> config = KafkaEventBusConfig(
            ...     security_protocol="SSL",
            ...     ssl_cafile="/path/to/ca.crt",
            ... )
            >>> context = config.create_ssl_context()
            >>> assert context is not None
        """
        if "SSL" not in self.security_protocol:
            return None

        context = ssl.create_default_context()

        # Load CA certificates
        if self.ssl_cafile:
            context.load_verify_locations(self.ssl_cafile)

        # Load client certificate for mTLS
        if self.ssl_certfile and self.ssl_keyfile:
            context.load_cert_chain(
                certfile=self.ssl_certfile,
                keyfile=self.ssl_keyfile,
            )

        # Configure hostname verification
        if not self.ssl_check_hostname:
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
        else:
            context.check_hostname = True
            context.verify_mode = ssl.CERT_REQUIRED

        return context

    def get_sanitized_config(self) -> dict[str, Any]:
        """Get configuration with sensitive values redacted.

        Returns a copy of the configuration suitable for logging and debugging,
        with passwords and private key paths replaced with placeholder values.
        This prevents accidental credential exposure in logs.

        Returns:
            Configuration dictionary safe for logging. Sensitive values
            (sasl_password, ssl_keyfile) are redacted with "***".

        Example:
            >>> config = KafkaEventBusConfig(
            ...     sasl_password="secret123",
            ...     ssl_keyfile="/path/to/key",
            ... )
            >>> sanitized = config.get_sanitized_config()
            >>> assert sanitized["sasl_password"] == "***"
            >>> assert sanitized["ssl_keyfile"] == "***"
        """
        return {
            "bootstrap_servers": self.bootstrap_servers,
            "topic_prefix": self.topic_prefix,
            "consumer_group": self.consumer_group,
            "consumer_name": self.consumer_name,
            "security_protocol": self.security_protocol,
            "sasl_mechanism": self.sasl_mechanism,
            "sasl_username": self.sasl_username,
            "sasl_password": "***" if self.sasl_password else None,
            "ssl_cafile": self.ssl_cafile,
            "ssl_certfile": self.ssl_certfile,
            "ssl_keyfile": "***" if self.ssl_keyfile else None,
            "ssl_check_hostname": self.ssl_check_hostname,
            "enable_dlq": self.enable_dlq,
            "enable_tracing": self.enable_tracing,
            "enable_metrics": self.enable_metrics,
        }


@dataclass
class KafkaEventBusStats:
    """Statistics for Kafka event bus operations.

    Tracks operational metrics for monitoring and debugging. Statistics are
    updated atomically during publish and consume operations.

    Attributes:
        events_published: Total number of events successfully published.
        events_consumed: Total number of events consumed from Kafka.
        events_processed_success: Events successfully processed by handlers.
        events_processed_failed: Events that failed handler processing.
        messages_sent_to_dlq: Events sent to dead letter queue.
        handler_errors: Total handler exceptions caught.
        reconnections: Number of reconnection attempts.
        rebalance_count: Number of consumer group rebalances.
        last_publish_at: Timestamp of last successful publish.
        last_consume_at: Timestamp of last successful consume.
        last_error_at: Timestamp of last error.
        connected_at: Timestamp when connection was established.
    """

    # Counters
    events_published: int = 0
    events_consumed: int = 0
    events_processed_success: int = 0
    events_processed_failed: int = 0
    messages_sent_to_dlq: int = 0
    handler_errors: int = 0
    reconnections: int = 0

    # Kafka-specific
    rebalance_count: int = 0

    # Timing
    last_publish_at: datetime | None = None
    last_consume_at: datetime | None = None
    last_error_at: datetime | None = None
    connected_at: datetime | None = None

    def get_stats_dict(self) -> dict[str, Any]:
        """Return statistics as a JSON-serializable dictionary.

        Returns:
            Dictionary with all statistics, datetimes converted to ISO format.
        """
        return {
            "events_published": self.events_published,
            "events_consumed": self.events_consumed,
            "events_processed_success": self.events_processed_success,
            "events_processed_failed": self.events_processed_failed,
            "messages_sent_to_dlq": self.messages_sent_to_dlq,
            "handler_errors": self.handler_errors,
            "reconnections": self.reconnections,
            "rebalance_count": self.rebalance_count,
            "last_publish_at": (self.last_publish_at.isoformat() if self.last_publish_at else None),
            "last_consume_at": (self.last_consume_at.isoformat() if self.last_consume_at else None),
            "last_error_at": (self.last_error_at.isoformat() if self.last_error_at else None),
            "connected_at": (self.connected_at.isoformat() if self.connected_at else None),
        }


class KafkaRebalanceListener(_ConsumerRebalanceListener):  # type: ignore[misc]
    """Consumer rebalance listener for handling partition assignment changes.

    This listener is called during consumer group rebalances to ensure proper
    offset management and prevent duplicate message processing during scaling
    events.

    The listener commits offsets for revoked partitions before they are
    reassigned to other consumers, ensuring at-least-once delivery guarantees
    are maintained during rebalances.

    Attributes:
        _bus: Reference to the KafkaEventBus instance for offset commits
            and metrics recording.
    """

    def __init__(self, bus: KafkaEventBus) -> None:
        """Initialize the rebalance listener.

        Args:
            bus: The KafkaEventBus instance to coordinate with.
        """
        self._bus = bus

    async def on_partitions_revoked(
        self,
        revoked: set[TopicPartition],
    ) -> None:
        """Called when partitions are being revoked from this consumer.

        This method commits offsets for all revoked partitions before they
        are assigned to other consumers. This ensures that:
        1. Messages processed but not yet committed are properly committed
        2. The new consumer starts from the correct offset
        3. No messages are processed twice due to rebalance

        Args:
            revoked: Set of TopicPartition objects being revoked.
        """
        if not revoked:
            return

        logger.info(
            "Partitions being revoked, committing offsets",
            extra={
                "revoked_partitions": [
                    {"topic": tp.topic, "partition": tp.partition} for tp in revoked
                ],
                "consumer_group": self._bus._config.consumer_group,
            },
        )

        # Commit offsets before partitions are revoked
        if self._bus._consumer:
            try:
                await self._bus._consumer.commit()
                logger.debug("Offsets committed before partition revocation")
            except IllegalStateError:
                # No partitions currently assigned - nothing to commit
                # This is expected during certain rebalance scenarios
                logger.debug("No partitions to commit during rebalance")
            except Exception as e:
                logger.warning(
                    "Failed to commit offsets during rebalance",
                    extra={"error": str(e)},
                )

        # Record the rebalance event
        self._bus.record_rebalance()

    async def on_partitions_assigned(
        self,
        assigned: set[TopicPartition],
    ) -> None:
        """Called when new partitions are assigned to this consumer.

        This method is called after partitions have been assigned and can
        be used for any initialization needed for the new partitions.

        Args:
            assigned: Set of TopicPartition objects newly assigned.
        """
        if not assigned:
            return

        logger.info(
            "New partitions assigned",
            extra={
                "assigned_partitions": [
                    {"topic": tp.topic, "partition": tp.partition} for tp in assigned
                ],
                "consumer_group": self._bus._config.consumer_group,
            },
        )


class KafkaEventBus(EventBus):
    """Kafka implementation of the EventBus interface.

    Provides a distributed event bus using Apache Kafka for high-throughput
    event distribution. Supports consumer groups, dead letter queues, and
    optional OpenTelemetry tracing.

    The bus creates events on a single topic ({topic_prefix}.stream) and uses
    the aggregate_id as the partition key to ensure ordering within aggregates.

    Thread Safety:
        - Subscription methods are thread-safe
        - Publishing and consuming should only be called from async context

    Example:
        >>> config = KafkaEventBusConfig(
        ...     bootstrap_servers="localhost:9092",
        ...     topic_prefix="myapp.events",
        ...     consumer_group="projections",
        ... )
        >>> async with KafkaEventBus(config=config) as bus:
        ...     await bus.publish([event])
        ...     bus.subscribe(OrderCreated, handler)
        ...     await bus.start_consuming()
    """

    def __init__(
        self,
        config: KafkaEventBusConfig | None = None,
        event_registry: EventRegistry | None = None,
        serializer: EventSerializer | None = None,
        *,
        tracer: Tracer | None = None,
    ) -> None:
        """Initialize the Kafka event bus.

        Args:
            config: Configuration for Kafka connection. Uses defaults if None.
            event_registry: Registry for event type resolution. Uses global
                registry if None.
            serializer: Custom event serializer. Uses JSON serializer if None.
                Implement EventSerializer to add Avro, Protobuf, or schema
                registry support.
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on config.enable_tracing setting.

        Raises:
            KafkaNotAvailableError: If aiokafka is not installed.
        """
        if not KAFKA_AVAILABLE:
            raise KafkaNotAvailableError()

        self._config = config or KafkaEventBusConfig()
        self._event_registry = event_registry
        self._serializer = serializer or EventSerializer()

        # Initialize tracing via composition (replaces TracingMixin)
        self._tracer = tracer or create_tracer(__name__, self._config.enable_tracing)
        self._enable_tracing = self._tracer.enabled

        # Connection state
        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._rebalance_listener: KafkaRebalanceListener | None = None
        self._connected = False
        self._consuming = False
        self._consume_task: asyncio.Task[None] | None = None

        # Handler storage (follows Redis/RabbitMQ pattern)
        self._handlers: dict[str, list[HandlerAdapter]] = defaultdict(list)
        self._wildcard_handlers: list[HandlerAdapter] = []

        # Statistics
        self._stats = KafkaEventBusStats()

        # Initialize metrics (lazy initialization like tracing)
        self._metrics: KafkaEventBusMetrics | None = None
        self._meter: Any = None  # Store meter for gauge registration
        if self._config.enable_metrics:
            self._meter = _get_meter()
            if self._meter:
                self._metrics = KafkaEventBusMetrics(self._meter)

        # Track if gauges are registered (gauges can only be registered once)
        self._connection_gauge_registered = False
        self._lag_gauge_registered = False

        # Shutdown coordination
        self._shutdown_event = asyncio.Event()

        logger.debug(
            "KafkaEventBus initialized",
            extra=self._config.get_sanitized_config(),
        )

    # =========================================================================
    # Properties
    # =========================================================================

    @property
    def is_connected(self) -> bool:
        """Check if connected to Kafka.

        Returns:
            True if producer and consumer are connected.
        """
        return self._connected

    @property
    def is_consuming(self) -> bool:
        """Check if actively consuming messages.

        Returns:
            True if consume loop is running.
        """
        return self._consuming

    @property
    def config(self) -> KafkaEventBusConfig:
        """Get the configuration.

        Returns:
            The KafkaEventBusConfig instance.
        """
        return self._config

    @property
    def stats(self) -> KafkaEventBusStats:
        """Get current statistics.

        Returns:
            The KafkaEventBusStats instance.
        """
        return self._stats

    # =========================================================================
    # Connection Lifecycle Methods
    # =========================================================================

    async def connect(self) -> None:
        """Connect to Kafka cluster.

        Creates and starts the producer and consumer clients. The consumer
        subscribes to the configured topic.

        Raises:
            KafkaError: If connection to Kafka fails.
        """
        if self._connected:
            logger.warning("KafkaEventBus already connected")
            return

        logger.info(
            "Connecting to Kafka",
            extra=self._config.get_sanitized_config(),
        )

        try:
            # Create and start producer
            self._producer = AIOKafkaProducer(**self._config.get_producer_config())
            await self._producer.start()

            # Create and start consumer
            # Note: We pass the topic to the constructor for proper metadata loading.
            # aiokafka handles consumer group rebalancing internally; we track
            # rebalance events through the record_rebalance() method which can be
            # called externally or during reconnection.
            self._consumer = AIOKafkaConsumer(
                self._config.topic_name,
                **self._config.get_consumer_config(),
            )
            await self._consumer.start()

            # Initialize rebalance listener (available for future use or manual tracking)
            self._rebalance_listener = KafkaRebalanceListener(self)

            self._connected = True
            self._stats.connected_at = datetime.now(UTC)

            # Register connection status gauge (only once)
            self._register_connection_gauge()

            logger.info(
                "Connected to Kafka",
                extra={
                    "topic": self._config.topic_name,
                    "consumer_group": self._config.consumer_group,
                },
            )

        except Exception as e:
            # Increment connection_errors counter
            if self._metrics:
                self._metrics.connection_errors.add(
                    1,
                    attributes={
                        "error.type": type(e).__name__,
                    },
                )

            logger.error(
                "Failed to connect to Kafka",
                extra={"error": str(e)},
                exc_info=True,
            )
            # Clean up partial connection
            await self._cleanup_connections()
            raise

    async def disconnect(self) -> None:
        """Disconnect from Kafka cluster.

        Stops consuming if active and closes producer/consumer connections.
        Safe to call multiple times.
        """
        if not self._connected:
            logger.debug("KafkaEventBus not connected, nothing to disconnect")
            return

        logger.info("Disconnecting from Kafka")

        # Stop consuming first
        if self._consuming:
            await self.stop_consuming()

        await self._cleanup_connections()
        self._connected = False

        logger.info("Disconnected from Kafka")

    async def _cleanup_connections(self) -> None:
        """Clean up producer and consumer connections."""
        if self._producer:
            try:
                await self._producer.stop()
            except Exception as e:
                logger.warning(f"Error stopping producer: {e}")
            self._producer = None

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer: {e}")
            self._consumer = None

    # =========================================================================
    # Context Manager Support
    # =========================================================================

    async def __aenter__(self) -> KafkaEventBus:
        """Enter async context manager.

        Connects to Kafka and returns the bus instance.

        Returns:
            The connected KafkaEventBus instance.
        """
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        """Exit async context manager.

        Gracefully shuts down the bus with configured timeout.
        """
        await self.shutdown(timeout=self._config.shutdown_timeout)

    # =========================================================================
    # Shutdown
    # =========================================================================

    async def shutdown(self, timeout: float | None = None) -> None:
        """Gracefully shutdown the event bus.

        Stops consuming, waits for in-flight messages, and disconnects.

        Args:
            timeout: Maximum time to wait for shutdown. Uses config default
                if None.
        """
        timeout = timeout or self._config.shutdown_timeout

        logger.info("Shutting down KafkaEventBus", extra={"timeout": timeout})

        # Signal shutdown
        self._shutdown_event.set()

        # Wait for consume task to finish
        if self._consume_task and not self._consume_task.done():
            try:
                await asyncio.wait_for(self._consume_task, timeout=timeout)
            except TimeoutError:
                logger.warning("Shutdown timed out, cancelling consume task")
                self._consume_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await self._consume_task

        # Disconnect
        await self.disconnect()

        logger.info("KafkaEventBus shutdown complete")

    async def stop_consuming(self) -> None:
        """Stop the consumer loop gracefully.

        Sets the consuming flag to False which will cause the consume loop
        to exit on its next iteration.
        """
        self._consuming = False
        logger.info("Stop consuming requested")

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def get_stats_dict(self) -> dict[str, Any]:
        """Get statistics as a dictionary.

        Returns:
            JSON-serializable dictionary of statistics.
        """
        return self._stats.get_stats_dict()

    async def get_topic_info(self) -> dict[str, Any]:
        """Get information about the configured topic.

        Returns:
            Dictionary with topic metadata.

        Raises:
            RuntimeError: If not connected.
        """
        if not self._connected or not self._consumer:
            raise RuntimeError("Not connected to Kafka")

        partitions = self._consumer.partitions_for_topic(self._config.topic_name)

        return {
            "topic": self._config.topic_name,
            "partitions": list(partitions) if partitions else [],
            "consumer_group": self._config.consumer_group,
            "connected": self._connected,
            "consuming": self._consuming,
        }

    def record_reconnection(self) -> None:
        """Record a reconnection event for metrics.

        Call this method when a reconnection to Kafka occurs. Updates both
        the internal stats counter and the OpenTelemetry metrics counter.

        This method is safe to call even if metrics are disabled.

        Example:
            >>> # In reconnection logic
            >>> await self._reconnect_to_kafka()
            >>> self.record_reconnection()
        """
        self._stats.reconnections += 1

        if self._metrics:
            self._metrics.reconnections.add(1)

        logger.debug("Reconnection recorded")

    def record_rebalance(self) -> None:
        """Record a consumer rebalance event for metrics.

        Call this method when a consumer group rebalance occurs. Updates both
        the internal stats counter and the OpenTelemetry metrics counter.

        This method is safe to call even if metrics are disabled.

        Example:
            >>> # In rebalance callback
            >>> def on_rebalance(revoked, assigned):
            ...     self.record_rebalance()
        """
        self._stats.rebalance_count += 1

        if self._metrics:
            self._metrics.rebalances.add(
                1,
                attributes={
                    "messaging.kafka.consumer_group": self._config.consumer_group,
                },
            )

        logger.debug(
            "Rebalance recorded",
            extra={"consumer_group": self._config.consumer_group},
        )

    # =========================================================================
    # Observable Gauge Methods
    # =========================================================================

    def _register_connection_gauge(self) -> None:
        """Register connection status as an observable gauge.

        Reports 1 when connected, 0 when disconnected. This provides
        visibility into connection uptime and disconnection events.

        The gauge is registered once and the callback is invoked by the
        OpenTelemetry SDK at its configured collection interval.
        """
        if not self._meter or not self._config.enable_metrics:
            return

        if self._connection_gauge_registered:
            return  # Gauges can only be registered once

        # Capture self reference for closure
        bus = self

        def connection_callback(options: CallbackOptions) -> Iterable[Observation]:
            """Callback to report connection status.

            Args:
                options: Callback options from OpenTelemetry SDK.

            Yields:
                Observation with connection status (0 or 1).
            """
            yield Observation(
                1 if bus._connected else 0,
                attributes={
                    "messaging.kafka.consumer_group": bus._config.consumer_group,
                },
            )

        self._meter.create_observable_gauge(
            name="kafka.eventbus.connections.active",
            callbacks=[connection_callback],
            description="Connection status (1=connected, 0=disconnected)",
            unit="connections",
        )

        self._connection_gauge_registered = True
        logger.debug("Connection status gauge registered")

    def _register_consumer_lag_gauge(self) -> None:
        """Register consumer lag as an observable gauge.

        The gauge reports lag per partition, calculated as the difference
        between the high watermark (latest offset) and the current position.
        This callback is invoked by the OpenTelemetry SDK at its configured
        collection interval (default: 60 seconds).

        Should be called after consumer starts and has partition assignments.
        """
        if not self._meter or not self._config.enable_metrics:
            return

        if self._lag_gauge_registered:
            return  # Gauges can only be registered once

        # Capture self reference for closure
        bus = self

        def lag_callback(options: CallbackOptions) -> Iterable[Observation]:
            """Callback to report consumer lag per partition.

            Args:
                options: Callback options from OpenTelemetry SDK.

            Yields:
                Observation objects with lag values and partition attributes.
            """
            # Only report when consuming and have a valid consumer
            if not bus._consuming or not bus._consumer or not bus._connected:
                return

            try:
                assignment = bus._consumer.assignment()
                if not assignment:
                    return

                for tp in assignment:
                    try:
                        # Get high watermark (latest offset in partition)
                        highwater = bus._consumer.highwater(tp)
                        # Get current position (next offset to be consumed)
                        position = bus._consumer.position(tp)

                        if highwater is not None and position is not None:
                            # Lag is the difference, clamped to >= 0
                            lag = max(0, highwater - position)
                            yield Observation(
                                lag,
                                attributes={
                                    "messaging.kafka.partition": tp.partition,
                                    "messaging.kafka.consumer_group": bus._config.consumer_group,
                                    "messaging.destination": tp.topic,
                                },
                            )
                    except Exception as e:
                        # Skip partitions with errors (e.g., during rebalance)
                        logger.debug(
                            "Skipping partition %s lag metric due to error: %s",
                            tp,
                            e,
                        )
            except Exception as e:
                # Skip if consumer is in invalid state
                logger.debug("Unable to collect consumer lag metrics: %s", e)

        self._meter.create_observable_gauge(
            name="kafka.eventbus.consumer.lag",
            callbacks=[lag_callback],
            description="Consumer lag per partition (messages behind)",
            unit="messages",
        )

        self._lag_gauge_registered = True
        logger.debug("Consumer lag gauge registered")

    # =========================================================================
    # Publish Methods
    # =========================================================================

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        """Publish events to Kafka.

        Events are published to the configured topic with the aggregate_id as
        the partition key. This ensures events for the same aggregate are
        processed in order.

        When tracing is enabled, creates OpenTelemetry spans for each publish
        operation and injects trace context into Kafka message headers for
        distributed tracing correlation.

        Args:
            events: List of domain events to publish.
            background: If True, don't wait for broker acknowledgment.
                Faster but less reliable.

        Raises:
            RuntimeError: If not connected to Kafka.
            KafkaError: If publishing fails.
        """
        if not self._connected or not self._producer:
            raise RuntimeError("Not connected to Kafka. Call connect() first.")

        if not events:
            return

        # Start timing for publish duration histogram
        start_time = time.perf_counter()

        logger.debug(
            "Publishing events to Kafka",
            extra={
                "event_count": len(events),
                "topic": self._config.topic_name,
                "background": background,
            },
        )

        for event in events:
            await self._publish_single_event(event, background)

        # Update statistics
        self._stats.events_published += len(events)
        self._stats.last_publish_at = datetime.now(UTC)

        # Record publish duration and batch size histograms
        if self._metrics:
            duration_ms = (time.perf_counter() - start_time) * 1000
            self._metrics.publish_duration.record(
                duration_ms,
                attributes={
                    "messaging.destination": self._config.topic_name,
                },
            )
            self._metrics.batch_size.record(len(events))

        logger.debug(
            "Events published successfully",
            extra={"event_count": len(events)},
        )

    async def _publish_single_event(
        self,
        event: DomainEvent,
        background: bool,
    ) -> None:
        """Publish a single event with optional tracing span.

        Creates an OpenTelemetry span for the publish operation if tracing
        is enabled. The span includes messaging semantic attributes and
        event metadata for distributed tracing correlation.

        Args:
            event: The event to publish.
            background: Whether to wait for acknowledgment.
        """
        if not self._producer:
            raise RuntimeError("Producer not connected")

        # Serialize event
        key = self._get_partition_key(event)
        value = self._serialize_event(event)
        headers = self._create_headers(event)

        # Create span for publish using composition-based tracer
        if self._enable_tracing:
            with self._tracer.span_with_kind(
                name=f"eventsource.event_bus.publish {event.event_type}",
                kind=SpanKindEnum.PRODUCER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "kafka",
                    ATTR_MESSAGING_DESTINATION: self._config.topic_name,
                    "messaging.destination_kind": "topic",
                    ATTR_MESSAGING_OPERATION: "publish",
                    ATTR_EVENT_TYPE: event.event_type,
                    ATTR_EVENT_ID: str(event.event_id),
                    ATTR_AGGREGATE_ID: str(event.aggregate_id),
                    ATTR_AGGREGATE_TYPE: event.aggregate_type,
                },
            ) as span:
                # Inject trace context into headers for distributed tracing
                if PROPAGATION_AVAILABLE and inject is not None:
                    carrier: dict[str, str] = {}
                    inject(carrier)
                    for trace_key, trace_value in carrier.items():
                        headers.append((trace_key, trace_value.encode("utf-8")))

                await self._send_to_kafka(key, value, headers, background, span, event)
        else:
            await self._send_to_kafka(key, value, headers, background, None, event)

    async def _send_to_kafka(
        self,
        key: bytes,
        value: bytes,
        headers: list[tuple[str, bytes]],
        background: bool,
        span: Any,
        event: DomainEvent,
    ) -> None:
        """Send message to Kafka.

        Args:
            key: Message key.
            value: Message value.
            headers: Message headers.
            background: Whether to wait for acknowledgment.
            span: The current tracing span, or None.
            event: The domain event being published (for metrics).
        """
        if not self._producer:
            raise RuntimeError("Producer not connected")

        try:
            future = await self._producer.send(
                topic=self._config.topic_name,
                key=key,
                value=value,
                headers=headers,
            )

            if background:
                # For background publishes, add a callback to track delivery
                # This ensures errors are logged and metrics are updated correctly
                self._track_background_publish(future, event, span)
            else:
                record_metadata = await future

                if span:
                    span.set_attribute("messaging.kafka.partition", record_metadata.partition)
                    span.set_attribute("messaging.kafka.offset", record_metadata.offset)

                logger.debug(
                    "Event published",
                    extra={
                        "topic": record_metadata.topic,
                        "partition": record_metadata.partition,
                        "offset": record_metadata.offset,
                    },
                )

                # Increment messages_published counter on confirmed success
                if self._metrics:
                    self._metrics.messages_published.add(
                        1,
                        attributes={
                            "messaging.system": "kafka",
                            "messaging.destination": self._config.topic_name,
                            "event.type": event.event_type,
                        },
                    )

        except Exception as e:
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)

            # Increment publish_errors counter
            if self._metrics:
                self._metrics.publish_errors.add(
                    1,
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": self._config.topic_name,
                        "event.type": event.event_type,
                        "error.type": type(e).__name__,
                    },
                )

            logger.error(
                "Failed to publish event",
                extra={"error": str(e)},
                exc_info=True,
            )
            self._stats.last_error_at = datetime.now(UTC)
            raise

    def _track_background_publish(
        self,
        future: Any,
        event: DomainEvent,
        span: Any,
    ) -> None:
        """Track a background publish using a callback.

        Adds a callback to the future to track delivery success/failure
        for background publishes. This ensures that errors are not silently
        lost and metrics are updated correctly.

        Args:
            future: The future from producer.send()
            event: The domain event being published (for metrics).
            span: The current tracing span, or None.
        """
        event_type = event.event_type
        event_id = str(event.event_id)
        topic = self._config.topic_name

        def on_send_success(record_metadata: Any) -> None:
            """Callback for successful background publish."""
            logger.debug(
                "Background event published successfully",
                extra={
                    "topic": record_metadata.topic,
                    "partition": record_metadata.partition,
                    "offset": record_metadata.offset,
                    "event_type": event_type,
                    "event_id": event_id,
                },
            )

            # Increment messages_published counter on confirmed success
            if self._metrics:
                self._metrics.messages_published.add(
                    1,
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": topic,
                        "event.type": event_type,
                    },
                )

        def on_send_error(exc: Exception) -> None:
            """Callback for failed background publish."""
            logger.error(
                "Background event publish failed",
                extra={
                    "error": str(exc),
                    "event_type": event_type,
                    "event_id": event_id,
                },
                exc_info=False,  # Exception is passed, don't need traceback
            )

            self._stats.last_error_at = datetime.now(UTC)

            # Increment publish_errors counter
            if self._metrics:
                self._metrics.publish_errors.add(
                    1,
                    attributes={
                        "messaging.system": "kafka",
                        "messaging.destination": topic,
                        "event.type": event_type,
                        "error.type": type(exc).__name__,
                    },
                )

            if span:
                span.set_status(Status(StatusCode.ERROR, str(exc)))
                span.record_exception(exc)

        # Add callbacks to track the result
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)

    def _get_partition_key(self, event: DomainEvent) -> bytes:
        """Get the partition key for an event.

        Uses aggregate_id to ensure events for the same aggregate
        are sent to the same partition, preserving order.

        Args:
            event: The domain event.

        Returns:
            Partition key as bytes.
        """
        return str(event.aggregate_id).encode("utf-8")

    def _serialize_event(self, event: DomainEvent) -> bytes:
        """Serialize an event to bytes using the configured serializer.

        Args:
            event: The domain event to serialize.

        Returns:
            Serialized event as bytes (format depends on serializer).
        """
        return self._serializer.serialize(event)

    def _create_headers(self, event: DomainEvent) -> list[tuple[str, bytes]]:
        """Create Kafka headers from event metadata.

        Headers enable routing and filtering without deserializing the
        message body.

        Args:
            event: The domain event.

        Returns:
            List of header tuples (name, value).
        """
        headers: list[tuple[str, bytes]] = [
            ("event_id", str(event.event_id).encode("utf-8")),
            ("event_type", event.event_type.encode("utf-8")),
            ("aggregate_id", str(event.aggregate_id).encode("utf-8")),
            ("aggregate_type", event.aggregate_type.encode("utf-8")),
            ("aggregate_version", str(event.aggregate_version).encode("utf-8")),
            ("occurred_at", event.occurred_at.isoformat().encode("utf-8")),
            ("correlation_id", str(event.correlation_id).encode("utf-8")),
        ]

        # Optional headers
        if event.tenant_id:
            headers.append(("tenant_id", str(event.tenant_id).encode("utf-8")))
        if event.causation_id:
            headers.append(("causation_id", str(event.causation_id).encode("utf-8")))
        if event.actor_id:
            headers.append(("actor_id", str(event.actor_id).encode("utf-8")))

        return headers

    # =========================================================================
    # Subscribe Methods
    # =========================================================================

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to a specific event type.

        The handler will be called when events of the specified type are
        consumed. Handlers are stored in memory and not persisted to Kafka.

        Both sync and async handlers are supported. Sync handlers are
        automatically wrapped to be async internally.

        Args:
            event_type: The event class to subscribe to.
            handler: The handler to invoke. Can be:
                - An EventHandler instance with handle() method
                - A sync callable taking a DomainEvent
                - An async callable taking a DomainEvent

        Example:
            >>> bus.subscribe(OrderCreated, my_handler)
            >>> bus.subscribe(OrderCreated, lambda e: print(e))
            >>> bus.subscribe(OrderCreated, MyEventHandler())
        """
        event_type_name = event_type.__name__
        adapter = HandlerAdapter(handler)
        self._handlers[event_type_name].append(adapter)

        logger.debug(
            "Handler subscribed",
            extra={
                "event_type": event_type_name,
                "handler": adapter.name,
                "total_handlers": len(self._handlers[event_type_name]),
            },
        )

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe a handler from a specific event type.

        Handlers are compared by identity (using 'is'), not equality.
        This means you must pass the exact same handler object that
        was used in subscribe().

        Args:
            event_type: The event class to unsubscribe from.
            handler: The handler to remove.

        Returns:
            True if the handler was found and removed, False otherwise.

        Example:
            >>> handler = lambda e: print(e)
            >>> bus.subscribe(OrderCreated, handler)
            >>> bus.unsubscribe(OrderCreated, handler)  # Returns True
            >>> bus.unsubscribe(OrderCreated, handler)  # Returns False
        """
        event_type_name = event_type.__name__
        target_adapter = HandlerAdapter(handler)

        if event_type_name not in self._handlers:
            logger.debug(
                "No handlers registered for event type",
                extra={"event_type": event_type_name},
            )
            return False

        # Find and remove the handler by identity (via HandlerAdapter equality)
        adapters = self._handlers[event_type_name]
        for i, adapter in enumerate(adapters):
            if adapter == target_adapter:
                adapters.pop(i)
                logger.debug(
                    "Handler unsubscribed",
                    extra={
                        "event_type": event_type_name,
                        "handler": adapter.name,
                        "remaining_handlers": len(adapters),
                    },
                )
                return True

        logger.debug(
            "Handler not found for event type",
            extra={
                "event_type": event_type_name,
                "handler": target_adapter.name,
            },
        )
        return False

    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
        """Subscribe an EventSubscriber to all its declared event types.

        Calls subscriber.subscribed_to() to get the list of event types,
        then registers subscriber.handle() for each type. This is a
        convenience method for subscribers that handle multiple event types.

        Args:
            subscriber: An EventSubscriber instance with subscribed_to()
                and handle() methods.

        Example:
            >>> class OrderProjection:
            ...     def subscribed_to(self) -> list[type[DomainEvent]]:
            ...         return [OrderCreated, OrderShipped]
            ...     async def handle(self, event: DomainEvent) -> None:
            ...         await update_projection(event)
            >>> bus.subscribe_all(OrderProjection())
        """
        event_types = subscriber.subscribed_to()

        for event_type in event_types:
            self.subscribe(event_type, subscriber)

        adapter = HandlerAdapter(subscriber)
        logger.info(
            "Subscriber registered for all event types",
            extra={
                "subscriber": adapter.name,
                "event_types": [et.__name__ for et in event_types],
                "event_count": len(event_types),
            },
        )

    def subscribe_to_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        """Subscribe a handler to all event types (wildcard subscription).

        The handler will receive every event regardless of type. This is
        useful for cross-cutting concerns such as:
        - Audit logging
        - Metrics collection
        - Debugging and tracing
        - Event replay recording

        Wildcard handlers are invoked after type-specific handlers.

        Args:
            handler: The handler to invoke for all events.

        Example:
            >>> async def audit_log(event: DomainEvent) -> None:
            ...     await log_event(event)
            >>> bus.subscribe_to_all_events(audit_log)
        """
        adapter = HandlerAdapter(handler)
        self._wildcard_handlers.append(adapter)

        logger.debug(
            "Wildcard handler subscribed",
            extra={
                "handler": adapter.name,
                "total_wildcard_handlers": len(self._wildcard_handlers),
            },
        )

    def unsubscribe_from_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        """Unsubscribe a handler from the wildcard subscription.

        Handlers are compared by identity (using 'is'), not equality.

        Args:
            handler: The handler to remove.

        Returns:
            True if the handler was found and removed, False otherwise.

        Example:
            >>> bus.subscribe_to_all_events(audit_handler)
            >>> bus.unsubscribe_from_all_events(audit_handler)  # Returns True
        """
        target_adapter = HandlerAdapter(handler)

        for i, adapter in enumerate(self._wildcard_handlers):
            if adapter == target_adapter:
                self._wildcard_handlers.pop(i)
                logger.debug(
                    "Wildcard handler unsubscribed",
                    extra={
                        "handler": adapter.name,
                        "remaining_wildcard_handlers": len(self._wildcard_handlers),
                    },
                )
                return True

        logger.debug(
            "Wildcard handler not found",
            extra={"handler": target_adapter.name},
        )
        return False

    # =========================================================================
    # Handler Management Helpers
    # =========================================================================

    def clear_subscribers(self) -> None:
        """Clear all registered handlers.

        Removes all type-specific and wildcard handlers. This is useful
        for testing or reconfiguration scenarios.

        Example:
            >>> bus.subscribe(OrderCreated, handler1)
            >>> bus.subscribe_to_all_events(handler2)
            >>> bus.clear_subscribers()
            >>> assert bus.get_subscriber_count() == 0
        """
        self._handlers.clear()
        self._wildcard_handlers.clear()
        logger.debug("All subscribers cleared")

    def get_subscriber_count(self, event_type: type[DomainEvent] | None = None) -> int:
        """Get the number of type-specific handlers.

        Args:
            event_type: If provided, count handlers for this specific event
                type only. If None, count all type-specific handlers.

        Returns:
            Total count of handlers. Does not include wildcard handlers.

        Example:
            >>> bus.subscribe(OrderCreated, handler1)
            >>> bus.subscribe(OrderCreated, handler2)
            >>> bus.subscribe(OrderShipped, handler3)
            >>> bus.get_subscriber_count()  # Returns 3
            >>> bus.get_subscriber_count(OrderCreated)  # Returns 2
        """
        if event_type is not None:
            event_type_name = event_type.__name__
            return len(self._handlers.get(event_type_name, []))
        return sum(len(handlers) for handlers in self._handlers.values())

    def get_wildcard_subscriber_count(self) -> int:
        """Get the number of wildcard handlers.

        Returns:
            Number of handlers subscribed to all events.

        Example:
            >>> bus.subscribe_to_all_events(handler1)
            >>> bus.subscribe_to_all_events(handler2)
            >>> bus.get_wildcard_subscriber_count()  # Returns 2
        """
        return len(self._wildcard_handlers)

    def get_handlers_for_event(self, event_type_name: str) -> list[HandlerAdapter]:
        """Get all handlers that should process an event type.

        Includes both type-specific handlers and wildcard handlers.
        This is used internally during event dispatch.

        Args:
            event_type_name: The event type name (e.g., "OrderCreated").

        Returns:
            List of HandlerAdapter instances to invoke. Type-specific handlers
            come first, followed by wildcard handlers.

        Example:
            >>> handlers = bus.get_handlers_for_event("OrderCreated")
            >>> for adapter in handlers:
            ...     await adapter.handle(event)
        """
        adapters = list(self._handlers.get(event_type_name, []))
        adapters.extend(self._wildcard_handlers)
        return adapters

    # =========================================================================
    # Consumer Methods
    # =========================================================================

    async def start_consuming(self, auto_reconnect: bool = True) -> None:
        """Start consuming events from Kafka.

        This method blocks and continuously polls for messages, dispatching
        them to registered handlers. Use stop_consuming() from another
        coroutine to stop.

        Events are processed sequentially within each partition. Offsets are
        committed after successful handler execution (at-least-once delivery).

        Auto-Reconnection:
            When auto_reconnect=True (default), the consumer will automatically
            attempt to reconnect on connection errors using exponential backoff.
            This prevents the consumer from dying on transient network issues.

        Args:
            auto_reconnect: If True, automatically reconnect on errors.

        Raises:
            RuntimeError: If not connected or already consuming.
        """
        if not self._connected or not self._consumer:
            raise RuntimeError("Not connected to Kafka. Call connect() first.")

        if self._consuming:
            logger.warning("Already consuming events")
            return

        self._consuming = True
        self._shutdown_event.clear()

        # Register consumer lag gauge (only once)
        self._register_consumer_lag_gauge()

        logger.info(
            "Starting Kafka consumer",
            extra={
                "topic": self._config.topic_name,
                "consumer_group": self._config.consumer_group,
                "auto_reconnect": auto_reconnect,
            },
        )

        reconnect_delay = 1.0  # Start with 1 second delay
        max_reconnect_delay = 60.0  # Cap at 60 seconds

        while self._consuming and not self._shutdown_event.is_set():
            try:
                async for message in self._consumer:
                    if self._shutdown_event.is_set() or not self._consuming:
                        break

                    await self._process_message(message)

                    # Reset reconnect delay on successful message processing
                    reconnect_delay = 1.0

                # async for loop completed - only exit if we should stop
                if not self._consuming or self._shutdown_event.is_set():
                    break
                # Otherwise continue the while loop to keep consuming

            except asyncio.CancelledError:
                logger.info("Consumer cancelled")
                raise
            except Exception as e:
                # Increment connection_errors counter for consumer errors
                if self._metrics:
                    self._metrics.connection_errors.add(
                        1,
                        attributes={
                            "error.type": type(e).__name__,
                        },
                    )

                logger.error(
                    "Consumer error",
                    extra={"error": str(e), "auto_reconnect": auto_reconnect},
                    exc_info=True,
                )

                if not auto_reconnect or not self._consuming:
                    raise

                # Attempt reconnection with exponential backoff
                logger.info(
                    "Attempting to reconnect consumer",
                    extra={"delay_seconds": reconnect_delay},
                )

                await asyncio.sleep(reconnect_delay)

                # Exponential backoff with cap
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)

                # Record reconnection attempt
                self.record_reconnection()

                # Try to restart the consumer
                try:
                    await self._reconnect_consumer()
                except Exception as reconnect_error:
                    logger.error(
                        "Failed to reconnect consumer",
                        extra={"error": str(reconnect_error)},
                        exc_info=True,
                    )
                    # Continue the loop to retry with backoff

        self._consuming = False
        logger.info("Consumer stopped")

    async def _reconnect_consumer(self) -> None:
        """Attempt to reconnect the consumer after an error.

        Stops the current consumer and creates a new one with the same
        configuration and topic subscriptions.
        """
        logger.debug("Reconnecting consumer")

        # Stop the current consumer if it exists
        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer during reconnect: {e}")

        # Create and start new consumer with topic
        self._consumer = AIOKafkaConsumer(
            self._config.topic_name,
            **self._config.get_consumer_config(),
        )
        await self._consumer.start()

        logger.info("Consumer reconnected successfully")

    def start_consuming_in_background(self) -> asyncio.Task[None]:
        """Start consuming in a background task.

        Returns:
            The background task running the consumer.

        Raises:
            RuntimeError: If consumer is already running in background.

        Usage:
            task = bus.start_consuming_in_background()
            # ... do other work ...
            await bus.stop_consuming()
            await task
        """
        if self._consume_task is not None and not self._consume_task.done():
            raise RuntimeError("Consumer already running in background")

        self._consume_task = asyncio.create_task(
            self.start_consuming(),
            name=f"kafka-consumer-{self._config.consumer_name}",
        )
        return self._consume_task

    async def _process_message(self, message: Any) -> None:
        """Process a single Kafka message with optional tracing.

        Deserializes the event, dispatches to handlers, and commits offset
        on success. Implements retry logic on failure.

        When tracing is enabled, creates OpenTelemetry spans for the consume
        operation and extracts trace context from Kafka message headers for
        distributed tracing correlation.

        Args:
            message: The Kafka ConsumerRecord to process.
        """
        self._stats.events_consumed += 1
        self._stats.last_consume_at = datetime.now(UTC)

        # Extract event type from headers for routing
        event_type_name = self._get_header_value(message.headers, "event_type")

        # Increment messages_consumed counter
        if self._metrics and event_type_name:
            self._metrics.messages_consumed.add(
                1,
                attributes={
                    "messaging.system": "kafka",
                    "messaging.destination": message.topic,
                    "messaging.kafka.partition": message.partition,
                    "event.type": event_type_name,
                },
            )

        if not event_type_name:
            logger.error(
                "Message missing event_type header",
                extra={
                    "topic": message.topic,
                    "partition": message.partition,
                    "offset": message.offset,
                },
            )
            # Commit to avoid reprocessing malformed message
            if self._consumer:
                await self._consumer.commit()
            return

        logger.debug(
            "Processing message",
            extra={
                "event_type": event_type_name,
                "partition": message.partition,
                "offset": message.offset,
            },
        )

        # Use composition-based tracer for tracing
        if self._enable_tracing and PROPAGATION_AVAILABLE and extract is not None:
            # Extract trace context from headers for distributed tracing
            carrier = self._extract_trace_context(message.headers)
            context = extract(carrier)

            # Create span for consume
            with self._tracer.span_with_kind(
                name=f"eventsource.event_bus.consume {event_type_name}",
                kind=SpanKindEnum.CONSUMER,
                attributes={
                    ATTR_MESSAGING_SYSTEM: "kafka",
                    "messaging.source": message.topic,
                    "messaging.source_kind": "topic",
                    ATTR_MESSAGING_OPERATION: "receive",
                    "messaging.kafka.partition": message.partition,
                    "messaging.kafka.offset": message.offset,
                    "messaging.kafka.consumer_group": self._config.consumer_group,
                    ATTR_EVENT_TYPE: event_type_name,
                },
                context=context,
            ) as span:
                await self._process_message_with_span(message, event_type_name, span)
        else:
            await self._process_message_with_span(message, event_type_name, None)

    def _extract_trace_context(
        self,
        headers: list[tuple[str, bytes]] | None,
    ) -> dict[str, str]:
        """Extract OpenTelemetry trace context from message headers.

        Extracts W3C Trace Context headers (traceparent, tracestate, baggage)
        from Kafka message headers for distributed tracing correlation.

        Args:
            headers: Kafka message headers.

        Returns:
            Dictionary of trace context headers suitable for OpenTelemetry
            propagator extraction.
        """
        carrier: dict[str, str] = {}
        if headers:
            for key, value in headers:
                # OpenTelemetry headers typically start with 'traceparent' or 'tracestate'
                if key in ("traceparent", "tracestate", "baggage"):
                    carrier[key] = value.decode("utf-8")
        return carrier

    async def _process_message_with_span(
        self,
        message: Any,
        event_type_name: str,
        span: Any,
    ) -> None:
        """Process message with optional span updates.

        This method contains the core message processing logic, optionally
        updating the provided tracing span with event metadata and status.

        Args:
            message: The Kafka message.
            event_type_name: The event type name.
            span: The current tracing span, or None.
        """
        # Start timing for consume duration histogram
        start_time = time.perf_counter()

        # Get retry count from headers (for retried messages)
        retry_count = self._get_retry_count(message.headers)

        try:
            # Deserialize event - catch DeserializationError separately
            # as these will never succeed on retry
            try:
                event = self._deserialize_message(message)
            except DeserializationError as e:
                # Deserialization errors are unrecoverable - send directly to DLQ
                logger.error(
                    "Deserialization error, sending directly to DLQ",
                    extra={
                        "event_type": event_type_name,
                        "error": str(e),
                    },
                )
                await self._send_to_dlq(message, e, retry_count, reason="deserialization_error")
                if self._consumer:
                    await self._consumer.commit()

                if span:
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.record_exception(e)

                # Record consume duration
                if self._metrics:
                    duration_ms = (time.perf_counter() - start_time) * 1000
                    self._metrics.consume_duration.record(
                        duration_ms,
                        attributes={"messaging.destination": message.topic},
                    )
                return

            if span:
                span.set_attribute(ATTR_EVENT_ID, str(event.event_id))
                span.set_attribute(ATTR_AGGREGATE_ID, str(event.aggregate_id))
                span.set_attribute(ATTR_AGGREGATE_TYPE, event.aggregate_type)

            # Get handlers for this event type
            handlers = self.get_handlers_for_event(event_type_name)

            if not handlers:
                logger.debug(
                    "No handlers for event type",
                    extra={"event_type": event_type_name},
                )
            else:
                # Dispatch to all handlers
                await self._dispatch_to_handlers(event, handlers)

            # Commit offset on success
            if self._consumer:
                await self._consumer.commit()
            self._stats.events_processed_success += 1

            if span:
                span.set_status(Status(StatusCode.OK))

            # Record consume duration histogram - success path
            if self._metrics:
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.consume_duration.record(
                    duration_ms,
                    attributes={
                        "messaging.destination": message.topic,
                    },
                )

        except Exception as e:
            # Record consume duration histogram - error path
            if self._metrics:
                duration_ms = (time.perf_counter() - start_time) * 1000
                self._metrics.consume_duration.record(
                    duration_ms,
                    attributes={
                        "messaging.destination": message.topic,
                    },
                )

            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
            await self._handle_processing_error(message, e, retry_count)

    def _deserialize_message(self, message: Any) -> DomainEvent:
        """Deserialize a Kafka message to a DomainEvent using the configured serializer.

        Args:
            message: The Kafka ConsumerRecord to deserialize.

        Returns:
            The deserialized DomainEvent.

        Raises:
            DeserializationError: If event type is unknown or deserialization fails.
        """
        event_type_name = self._get_header_value(message.headers, "event_type")

        if not event_type_name:
            raise DeserializationError("Message missing event_type header")

        # Get event class from registry
        event_class = self._get_event_class(event_type_name)

        if not event_class:
            raise DeserializationError(f"Unknown event type: {event_type_name}")

        # Deserialize using the configured serializer
        return self._serializer.deserialize(message.value, event_type_name, event_class)

    def _get_event_class(self, event_type_name: str) -> type[DomainEvent] | None:
        """Get event class by name from registry.

        Args:
            event_type_name: Name of the event class to look up.

        Returns:
            The event class if found, None otherwise.
        """
        if self._event_registry:
            return self._event_registry.get_or_none(event_type_name)

        # Fallback to default registry
        from eventsource.events.registry import default_registry

        return default_registry.get_or_none(event_type_name)

    def _get_header_value(
        self,
        headers: list[tuple[str, bytes]] | None,
        key: str,
    ) -> str | None:
        """Get a header value by key.

        Args:
            headers: List of header tuples.
            key: The header key to find.

        Returns:
            The decoded header value, or None if not found.
        """
        if not headers:
            return None

        for header_key, header_value in headers:
            if header_key == key:
                return header_value.decode("utf-8")

        return None

    def _get_retry_count(self, headers: list[tuple[str, bytes]] | None) -> int:
        """Get the retry count from message headers.

        Args:
            headers: Message headers.

        Returns:
            The retry count, or 0 if not present.
        """
        value = self._get_header_value(headers, "retry_count")
        if value:
            try:
                return int(value)
            except ValueError:
                return 0
        return 0

    async def _dispatch_to_handlers(
        self,
        event: DomainEvent,
        handlers: list[HandlerAdapter],
    ) -> None:
        """Dispatch an event to all registered handlers with optional tracing.

        When tracing is enabled, creates child spans for each handler
        invocation to provide detailed visibility into handler execution.

        Args:
            event: The event to dispatch.
            handlers: List of HandlerAdapter instances to invoke.

        Raises:
            Exception: Re-raises any handler exception.
        """
        for adapter in handlers:
            handler_name = adapter.name

            # Start timing for handler duration histogram
            handler_start_time = time.perf_counter()

            # Use composition-based tracer for tracing
            if self._enable_tracing:
                with self._tracer.span_with_kind(
                    name=f"eventsource.event_bus.dispatch {handler_name}",
                    kind=SpanKindEnum.INTERNAL,
                    attributes={
                        ATTR_HANDLER_NAME: handler_name,
                        ATTR_EVENT_TYPE: event.event_type,
                        ATTR_EVENT_ID: str(event.event_id),
                    },
                ) as span:
                    try:
                        logger.debug(
                            "Dispatching to handler",
                            extra={
                                "event_type": event.event_type,
                                "event_id": str(event.event_id),
                                "handler": handler_name,
                            },
                        )

                        await adapter.handle(event)

                        # Record handler duration and increment counter on success
                        if self._metrics:
                            handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                            self._metrics.handler_duration.record(
                                handler_duration_ms,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )
                            self._metrics.handler_invocations.add(
                                1,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )

                        logger.debug(
                            "Handler completed",
                            extra={
                                "event_type": event.event_type,
                                "handler": handler_name,
                            },
                        )

                    except Exception as e:
                        # Record handler duration on error path
                        if self._metrics:
                            handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                            self._metrics.handler_duration.record(
                                handler_duration_ms,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                },
                            )
                            self._metrics.handler_errors.add(
                                1,
                                attributes={
                                    "handler.name": handler_name,
                                    "event.type": event.event_type,
                                    "error.type": type(e).__name__,
                                },
                            )

                        if span is not None:
                            span.set_status(Status(StatusCode.ERROR, str(e)))
                            span.record_exception(e)
                        self._stats.handler_errors += 1

                        logger.error(
                            "Handler error",
                            extra={
                                "event_type": event.event_type,
                                "event_id": str(event.event_id),
                                "handler": handler_name,
                                "error": str(e),
                            },
                            exc_info=True,
                        )
                        raise
            else:
                try:
                    logger.debug(
                        "Dispatching to handler",
                        extra={
                            "event_type": event.event_type,
                            "event_id": str(event.event_id),
                            "handler": handler_name,
                        },
                    )

                    await adapter.handle(event)

                    # Record handler duration and increment counter on success
                    if self._metrics:
                        handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                        self._metrics.handler_duration.record(
                            handler_duration_ms,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )
                        self._metrics.handler_invocations.add(
                            1,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )

                    logger.debug(
                        "Handler completed",
                        extra={
                            "event_type": event.event_type,
                            "handler": handler_name,
                        },
                    )

                except Exception as e:
                    # Record handler duration on error path
                    if self._metrics:
                        handler_duration_ms = (time.perf_counter() - handler_start_time) * 1000
                        self._metrics.handler_duration.record(
                            handler_duration_ms,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                            },
                        )
                        self._metrics.handler_errors.add(
                            1,
                            attributes={
                                "handler.name": handler_name,
                                "event.type": event.event_type,
                                "error.type": type(e).__name__,
                            },
                        )

                    self._stats.handler_errors += 1

                    logger.error(
                        "Handler error",
                        extra={
                            "event_type": event.event_type,
                            "event_id": str(event.event_id),
                            "handler": handler_name,
                            "error": str(e),
                        },
                        exc_info=True,
                    )
                    raise

    async def _handle_processing_error(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
    ) -> None:
        """Handle a message processing error.

        Implements non-blocking retry by republishing the message with an
        incremented retry count. After max_retries, the message is sent to DLQ.

        This approach does not block the consumer loop, allowing other messages
        to be processed while failed messages are retried. The retry delay is
        encoded in the message headers and a scheduled retry topic could be
        used for delayed processing (future enhancement).

        Args:
            message: The failed message.
            error: The exception that occurred.
            retry_count: Current retry attempt number.
        """
        self._stats.events_processed_failed += 1
        self._stats.last_error_at = datetime.now(UTC)

        event_type = self._get_header_value(message.headers, "event_type")
        event_id = self._get_header_value(message.headers, "event_id")

        if retry_count >= self._config.max_retries:
            logger.error(
                "Max retries exceeded, message will be sent to DLQ",
                extra={
                    "event_type": event_type,
                    "event_id": event_id,
                    "retry_count": retry_count,
                    "max_retries": self._config.max_retries,
                    "error": str(error),
                },
            )
            # Send to DLQ
            await self._send_to_dlq(message, error, retry_count)
            # Commit to avoid infinite loop
            if self._consumer:
                await self._consumer.commit()
            return

        # Calculate retry delay for logging (actual delay happens on next consumption)
        delay = self._calculate_retry_delay(retry_count)

        logger.warning(
            "Message processing failed, republishing for retry",
            extra={
                "event_type": event_type,
                "event_id": event_id,
                "retry_count": retry_count + 1,
                "max_retries": self._config.max_retries,
                "retry_delay": delay,
                "error": str(error),
            },
        )

        # Republish the message with incremented retry count (non-blocking retry)
        await self._republish_for_retry(message, retry_count + 1, delay)

        # Commit the original message to avoid reprocessing
        if self._consumer:
            await self._consumer.commit()

    async def _republish_for_retry(
        self,
        message: Any,
        new_retry_count: int,
        delay: float,
    ) -> None:
        """Republish a failed message for retry.

        Copies the original message with an updated retry count header
        and a scheduled retry timestamp. This enables non-blocking retries
        that don't hold up the consumer.

        Args:
            message: The original failed message.
            new_retry_count: The incremented retry count.
            delay: Suggested delay before processing (for logging/future use).
        """
        if not self._producer:
            logger.error("Cannot republish for retry: producer not connected")
            return

        # Build new headers with updated retry count and retry timestamp
        new_headers: list[tuple[str, bytes]] = []
        if message.headers:
            for key, value in message.headers:
                if key != "retry_count" and key != "retry_after":
                    new_headers.append((key, value))

        # Add retry metadata
        new_headers.append(("retry_count", str(new_retry_count).encode("utf-8")))
        retry_after = datetime.now(UTC).timestamp() + delay
        new_headers.append(("retry_after", str(retry_after).encode("utf-8")))

        try:
            await self._producer.send(
                topic=self._config.topic_name,
                key=message.key,
                value=message.value,
                headers=new_headers,
            )

            logger.debug(
                "Message republished for retry",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "retry_count": new_retry_count,
                    "retry_after": retry_after,
                },
            )
        except Exception as e:
            logger.error(
                "Failed to republish message for retry, sending to DLQ",
                extra={"error": str(e)},
                exc_info=True,
            )
            # If we can't republish, send to DLQ to avoid message loss
            await self._send_to_dlq(message, e, new_retry_count - 1, reason="republish_failed")

    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate delay for retry with exponential backoff and jitter.

        Args:
            retry_count: The current retry attempt (0-based).

        Returns:
            Delay in seconds before next retry.
        """
        # Exponential backoff: base_delay * 2^retry_count
        delay: float = self._config.retry_base_delay * (2**retry_count)

        # Cap at max delay
        delay = min(delay, self._config.retry_max_delay)

        # Add jitter (using random for non-cryptographic retry timing)
        jitter: float = delay * self._config.retry_jitter * random.random()  # nosec B311
        delay += jitter

        return delay

    async def _send_to_dlq(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str = "max_retries_exceeded",
    ) -> None:
        """Send a failed message to the dead letter queue.

        Preserves the original message and adds DLQ-specific metadata
        in headers for debugging and replay.

        Args:
            message: The failed Kafka message.
            error: The exception that caused the failure.
            retry_count: Number of retry attempts made.
            reason: Reason for DLQ routing. Options:
                - "max_retries_exceeded": Handler failed after max retries
                - "deserialization_error": Message could not be deserialized
                - "handler_error": Handler raised an unrecoverable error
        """
        if not self._config.enable_dlq:
            logger.warning(
                "DLQ disabled, dropping failed message",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "error": str(error),
                },
            )
            return

        if not self._producer:
            logger.error(
                "Cannot send to DLQ: producer not connected",
                extra={
                    "event_type": self._get_header_value(message.headers, "event_type"),
                },
            )
            return

        # Create DLQ headers with failure metadata
        dlq_headers = self._create_dlq_headers(message, error, retry_count, reason)

        # Combine original headers with DLQ headers
        original_headers = list(message.headers) if message.headers else []
        all_headers = original_headers + dlq_headers

        try:
            # Send to DLQ topic
            await self._producer.send(
                topic=self._config.dlq_topic_name,
                key=message.key,
                value=message.value,
                headers=all_headers,
            )

            self._stats.messages_sent_to_dlq += 1

            # Increment dlq_messages counter
            if self._metrics:
                self._metrics.dlq_messages.add(
                    1,
                    attributes={
                        "dlq.reason": reason,
                        "error.type": type(error).__name__,
                    },
                )

            logger.info(
                "Message sent to DLQ",
                extra={
                    "dlq_topic": self._config.dlq_topic_name,
                    "event_type": self._get_header_value(message.headers, "event_type"),
                    "event_id": self._get_header_value(message.headers, "event_id"),
                    "reason": reason,
                    "error": str(error)[:200],
                },
            )

        except Exception as e:
            logger.error(
                "Failed to send message to DLQ",
                extra={
                    "error": str(e),
                    "original_error": str(error),
                },
                exc_info=True,
            )
            raise

    def _create_dlq_headers(
        self,
        message: Any,
        error: Exception,
        retry_count: int,
        reason: str,
    ) -> list[tuple[str, bytes]]:
        """Create DLQ-specific headers.

        Args:
            message: The failed message.
            error: The exception that caused the failure.
            retry_count: Number of retry attempts.
            reason: Reason for DLQ routing.

        Returns:
            List of DLQ header tuples.
        """
        error_message = str(error)[:1000]  # Truncate to avoid huge headers

        return [
            ("dlq_reason", reason.encode("utf-8")),
            ("dlq_error_type", type(error).__name__.encode("utf-8")),
            ("dlq_error_message", error_message.encode("utf-8")),
            ("dlq_retry_count", str(retry_count).encode("utf-8")),
            ("dlq_timestamp", datetime.now(UTC).isoformat().encode("utf-8")),
            ("dlq_original_topic", message.topic.encode("utf-8")),
            ("dlq_original_partition", str(message.partition).encode("utf-8")),
            ("dlq_original_offset", str(message.offset).encode("utf-8")),
            ("dlq_consumer_group", self._config.consumer_group.encode("utf-8")),
        ]

    def _get_security_config(self) -> dict[str, Any]:
        """Get security configuration for additional consumers.

        Creates a dictionary of security settings suitable for creating
        additional Kafka consumers (e.g., for DLQ inspection).

        Returns:
            Dictionary of security settings.
        """
        config: dict[str, Any] = {
            "security_protocol": self._config.security_protocol,
        }

        if self._config.sasl_mechanism:
            config["sasl_mechanism"] = self._config.sasl_mechanism
        if self._config.sasl_username:
            config["sasl_plain_username"] = self._config.sasl_username
        if self._config.sasl_password:
            config["sasl_plain_password"] = self._config.sasl_password
        if self._config.ssl_cafile:
            config["ssl_cafile"] = self._config.ssl_cafile
        if self._config.ssl_certfile:
            config["ssl_certfile"] = self._config.ssl_certfile
        if self._config.ssl_keyfile:
            config["ssl_keyfile"] = self._config.ssl_keyfile

        return config

    # =========================================================================
    # DLQ Inspection and Replay Methods
    # =========================================================================

    async def get_dlq_messages(
        self,
        limit: int = 100,
        timeout_ms: int = 5000,
        use_consumer_group: bool = False,
    ) -> list[dict[str, Any]]:
        """Retrieve messages from the dead letter queue.

        Creates a consumer to read DLQ messages. By default, reads without
        committing offsets (inspection mode). When use_consumer_group=True,
        uses the configured DLQ consumer group for coordinated processing.

        Args:
            limit: Maximum number of messages to retrieve.
            timeout_ms: Timeout for polling in milliseconds.
            use_consumer_group: If True, use dlq_consumer_group for coordinated
                DLQ processing. Messages will be committed after retrieval.

        Returns:
            List of DLQ message dictionaries with headers and payload.
            Each message contains:
            - topic: DLQ topic name
            - partition: Partition number
            - offset: Message offset
            - key: Message key (decoded)
            - timestamp: Message timestamp
            - headers: All headers as string dict
            - payload: Deserialized JSON payload or hex-encoded bytes
            - replay_count: Number of times this message has been replayed

        Raises:
            RuntimeError: If not connected to Kafka.
            ValueError: If use_consumer_group=True but dlq_consumer_group not set.
        """
        if not self._connected:
            raise RuntimeError("Not connected to Kafka")

        group_id = None
        if use_consumer_group:
            if not self._config.dlq_consumer_group:
                raise ValueError(
                    "dlq_consumer_group must be set in config to use consumer group mode"
                )
            group_id = self._config.dlq_consumer_group

        # Create a consumer for DLQ inspection/processing
        dlq_consumer = AIOKafkaConsumer(
            self._config.dlq_topic_name,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,
            consumer_timeout_ms=timeout_ms,
            **self._get_security_config(),
        )

        messages: list[dict[str, Any]] = []

        try:
            await dlq_consumer.start()

            count = 0
            async for message in dlq_consumer:
                if count >= limit:
                    break

                # Parse headers into dict
                headers: dict[str, str] = {}
                if message.headers:
                    for key, value in message.headers:
                        headers[key] = value.decode("utf-8")

                # Get replay count from headers
                replay_count = int(headers.get("dlq_replay_count", "0"))

                # Try to decode value as JSON
                try:
                    import json

                    payload = json.loads(message.value.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    payload = message.value.hex() if message.value else None

                messages.append(
                    {
                        "topic": message.topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "key": message.key.decode("utf-8") if message.key else None,
                        "timestamp": message.timestamp,
                        "headers": headers,
                        "payload": payload,
                        "replay_count": replay_count,
                    }
                )

                count += 1

            # Commit offsets if using consumer group
            if use_consumer_group and messages:
                await dlq_consumer.commit()

        except TimeoutError:
            # Consumer timed out - this is expected when no more messages
            pass
        finally:
            await dlq_consumer.stop()

        logger.debug(
            "Retrieved DLQ messages",
            extra={"count": len(messages), "limit": limit, "consumer_group": group_id},
        )

        return messages

    async def replay_dlq_message(
        self,
        partition: int,
        offset: int,
        force: bool = False,
    ) -> bool:
        """Replay a specific message from the dead letter queue.

        Reads the message from DLQ and republishes it to the main topic
        for reprocessing. The DLQ message is not deleted (Kafka limitation).

        Replay Loop Protection:
            Each replay increments a dlq_replay_count header. If the count
            exceeds dlq_max_replay_attempts (default 3), the replay is rejected
            to prevent infinite replay loops. Use force=True to override.

        The replayed message:
        - Has all DLQ-specific headers removed except dlq_replay_count
        - Has retry_count reset to 0
        - Has dlq_replay_count incremented
        - Maintains original event headers

        Args:
            partition: The DLQ partition containing the message.
            offset: The offset of the message to replay.
            force: If True, replay even if max replay attempts exceeded.

        Returns:
            True if message was successfully republished.

        Raises:
            RuntimeError: If not connected to Kafka.
            ValueError: If message not found at specified location or max replays exceeded.
        """
        if not self._connected or not self._producer:
            raise RuntimeError("Not connected to Kafka")

        dlq_consumer = AIOKafkaConsumer(
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=None,
            enable_auto_commit=False,
            **self._get_security_config(),
        )

        try:
            await dlq_consumer.start()

            # Assign to specific partition
            tp = TopicPartition(self._config.dlq_topic_name, partition)
            dlq_consumer.assign([tp])

            # Seek to specific offset
            dlq_consumer.seek(tp, offset)

            # Read the message
            message = await asyncio.wait_for(
                dlq_consumer.getone(),
                timeout=5.0,
            )

            if message.offset != offset:
                raise ValueError(f"Message not found at offset {offset}")

            # Check replay count for loop protection
            current_replay_count = 0
            if message.headers:
                for key, value in message.headers:
                    if key == "dlq_replay_count":
                        with contextlib.suppress(ValueError, UnicodeDecodeError):
                            current_replay_count = int(value.decode("utf-8"))
                        break

            # Enforce replay limit unless forced
            if not force and current_replay_count >= self._config.dlq_max_replay_attempts:
                event_type = self._get_header_value(message.headers, "event_type")
                logger.warning(
                    "Replay rejected: max replay attempts exceeded",
                    extra={
                        "dlq_partition": partition,
                        "dlq_offset": offset,
                        "replay_count": current_replay_count,
                        "max_replay_attempts": self._config.dlq_max_replay_attempts,
                        "event_type": event_type,
                    },
                )
                raise ValueError(
                    f"Message at partition {partition}, offset {offset} has been replayed "
                    f"{current_replay_count} times, exceeding max of "
                    f"{self._config.dlq_max_replay_attempts}. Use force=True to override."
                )

            # Build headers for republish
            original_headers: list[tuple[str, bytes]] = []
            if message.headers:
                for key, value in message.headers:
                    # Remove DLQ headers except replay count (we'll update it)
                    if not key.startswith("dlq_") and key != "retry_count":
                        original_headers.append((key, value))

            # Add retry_count header (reset to 0 for fresh attempt)
            original_headers.append(("retry_count", b"0"))

            # Increment and add replay count for loop protection
            new_replay_count = current_replay_count + 1
            original_headers.append(("dlq_replay_count", str(new_replay_count).encode("utf-8")))

            # Republish to main topic
            await self._producer.send(
                topic=self._config.topic_name,
                key=message.key,
                value=message.value,
                headers=original_headers,
            )

            logger.info(
                "DLQ message replayed",
                extra={
                    "dlq_partition": partition,
                    "dlq_offset": offset,
                    "target_topic": self._config.topic_name,
                    "event_type": self._get_header_value(original_headers, "event_type"),
                    "replay_count": new_replay_count,
                },
            )

            return True

        except TimeoutError as err:
            raise ValueError(
                f"Timeout reading message at partition {partition}, offset {offset}"
            ) from err
        finally:
            await dlq_consumer.stop()

    async def get_dlq_message_count(self) -> int:
        """Get the approximate number of messages in the DLQ.

        Uses consumer lag calculation to estimate DLQ size by comparing
        beginning and end offsets for each partition.

        Returns:
            Approximate count of DLQ messages across all partitions.

        Raises:
            RuntimeError: If not connected to Kafka.
        """
        if not self._connected:
            raise RuntimeError("Not connected to Kafka")

        dlq_consumer = AIOKafkaConsumer(
            self._config.dlq_topic_name,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=None,
            **self._get_security_config(),
        )

        total_count = 0

        try:
            await dlq_consumer.start()

            partitions = dlq_consumer.partitions_for_topic(self._config.dlq_topic_name)
            if not partitions:
                return 0

            for partition_id in partitions:
                tp = TopicPartition(self._config.dlq_topic_name, partition_id)
                dlq_consumer.assign([tp])

                # Get beginning and end offsets
                beginning = await dlq_consumer.beginning_offsets([tp])
                end = await dlq_consumer.end_offsets([tp])

                start_offset = beginning.get(tp, 0)
                end_offset = end.get(tp, 0)
                total_count += max(0, end_offset - start_offset)

        finally:
            await dlq_consumer.stop()

        return total_count


__all__ = [
    "DeserializationError",
    "EventSerializer",
    "KAFKA_AVAILABLE",
    "KafkaEventBus",
    "KafkaEventBusConfig",
    "KafkaEventBusMetrics",
    "KafkaEventBusStats",
    "KafkaNotAvailableError",
    "KafkaRebalanceListener",
]
