"""Configuration for the Kafka event bus."""

from __future__ import annotations

import logging
import socket
import ssl
import uuid
from dataclasses import dataclass
from typing import Any

# Pinned explicitly so log records from this module carry the same public
# logger name as the rest of the Kafka backend.
logger = logging.getLogger("eventsource.bus.kafka")


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
        fetch_max_bytes: Maximum bytes the broker returns per fetch across
            all partitions, bounding in-process buffering. Forwarded as-is
            to ``AIOKafkaConsumer(fetch_max_bytes=...)``.
        max_partition_fetch_bytes: Maximum bytes the broker returns per
            partition in a single fetch. Forwarded as-is to
            ``AIOKafkaConsumer(max_partition_fetch_bytes=...)``.
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
    fetch_max_bytes: int = 52428800  # aiokafka default (50 MiB)
    max_partition_fetch_bytes: int = 1048576  # aiokafka default (1 MiB)

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

    def get_consumer_config(self, **overrides: Any) -> dict[str, Any]:
        """Get aiokafka consumer configuration dict.

        This is the single construction path for every ``AIOKafkaConsumer``
        built from this config -- the long-lived main consumer (via
        :class:`~eventsource.adapters.kafka.connection.KafkaConnectionManager`)
        and every short-lived DLQ consumer (via
        :class:`~eventsource.adapters.kafka.dlq.KafkaDLQAdmin`) both call this
        method so a field added here reaches every consumer automatically.
        Callers that need different values for a specific client (e.g. a DLQ
        consumer's ``group_id`` or a one-shot ``consumer_timeout_ms``) pass
        them as keyword overrides rather than constructing a second dict.

        Args:
            **overrides: Keys to override or add on top of the derived
                defaults, e.g. ``group_id=None`` for an unmanaged DLQ
                consumer or ``consumer_timeout_ms=5000`` for a bounded poll.

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
            "fetch_max_bytes": self.fetch_max_bytes,
            "max_partition_fetch_bytes": self.max_partition_fetch_bytes,
            "enable_auto_commit": False,  # Manual commit for at-least-once
        }
        self._add_security_config(config)
        config.update(overrides)
        return config

    def get_security_config(self) -> dict[str, Any]:
        """Get just the security portion of the config as its own dict.

        For code paths that need to build Kafka client kwargs by some means
        other than :meth:`get_producer_config` / :meth:`get_consumer_config`
        (e.g. clients that intentionally omit consumer-specific settings).
        Delegates to :meth:`_add_security_config` so there is exactly one
        derivation of the security dict, not a second copy that can drift.

        Returns:
            Dictionary of security settings.
        """
        config: dict[str, Any] = {}
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
