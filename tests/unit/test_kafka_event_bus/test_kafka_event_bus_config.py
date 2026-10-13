"""Tests for KafkaEventBusConfig dataclass, sanitization, and producer/consumer config."""

from unittest.mock import patch

from eventsource.adapters.kafka import KafkaEventBusConfig


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
        assert config.producer_max_batch_bytes == 16384
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


class TestKafkaEventBusConfigSanitization:
    """Tests for credential sanitization in logging."""

    def test_sanitized_config_redacts_password(self) -> None:
        """Test sasl_password is redacted in sanitized config."""
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="myuser",
            sasl_password="supersecret",  # pragma: allowlist secret
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


class TestKafkaEventBusConfigProducerConsumer:
    """Tests for producer and consumer configuration generation."""

    def test_get_producer_config_basic(self) -> None:
        """Test basic producer configuration."""
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            acks="all",
            compression_type="gzip",
            producer_max_batch_bytes=32768,
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
            sasl_password="pass",  # pragma: allowlist secret
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
        assert consumer_config["fetch_max_bytes"] == 52428800
        assert consumer_config["max_partition_fetch_bytes"] == 1048576

    def test_get_consumer_config_fetch_bounds_configurable(self) -> None:
        """fetch_max_bytes/max_partition_fetch_bytes reach the consumer kwargs."""
        config = KafkaEventBusConfig(
            fetch_max_bytes=1024,
            max_partition_fetch_bytes=512,
            security_protocol="PLAINTEXT",
        )

        consumer_config = config.get_consumer_config()

        assert consumer_config["fetch_max_bytes"] == 1024
        assert consumer_config["max_partition_fetch_bytes"] == 512

    def test_get_consumer_config_overrides(self) -> None:
        """Keyword overrides win over the derived defaults (used by DLQ consumers)."""
        config = KafkaEventBusConfig(
            consumer_group="main-group",
            security_protocol="PLAINTEXT",
        )

        consumer_config = config.get_consumer_config(group_id=None, consumer_timeout_ms=5000)

        assert consumer_config["group_id"] is None
        assert consumer_config["consumer_timeout_ms"] == 5000
        # Unrelated defaults are still present.
        assert consumer_config["fetch_max_bytes"] == 52428800

    def test_get_security_config_matches_producer_and_consumer(self) -> None:
        """get_security_config() is the same derivation used everywhere else."""
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
            ssl_check_hostname=False,
        )

        security_config = config.get_security_config()
        consumer_config = config.get_consumer_config()
        producer_config = config.get_producer_config()

        assert security_config["ssl_check_hostname"] is False
        for key in security_config:
            assert consumer_config[key] == security_config[key]
            assert producer_config[key] == security_config[key]

    def test_get_consumer_config_with_security(self) -> None:
        """Test consumer configuration with security settings."""
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
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
