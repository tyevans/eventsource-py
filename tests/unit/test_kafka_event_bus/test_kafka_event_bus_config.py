"""Tests for KafkaEventBusConfig dataclass, sanitization, and producer/consumer config."""

from unittest.mock import patch

from eventsource.adapters.kafka import KafkaEventBusConfig


def _cfg(**kwargs) -> KafkaEventBusConfig:
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        return KafkaEventBusConfig(**kwargs)


class TestKafkaEventBusConfig:
    """Tests for KafkaEventBusConfig dataclass."""

    def test_default_values(self) -> None:
        config = _cfg()
        assert config.bootstrap_servers == "localhost:9092" and config.topic_prefix == "events"
        assert config.consumer_group == "default" and config.consumer_name is not None
        assert config.acks == "all" and config.compression_type == "gzip"
        assert config.producer_max_batch_bytes == 16384 and config.linger_ms == 5
        assert config.auto_offset_reset == "earliest" and config.security_protocol == "PLAINTEXT"
        assert config.enable_dlq is True and config.enable_tracing is True

    def test_custom_values(self) -> None:
        config = _cfg(
            bootstrap_servers="kafka1:9092,kafka2:9092",
            topic_prefix="myapp.events",
            consumer_group="projections",
            consumer_name="worker-1",
            acks="1",
            compression_type="snappy",
        )
        assert config.bootstrap_servers == "kafka1:9092,kafka2:9092"
        assert config.topic_prefix == "myapp.events" and config.consumer_group == "projections"
        assert config.consumer_name == "worker-1" and config.acks == "1"
        assert config.compression_type == "snappy"

    def test_topic_name_property(self) -> None:
        assert _cfg(topic_prefix="orders").topic_name == "orders.stream"

    def test_dlq_topic_name_property(self) -> None:
        config = _cfg(topic_prefix="orders", dlq_topic_suffix=".dead-letters")
        assert config.dlq_topic_name == "orders.stream.dead-letters"

    def test_auto_generated_consumer_name(self) -> None:
        config = _cfg()
        assert config.consumer_name is not None and "-" in config.consumer_name


class TestKafkaEventBusConfigSanitization:
    """Tests for credential sanitization in logging."""

    def test_sanitized_config_redacts_password(self) -> None:
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="myuser",
            sasl_password="supersecret",  # pragma: allowlist secret
        )
        sanitized = config.get_sanitized_config()
        assert sanitized["sasl_password"] == "***" and sanitized["sasl_username"] == "myuser"
        assert sanitized["bootstrap_servers"] == "kafka:9092"

    def test_sanitized_config_redacts_ssl_keyfile(self) -> None:
        config = KafkaEventBusConfig(
            security_protocol="SSL",
            ssl_certfile="/path/to/cert.pem",
            ssl_keyfile="/path/to/private.key",
        )
        sanitized = config.get_sanitized_config()
        assert sanitized["ssl_keyfile"] == "***"
        assert sanitized["ssl_certfile"] == "/path/to/cert.pem"

    def test_sanitized_config_handles_none_values(self) -> None:
        sanitized = KafkaEventBusConfig(security_protocol="PLAINTEXT").get_sanitized_config()
        for key in ("sasl_password", "ssl_keyfile", "sasl_username", "sasl_mechanism"):
            assert sanitized[key] is None

    def test_sanitized_config_includes_all_fields(self) -> None:
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
        expected = [
            "bootstrap_servers",
            "topic_prefix",
            "consumer_group",
            "consumer_name",
            "security_protocol",
            "sasl_mechanism",
            "sasl_username",
            "sasl_password",
            "ssl_cafile",
            "ssl_certfile",
            "ssl_keyfile",
            "ssl_check_hostname",
            "enable_dlq",
            "enable_tracing",
        ]
        assert set(expected).issubset(sanitized)


class TestKafkaEventBusConfigProducerConsumer:
    """Tests for producer and consumer configuration generation."""

    def test_get_producer_config_basic(self) -> None:
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            acks="all",
            compression_type="gzip",
            producer_max_batch_bytes=32768,
            linger_ms=10,
            security_protocol="PLAINTEXT",
        )
        p_cfg = config.get_producer_config()
        assert p_cfg["bootstrap_servers"] == "kafka:9092" and p_cfg["acks"] == "all"
        assert p_cfg["compression_type"] == "gzip" and p_cfg["max_batch_size"] == 32768
        assert p_cfg["linger_ms"] == 10 and p_cfg["security_protocol"] == "PLAINTEXT"

    def test_get_producer_config_with_security(self) -> None:
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
            ssl_cafile="/path/to/ca.crt",
        )
        p_cfg = config.get_producer_config()
        assert p_cfg["security_protocol"] == "SASL_SSL"
        assert p_cfg["sasl_mechanism"] == "SCRAM-SHA-512"
        assert p_cfg["sasl_plain_username"] == "user" and p_cfg["sasl_plain_password"] == "pass"
        assert p_cfg["ssl_cafile"] == "/path/to/ca.crt" and p_cfg["ssl_check_hostname"] is True

    def test_get_consumer_config_basic(self) -> None:
        config = KafkaEventBusConfig(
            bootstrap_servers="kafka:9092",
            consumer_group="my-group",
            consumer_name="worker-1",
            auto_offset_reset="earliest",
            security_protocol="PLAINTEXT",
        )
        c_cfg = config.get_consumer_config()
        assert c_cfg["bootstrap_servers"] == "kafka:9092" and c_cfg["group_id"] == "my-group"
        assert c_cfg["client_id"] == "worker-1" and c_cfg["auto_offset_reset"] == "earliest"
        assert c_cfg["enable_auto_commit"] is False and c_cfg["fetch_max_bytes"] == 52428800
        assert c_cfg["max_partition_fetch_bytes"] == 1048576

    def test_get_consumer_config_fetch_bounds_configurable(self) -> None:
        config = KafkaEventBusConfig(
            fetch_max_bytes=1024, max_partition_fetch_bytes=512, security_protocol="PLAINTEXT"
        )
        c_cfg = config.get_consumer_config()
        assert c_cfg["fetch_max_bytes"] == 1024 and c_cfg["max_partition_fetch_bytes"] == 512

    def test_get_consumer_config_overrides(self) -> None:
        config = KafkaEventBusConfig(consumer_group="main-group", security_protocol="PLAINTEXT")
        c_cfg = config.get_consumer_config(group_id=None, consumer_timeout_ms=5000)
        assert c_cfg["group_id"] is None and c_cfg["consumer_timeout_ms"] == 5000
        assert c_cfg["fetch_max_bytes"] == 52428800

    def test_get_security_config_matches_producer_and_consumer(self) -> None:
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
            ssl_check_hostname=False,
        )
        sec_cfg = config.get_security_config()
        c_cfg, p_cfg = config.get_consumer_config(), config.get_producer_config()
        assert sec_cfg["ssl_check_hostname"] is False
        for key in sec_cfg:
            assert c_cfg[key] == sec_cfg[key] and p_cfg[key] == sec_cfg[key]

    def test_get_consumer_config_with_security(self) -> None:
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
            ssl_check_hostname=False,
        )
        c_cfg = config.get_consumer_config()
        assert c_cfg["security_protocol"] == "SASL_SSL" and c_cfg["sasl_mechanism"] == "PLAIN"
        assert c_cfg["sasl_plain_username"] == "user" and c_cfg["sasl_plain_password"] == "pass"
        assert c_cfg["ssl_check_hostname"] is False

    def test_ssl_check_hostname_not_included_for_plaintext(self) -> None:
        config = KafkaEventBusConfig(security_protocol="PLAINTEXT")
        assert "ssl_check_hostname" not in config.get_producer_config()
        assert "ssl_check_hostname" not in config.get_consumer_config()
