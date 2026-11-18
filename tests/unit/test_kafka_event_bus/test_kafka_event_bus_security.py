"""Tests for KafkaEventBus security configuration validation."""

import pytest

from eventsource.adapters.kafka import KafkaEventBusConfig


class TestKafkaEventBusConfigSecurity:
    """Tests for security configuration validation."""

    def test_valid_security_protocols(self, caplog: pytest.LogCaptureFixture) -> None:
        for protocol in ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]:
            caplog.clear()
            if protocol.startswith("SASL_"):
                config = KafkaEventBusConfig(
                    security_protocol=protocol,
                    sasl_mechanism="PLAIN",
                    sasl_username="user",
                    sasl_password="pass",  # pragma: allowlist secret
                )
            else:
                config = KafkaEventBusConfig(security_protocol=protocol)
            assert config.security_protocol == protocol

    def test_invalid_security_protocol_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid security_protocol"):
            KafkaEventBusConfig(security_protocol="INVALID")

    def test_sasl_requires_mechanism(self) -> None:
        with pytest.raises(ValueError, match="sasl_mechanism required"):
            KafkaEventBusConfig(security_protocol="SASL_SSL", sasl_mechanism=None)

    def test_sasl_requires_credentials(self) -> None:
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
                sasl_password="pass",  # pragma: allowlist secret
            )

    def test_invalid_sasl_mechanism_raises(self) -> None:
        with pytest.raises(ValueError, match="Invalid sasl_mechanism"):
            KafkaEventBusConfig(
                security_protocol="SASL_SSL",
                sasl_mechanism="GSSAPI",
                sasl_username="user",
                sasl_password="pass",  # pragma: allowlist secret
            )

    @pytest.mark.parametrize("mechanism", ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    def test_valid_sasl_mechanisms(self, mechanism: str) -> None:
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism=mechanism,
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
        )
        assert config.sasl_mechanism == mechanism

    def test_mtls_requires_both_cert_and_key(self) -> None:
        with pytest.raises(ValueError, match="ssl_keyfile required"):
            KafkaEventBusConfig(
                security_protocol="SSL", ssl_certfile="/path/to/cert", ssl_keyfile=None
            )
        with pytest.raises(ValueError, match="ssl_certfile required"):
            KafkaEventBusConfig(
                security_protocol="SSL", ssl_certfile=None, ssl_keyfile="/path/to/key"
            )

    def test_mtls_with_both_files_succeeds(self) -> None:
        config = KafkaEventBusConfig(
            security_protocol="SSL",
            ssl_certfile="/path/to/cert.pem",
            ssl_keyfile="/path/to/key.pem",
        )
        assert config.ssl_certfile == "/path/to/cert.pem"
        assert config.ssl_keyfile == "/path/to/key.pem"

    def test_plaintext_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING"):
            KafkaEventBusConfig(security_protocol="PLAINTEXT")
        assert "not recommended for production" in caplog.text

    def test_sasl_plaintext_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING"):
            KafkaEventBusConfig(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="PLAIN",
                sasl_username="user",
                sasl_password="pass",  # pragma: allowlist secret
            )
        assert "credentials sent in plain text" in caplog.text

    def test_disabled_hostname_check_logs_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level("WARNING"):
            KafkaEventBusConfig(security_protocol="SSL", ssl_check_hostname=False)
        assert "vulnerable to MITM" in caplog.text
