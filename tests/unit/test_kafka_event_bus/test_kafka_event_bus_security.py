"""Tests for KafkaEventBus security configuration validation and SSL context creation."""

import ssl
from typing import Any

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


class TestKafkaEventBusConfigSSLContext:
    """Tests for SSL context creation."""

    def test_ssl_context_created_for_ssl_protocol(self) -> None:
        context = KafkaEventBusConfig(security_protocol="SSL").create_ssl_context()
        assert isinstance(context, ssl.SSLContext)

    def test_ssl_context_created_for_sasl_ssl_protocol(self) -> None:
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
        )
        context = config.create_ssl_context()
        assert isinstance(context, ssl.SSLContext)

    def test_ssl_context_none_for_plaintext(self) -> None:
        assert KafkaEventBusConfig(security_protocol="PLAINTEXT").create_ssl_context() is None

    def test_ssl_context_none_for_sasl_plaintext(self) -> None:
        config = KafkaEventBusConfig(
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="PLAIN",
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
        )
        assert config.create_ssl_context() is None

    def test_ssl_context_hostname_verification_enabled(self) -> None:
        config = KafkaEventBusConfig(security_protocol="SSL", ssl_check_hostname=True)
        context = config.create_ssl_context()
        assert context is not None
        assert context.check_hostname is True
        assert context.verify_mode == ssl.CERT_REQUIRED

    def test_ssl_context_hostname_verification_disabled(self) -> None:
        config = KafkaEventBusConfig(security_protocol="SSL", ssl_check_hostname=False)
        context = config.create_ssl_context()
        assert context is not None
        assert context.check_hostname is False
        assert context.verify_mode == ssl.CERT_NONE

    def test_ssl_context_loads_ca_file(self, tmp_path: Any) -> None:
        ca_file = tmp_path / "ca.crt"
        ca_file.write_text("dummy")
        config = KafkaEventBusConfig(security_protocol="SSL", ssl_cafile=str(ca_file))
        with pytest.raises(ssl.SSLError):
            config.create_ssl_context()
