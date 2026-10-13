"""Tests for KafkaEventBus security configuration validation and SSL context creation."""

import ssl
from typing import Any

import pytest

from eventsource.adapters.kafka import KafkaEventBusConfig


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
                    sasl_password="pass",  # pragma: allowlist secret
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
                sasl_password="pass",  # pragma: allowlist secret
            )

    def test_invalid_sasl_mechanism_raises(self) -> None:
        """Test invalid SASL mechanism raises ValueError."""
        with pytest.raises(ValueError, match="Invalid sasl_mechanism"):
            KafkaEventBusConfig(
                security_protocol="SASL_SSL",
                sasl_mechanism="GSSAPI",  # Kerberos not supported
                sasl_username="user",
                sasl_password="pass",  # pragma: allowlist secret
            )

    @pytest.mark.parametrize("mechanism", ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"])
    def test_valid_sasl_mechanisms(self, mechanism: str) -> None:
        """Test all valid SASL mechanisms are accepted."""
        config = KafkaEventBusConfig(
            security_protocol="SASL_SSL",
            sasl_mechanism=mechanism,
            sasl_username="user",
            sasl_password="pass",  # pragma: allowlist secret
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
                sasl_password="pass",  # pragma: allowlist secret
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
            sasl_password="pass",  # pragma: allowlist secret
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
            sasl_password="pass",  # pragma: allowlist secret
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
