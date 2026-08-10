"""
Unit tests for error classification system (P4-004).

Tests cover:
- ErrorSeverity enum and properties
- ErrorRecoverability enum and properties
- RetryConfig validation and delay calculation
- ErrorClassification dataclass
- Integration: taxonomy exceptions carry valid classification + retry config
"""

from uuid import uuid4

import pytest

from eventsource.application.migration.error_classification import (
    CONNECTIVITY_RETRY_CONFIG,
    CUTOVER_RETRY_CONFIG,
    # Default configs
    TRANSIENT_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    # Enums
    ErrorSeverity,
    # Configuration classes
    RetryConfig,
)
from eventsource.application.migration.exceptions import (
    BulkCopyError,
    CircuitBreakerOpenError,
    ConsistencyError,
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
    DualWriteError,
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    # Exceptions to test
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
    PositionMappingError,
)
from eventsource.ports.migration.models import MigrationPhase


class TestErrorSeverity:
    """Tests for ErrorSeverity enum."""

    def test_severity_values(self) -> None:
        """Test severity enum values."""
        assert ErrorSeverity.CRITICAL.value == "critical"
        assert ErrorSeverity.ERROR.value == "error"
        assert ErrorSeverity.WARNING.value == "warning"
        assert ErrorSeverity.INFO.value == "info"

    def test_should_alert(self) -> None:
        """Test should_alert property."""
        assert ErrorSeverity.CRITICAL.should_alert is True
        assert ErrorSeverity.ERROR.should_alert is True
        assert ErrorSeverity.WARNING.should_alert is False
        assert ErrorSeverity.INFO.should_alert is False

    def test_log_level(self) -> None:
        """Test log_level property returns correct logging levels."""
        import logging

        assert ErrorSeverity.CRITICAL.log_level == logging.CRITICAL
        assert ErrorSeverity.ERROR.log_level == logging.ERROR
        assert ErrorSeverity.WARNING.log_level == logging.WARNING
        assert ErrorSeverity.INFO.log_level == logging.INFO


class TestErrorRecoverability:
    """Tests for ErrorRecoverability enum."""

    def test_recoverability_values(self) -> None:
        """Test recoverability enum values."""
        assert ErrorRecoverability.RECOVERABLE.value == "recoverable"
        assert ErrorRecoverability.TRANSIENT.value == "transient"
        assert ErrorRecoverability.FATAL.value == "fatal"

    def test_should_retry(self) -> None:
        """Test should_retry property."""
        assert ErrorRecoverability.RECOVERABLE.should_retry is False
        assert ErrorRecoverability.TRANSIENT.should_retry is True
        assert ErrorRecoverability.FATAL.should_retry is False

    def test_should_abort(self) -> None:
        """Test should_abort property."""
        assert ErrorRecoverability.RECOVERABLE.should_abort is False
        assert ErrorRecoverability.TRANSIENT.should_abort is False
        assert ErrorRecoverability.FATAL.should_abort is True


class TestRetryConfig:
    """Tests for RetryConfig dataclass."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay_ms == 100.0
        assert config.max_delay_ms == 30000.0
        assert config.exponential_base == 2.0
        assert config.jitter_factor == 0.1

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = RetryConfig(
            max_attempts=5,
            base_delay_ms=500.0,
            max_delay_ms=60000.0,
            exponential_base=3.0,
            jitter_factor=0.2,
        )
        assert config.max_attempts == 5
        assert config.base_delay_ms == 500.0
        assert config.max_delay_ms == 60000.0
        assert config.exponential_base == 3.0
        assert config.jitter_factor == 0.2

    def test_validation_max_attempts(self) -> None:
        """Test validation for max_attempts."""
        with pytest.raises(ValueError, match="max_attempts must be >= 1"):
            RetryConfig(max_attempts=0)

    def test_validation_base_delay(self) -> None:
        """Test validation for base_delay_ms."""
        with pytest.raises(ValueError, match="base_delay_ms must be >= 0"):
            RetryConfig(base_delay_ms=-1)

    def test_validation_max_delay(self) -> None:
        """Test validation for max_delay_ms."""
        with pytest.raises(ValueError, match="max_delay_ms .* must be >= base_delay_ms"):
            RetryConfig(base_delay_ms=1000, max_delay_ms=500)

    def test_validation_exponential_base(self) -> None:
        """Test validation for exponential_base."""
        with pytest.raises(ValueError, match="exponential_base must be >= 1.0"):
            RetryConfig(exponential_base=0.5)

    def test_validation_jitter_factor(self) -> None:
        """Test validation for jitter_factor."""
        with pytest.raises(ValueError, match="jitter_factor must be between 0.0 and 1.0"):
            RetryConfig(jitter_factor=1.5)

    def test_get_delay_exponential(self) -> None:
        """Test exponential backoff calculation."""
        config = RetryConfig(
            base_delay_ms=100.0,
            max_delay_ms=10000.0,
            exponential_base=2.0,
            jitter_factor=0.0,  # Disable jitter for deterministic test
        )

        # Attempt 0: 100 * 2^0 = 100
        assert config.get_delay_ms(0) == 100.0
        # Attempt 1: 100 * 2^1 = 200
        assert config.get_delay_ms(1) == 200.0
        # Attempt 2: 100 * 2^2 = 400
        assert config.get_delay_ms(2) == 400.0
        # Attempt 3: 100 * 2^3 = 800
        assert config.get_delay_ms(3) == 800.0

    def test_get_delay_capped_at_max(self) -> None:
        """Test delay is capped at max_delay_ms."""
        config = RetryConfig(
            base_delay_ms=100.0,
            max_delay_ms=500.0,
            exponential_base=2.0,
            jitter_factor=0.0,
        )

        # Attempt 5: 100 * 2^5 = 3200, but capped at 500
        assert config.get_delay_ms(5) == 500.0

    def test_get_delay_with_jitter(self) -> None:
        """Test delay includes jitter."""
        config = RetryConfig(
            base_delay_ms=100.0,
            max_delay_ms=10000.0,
            exponential_base=2.0,
            jitter_factor=0.1,
        )

        # With jitter, delay should be between base and base * (1 + jitter)
        # For attempt 0: between 100 and 110
        delay = config.get_delay_ms(0)
        assert 100.0 <= delay <= 110.0

    def test_to_dict(self) -> None:
        """Test conversion to dictionary."""
        config = RetryConfig(max_attempts=5)
        data = config.to_dict()

        assert data["max_attempts"] == 5
        assert data["base_delay_ms"] == 100.0
        assert data["max_delay_ms"] == 30000.0
        assert data["exponential_base"] == 2.0
        assert data["jitter_factor"] == 0.1


class TestDefaultRetryConfigs:
    """Tests for default retry configuration instances."""

    def test_transient_config(self) -> None:
        """Test TRANSIENT_RETRY_CONFIG."""
        assert TRANSIENT_RETRY_CONFIG.max_attempts == 5
        assert TRANSIENT_RETRY_CONFIG.base_delay_ms == 100.0

    def test_connectivity_config(self) -> None:
        """Test CONNECTIVITY_RETRY_CONFIG."""
        assert CONNECTIVITY_RETRY_CONFIG.max_attempts == 10
        assert CONNECTIVITY_RETRY_CONFIG.base_delay_ms == 500.0

    def test_cutover_config(self) -> None:
        """Test CUTOVER_RETRY_CONFIG."""
        assert CUTOVER_RETRY_CONFIG.max_attempts == 3
        assert CUTOVER_RETRY_CONFIG.base_delay_ms == 1000.0


class TestErrorClassification:
    """Tests for ErrorClassification dataclass."""

    def test_basic_creation(self) -> None:
        """Test creating a basic classification."""
        classification = ErrorClassification(
            severity=ErrorSeverity.ERROR,
            recoverability=ErrorRecoverability.TRANSIENT,
            error_code="TEST_ERROR",
            category="test",
            suggested_action="Fix the test",
        )

        assert classification.severity == ErrorSeverity.ERROR
        assert classification.recoverability == ErrorRecoverability.TRANSIENT
        assert classification.error_code == "TEST_ERROR"
        assert classification.category == "test"
        assert classification.suggested_action == "Fix the test"
        assert classification.retry_config is None
        assert classification.documentation_url is None
        assert classification.metrics_labels == {}

    def test_with_retry_config(self) -> None:
        """Test classification with retry config."""
        retry_config = RetryConfig(max_attempts=5)
        classification = ErrorClassification(
            severity=ErrorSeverity.WARNING,
            recoverability=ErrorRecoverability.TRANSIENT,
            error_code="TRANSIENT_ERROR",
            category="connectivity",
            suggested_action="Retry",
            retry_config=retry_config,
        )

        assert classification.retry_config is not None
        assert classification.retry_config.max_attempts == 5

    def test_to_dict_basic(self) -> None:
        """Test conversion to dictionary."""
        classification = ErrorClassification(
            severity=ErrorSeverity.ERROR,
            recoverability=ErrorRecoverability.FATAL,
            error_code="FATAL_ERROR",
            category="data",
            suggested_action="Contact support",
        )

        data = classification.to_dict()
        assert data["severity"] == "error"
        assert data["recoverability"] == "fatal"
        assert data["error_code"] == "FATAL_ERROR"
        assert data["category"] == "data"
        assert data["suggested_action"] == "Contact support"
        assert "retry_config" not in data
        assert "documentation_url" not in data
        assert "metrics_labels" not in data

    def test_to_dict_with_optional_fields(self) -> None:
        """Test conversion to dictionary with optional fields."""
        classification = ErrorClassification(
            severity=ErrorSeverity.WARNING,
            recoverability=ErrorRecoverability.TRANSIENT,
            error_code="CONN_ERROR",
            category="connectivity",
            suggested_action="Retry",
            retry_config=RetryConfig(max_attempts=3),
            documentation_url="https://docs.example.com/errors",
            metrics_labels={"component": "migration"},
        )

        data = classification.to_dict()
        assert "retry_config" in data
        assert data["retry_config"]["max_attempts"] == 3
        assert data["documentation_url"] == "https://docs.example.com/errors"
        assert data["metrics_labels"] == {"component": "migration"}


class TestErrorIntegration:
    """Integration tests for error classification system."""

    def test_all_exceptions_have_classification(self) -> None:
        """Test all exception classes have valid classifications."""
        exceptions = [
            MigrationError("test"),
            MigrationNotFoundError(uuid4()),
            MigrationAlreadyExistsError(uuid4(), uuid4()),
            MigrationStateError("test", uuid4(), MigrationPhase.PENDING),
            InvalidPhaseTransitionError(uuid4(), MigrationPhase.PENDING, MigrationPhase.CUTOVER),
            CutoverError("test", uuid4()),
            CutoverTimeoutError(uuid4(), 150.0, 100.0),
            CutoverLagError(uuid4(), 500, 100),
            ConsistencyError("test", uuid4()),
            BulkCopyError(uuid4(), None, "error"),
            DualWriteError(uuid4(), "error"),
            PositionMappingError("test", uuid4()),
            CircuitBreakerOpenError("op", 10.0),
        ]

        for exc in exceptions:
            classification = exc.classification
            assert classification.severity in ErrorSeverity
            assert classification.recoverability in ErrorRecoverability
            assert len(classification.error_code) > 0
            assert len(classification.category) > 0
            assert len(classification.suggested_action) > 0

    def test_transient_errors_have_retry_config(self) -> None:
        """Test transient errors include retry configuration."""
        transient_errors = [
            CutoverTimeoutError(uuid4(), 150.0, 100.0),
            CutoverLagError(uuid4(), 500, 100),
            BulkCopyError(uuid4(), None, "error"),
            DualWriteError(uuid4(), "error"),
            CircuitBreakerOpenError("op", 10.0),
        ]

        for exc in transient_errors:
            assert exc.recoverability_type == ErrorRecoverability.TRANSIENT
            assert exc.retry_config is not None, f"{type(exc).__name__} missing retry_config"
