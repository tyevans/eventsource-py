"""Unit tests for error classification system (P4-004)."""

import logging
from uuid import uuid4

import pytest

from eventsource.application.migration.error_classification import (
    CONNECTIVITY_RETRY_CONFIG,
    CUTOVER_RETRY_CONFIG,
    TRANSIENT_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
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
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
    PositionMappingError,
)
from eventsource.ports.migration.models import MigrationPhase


class TestErrorSeverity:
    def test_severity_values(self) -> None:
        assert ErrorSeverity.CRITICAL.value == "critical"
        assert ErrorSeverity.ERROR.value == "error"
        assert ErrorSeverity.WARNING.value == "warning"
        assert ErrorSeverity.INFO.value == "info"

    def test_should_alert(self) -> None:
        assert ErrorSeverity.CRITICAL.should_alert is True
        assert ErrorSeverity.ERROR.should_alert is True
        assert ErrorSeverity.WARNING.should_alert is False
        assert ErrorSeverity.INFO.should_alert is False

    def test_log_level(self) -> None:
        assert ErrorSeverity.CRITICAL.log_level == logging.CRITICAL
        assert ErrorSeverity.ERROR.log_level == logging.ERROR
        assert ErrorSeverity.WARNING.log_level == logging.WARNING
        assert ErrorSeverity.INFO.log_level == logging.INFO


class TestErrorRecoverability:
    def test_recoverability_values(self) -> None:
        assert ErrorRecoverability.RECOVERABLE.value == "recoverable"
        assert ErrorRecoverability.TRANSIENT.value == "transient"
        assert ErrorRecoverability.FATAL.value == "fatal"

    def test_should_retry(self) -> None:
        assert ErrorRecoverability.RECOVERABLE.should_retry is False
        assert ErrorRecoverability.TRANSIENT.should_retry is True
        assert ErrorRecoverability.FATAL.should_retry is False

    def test_should_abort(self) -> None:
        assert ErrorRecoverability.RECOVERABLE.should_abort is False
        assert ErrorRecoverability.TRANSIENT.should_abort is False
        assert ErrorRecoverability.FATAL.should_abort is True


class TestRetryConfig:
    def test_default_values(self) -> None:
        config = RetryConfig()
        assert (
            config.max_attempts,
            config.base_delay_ms,
            config.max_delay_ms,
            config.exponential_base,
            config.jitter_factor,
        ) == (3, 100.0, 30000.0, 2.0, 0.1)

    def test_custom_values(self) -> None:
        config = RetryConfig(
            max_attempts=5,
            base_delay_ms=500.0,
            max_delay_ms=60000.0,
            exponential_base=3.0,
            jitter_factor=0.2,
        )
        assert (
            config.max_attempts,
            config.base_delay_ms,
            config.max_delay_ms,
            config.exponential_base,
            config.jitter_factor,
        ) == (5, 500.0, 60000.0, 3.0, 0.2)

    def test_validation_max_attempts(self) -> None:
        with pytest.raises(ValueError, match="max_attempts must be >= 1"):
            RetryConfig(max_attempts=0)

    def test_validation_base_delay(self) -> None:
        with pytest.raises(ValueError, match="base_delay_ms must be >= 0"):
            RetryConfig(base_delay_ms=-1)

    def test_validation_max_delay(self) -> None:
        with pytest.raises(ValueError, match="max_delay_ms .* must be >= base_delay_ms"):
            RetryConfig(base_delay_ms=1000, max_delay_ms=500)

    def test_validation_exponential_base(self) -> None:
        with pytest.raises(ValueError, match="exponential_base must be >= 1.0"):
            RetryConfig(exponential_base=0.5)

    def test_validation_jitter_factor(self) -> None:
        with pytest.raises(ValueError, match="jitter_factor must be between 0.0 and 1.0"):
            RetryConfig(jitter_factor=1.5)

    def test_get_delay_exponential(self) -> None:
        config = RetryConfig(
            base_delay_ms=100.0, max_delay_ms=10000.0, exponential_base=2.0, jitter_factor=0.0
        )
        assert [config.get_delay_ms(i) for i in range(4)] == [100.0, 200.0, 400.0, 800.0]

    def test_get_delay_capped_at_max(self) -> None:
        config = RetryConfig(
            base_delay_ms=100.0, max_delay_ms=500.0, exponential_base=2.0, jitter_factor=0.0
        )
        assert config.get_delay_ms(5) == 500.0

    def test_get_delay_with_jitter(self) -> None:
        config = RetryConfig(
            base_delay_ms=100.0, max_delay_ms=10000.0, exponential_base=2.0, jitter_factor=0.1
        )
        assert 100.0 <= config.get_delay_ms(0) <= 110.0

    def test_to_dict(self) -> None:
        data = RetryConfig(max_attempts=5).to_dict()
        assert data == {
            "max_attempts": 5,
            "base_delay_ms": 100.0,
            "max_delay_ms": 30000.0,
            "exponential_base": 2.0,
            "jitter_factor": 0.1,
        }


class TestDefaultRetryConfigs:
    def test_transient_config(self) -> None:
        assert TRANSIENT_RETRY_CONFIG.max_attempts == 5
        assert TRANSIENT_RETRY_CONFIG.base_delay_ms == 100.0

    def test_connectivity_config(self) -> None:
        assert CONNECTIVITY_RETRY_CONFIG.max_attempts == 10
        assert CONNECTIVITY_RETRY_CONFIG.base_delay_ms == 500.0

    def test_cutover_config(self) -> None:
        assert CUTOVER_RETRY_CONFIG.max_attempts == 3
        assert CUTOVER_RETRY_CONFIG.base_delay_ms == 1000.0


class TestErrorClassification:
    def test_basic_creation(self) -> None:
        c = ErrorClassification(
            severity=ErrorSeverity.ERROR,
            recoverability=ErrorRecoverability.TRANSIENT,
            error_code="TEST_ERROR",
            category="test",
            suggested_action="Fix the test",
        )
        assert (c.severity, c.recoverability, c.error_code, c.category, c.suggested_action) == (
            ErrorSeverity.ERROR,
            ErrorRecoverability.TRANSIENT,
            "TEST_ERROR",
            "test",
            "Fix the test",
        )
        assert (c.retry_config, c.documentation_url, c.metrics_labels) == (None, None, {})

    def test_with_retry_config(self) -> None:
        c = ErrorClassification(
            severity=ErrorSeverity.WARNING,
            recoverability=ErrorRecoverability.TRANSIENT,
            error_code="TRANSIENT_ERROR",
            category="connectivity",
            suggested_action="Retry",
            retry_config=RetryConfig(max_attempts=5),
        )
        assert c.retry_config is not None and c.retry_config.max_attempts == 5

    def test_to_dict_basic(self) -> None:
        data = ErrorClassification(
            severity=ErrorSeverity.ERROR,
            recoverability=ErrorRecoverability.FATAL,
            error_code="FATAL_ERROR",
            category="data",
            suggested_action="Contact support",
        ).to_dict()
        assert data == {
            "severity": "error",
            "recoverability": "fatal",
            "error_code": "FATAL_ERROR",
            "category": "data",
            "suggested_action": "Contact support",
        }

    def test_to_dict_with_optional_fields(self) -> None:
        data = ErrorClassification(
            severity=ErrorSeverity.WARNING,
            recoverability=ErrorRecoverability.TRANSIENT,
            error_code="CONN_ERROR",
            category="connectivity",
            suggested_action="Retry",
            retry_config=RetryConfig(max_attempts=3),
            documentation_url="https://docs.example.com/errors",
            metrics_labels={"component": "migration"},
        ).to_dict()
        assert data["retry_config"]["max_attempts"] == 3
        assert data["documentation_url"] == "https://docs.example.com/errors"
        assert data["metrics_labels"] == {"component": "migration"}


class TestErrorIntegration:
    def test_all_exceptions_have_classification(self) -> None:
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
            c = exc.classification
            assert c.severity in ErrorSeverity and c.recoverability in ErrorRecoverability
            assert c.error_code and c.category and c.suggested_action

    def test_transient_errors_have_retry_config(self) -> None:
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
