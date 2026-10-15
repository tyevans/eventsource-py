"""Unit tests for error classification system (P4-004)."""

import logging
from uuid import uuid4

import pytest

import eventsource.application.migration.exceptions as mig_exc
from eventsource.application.migration.error_classification import (
    CONNECTIVITY_RETRY_CONFIG,
    CUTOVER_RETRY_CONFIG,
    TRANSIENT_RETRY_CONFIG,
    ErrorClassification,
    ErrorRecoverability,
    ErrorSeverity,
    RetryConfig,
)
from eventsource.ports.migration.models import MigrationPhase


class TestErrorSeverity:
    def test_severity_values(self) -> None:
        assert (ErrorSeverity.CRITICAL.value, ErrorSeverity.ERROR.value) == ("critical", "error")
        assert (ErrorSeverity.WARNING.value, ErrorSeverity.INFO.value) == ("warning", "info")

    def test_should_alert(self) -> None:
        assert ErrorSeverity.CRITICAL.should_alert and ErrorSeverity.ERROR.should_alert
        assert not ErrorSeverity.WARNING.should_alert and not ErrorSeverity.INFO.should_alert

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
        assert not ErrorRecoverability.RECOVERABLE.should_retry
        assert ErrorRecoverability.TRANSIENT.should_retry
        assert not ErrorRecoverability.FATAL.should_retry

    def test_should_abort(self) -> None:
        assert not ErrorRecoverability.RECOVERABLE.should_abort
        assert not ErrorRecoverability.TRANSIENT.should_abort
        assert ErrorRecoverability.FATAL.should_abort


class TestRetryConfig:
    def test_default_values(self) -> None:
        c = RetryConfig()
        assert (c.max_attempts, c.base_delay_ms, c.max_delay_ms) == (3, 100.0, 30000.0)
        assert (c.exponential_base, c.jitter_factor) == (2.0, 0.1)

    def test_custom_values(self) -> None:
        c = RetryConfig(5, 500.0, 60000.0, 3.0, 0.2)
        assert (c.max_attempts, c.base_delay_ms, c.max_delay_ms) == (5, 500.0, 60000.0)
        assert (c.exponential_base, c.jitter_factor) == (3.0, 0.2)

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
        cfg = RetryConfig(base_delay_ms=100.0, max_delay_ms=10000.0, jitter_factor=0.0)
        assert [cfg.get_delay_ms(i) for i in range(4)] == [100.0, 200.0, 400.0, 800.0]

    def test_get_delay_capped_at_max(self) -> None:
        cfg = RetryConfig(base_delay_ms=100.0, max_delay_ms=500.0, jitter_factor=0.0)
        assert cfg.get_delay_ms(5) == 500.0

    def test_get_delay_with_jitter(self) -> None:
        cfg = RetryConfig(base_delay_ms=100.0, max_delay_ms=10000.0, jitter_factor=0.1)
        assert 100.0 <= cfg.get_delay_ms(0) <= 110.0

    def test_to_dict(self) -> None:
        assert RetryConfig(max_attempts=5).to_dict() == {
            "max_attempts": 5,
            "base_delay_ms": 100.0,
            "max_delay_ms": 30000.0,
            "exponential_base": 2.0,
            "jitter_factor": 0.1,
        }


class TestDefaultRetryConfigs:
    def test_transient_config(self) -> None:
        c = TRANSIENT_RETRY_CONFIG
        assert (c.max_attempts, c.base_delay_ms) == (5, 100.0)

    def test_connectivity_config(self) -> None:
        c = CONNECTIVITY_RETRY_CONFIG
        assert (c.max_attempts, c.base_delay_ms) == (10, 500.0)

    def test_cutover_config(self) -> None:
        c = CUTOVER_RETRY_CONFIG
        assert (c.max_attempts, c.base_delay_ms) == (3, 1000.0)


class TestErrorClassification:
    def test_basic_creation(self) -> None:
        c = ErrorClassification(
            ErrorSeverity.ERROR, ErrorRecoverability.TRANSIENT, "TEST_ERROR", "test", "Fix the test"
        )
        assert c.severity == ErrorSeverity.ERROR
        assert c.recoverability == ErrorRecoverability.TRANSIENT
        assert c.error_code == "TEST_ERROR" and c.category == "test"
        assert c.suggested_action == "Fix the test"
        assert (c.retry_config, c.documentation_url, c.metrics_labels) == (None, None, {})

    def test_with_retry_config(self) -> None:
        rc = RetryConfig(max_attempts=5)
        c = ErrorClassification(
            ErrorSeverity.WARNING,
            ErrorRecoverability.TRANSIENT,
            "TRANSIENT_ERROR",
            "connectivity",
            "Retry",
            retry_config=rc,
        )
        assert c.retry_config is not None and c.retry_config.max_attempts == 5

    def test_to_dict_basic(self) -> None:
        c = ErrorClassification(
            ErrorSeverity.ERROR, ErrorRecoverability.FATAL, "FATAL_ERROR", "data", "Contact support"
        )
        assert c.to_dict() == {
            "severity": "error",
            "recoverability": "fatal",
            "error_code": "FATAL_ERROR",
            "category": "data",
            "suggested_action": "Contact support",
        }

    def test_to_dict_with_optional_fields(self) -> None:
        data = ErrorClassification(
            ErrorSeverity.WARNING,
            ErrorRecoverability.TRANSIENT,
            "CONN_ERROR",
            "connectivity",
            "Retry",
            retry_config=RetryConfig(max_attempts=3),
            documentation_url="https://docs.example.com/errors",
            metrics_labels={"component": "migration"},
        ).to_dict()
        assert data["retry_config"]["max_attempts"] == 3
        assert data["documentation_url"] == "https://docs.example.com/errors"
        assert data["metrics_labels"] == {"component": "migration"}


class TestErrorIntegration:
    def test_all_exceptions_have_classification(self) -> None:
        uid, p1, p2 = uuid4(), MigrationPhase.PENDING, MigrationPhase.CUTOVER
        exceptions = [
            mig_exc.MigrationError("test"),
            mig_exc.MigrationNotFoundError(uid),
            mig_exc.MigrationAlreadyExistsError(uid, uid),
            mig_exc.MigrationStateError("test", uid, p1),
            mig_exc.InvalidPhaseTransitionError(uid, p1, p2),
            mig_exc.CutoverError("test", uid),
            mig_exc.CutoverTimeoutError(uid, 150.0, 100.0),
            mig_exc.CutoverLagError(uid, 500, 100),
            mig_exc.ConsistencyError("test", uid),
            mig_exc.BulkCopyError(uid, None, "error"),
            mig_exc.DualWriteError(uid, "error"),
            mig_exc.PositionMappingError("test", uid),
            mig_exc.CircuitBreakerOpenError("op", 10.0),
        ]
        for exc in exceptions:
            c = exc.classification
            assert c.severity in ErrorSeverity and c.recoverability in ErrorRecoverability
            assert c.error_code and c.category and c.suggested_action

    def test_transient_errors_have_retry_config(self) -> None:
        uid = uuid4()
        for exc in [
            mig_exc.CutoverTimeoutError(uid, 150.0, 100.0),
            mig_exc.CutoverLagError(uid, 500, 100),
            mig_exc.BulkCopyError(uid, None, "error"),
            mig_exc.DualWriteError(uid, "error"),
            mig_exc.CircuitBreakerOpenError("op", 10.0),
        ]:
            assert exc.recoverability_type == ErrorRecoverability.TRANSIENT
            assert exc.retry_config is not None, f"{type(exc).__name__} missing retry_config"
