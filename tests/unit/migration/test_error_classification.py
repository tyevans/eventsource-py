"""
Unit tests for error classification system (P4-004).

Tests cover:
- ErrorSeverity enum and properties
- ErrorRecoverability enum and properties
- RetryConfig validation and delay calculation
- ErrorClassification dataclass
- Exception classification integration
- CircuitBreaker state management
- ErrorHandler retry logic
"""

import asyncio
from uuid import uuid4

import pytest

from eventsource.migration.exceptions import (
    CONNECTIVITY_RETRY_CONFIG,
    CUTOVER_RETRY_CONFIG,
    # Default configs
    TRANSIENT_RETRY_CONFIG,
    BulkCopyError,
    # Circuit breaker
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerOpenError,
    CircuitState,
    ConsistencyError,
    CutoverError,
    CutoverLagError,
    CutoverTimeoutError,
    DualWriteError,
    ErrorClassification,
    # Error handler
    ErrorHandler,
    ErrorRecoverability,
    # Enums
    ErrorSeverity,
    InvalidPhaseTransitionError,
    MigrationAlreadyExistsError,
    # Exceptions to test
    MigrationError,
    MigrationNotFoundError,
    MigrationStateError,
    PositionMappingError,
    # Configuration classes
    RetryConfig,
    RoutingError,
    classify_exception,
)
from eventsource.migration.models import MigrationPhase


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


class TestExceptionClassification:
    """Tests for exception classification integration."""

    def test_migration_error_classification(self) -> None:
        """Test base MigrationError classification."""
        error = MigrationError("Test error")
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "MIGRATION_ERROR"

    def test_migration_not_found_classification(self) -> None:
        """Test MigrationNotFoundError classification."""
        error = MigrationNotFoundError(uuid4())
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "MIGRATION_NOT_FOUND"

    def test_migration_already_exists_classification(self) -> None:
        """Test MigrationAlreadyExistsError classification."""
        error = MigrationAlreadyExistsError(uuid4(), uuid4())
        assert error.severity == ErrorSeverity.WARNING
        assert error.recoverability_type == ErrorRecoverability.RECOVERABLE
        assert error.error_code == "MIGRATION_ALREADY_EXISTS"

    def test_migration_state_error_classification(self) -> None:
        """Test MigrationStateError classification."""
        error = MigrationStateError(
            "Invalid state",
            migration_id=uuid4(),
            current_phase=MigrationPhase.PENDING,
        )
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "MIGRATION_STATE_ERROR"

    def test_invalid_phase_transition_classification(self) -> None:
        """Test InvalidPhaseTransitionError classification."""
        error = InvalidPhaseTransitionError(
            uuid4(),
            current_phase=MigrationPhase.PENDING,
            target_phase=MigrationPhase.CUTOVER,
        )
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "INVALID_PHASE_TRANSITION"

    def test_cutover_error_classification(self) -> None:
        """Test CutoverError classification."""
        error = CutoverError("Cutover failed", migration_id=uuid4())
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.RECOVERABLE
        assert error.error_code == "CUTOVER_ERROR"
        assert error.retry_config is not None

    def test_cutover_timeout_classification(self) -> None:
        """Test CutoverTimeoutError classification."""
        error = CutoverTimeoutError(uuid4(), elapsed_ms=150.0, timeout_ms=100.0)
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "CUTOVER_TIMEOUT"
        assert error.retry_config is not None

    def test_cutover_lag_classification(self) -> None:
        """Test CutoverLagError classification."""
        error = CutoverLagError(uuid4(), current_lag=500, max_lag=100)
        assert error.severity == ErrorSeverity.WARNING
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "CUTOVER_LAG_TOO_HIGH"
        assert error.retry_config is not None

    def test_consistency_error_classification(self) -> None:
        """Test ConsistencyError classification."""
        error = ConsistencyError("Data mismatch", migration_id=uuid4())
        assert error.severity == ErrorSeverity.CRITICAL
        assert error.recoverability_type == ErrorRecoverability.RECOVERABLE
        assert error.error_code == "CONSISTENCY_ERROR"

    def test_bulk_copy_error_classification(self) -> None:
        """Test BulkCopyError classification."""
        error = BulkCopyError(uuid4(), last_position=1000, error="Connection lost")
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "BULK_COPY_ERROR"
        assert error.retry_config is not None

    def test_dual_write_error_classification(self) -> None:
        """Test DualWriteError classification."""
        error = DualWriteError(uuid4(), target_error="Write failed")
        assert error.severity == ErrorSeverity.WARNING
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "DUAL_WRITE_ERROR"
        assert error.retry_config is not None

    def test_position_mapping_error_classification(self) -> None:
        """Test PositionMappingError classification."""
        error = PositionMappingError("Position not found", migration_id=uuid4())
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.RECOVERABLE
        assert error.error_code == "POSITION_MAPPING_ERROR"

    def test_routing_error_classification(self) -> None:
        """Test RoutingError classification."""
        error = RoutingError("No route", tenant_id=uuid4())
        assert error.severity == ErrorSeverity.ERROR
        assert error.recoverability_type == ErrorRecoverability.FATAL
        assert error.error_code == "ROUTING_ERROR"

    def test_error_to_dict(self) -> None:
        """Test exception to_dict includes classification."""
        error = CutoverTimeoutError(uuid4(), elapsed_ms=150.0, timeout_ms=100.0)
        data = error.to_dict()

        assert "message" in data
        assert "classification" in data
        assert data["classification"]["error_code"] == "CUTOVER_TIMEOUT"
        assert data["classification"]["severity"] == "error"


class TestClassifyException:
    """Tests for classify_exception function."""

    def test_classify_migration_error(self) -> None:
        """Test classifying a MigrationError."""
        error = BulkCopyError(uuid4(), last_position=0, error="test")
        classification = classify_exception(error)

        assert classification.error_code == "BULK_COPY_ERROR"
        assert classification.severity == ErrorSeverity.ERROR
        assert classification.recoverability == ErrorRecoverability.TRANSIENT

    def test_classify_generic_exception(self) -> None:
        """Test classifying a non-MigrationError."""
        error = ValueError("Something went wrong")
        classification = classify_exception(error)

        assert classification.error_code == "UNKNOWN_ERROR"
        assert classification.severity == ErrorSeverity.ERROR
        assert classification.recoverability == ErrorRecoverability.FATAL


class TestCircuitBreaker:
    """Tests for CircuitBreaker implementation."""

    @pytest.fixture
    def circuit_breaker(self) -> CircuitBreaker:
        """Create a circuit breaker with low thresholds for testing."""
        config = CircuitBreakerConfig(
            failure_threshold=3,
            success_threshold=2,
            timeout_seconds=0.1,  # Very short for testing
        )
        return CircuitBreaker(config=config, name="test")

    @pytest.mark.asyncio
    async def test_initial_state_is_closed(self, circuit_breaker: CircuitBreaker) -> None:
        """Test circuit starts in closed state."""
        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_successful_operation(self, circuit_breaker: CircuitBreaker) -> None:
        """Test successful operation keeps circuit closed."""
        ctx = await circuit_breaker.protect("test_op")
        async with ctx:
            pass  # Success

        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_failure_increments_count(self, circuit_breaker: CircuitBreaker) -> None:
        """Test failure increments failure count."""
        ctx = await circuit_breaker.protect("test_op")
        with pytest.raises(ValueError):
            async with ctx:
                raise ValueError("Test error")

        assert circuit_breaker.failure_count == 1
        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_circuit_opens_after_threshold(self, circuit_breaker: CircuitBreaker) -> None:
        """Test circuit opens after failure threshold."""
        # Cause enough failures to open circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        assert circuit_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_open_circuit_rejects_operations(self, circuit_breaker: CircuitBreaker) -> None:
        """Test open circuit rejects new operations."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        # Next operation should be rejected
        with pytest.raises(CircuitBreakerOpenError) as exc_info:
            await circuit_breaker.protect("test_op")

        assert "Circuit breaker open" in str(exc_info.value)
        assert exc_info.value.time_until_retry >= 0

    @pytest.mark.asyncio
    async def test_circuit_transitions_to_half_open(self, circuit_breaker: CircuitBreaker) -> None:
        """Test circuit transitions to half-open after timeout."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        assert circuit_breaker.state == CircuitState.OPEN

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Should allow operation in half-open state
        ctx = await circuit_breaker.protect("test_op")
        assert circuit_breaker.state == CircuitState.HALF_OPEN

    @pytest.mark.asyncio
    async def test_half_open_closes_on_success(self, circuit_breaker: CircuitBreaker) -> None:
        """Test half-open circuit closes after successful operations."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Success in half-open state
        for _ in range(2):  # success_threshold
            ctx = await circuit_breaker.protect("test_op")
            async with ctx:
                pass

        assert circuit_breaker.state == CircuitState.CLOSED

    @pytest.mark.asyncio
    async def test_half_open_reopens_on_failure(self, circuit_breaker: CircuitBreaker) -> None:
        """Test half-open circuit reopens on failure."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        # Wait for timeout
        await asyncio.sleep(0.15)

        # Failure in half-open state
        ctx = await circuit_breaker.protect("test_op")
        with pytest.raises(ValueError):
            async with ctx:
                raise ValueError("Test error")

        assert circuit_breaker.state == CircuitState.OPEN

    @pytest.mark.asyncio
    async def test_reset(self, circuit_breaker: CircuitBreaker) -> None:
        """Test circuit can be manually reset."""
        # Open the circuit
        for _ in range(3):
            ctx = await circuit_breaker.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Test error")

        assert circuit_breaker.state == CircuitState.OPEN

        # Reset
        circuit_breaker.reset()

        assert circuit_breaker.state == CircuitState.CLOSED
        assert circuit_breaker.failure_count == 0

    @pytest.mark.asyncio
    async def test_excluded_exceptions_dont_trip_circuit(self) -> None:
        """Test excluded exceptions don't trip the circuit."""
        config = CircuitBreakerConfig(
            failure_threshold=2,
            excluded_exceptions=(ValueError,),
        )
        cb = CircuitBreaker(config=config, name="test")

        # Excluded exception shouldn't count
        for _ in range(5):
            ctx = await cb.protect("test_op")
            with pytest.raises(ValueError):
                async with ctx:
                    raise ValueError("Excluded")

        assert cb.state == CircuitState.CLOSED

        # Non-excluded exception should count
        for _ in range(2):
            ctx = await cb.protect("test_op")
            with pytest.raises(RuntimeError):
                async with ctx:
                    raise RuntimeError("Not excluded")

        assert cb.state == CircuitState.OPEN


class TestCircuitBreakerOpenError:
    """Tests for CircuitBreakerOpenError exception."""

    def test_classification(self) -> None:
        """Test CircuitBreakerOpenError classification."""
        error = CircuitBreakerOpenError("test_op", time_until_retry=30.0)
        assert error.severity == ErrorSeverity.WARNING
        assert error.recoverability_type == ErrorRecoverability.TRANSIENT
        assert error.error_code == "CIRCUIT_BREAKER_OPEN"

    def test_attributes(self) -> None:
        """Test CircuitBreakerOpenError attributes."""
        error = CircuitBreakerOpenError("my_operation", time_until_retry=15.5, migration_id=uuid4())
        assert error.operation_name == "my_operation"
        assert error.time_until_retry == 15.5
        assert "my_operation" in str(error)
        assert "15.5s" in str(error)


class TestErrorHandler:
    """Tests for ErrorHandler with retry logic."""

    @pytest.fixture
    def handler(self) -> ErrorHandler:
        """Create an error handler for testing."""
        return ErrorHandler()

    @pytest.mark.asyncio
    async def test_successful_operation(self, handler: ErrorHandler) -> None:
        """Test successful operation returns result."""

        async def operation() -> str:
            return "success"

        result = await handler.execute_with_retry(operation, "test_op")
        assert result == "success"

    @pytest.mark.asyncio
    async def test_retry_transient_error(self, handler: ErrorHandler) -> None:
        """Test transient error is retried."""
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise DualWriteError(uuid4(), "Temporary failure")
            return "success"

        config = RetryConfig(max_attempts=5, base_delay_ms=1, jitter_factor=0)
        result = await handler.execute_with_retry(operation, "test_op", retry_config=config)

        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_no_retry_fatal_error(self, handler: ErrorHandler) -> None:
        """Test fatal error is not retried."""
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            raise MigrationNotFoundError(uuid4())

        with pytest.raises(MigrationNotFoundError):
            await handler.execute_with_retry(operation, "test_op")

        assert call_count == 1

    @pytest.mark.asyncio
    async def test_exhausted_retries(self, handler: ErrorHandler) -> None:
        """Test error raised after exhausting retries."""

        async def operation() -> str:
            raise DualWriteError(uuid4(), "Always fails")

        config = RetryConfig(max_attempts=3, base_delay_ms=1, jitter_factor=0)
        with pytest.raises(DualWriteError):
            await handler.execute_with_retry(operation, "test_op", retry_config=config)

    @pytest.mark.asyncio
    async def test_on_retry_callback(self, handler: ErrorHandler) -> None:
        """Test on_retry callback is called."""
        retries: list[tuple[int, Exception, float]] = []

        def on_retry(attempt: int, exc: Exception, delay_ms: float) -> None:
            retries.append((attempt, exc, delay_ms))

        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise DualWriteError(uuid4(), "Temporary")
            return "success"

        config = RetryConfig(max_attempts=5, base_delay_ms=10, jitter_factor=0)
        await handler.execute_with_retry(
            operation, "test_op", retry_config=config, on_retry=on_retry
        )

        assert len(retries) == 2
        assert retries[0][0] == 0  # First retry
        assert retries[1][0] == 1  # Second retry

    @pytest.mark.asyncio
    async def test_alert_callback(self) -> None:
        """Test alert callback is invoked for alerting severity."""
        alerts: list[MigrationError] = []

        def alert_callback(error: MigrationError) -> None:
            alerts.append(error)

        handler = ErrorHandler(alert_callback=alert_callback)

        async def operation() -> str:
            raise ConsistencyError("Critical issue", migration_id=uuid4())

        with pytest.raises(ConsistencyError):
            await handler.execute_with_retry(operation, "test_op")

        assert len(alerts) == 1
        assert alerts[0].severity == ErrorSeverity.CRITICAL

    @pytest.mark.asyncio
    async def test_metrics_callback(self) -> None:
        """Test metrics callback is invoked."""
        metrics: list[tuple[MigrationError, bool]] = []

        def metrics_callback(error: MigrationError, retried: bool) -> None:
            metrics.append((error, retried))

        handler = ErrorHandler(metrics_callback=metrics_callback)

        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise DualWriteError(uuid4(), "Temporary")
            return "success"

        config = RetryConfig(max_attempts=3, base_delay_ms=1, jitter_factor=0)
        await handler.execute_with_retry(operation, "test_op", retry_config=config)

        assert len(metrics) == 1
        assert metrics[0][1] is True  # retried = True for transient

    @pytest.mark.asyncio
    async def test_with_circuit_breaker(self) -> None:
        """Test error handler with circuit breaker."""
        cb_config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=0.1)
        cb = CircuitBreaker(config=cb_config, name="test")
        handler = ErrorHandler(circuit_breaker=cb)

        async def failing_operation() -> str:
            raise MigrationNotFoundError(uuid4())

        # Fail twice to open circuit
        for _ in range(2):
            with pytest.raises(MigrationNotFoundError):
                await handler.execute_with_retry(failing_operation, "test_op")

        # Next call should get circuit breaker error
        with pytest.raises(CircuitBreakerOpenError):
            await handler.execute_with_retry(failing_operation, "test_op")

    def test_with_retry_decorator(self, handler: ErrorHandler) -> None:
        """Test with_retry decorator."""
        call_count = 0

        @handler.with_retry(
            operation_name="decorated_op",
            retry_config=RetryConfig(max_attempts=3, base_delay_ms=1, jitter_factor=0),
        )
        async def decorated_operation() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise DualWriteError(uuid4(), "Temporary")
            return "decorated_success"

        async def run_test() -> None:
            result = await decorated_operation()
            assert result == "decorated_success"
            assert call_count == 2

        asyncio.get_event_loop().run_until_complete(run_test())

    @pytest.mark.asyncio
    async def test_non_migration_error_not_retried(self, handler: ErrorHandler) -> None:
        """Test non-MigrationError exceptions are not retried."""
        call_count = 0

        async def operation() -> str:
            nonlocal call_count
            call_count += 1
            raise RuntimeError("Unexpected error")

        with pytest.raises(RuntimeError):
            await handler.execute_with_retry(operation, "test_op")

        assert call_count == 1


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
            BulkCopyError(uuid4(), 0, "error"),
            DualWriteError(uuid4(), "error"),
            PositionMappingError("test", uuid4()),
            RoutingError("test", uuid4()),
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
            BulkCopyError(uuid4(), 0, "error"),
            DualWriteError(uuid4(), "error"),
            CircuitBreakerOpenError("op", 10.0),
        ]

        for exc in transient_errors:
            assert exc.recoverability_type == ErrorRecoverability.TRANSIENT
            assert exc.retry_config is not None, f"{type(exc).__name__} missing retry_config"
