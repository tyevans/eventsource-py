"""Unit tests for retry configuration, backoff calculation, and retry policies."""

import asyncio

import pytest

from eventsource.application.subscriptions.retry import (
    TRANSIENT_EXCEPTIONS,
    RetryConfig,
    RetryError,
    RetryStats,
    calculate_backoff,
    is_retryable_exception,
)


class TestRetryConfigCreation:
    def test_default_values(self):
        config = RetryConfig()
        assert config.max_retries == 5
        assert config.initial_delay == 1.0
        assert config.max_delay == 60.0
        assert config.exponential_base == 2.0
        assert config.jitter == 0.1

    def test_custom_values(self):
        config = RetryConfig(
            max_retries=3, initial_delay=0.5, max_delay=30.0, exponential_base=3.0, jitter=0.2
        )
        assert config.max_retries == 3
        assert config.initial_delay == 0.5
        assert config.max_delay == 30.0
        assert config.exponential_base == 3.0
        assert config.jitter == 0.2

    def test_zero_retries_allowed(self):
        assert RetryConfig(max_retries=0).max_retries == 0

    def test_negative_max_retries_raises(self):
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(max_retries=-1)
        assert "max_retries must be >= 0" in str(exc_info.value)

    def test_zero_initial_delay_raises(self):
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(initial_delay=0)
        assert "initial_delay must be positive" in str(exc_info.value)

    def test_negative_initial_delay_raises(self):
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(initial_delay=-1.0)
        assert "initial_delay must be positive" in str(exc_info.value)

    def test_zero_max_delay_raises(self):
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(max_delay=0)
        assert "max_delay must be positive" in str(exc_info.value)

    def test_max_delay_less_than_initial_raises(self):
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(initial_delay=10.0, max_delay=5.0)
        assert "max_delay" in str(exc_info.value) and "initial_delay" in str(exc_info.value)

    def test_exponential_base_too_low_raises(self):
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(exponential_base=1.0)
        assert "exponential_base must be > 1.0" in str(exc_info.value)

    def test_jitter_negative_raises(self):
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(jitter=-0.1)
        assert "jitter must be between 0.0 and 1.0" in str(exc_info.value)

    def test_jitter_too_high_raises(self):
        with pytest.raises(ValueError) as exc_info:
            RetryConfig(jitter=1.1)
        assert "jitter must be between 0.0 and 1.0" in str(exc_info.value)

    def test_jitter_zero_allowed(self):
        assert RetryConfig(jitter=0.0).jitter == 0.0

    def test_jitter_one_allowed(self):
        assert RetryConfig(jitter=1.0).jitter == 1.0


class TestRetryStats:
    def test_default_values(self):
        stats = RetryStats()
        assert stats.attempts == 0
        assert stats.successes == 0
        assert stats.failures == 0
        assert stats.total_delay_seconds == 0.0
        assert stats.last_error is None

    def test_to_dict(self):
        stats = RetryStats(
            attempts=5,
            successes=1,
            failures=4,
            total_delay_seconds=15.5,
            last_error="Connection refused",
        )
        result = stats.to_dict()
        assert result["attempts"] == 5
        assert result["successes"] == 1
        assert result["failures"] == 4
        assert result["total_delay_seconds"] == 15.5
        assert result["last_error"] == "Connection refused"

    def test_to_dict_with_none_error(self):
        assert RetryStats().to_dict()["last_error"] is None


class TestRetryError:
    def test_create_error(self):
        original = ValueError("Original error")
        error = RetryError("Failed after 5 attempts", attempts=5, last_error=original)
        assert "Failed after 5 attempts" in str(error)
        assert error.attempts == 5
        assert error.last_error is original

    def test_error_is_exception(self):
        original = ConnectionError("Connection refused")
        error = RetryError("Failed", attempts=3, last_error=original)
        assert isinstance(error, Exception)


class TestCalculateBackoff:
    def test_first_attempt(self):
        config = RetryConfig(initial_delay=1.0, jitter=0.0)
        assert calculate_backoff(0, config) == 1.0

    def test_exponential_growth(self):
        config = RetryConfig(initial_delay=1.0, exponential_base=2.0, max_delay=100.0, jitter=0.0)
        assert calculate_backoff(0, config) == 1.0
        assert calculate_backoff(1, config) == 2.0
        assert calculate_backoff(2, config) == 4.0
        assert calculate_backoff(3, config) == 8.0
        assert calculate_backoff(4, config) == 16.0

    def test_max_delay_cap(self):
        config = RetryConfig(initial_delay=1.0, max_delay=10.0, jitter=0.0)
        assert calculate_backoff(4, config) == 10.0
        assert calculate_backoff(10, config) == 10.0

    def test_jitter_adds_variation(self):
        config = RetryConfig(initial_delay=10.0, jitter=0.5, max_delay=100.0)
        delays = [calculate_backoff(0, config) for _ in range(100)]
        assert min(delays) >= 5.0
        assert max(delays) <= 15.0
        assert len({round(d, 2) for d in delays}) > 1

    def test_jitter_zero_no_variation(self):
        config = RetryConfig(initial_delay=5.0, jitter=0.0)
        assert all(calculate_backoff(0, config) == 5.0 for _ in range(10))

    def test_delay_never_negative(self):
        config = RetryConfig(initial_delay=0.1, jitter=1.0)
        for _ in range(100):
            assert calculate_backoff(0, config) >= 0


class TestIsRetryableException:
    def test_connection_error_is_retryable(self):
        assert is_retryable_exception(ConnectionError("Connection refused"))

    def test_timeout_error_is_retryable(self):
        assert is_retryable_exception(TimeoutError("Timed out"))

    def test_asyncio_timeout_is_retryable(self):
        assert is_retryable_exception(TimeoutError())

    def test_os_error_is_retryable(self):
        assert is_retryable_exception(OSError("Network unreachable"))

    def test_value_error_not_retryable(self):
        assert not is_retryable_exception(ValueError("Invalid value"))

    def test_custom_retryable_exceptions(self):
        class CustomError(Exception):
            pass

        assert is_retryable_exception(CustomError("Custom"), retryable_exceptions=(CustomError,))
        assert not is_retryable_exception(ValueError("Value"), retryable_exceptions=(CustomError,))


class TestTransientExceptions:
    def test_includes_connection_error(self):
        assert ConnectionError in TRANSIENT_EXCEPTIONS

    def test_includes_timeout_error(self):
        assert TimeoutError in TRANSIENT_EXCEPTIONS

    def test_includes_asyncio_timeout(self):
        assert asyncio.TimeoutError in TRANSIENT_EXCEPTIONS

    def test_includes_os_error(self):
        assert OSError in TRANSIENT_EXCEPTIONS
