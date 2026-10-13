"""Shared fixtures and helper functions for retry tests."""

from unittest.mock import AsyncMock

import pytest

from eventsource.application.subscriptions.retry import (
    CircuitBreaker,
    CircuitBreakerConfig,
    RetryConfig,
)


@pytest.fixture
def default_retry_config() -> RetryConfig:
    """Create default retry configuration for tests."""
    return RetryConfig(max_retries=3, initial_delay=0.01)


@pytest.fixture
def default_breaker_config() -> CircuitBreakerConfig:
    """Create default circuit breaker configuration for tests."""
    return CircuitBreakerConfig(failure_threshold=3, recovery_timeout=0.05)


@pytest.fixture
def circuit_breaker(default_breaker_config: CircuitBreakerConfig) -> CircuitBreaker:
    """Create circuit breaker instance for tests."""
    return CircuitBreaker(default_breaker_config)


@pytest.fixture
def mock_operation() -> AsyncMock:
    """Create mock async operation returning success."""
    return AsyncMock(return_value="success")
