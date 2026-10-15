"""Unit tests extracted for test_rabbitmq_dlq_retry.py."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestRetryConfiguration:
    """Tests for retry-related configuration options."""

    def test_default_retry_config_values(self) -> None:
        """Test that default retry config values are set."""
        config = RabbitMQEventBusConfig()

        assert config.retry_base_delay == 1.0
        assert config.retry_max_delay == 60.0
        assert config.retry_jitter == 0.1

    def test_custom_retry_config_values(self) -> None:
        """Test that custom retry config values are accepted."""
        config = RabbitMQEventBusConfig(
            retry_base_delay=2.0,
            retry_max_delay=120.0,
            retry_jitter=0.2,
        )

        assert config.retry_base_delay == 2.0
        assert config.retry_max_delay == 120.0
        assert config.retry_jitter == 0.2

    def test_zero_jitter_config(self) -> None:
        """Test that zero jitter is accepted (no random delay)."""
        config = RabbitMQEventBusConfig(retry_jitter=0.0)

        assert config.retry_jitter == 0.0


class TestCalculateRetryDelay:
    """Tests for the _calculate_retry_delay method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test config with predictable values."""
        return RabbitMQEventBusConfig(
            retry_base_delay=1.0,
            retry_max_delay=60.0,
            retry_jitter=0.0,  # No jitter for deterministic tests
        )

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config)

    def test_exponential_backoff_retry_0(self, bus: RabbitMQEventBus) -> None:
        """Test delay for retry count 0 (first retry)."""
        delay = bus._calculate_retry_delay(0)
        assert delay == 1.0  # 1.0 * (2 ** 0) = 1.0

    def test_exponential_backoff_retry_1(self, bus: RabbitMQEventBus) -> None:
        """Test delay for retry count 1."""
        delay = bus._calculate_retry_delay(1)
        assert delay == 2.0  # 1.0 * (2 ** 1) = 2.0

    def test_exponential_backoff_retry_2(self, bus: RabbitMQEventBus) -> None:
        """Test delay for retry count 2."""
        delay = bus._calculate_retry_delay(2)
        assert delay == 4.0  # 1.0 * (2 ** 2) = 4.0

    def test_exponential_backoff_retry_3(self, bus: RabbitMQEventBus) -> None:
        """Test delay for retry count 3."""
        delay = bus._calculate_retry_delay(3)
        assert delay == 8.0  # 1.0 * (2 ** 3) = 8.0

    def test_delay_capped_at_max(self, bus: RabbitMQEventBus) -> None:
        """Test that delay is capped at max_delay."""
        delay = bus._calculate_retry_delay(10)
        assert delay == 60.0  # Would be 1024.0 but capped at 60.0

    def test_custom_base_delay(self) -> None:
        """Test with custom base delay."""
        config = RabbitMQEventBusConfig(
            retry_base_delay=0.5,
            retry_max_delay=60.0,
            retry_jitter=0.0,
        )
        bus = RabbitMQEventBus(config=config)

        assert bus._calculate_retry_delay(0) == 0.5
        assert bus._calculate_retry_delay(1) == 1.0
        assert bus._calculate_retry_delay(2) == 2.0

    def test_jitter_adds_randomness(self) -> None:
        """Test that jitter adds randomness to delay."""
        config = RabbitMQEventBusConfig(
            retry_base_delay=1.0,
            retry_max_delay=60.0,
            retry_jitter=0.5,  # 50% jitter
        )
        bus = RabbitMQEventBus(config=config)

        # Collect multiple delay values
        delays = [bus._calculate_retry_delay(1) for _ in range(20)]

        # With 50% jitter on base delay of 2.0, values should range from 1.0 to 3.0
        # Check that we have some variance (not all the same)
        assert len(set(delays)) > 1, "Jitter should cause variance in delays"

        # Check all values are within expected range
        for delay in delays:
            assert 1.0 <= delay <= 3.0


class TestRepublishForRetry:
    """Tests for the _republish_for_retry method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig()

    @pytest.fixture
    def bus(self, config: RabbitMQEventBusConfig) -> RabbitMQEventBus:
        """Create bus instance."""
        return RabbitMQEventBus(config=config)

    @pytest.fixture
    def mock_message(self) -> MagicMock:
        """Create mock incoming message."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-retry-count": 0,
        }
        message.body = b'{"test": "data"}'
        message.content_type = "application/json"
        message.content_encoding = "utf-8"
        message.message_id = "test-message-id"
        message.routing_key = "TestAggregate.TestEvent"
        return message

    @pytest.mark.asyncio
    async def test_republish_increments_retry_count(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that retry count is incremented in republished message."""
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 1)

        # Verify publish was called
        mock_exchange.publish.assert_called_once()

        # Get the published message
        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        # Verify header was updated
        assert new_message.headers["x-retry-count"] == 1

    @pytest.mark.asyncio
    async def test_republish_adds_timestamp_header(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that x-last-retry-at timestamp is added."""
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 1)

        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        assert "x-last-retry-at" in new_message.headers
        # Verify it looks like an ISO timestamp
        timestamp = new_message.headers["x-last-retry-at"]
        assert "T" in timestamp  # ISO format includes T separator

    @pytest.mark.asyncio
    async def test_republish_preserves_original_headers(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that original headers are preserved."""
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 2)

        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        assert new_message.headers["event_type"] == "TestEvent"
        assert new_message.headers["aggregate_type"] == "TestAggregate"

    @pytest.mark.asyncio
    async def test_republish_preserves_body(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that message body is preserved."""
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 1)

        call_args = mock_exchange.publish.call_args
        new_message = call_args[0][0]

        assert new_message.body == b'{"test": "data"}'

    @pytest.mark.asyncio
    async def test_republish_uses_original_routing_key(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that original routing key is used."""
        mock_exchange = AsyncMock()
        bus._topology._exchange = mock_exchange

        await bus._republish_for_retry(mock_message, 1)

        call_args = mock_exchange.publish.call_args
        routing_key = call_args[1]["routing_key"]

        assert routing_key == "TestAggregate.TestEvent"

    @pytest.mark.asyncio
    async def test_republish_raises_if_exchange_not_initialized(
        self, bus: RabbitMQEventBus, mock_message: MagicMock
    ) -> None:
        """Test that RuntimeError is raised if exchange not initialized."""
        bus._topology._exchange = None

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await bus._republish_for_retry(mock_message, 1)


def test_retry_delay_comes_from_the_shared_policy() -> None:
    """The bus must delegate backoff to RetryPolicy, not compute it inline."""
    from eventsource.adapters._bus.retry import RetryPolicy

    config = RabbitMQEventBusConfig(
        rabbitmq_url="amqp://guest:guest@localhost/",
        retry_base_delay=2.0,
        retry_max_delay=30.0,
        retry_jitter=0.0,
        max_retries=5,
    )
    bus = RabbitMQEventBus(config=config)

    assert isinstance(bus._retry_policy, RetryPolicy)
    assert bus._retry_policy.base_delay == 2.0
    assert bus._retry_policy.max_delay == 30.0
    assert bus._retry_policy.jitter == 0.0
    assert bus._retry_policy.max_retries == 5
    assert bus._calculate_retry_delay(2) == 8.0
