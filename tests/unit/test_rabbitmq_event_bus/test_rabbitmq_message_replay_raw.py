"""Unit tests extracted for test_rabbitmq_message_replay_raw.py."""

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestReplayMessage:
    """Tests for _replay_message helper method."""

    @pytest.fixture
    def config(self) -> RabbitMQEventBusConfig:
        """Create test configuration."""
        return RabbitMQEventBusConfig(
            rabbitmq_url="amqp://test:test@localhost/",
            exchange_name="test-events",
            consumer_group="test-group",
        )

    @pytest.mark.asyncio
    async def test_raises_when_exchange_not_initialized(
        self, config: RabbitMQEventBusConfig
    ) -> None:
        """Test _replay_message raises when exchange not initialized."""
        bus = RabbitMQEventBus(config=config)
        bus._exchange = None

        mock_message = AsyncMock()

        with pytest.raises(RuntimeError, match="Exchange not initialized"):
            await bus._replay_message(mock_message)

    @pytest.mark.asyncio
    async def test_removes_dlq_headers(self, config: RabbitMQEventBusConfig) -> None:
        """Test that DLQ headers are removed from replayed message."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b'{"data": "test"}'
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "dlq-key"
        mock_message.headers = {
            "event_type": "TestEvent",
            "x-dlq-reason": "Error",
            "x-dlq-error-type": "ValueError",
            "x-dlq-retry-count": 3,
            "x-dlq-timestamp": "2024-01-15T10:00:00Z",
            "x-original-routing-key": "Test.TestEvent",
            "x-death": [{"queue": "test"}],
        }

        await bus._replay_message(mock_message)

        # Verify the published message has DLQ headers removed
        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert "x-dlq-reason" not in replay_msg.headers
        assert "x-dlq-error-type" not in replay_msg.headers
        assert "x-dlq-retry-count" not in replay_msg.headers
        assert "x-dlq-timestamp" not in replay_msg.headers
        assert "x-original-routing-key" not in replay_msg.headers
        assert "x-death" not in replay_msg.headers

    @pytest.mark.asyncio
    async def test_resets_retry_count(self, config: RabbitMQEventBusConfig) -> None:
        """Test that retry count is reset to 0."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "key"
        mock_message.headers = {"x-retry-count": 5}

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert replay_msg.headers["x-retry-count"] == 0

    @pytest.mark.asyncio
    async def test_adds_replay_timestamp(self, config: RabbitMQEventBusConfig) -> None:
        """Test that x-replayed-from-dlq timestamp is added."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "key"
        mock_message.headers = {}

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert "x-replayed-from-dlq" in replay_msg.headers
        # Verify it looks like an ISO timestamp
        assert "T" in replay_msg.headers["x-replayed-from-dlq"]

    @pytest.mark.asyncio
    async def test_uses_original_routing_key(self, config: RabbitMQEventBusConfig) -> None:
        """Test that original routing key is used for republishing."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "dlq-routing-key"
        mock_message.headers = {
            "x-original-routing-key": "Original.RoutingKey",
        }

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        routing_key = publish_call.kwargs["routing_key"]

        assert routing_key == "Original.RoutingKey"

    @pytest.mark.asyncio
    async def test_falls_back_to_message_routing_key(self, config: RabbitMQEventBusConfig) -> None:
        """Test fallback to message routing key when original not in headers."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "fallback-routing-key"
        mock_message.headers = {}

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        routing_key = publish_call.kwargs["routing_key"]

        assert routing_key == "fallback-routing-key"

    @pytest.mark.asyncio
    async def test_preserves_non_dlq_headers(self, config: RabbitMQEventBusConfig) -> None:
        """Test that non-DLQ headers are preserved."""
        bus = RabbitMQEventBus(config=config)
        mock_exchange = AsyncMock()
        bus._exchange = mock_exchange

        mock_message = AsyncMock()
        mock_message.body = b"{}"
        mock_message.content_type = "application/json"
        mock_message.content_encoding = "utf-8"
        mock_message.message_id = "test-id"
        mock_message.routing_key = "key"
        mock_message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "correlation_id": "corr-123",
            "x-dlq-reason": "Error",  # Should be removed
        }

        await bus._replay_message(mock_message)

        publish_call = mock_exchange.publish.call_args
        replay_msg = publish_call[0][0]

        assert replay_msg.headers["event_type"] == "TestEvent"
        assert replay_msg.headers["aggregate_type"] == "TestAggregate"
        assert replay_msg.headers["correlation_id"] == "corr-123"
