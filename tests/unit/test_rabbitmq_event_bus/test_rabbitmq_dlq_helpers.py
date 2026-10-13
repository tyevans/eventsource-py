"""Unit tests extracted for test_rabbitmq_dlq_helpers.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class TestRabbitMQDLQHelperMethods:
    """Tests for DLQ helper methods (P2-001)."""

    @pytest.fixture
    def mock_message_no_death(self) -> MagicMock:
        """Create mock message without x-death headers."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
        }
        return message

    @pytest.fixture
    def mock_message_with_death(self) -> MagicMock:
        """Create mock message with x-death headers (dead-lettered)."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-death": [
                {
                    "count": 1,
                    "reason": "rejected",
                    "queue": "events.default",
                    "exchange": "events",
                    "routing-keys": ["TestAggregate.TestEvent"],
                    "time": 1699999999,
                }
            ],
            "x-first-death-queue": "events.default",
            "x-first-death-reason": "rejected",
            "x-first-death-exchange": "events",
        }
        return message

    @pytest.fixture
    def mock_message_multiple_deaths(self) -> MagicMock:
        """Create mock message with multiple death records."""
        message = MagicMock()
        message.headers = {
            "event_type": "TestEvent",
            "aggregate_type": "TestAggregate",
            "x-death": [
                {
                    "count": 3,
                    "reason": "rejected",
                    "queue": "events.default",
                    "exchange": "events",
                    "routing-keys": ["TestAggregate.TestEvent"],
                },
                {
                    "count": 2,
                    "reason": "expired",
                    "queue": "events.retry",
                    "exchange": "events_retry",
                    "routing-keys": ["events.default"],
                },
            ],
            "x-first-death-queue": "events.default",
            "x-first-death-reason": "rejected",
            "x-first-death-exchange": "events",
        }
        return message

    @pytest.fixture
    def mock_message_empty_headers(self) -> MagicMock:
        """Create mock message with empty headers."""
        message = MagicMock()
        message.headers = {}
        return message

    @pytest.fixture
    def mock_message_none_headers(self) -> MagicMock:
        """Create mock message with None headers."""
        message = MagicMock()
        message.headers = None
        return message

    def test_get_death_count_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_death_count returns 0 when no x-death header."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_no_death)
        assert count == 0

    def test_get_death_count_single_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_death_count returns correct count for single death."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_with_death)
        assert count == 1

    def test_get_death_count_multiple_deaths(self, mock_message_multiple_deaths: MagicMock) -> None:
        """Test get_death_count sums counts from multiple death records."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_multiple_deaths)
        assert count == 5  # 3 + 2

    def test_get_death_count_empty_headers(self, mock_message_empty_headers: MagicMock) -> None:
        """Test get_death_count returns 0 for empty headers."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_empty_headers)
        assert count == 0

    def test_get_death_count_none_headers(self, mock_message_none_headers: MagicMock) -> None:
        """Test get_death_count returns 0 for None headers."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        count = RabbitMQEventBus.get_death_count(mock_message_none_headers)
        assert count == 0

    def test_get_first_death_queue_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_first_death_queue returns None when not dead-lettered."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        queue = RabbitMQEventBus.get_first_death_queue(mock_message_no_death)
        assert queue is None

    def test_get_first_death_queue_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_first_death_queue returns correct queue name."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        queue = RabbitMQEventBus.get_first_death_queue(mock_message_with_death)
        assert queue == "events.default"

    def test_get_first_death_reason_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_first_death_reason returns None when not dead-lettered."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        reason = RabbitMQEventBus.get_first_death_reason(mock_message_no_death)
        assert reason is None

    def test_get_first_death_reason_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_first_death_reason returns correct reason."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        reason = RabbitMQEventBus.get_first_death_reason(mock_message_with_death)
        assert reason == "rejected"

    def test_get_first_death_exchange_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_first_death_exchange returns None when not dead-lettered."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        exchange = RabbitMQEventBus.get_first_death_exchange(mock_message_no_death)
        assert exchange is None

    def test_get_first_death_exchange_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_first_death_exchange returns correct exchange name."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        exchange = RabbitMQEventBus.get_first_death_exchange(mock_message_with_death)
        assert exchange == "events"

    def test_get_original_routing_key_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_original_routing_key returns None when not dead-lettered."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        routing_key = RabbitMQEventBus.get_original_routing_key(mock_message_no_death)
        assert routing_key is None

    def test_get_original_routing_key_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_original_routing_key returns correct routing key."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        routing_key = RabbitMQEventBus.get_original_routing_key(mock_message_with_death)
        assert routing_key == "TestAggregate.TestEvent"

    def test_is_from_dlq_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test is_from_dlq returns False when not dead-lettered."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        result = RabbitMQEventBus.is_from_dlq(mock_message_no_death)
        assert result is False

    def test_is_from_dlq_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test is_from_dlq returns True when dead-lettered."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        result = RabbitMQEventBus.is_from_dlq(mock_message_with_death)
        assert result is True

    def test_is_from_dlq_empty_headers(self, mock_message_empty_headers: MagicMock) -> None:
        """Test is_from_dlq returns False for empty headers."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        result = RabbitMQEventBus.is_from_dlq(mock_message_empty_headers)
        assert result is False

    def test_is_from_dlq_none_headers(self, mock_message_none_headers: MagicMock) -> None:
        """Test is_from_dlq returns False for None headers."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        result = RabbitMQEventBus.is_from_dlq(mock_message_none_headers)
        assert result is False

    def test_is_from_dlq_empty_x_death_list(self) -> None:
        """Test is_from_dlq returns False for empty x-death list."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        message = MagicMock()
        message.headers = {"x-death": []}
        result = RabbitMQEventBus.is_from_dlq(message)
        assert result is False

    def test_get_death_info_no_death(self, mock_message_no_death: MagicMock) -> None:
        """Test get_death_info returns complete dict for non-dead-lettered message."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        info = RabbitMQEventBus.get_death_info(mock_message_no_death)

        assert info["is_dead_lettered"] is False
        assert info["death_count"] == 0
        assert info["first_death_queue"] is None
        assert info["first_death_reason"] is None
        assert info["first_death_exchange"] is None
        assert info["original_routing_key"] is None
        assert info["x_death"] is None

    def test_get_death_info_with_death(self, mock_message_with_death: MagicMock) -> None:
        """Test get_death_info returns complete dict for dead-lettered message."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        info = RabbitMQEventBus.get_death_info(mock_message_with_death)

        assert info["is_dead_lettered"] is True
        assert info["death_count"] == 1
        assert info["first_death_queue"] == "events.default"
        assert info["first_death_reason"] == "rejected"
        assert info["first_death_exchange"] == "events"
        assert info["original_routing_key"] == "TestAggregate.TestEvent"
        assert info["x_death"] is not None
        assert len(info["x_death"]) == 1
