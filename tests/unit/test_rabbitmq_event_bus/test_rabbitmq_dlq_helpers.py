"""Unit tests extracted for test_rabbitmq_dlq_helpers.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus


class TestRabbitMQDLQHelperMethods:
    """Tests for DLQ helper methods (P2-001)."""

    @pytest.fixture
    def mock_message_no_death(self) -> MagicMock:
        return MagicMock(headers={"event_type": "TestEvent", "aggregate_type": "TestAggregate"})

    @pytest.fixture
    def mock_message_with_death(self) -> MagicMock:
        return MagicMock(
            headers={
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
        )

    @pytest.fixture
    def mock_message_multiple_deaths(self) -> MagicMock:
        return MagicMock(
            headers={
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
        )

    @pytest.fixture
    def mock_message_empty_headers(self) -> MagicMock:
        return MagicMock(headers={})

    @pytest.fixture
    def mock_message_none_headers(self) -> MagicMock:
        return MagicMock(headers=None)

    def test_get_death_count_no_death(self, mock_message_no_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_death_count(mock_message_no_death) == 0

    def test_get_death_count_single_death(self, mock_message_with_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_death_count(mock_message_with_death) == 1

    def test_get_death_count_multiple_deaths(self, mock_message_multiple_deaths: MagicMock) -> None:
        assert RabbitMQEventBus.get_death_count(mock_message_multiple_deaths) == 5

    def test_get_death_count_empty_headers(self, mock_message_empty_headers: MagicMock) -> None:
        assert RabbitMQEventBus.get_death_count(mock_message_empty_headers) == 0

    def test_get_death_count_none_headers(self, mock_message_none_headers: MagicMock) -> None:
        assert RabbitMQEventBus.get_death_count(mock_message_none_headers) == 0

    def test_get_first_death_queue_no_death(self, mock_message_no_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_first_death_queue(mock_message_no_death) is None

    def test_get_first_death_queue_with_death(self, mock_message_with_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_first_death_queue(mock_message_with_death) == "events.default"

    def test_get_first_death_reason_no_death(self, mock_message_no_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_first_death_reason(mock_message_no_death) is None

    def test_get_first_death_reason_with_death(self, mock_message_with_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_first_death_reason(mock_message_with_death) == "rejected"

    def test_get_first_death_exchange_no_death(self, mock_message_no_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_first_death_exchange(mock_message_no_death) is None

    def test_get_first_death_exchange_with_death(self, mock_message_with_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_first_death_exchange(mock_message_with_death) == "events"

    def test_get_original_routing_key_no_death(self, mock_message_no_death: MagicMock) -> None:
        assert RabbitMQEventBus.get_original_routing_key(mock_message_no_death) is None

    def test_get_original_routing_key_with_death(self, mock_message_with_death: MagicMock) -> None:
        assert (
            RabbitMQEventBus.get_original_routing_key(mock_message_with_death)
            == "TestAggregate.TestEvent"
        )

    def test_is_from_dlq_no_death(self, mock_message_no_death: MagicMock) -> None:
        assert RabbitMQEventBus.is_from_dlq(mock_message_no_death) is False

    def test_is_from_dlq_with_death(self, mock_message_with_death: MagicMock) -> None:
        assert RabbitMQEventBus.is_from_dlq(mock_message_with_death) is True

    def test_is_from_dlq_empty_headers(self, mock_message_empty_headers: MagicMock) -> None:
        assert RabbitMQEventBus.is_from_dlq(mock_message_empty_headers) is False

    def test_is_from_dlq_none_headers(self, mock_message_none_headers: MagicMock) -> None:
        assert RabbitMQEventBus.is_from_dlq(mock_message_none_headers) is False

    def test_is_from_dlq_empty_x_death_list(self) -> None:
        assert RabbitMQEventBus.is_from_dlq(MagicMock(headers={"x-death": []})) is False

    def test_get_death_info_no_death(self, mock_message_no_death: MagicMock) -> None:
        info = RabbitMQEventBus.get_death_info(mock_message_no_death)
        assert info["is_dead_lettered"] is False and info["death_count"] == 0
        assert info["first_death_queue"] is None and info["first_death_reason"] is None
        assert info["first_death_exchange"] is None and info["original_routing_key"] is None
        assert info["x_death"] is None

    def test_get_death_info_with_death(self, mock_message_with_death: MagicMock) -> None:
        info = RabbitMQEventBus.get_death_info(mock_message_with_death)
        assert info["is_dead_lettered"] is True and info["death_count"] == 1
        assert info["first_death_queue"] == "events.default"
        assert info["first_death_reason"] == "rejected"
        assert info["first_death_exchange"] == "events"
        assert info["original_routing_key"] == "TestAggregate.TestEvent"
        assert info["x_death"] is not None and len(info["x_death"]) == 1
