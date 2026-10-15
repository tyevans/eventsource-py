"""Unit tests extracted for test_rabbitmq_stats_dict.py."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)

from ._fixtures import PublishTestEvent


class TestGetStatsDictMethod:
    """Tests for RabbitMQEventBus.get_stats_dict() method."""

    def test_get_stats_dict_returns_dict(self) -> None:
        """Test that get_stats_dict() returns a dictionary."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        assert isinstance(stats_dict, dict)

    def test_get_stats_dict_contains_all_counter_fields(self) -> None:
        """Test that get_stats_dict() contains all counter fields."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        expected_fields = [
            "events_published",
            "events_consumed",
            "events_processed_success",
            "events_processed_failed",
            "messages_sent_to_dlq",
            "handler_errors",
            "reconnections",
            "publish_confirms",
            "publish_returns",
        ]

        for field in expected_fields:
            assert field in stats_dict, f"Missing counter field: {field}"

    def test_get_stats_dict_contains_timing_fields(self) -> None:
        """Test that get_stats_dict() contains timing fields."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        expected_fields = [
            "last_publish_at",
            "last_consume_at",
            "last_error_at",
            "connected_at",
        ]

        for field in expected_fields:
            assert field in stats_dict, f"Missing timing field: {field}"

    def test_get_stats_dict_contains_connection_state(self) -> None:
        """Test that get_stats_dict() contains connection state fields."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        assert "is_connected" in stats_dict
        assert "is_consuming" in stats_dict
        assert "uptime_seconds" in stats_dict

    def test_get_stats_dict_uptime_none_when_not_connected(self) -> None:
        """Test that uptime_seconds is None when not connected."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        assert stats_dict["uptime_seconds"] is None
        assert stats_dict["is_connected"] is False

    def test_get_stats_dict_uptime_calculated_when_connected(self) -> None:
        """Test that uptime_seconds is calculated when connected_at is set."""
        bus = RabbitMQEventBus()
        # Simulate being connected 60 seconds ago
        bus._stats.connected_at = datetime.now(UTC)
        bus._connection_manager._connected = True
        bus._connection_manager._connection = MagicMock()
        bus._connection_manager._connection.is_closed = False

        stats_dict = bus.get_stats_dict()

        assert stats_dict["uptime_seconds"] is not None
        assert stats_dict["uptime_seconds"] >= 0
        # Should be very close to 0 since we just set connected_at
        assert stats_dict["uptime_seconds"] < 1.0

    def test_get_stats_dict_timing_fields_as_iso_strings(self) -> None:
        """Test that timing fields are converted to ISO format strings."""
        bus = RabbitMQEventBus()
        now = datetime(2024, 6, 15, 12, 30, 45, tzinfo=UTC)
        bus._stats.last_publish_at = now
        bus._stats.last_consume_at = now

        stats_dict = bus.get_stats_dict()

        # Check ISO format string
        assert stats_dict["last_publish_at"] == now.isoformat()
        assert stats_dict["last_consume_at"] == now.isoformat()

    def test_get_stats_dict_timing_fields_none_when_not_set(self) -> None:
        """Test that timing fields are None when not set."""
        bus = RabbitMQEventBus()
        stats_dict = bus.get_stats_dict()

        assert stats_dict["last_publish_at"] is None
        assert stats_dict["last_consume_at"] is None
        assert stats_dict["last_error_at"] is None
        assert stats_dict["connected_at"] is None

    def test_get_stats_dict_is_json_serializable(self) -> None:
        """Test that get_stats_dict() returns JSON serializable data."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100
        bus._stats.last_publish_at = datetime.now(UTC)
        stats_dict = bus.get_stats_dict()

        # Should not raise any exceptions
        json_str = json.dumps(stats_dict)
        assert isinstance(json_str, str)

        # Round-trip should work
        parsed = json.loads(json_str)
        assert parsed["events_published"] == 100

    def test_get_stats_dict_reflects_current_values(self) -> None:
        """Test that get_stats_dict() returns current values."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 10
        bus._stats.events_consumed = 8
        bus._stats.handler_errors = 2

        stats_dict = bus.get_stats_dict()

        assert stats_dict["events_published"] == 10
        assert stats_dict["events_consumed"] == 8
        assert stats_dict["handler_errors"] == 2


class TestBatchPublishingStatsDict:
    """Tests for batch stats in get_stats_dict()."""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_stats_dict_includes_batch_fields(self, mock_aio_pika: MagicMock) -> None:
        """Test that get_stats_dict includes batch publishing fields."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_queue = AsyncMock()
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        mock_channel.declare_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        events = [PublishTestEvent(aggregate_id=uuid4(), data=f"event-{i}") for i in range(5)]
        await bus.publish(events)

        stats_dict = bus.get_stats_dict()

        assert "batch_publishes" in stats_dict
        assert "batch_events_published" in stats_dict
        assert "batch_partial_failures" in stats_dict
        assert stats_dict["batch_publishes"] == 1
        assert stats_dict["batch_events_published"] == 5
        assert stats_dict["batch_partial_failures"] == 0
