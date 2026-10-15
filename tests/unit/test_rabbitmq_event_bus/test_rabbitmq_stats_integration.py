"""Unit tests extracted for test_rabbitmq_stats_integration.py."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
)


class TestResetStatsMethod:
    """Tests for RabbitMQEventBus.reset_stats() method."""

    def test_reset_stats_clears_counters(self) -> None:
        """Test that reset_stats() resets all counters to zero."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100
        bus._stats.events_consumed = 95
        bus._stats.events_processed_success = 90
        bus._stats.events_processed_failed = 5
        bus._stats.handler_errors = 3
        bus._stats.reconnections = 2

        bus.reset_stats()

        assert bus.stats.events_published == 0
        assert bus.stats.events_consumed == 0
        assert bus.stats.events_processed_success == 0
        assert bus.stats.events_processed_failed == 0
        assert bus.stats.handler_errors == 0
        assert bus.stats.reconnections == 0

    def test_reset_stats_clears_timing_when_disconnected(self) -> None:
        """Test that reset_stats() clears timing fields when disconnected."""
        bus = RabbitMQEventBus()
        now = datetime.now(UTC)
        bus._stats.last_publish_at = now
        bus._stats.last_consume_at = now
        bus._stats.last_error_at = now

        bus.reset_stats()

        assert bus.stats.last_publish_at is None
        assert bus.stats.last_consume_at is None
        assert bus.stats.last_error_at is None
        assert bus.stats.connected_at is None

    def test_reset_stats_preserves_connected_at_when_connected(self) -> None:
        """Test that reset_stats() preserves connected_at when connected."""
        bus = RabbitMQEventBus()
        connect_time = datetime.now(UTC)
        bus._stats.connected_at = connect_time
        bus._connection_manager._connected = True
        bus._stats.events_published = 100

        bus.reset_stats()

        # connected_at should be preserved
        assert bus.stats.connected_at == connect_time
        # But counters should be reset
        assert bus.stats.events_published == 0

    def test_reset_stats_clears_connected_at_when_disconnected(self) -> None:
        """Test that reset_stats() clears connected_at when disconnected."""
        bus = RabbitMQEventBus()
        bus._stats.connected_at = datetime.now(UTC)
        bus._connection_manager._connected = False  # Not connected

        bus.reset_stats()

        assert bus.stats.connected_at is None

    def test_reset_stats_logs_message(self, caplog: pytest.LogCaptureFixture) -> None:
        """Test that reset_stats() logs a message."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100

        with caplog.at_level(logging.INFO):
            bus.reset_stats()

        assert "Statistics reset" in caplog.text

    def test_reset_stats_creates_new_stats_object(self) -> None:
        """Test that reset_stats() creates a new stats object."""
        bus = RabbitMQEventBus()
        original_stats = bus._stats
        original_stats.events_published = 100

        bus.reset_stats()

        # Should be a new object
        assert bus._stats is not original_stats


class TestStatisticsIntegration:
    """Integration tests for statistics tracking."""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_sets_connected_at(self, mock_aio_pika: MagicMock) -> None:
        """Test that connect() sets connected_at timestamp."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)

        assert bus.stats.connected_at is None

        await bus.connect()

        assert bus.stats.connected_at is not None
        assert isinstance(bus.stats.connected_at, datetime)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_clears_connected_at(self, mock_aio_pika: MagicMock) -> None:
        """Test that disconnect() clears connected_at timestamp."""
        mock_connection = AsyncMock()
        mock_connection.is_closed = False
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)

        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)
        await bus.connect()

        assert bus.stats.connected_at is not None

        await bus.disconnect()

        assert bus.stats.connected_at is None

    def test_initial_stats_are_zero(self) -> None:
        """Test that initial statistics are all zero."""
        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)
        stats = bus.get_stats()

        assert stats.events_published == 0
        assert stats.events_consumed == 0
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.publish_confirms == 0
        assert stats.publish_returns == 0

    def test_stats_dict_contains_all_fields(self) -> None:
        """Test that get_stats_dict() contains all expected fields."""
        config = RabbitMQEventBusConfig()
        bus = RabbitMQEventBus(config=config)
        stats_dict = bus.get_stats_dict()

        # All counter fields
        assert "events_published" in stats_dict
        assert "events_consumed" in stats_dict
        assert "events_processed_success" in stats_dict
        assert "events_processed_failed" in stats_dict
        assert "messages_sent_to_dlq" in stats_dict
        assert "handler_errors" in stats_dict
        assert "reconnections" in stats_dict
        assert "publish_confirms" in stats_dict
        assert "publish_returns" in stats_dict

        # Timing fields
        assert "last_publish_at" in stats_dict
        assert "last_consume_at" in stats_dict
        assert "last_error_at" in stats_dict
        assert "connected_at" in stats_dict

        # Connection state
        assert "is_connected" in stats_dict
        assert "is_consuming" in stats_dict
        assert "uptime_seconds" in stats_dict

    def test_reset_stats_then_get_stats(self) -> None:
        """Test that get_stats() works correctly after reset_stats()."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100

        bus.reset_stats()
        stats = bus.get_stats()

        assert stats.events_published == 0

    def test_reset_stats_then_get_stats_dict(self) -> None:
        """Test that get_stats_dict() works correctly after reset_stats()."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100

        bus.reset_stats()
        stats_dict = bus.get_stats_dict()

        assert stats_dict["events_published"] == 0
