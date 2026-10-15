"""Unit tests extracted for test_rabbitmq_stats_integration.py."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig


class TestResetStatsMethod:
    """Tests for RabbitMQEventBus.reset_stats() method."""

    def test_reset_stats_clears_counters(self) -> None:
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100
        bus._stats.events_consumed = 95
        bus._stats.events_processed_success = 90
        bus._stats.events_processed_failed = 5
        bus._stats.handler_errors = 3
        bus._stats.reconnections = 2
        bus.reset_stats()
        assert bus.stats.events_published == 0 and bus.stats.events_consumed == 0
        assert bus.stats.events_processed_success == 0 and bus.stats.events_processed_failed == 0
        assert bus.stats.handler_errors == 0 and bus.stats.reconnections == 0

    def test_reset_stats_clears_timing_when_disconnected(self) -> None:
        bus = RabbitMQEventBus()
        now = datetime.now(UTC)
        bus._stats.last_publish_at = now
        bus._stats.last_consume_at = now
        bus._stats.last_error_at = now
        bus.reset_stats()
        assert bus.stats.last_publish_at is None and bus.stats.last_consume_at is None
        assert bus.stats.last_error_at is None and bus.stats.connected_at is None

    def test_reset_stats_preserves_connected_at_when_connected(self) -> None:
        bus = RabbitMQEventBus()
        connect_time = datetime.now(UTC)
        bus._stats.connected_at = connect_time
        bus._connection_manager._connected = True
        bus._stats.events_published = 100
        bus.reset_stats()
        assert bus.stats.connected_at == connect_time and bus.stats.events_published == 0

    def test_reset_stats_clears_connected_at_when_disconnected(self) -> None:
        bus = RabbitMQEventBus()
        bus._stats.connected_at = datetime.now(UTC)
        bus._connection_manager._connected = False
        bus.reset_stats()
        assert bus.stats.connected_at is None

    def test_reset_stats_logs_message(self, caplog: pytest.LogCaptureFixture) -> None:
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100
        with caplog.at_level(logging.INFO):
            bus.reset_stats()
        assert "Statistics reset" in caplog.text

    def test_reset_stats_creates_new_stats_object(self) -> None:
        bus = RabbitMQEventBus()
        original_stats = bus._stats
        original_stats.events_published = 100
        bus.reset_stats()
        assert bus._stats is not original_stats


class TestStatisticsIntegration:
    """Integration tests for statistics tracking."""

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_connect_sets_connected_at(self, mock_aio_pika: MagicMock) -> None:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        bus = RabbitMQEventBus(config=RabbitMQEventBusConfig())
        assert bus.stats.connected_at is None
        await bus.connect()
        assert isinstance(bus.stats.connected_at, datetime)

    @patch("eventsource.adapters.rabbitmq.connection.aio_pika")
    @pytest.mark.asyncio
    async def test_disconnect_clears_connected_at(self, mock_aio_pika: MagicMock) -> None:
        mock_connection = AsyncMock(is_closed=False)
        mock_channel = AsyncMock(is_closed=False)
        mock_aio_pika.connect_robust = AsyncMock(return_value=mock_connection)
        mock_connection.channel = AsyncMock(return_value=mock_channel)
        bus = RabbitMQEventBus(config=RabbitMQEventBusConfig())
        await bus.connect()
        assert bus.stats.connected_at is not None
        await bus.disconnect()
        assert bus.stats.connected_at is None

    def test_initial_stats_are_zero(self) -> None:
        stats = RabbitMQEventBus(config=RabbitMQEventBusConfig()).get_stats()
        assert stats.events_published == 0 and stats.events_consumed == 0
        assert stats.events_processed_success == 0 and stats.events_processed_failed == 0
        assert stats.handler_errors == 0 and stats.reconnections == 0
        assert stats.publish_confirms == 0 and stats.publish_returns == 0

    def test_stats_dict_contains_all_fields(self) -> None:
        stats_dict = RabbitMQEventBus(config=RabbitMQEventBusConfig()).get_stats_dict()
        for field in (
            "events_published",
            "events_consumed",
            "events_processed_success",
            "events_processed_failed",
            "messages_sent_to_dlq",
            "handler_errors",
            "reconnections",
            "publish_confirms",
            "publish_returns",
            "last_publish_at",
            "last_consume_at",
            "last_error_at",
            "connected_at",
            "is_connected",
            "is_consuming",
            "uptime_seconds",
        ):
            assert field in stats_dict

    def test_reset_stats_then_get_stats(self) -> None:
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100
        bus.reset_stats()
        assert bus.get_stats().events_published == 0

    def test_reset_stats_then_get_stats_dict(self) -> None:
        bus = RabbitMQEventBus()
        bus._stats.events_published = 100
        bus.reset_stats()
        assert bus.get_stats_dict()["events_published"] == 0
