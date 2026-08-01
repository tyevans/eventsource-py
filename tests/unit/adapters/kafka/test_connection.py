"""Unit tests for the extracted ``KafkaConnectionManager``.

Covers connect/disconnect/cleanup/reconnect lifecycle and record_reconnection
/record_rebalance logic in isolation from ``KafkaEventBus``.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.adapters.kafka.config import KafkaEventBusConfig  # noqa: E402
from eventsource.adapters.kafka.connection import (  # noqa: E402
    KafkaConnectionManager,
    KafkaRebalanceListener,
)
from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics  # noqa: E402
from eventsource.adapters.kafka.models import KafkaEventBusStats  # noqa: E402


def _make_manager(**config_kwargs: object) -> KafkaConnectionManager:
    with patch.object(KafkaEventBusConfig, "_validate_security_config"):
        config = KafkaEventBusConfig(**config_kwargs)  # type: ignore[arg-type]
    stats = KafkaEventBusStats()
    metrics = MagicMock(spec=KafkaEventBusMetrics)
    metrics.reconnections = MagicMock()
    metrics.rebalances = MagicMock()
    metrics.connection_errors = MagicMock()
    return KafkaConnectionManager(config=config, stats=stats, metrics=metrics)


class TestConnect:
    @pytest.mark.asyncio
    async def test_connect_starts_producer_and_consumer(self) -> None:
        manager = _make_manager()
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer",
                return_value=mock_producer,
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer",
                return_value=mock_consumer,
            ),
        ):
            await manager.connect()

        mock_producer.start.assert_awaited_once()
        mock_consumer.start.assert_awaited_once()
        assert manager.is_connected is True
        assert manager.producer is mock_producer
        assert manager.consumer is mock_consumer

    @pytest.mark.asyncio
    async def test_connect_already_connected_is_noop(self) -> None:
        manager = _make_manager()
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer",
                return_value=mock_producer,
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer",
                return_value=mock_consumer,
            ),
        ):
            await manager.connect()
            await manager.connect()

        mock_producer.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_connect_failure_cleans_up_partial_connection(self) -> None:
        manager = _make_manager()
        mock_producer = AsyncMock()
        mock_producer.start.side_effect = Exception("boom")

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer",
                return_value=mock_producer,
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer",
                return_value=AsyncMock(),
            ),
            pytest.raises(Exception, match="boom"),
        ):
            await manager.connect()

        assert manager.is_connected is False
        assert manager.producer is None


class TestDisconnect:
    @pytest.mark.asyncio
    async def test_disconnect_stops_both_and_flips_connected(self) -> None:
        manager = _make_manager()
        mock_producer = AsyncMock()
        mock_consumer = AsyncMock()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer",
                return_value=mock_producer,
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer",
                return_value=mock_consumer,
            ),
        ):
            await manager.connect()
            await manager.disconnect()

        mock_producer.stop.assert_awaited_once()
        mock_consumer.stop.assert_awaited_once()
        assert manager.is_connected is False
        assert manager.producer is None
        assert manager.consumer is None

    @pytest.mark.asyncio
    async def test_disconnect_is_double_call_safe(self) -> None:
        manager = _make_manager()

        await manager.disconnect()
        await manager.disconnect()

        assert manager.is_connected is False


class TestRequireProducer:
    def test_require_producer_raises_before_connect(self) -> None:
        manager = _make_manager()

        with pytest.raises(RuntimeError):
            manager.require_producer()

    @pytest.mark.asyncio
    async def test_require_producer_returns_producer_after_connect(self) -> None:
        manager = _make_manager()
        mock_producer = AsyncMock()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer",
                return_value=mock_producer,
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer",
                return_value=AsyncMock(),
            ),
        ):
            await manager.connect()

        assert manager.require_producer() is mock_producer


class TestReconnectConsumer:
    @pytest.mark.asyncio
    async def test_reconnect_consumer_replaces_consumer(self) -> None:
        manager = _make_manager()
        old_consumer = AsyncMock()
        new_consumer = AsyncMock()

        with (
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaProducer",
                return_value=AsyncMock(),
            ),
            patch(
                "eventsource.adapters.kafka.connection.AIOKafkaConsumer",
                return_value=old_consumer,
            ),
        ):
            await manager.connect()

        with patch(
            "eventsource.adapters.kafka.connection.AIOKafkaConsumer",
            return_value=new_consumer,
        ):
            await manager.reconnect_consumer()

        old_consumer.stop.assert_awaited_once()
        new_consumer.start.assert_awaited_once()
        assert manager.consumer is new_consumer


class TestRecordReconnection:
    def test_increments_stats_and_metrics(self) -> None:
        manager = _make_manager()

        manager.record_reconnection()
        manager.record_reconnection()

        assert manager._stats.reconnections == 2
        assert manager._metrics.reconnections.add.call_count == 2


class TestRecordRebalance:
    def test_increments_stats_and_metrics(self) -> None:
        manager = _make_manager()

        manager.record_rebalance()

        assert manager._stats.rebalance_count == 1
        manager._metrics.rebalances.add.assert_called_once()


class TestKafkaRebalanceListener:
    @pytest.mark.asyncio
    async def test_on_partitions_revoked_commits_and_records_rebalance(self) -> None:
        manager = _make_manager()
        manager._consumer = AsyncMock()
        listener = KafkaRebalanceListener(manager)

        tp = MagicMock()
        tp.topic = "t"
        tp.partition = 0

        await listener.on_partitions_revoked({tp})

        manager._consumer.commit.assert_awaited_once()
        assert manager._stats.rebalance_count == 1

    @pytest.mark.asyncio
    async def test_on_partitions_revoked_noop_for_empty_set(self) -> None:
        manager = _make_manager()
        listener = KafkaRebalanceListener(manager)

        await listener.on_partitions_revoked(set())

        assert manager._stats.rebalance_count == 0

    @pytest.mark.asyncio
    async def test_on_partitions_assigned_noop(self) -> None:
        manager = _make_manager()
        listener = KafkaRebalanceListener(manager)

        tp = MagicMock()
        tp.topic = "t"
        tp.partition = 0

        # Should not raise; nothing to assert beyond no exception.
        await listener.on_partitions_assigned({tp})
