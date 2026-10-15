"""Unit tests for RabbitMQEventBus facade composition (Task 9)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from eventsource.adapters.rabbitmq.bus import RabbitMQEventBus
from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.models import HealthCheckResult, QueueInfo


@pytest.fixture
def calls() -> list[str]:
    return []


@pytest.fixture
def bus_with_mocks(calls: list[str]) -> RabbitMQEventBus:
    """A real RabbitMQEventBus with its collaborators swapped for mocks."""
    config = RabbitMQEventBusConfig()
    bus = RabbitMQEventBus(config=config)

    mock_connection_manager = MagicMock()
    mock_connection_manager.disconnect = AsyncMock(
        side_effect=lambda: calls.append("connection.disconnect")
    )
    mock_connection_manager.health_slice = MagicMock(
        return_value={
            "healthy": True,
            "connection_status": "connected",
            "channel_status": "open",
            "errors": [],
        }
    )
    mock_connection_manager._connection = MagicMock(is_closed=False)
    mock_connection_manager._channel = MagicMock(is_closed=False)
    mock_connection_manager._connected = True

    mock_topology = MagicMock()
    mock_topology.redeclare = AsyncMock(side_effect=lambda: calls.append("topology.redeclare"))
    mock_topology.queue_health = AsyncMock(
        return_value=QueueInfo(
            name=config.queue_name, message_count=0, consumer_count=1, state="running"
        )
    )
    mock_topology.consumer_queue = MagicMock()
    mock_topology.dlq_queue = MagicMock()
    mock_topology.exchange = MagicMock()
    mock_topology.dlq_exchange = MagicMock()

    mock_consumer = MagicMock()
    mock_consumer.stop_gracefully = AsyncMock(
        side_effect=lambda timeout: calls.append("consumer.stop_gracefully")
    )
    mock_consumer.drain_in_flight = AsyncMock(
        side_effect=lambda timeout: calls.append("consumer.drain_in_flight")
    )
    mock_consumer._consuming = False
    mock_consumer._consumer_task = None

    bus._connection_manager = mock_connection_manager
    bus._topology = mock_topology
    bus._consumer = mock_consumer
    bus._publisher = MagicMock()
    bus._dlq_admin = MagicMock()
    return bus


@pytest.mark.asyncio
async def test_reconnect_wires_topology_redeclare_only_and_fires_once() -> None:
    config = RabbitMQEventBusConfig()
    bus = RabbitMQEventBus(config=config)

    callbacks = bus._connection_manager._reconnect_callbacks
    assert len(callbacks) == 1
    assert callbacks[0] == bus._topology.redeclare

    calls: list[str] = []

    async def fake_redeclare() -> None:
        calls.append("topology.redeclare")

    bus._topology.redeclare = fake_redeclare  # type: ignore[method-assign]
    bus._connection_manager._reconnect_callbacks = [bus._topology.redeclare]

    await bus._connection_manager._run_reconnect_callbacks()
    assert calls == ["topology.redeclare"]

    await bus._connection_manager._run_reconnect_callbacks()
    assert calls == ["topology.redeclare", "topology.redeclare"]


@pytest.mark.asyncio
async def test_shutdown_ordering(bus_with_mocks: RabbitMQEventBus, calls: list[str]) -> None:
    await bus_with_mocks.shutdown(timeout=1.0)
    assert calls == [
        "consumer.stop_gracefully",
        "consumer.drain_in_flight",
        "connection.disconnect",
    ]


@pytest.mark.asyncio
async def test_health_check_composes_slices(bus_with_mocks: RabbitMQEventBus) -> None:
    result = await bus_with_mocks.health_check()

    assert isinstance(result, HealthCheckResult)
    assert result.healthy is True
    assert result.connection_status == "connected"
    assert result.channel_status == "open"
    assert result.queue_status == "accessible"
    assert result.dlq_status == "accessible"
    assert result.error is None

    bus_with_mocks._connection_manager.health_slice.assert_called_once()
    assert bus_with_mocks._topology.queue_health.await_count == 2


@pytest.mark.asyncio
async def test_health_check_reflects_unhealthy_connection_slice(
    bus_with_mocks: RabbitMQEventBus,
) -> None:
    bus_with_mocks._connection_manager.health_slice = MagicMock(
        return_value={
            "healthy": False,
            "connection_status": "disconnected",
            "channel_status": "not_initialized",
            "errors": ["Not connected to RabbitMQ"],
        }
    )

    result = await bus_with_mocks.health_check()
    assert result.healthy is False
    assert result.connection_status == "disconnected"
    assert result.error == "Not connected to RabbitMQ"


@pytest.mark.asyncio
async def test_health_check_queue_race_reports_error_not_accessible(
    bus_with_mocks: RabbitMQEventBus,
) -> None:
    bus_with_mocks._topology.queue_health = AsyncMock(return_value=None)
    result = await bus_with_mocks.health_check()

    assert result.queue_status.startswith("error:")
    assert result.healthy is False
    assert result.error is not None
    assert "Queue check failed" in result.error


@pytest.mark.asyncio
async def test_health_check_dlq_race_reports_error_not_accessible(
    bus_with_mocks: RabbitMQEventBus,
) -> None:
    bus_with_mocks._topology.queue_health = AsyncMock(return_value=None)
    result = await bus_with_mocks.health_check()

    assert result.dlq_status is not None
    assert result.dlq_status.startswith("error:")


def test_stats_accumulate_across_collaborators(bus_with_mocks: RabbitMQEventBus) -> None:
    stats = bus_with_mocks.get_stats()
    assert bus_with_mocks._stats is stats

    stats.events_published += 1
    assert bus_with_mocks.get_stats().events_published == 1
    assert bus_with_mocks.stats.events_published == 1
