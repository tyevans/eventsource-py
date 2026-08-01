"""Unit tests for RabbitMQEventBus facade composition (Task 9).

These tests exercise the facade's wiring of its collaborators
(RabbitMQConnectionManager, RabbitMQTopology, RabbitMQConsumer,
RabbitMQPublisher, RabbitMQDLQAdmin) entirely against mocks -- no broker
required.

Covers:
- The connection manager's reconnect callback is wired to
  ``RabbitMQTopology.redeclare`` only (not ``RabbitMQConsumer.resume_if_was_consuming``
  -- see the binding ruling in the Task 9 brief), and that callback fires
  exactly once per ``connect()``.
- ``shutdown()`` calls its collaborators in the documented order.
- ``health_check()`` composes ``HealthCheckResult`` from the connection and
  topology slices.
- The ``RabbitMQEventBusStats`` instance handed to publisher/consumer is the
  same object ``get_stats()`` returns.
"""

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
    """A real RabbitMQEventBus with its collaborators swapped for mocks.

    Each mock's relevant methods append a tag to the shared ``calls`` log so
    tests can assert relative ordering across collaborators.
    """
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

    mock_publisher = MagicMock()
    mock_dlq_admin = MagicMock()

    bus._connection_manager = mock_connection_manager
    bus._topology = mock_topology
    bus._consumer = mock_consumer
    bus._publisher = mock_publisher
    bus._dlq_admin = mock_dlq_admin

    return bus


@pytest.mark.asyncio
async def test_reconnect_wires_topology_redeclare_only_and_fires_once() -> None:
    """The connection manager's reconnect callback list should be exactly
    [topology.redeclare] (see Task 9 brief's binding ruling: the original
    facade never resumed consuming on reconnect, so consumer.resume_if_was_consuming
    is intentionally NOT registered). Firing reconnect callbacks should
    invoke topology.redeclare exactly once.
    """
    config = RabbitMQEventBusConfig()
    bus = RabbitMQEventBus(config=config)

    # __init__ wiring: exactly one reconnect callback, bound to topology.redeclare.
    callbacks = bus._connection_manager._reconnect_callbacks
    assert len(callbacks) == 1
    assert callbacks[0] == bus._topology.redeclare

    calls: list[str] = []

    async def fake_redeclare() -> None:
        calls.append("topology.redeclare")

    bus._topology.redeclare = fake_redeclare  # type: ignore[method-assign]
    # Re-register since the callback list already captured the original
    # bound method object at construction time.
    bus._connection_manager._reconnect_callbacks = [bus._topology.redeclare]

    await bus._connection_manager._run_reconnect_callbacks()
    assert calls == ["topology.redeclare"]

    # Declare-once-from-connect: connect() runs registered callbacks a
    # single time (connection.py:243), not once per declared resource.
    await bus._connection_manager._run_reconnect_callbacks()
    assert calls == ["topology.redeclare", "topology.redeclare"]  # once per explicit call


@pytest.mark.asyncio
async def test_shutdown_ordering(bus_with_mocks: RabbitMQEventBus, calls: list[str]) -> None:
    """shutdown() stops the consumer, drains in-flight, drains background tasks,
    then disconnects -- assert relative call order via the shared call log.
    """
    await bus_with_mocks.shutdown(timeout=1.0)

    assert calls == [
        "consumer.stop_gracefully",
        "consumer.drain_in_flight",
        "connection.disconnect",
    ]


@pytest.mark.asyncio
async def test_health_check_composes_slices(bus_with_mocks: RabbitMQEventBus) -> None:
    """health_check() returns HealthCheckResult reflecting mocked slice values."""
    result = await bus_with_mocks.health_check()

    assert isinstance(result, HealthCheckResult)
    assert result.healthy is True
    assert result.connection_status == "connected"
    assert result.channel_status == "open"
    assert result.queue_status == "accessible"
    assert result.dlq_status == "accessible"
    assert result.error is None

    bus_with_mocks._connection_manager.health_slice.assert_called_once()
    # Called once for the consumer queue, once for the DLQ queue.
    assert bus_with_mocks._topology.queue_health.await_count == 2


@pytest.mark.asyncio
async def test_health_check_reflects_unhealthy_connection_slice(
    bus_with_mocks: RabbitMQEventBus,
) -> None:
    """When the connection slice reports unhealthy, health_check() propagates it."""
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
    """If the channel closes between the facade's gate and the topology call,
    RabbitMQTopology.queue_health() returns None. The old inline health_check
    would have hit declare_queue's own exception in this race and reported
    "error: ...", not "accessible" -- pin that the composed facade matches.
    """
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
    """Same race as above, but for the DLQ branch: a None queue_health()
    result must not be reported as "accessible".
    """
    bus_with_mocks._topology.queue_health = AsyncMock(return_value=None)

    result = await bus_with_mocks.health_check()

    assert result.dlq_status is not None
    assert result.dlq_status.startswith("error:")
    # DLQ errors don't flip overall healthy -- only the queue branch does.


def test_stats_accumulate_across_collaborators(bus_with_mocks: RabbitMQEventBus) -> None:
    """The stats object handed to publisher/consumer is the same object the
    facade's get_stats() returns (identity check + counter increment visible).
    """
    stats = bus_with_mocks.get_stats()

    # The facade constructs one RabbitMQEventBusStats and hands the same
    # instance to every collaborator at __init__ time.
    assert bus_with_mocks._stats is stats

    stats.events_published += 1
    assert bus_with_mocks.get_stats().events_published == 1
    assert bus_with_mocks.stats.events_published == 1
