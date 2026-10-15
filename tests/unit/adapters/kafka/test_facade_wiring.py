"""Unit tests for KafkaEventBus facade composition (Task 17)."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.adapters.kafka.bus import KafkaEventBus  # noqa: E402
from eventsource.adapters.kafka.config import KafkaEventBusConfig  # noqa: E402
from eventsource.domain.event import DomainEvent  # noqa: E402


class SampleEvent(DomainEvent):
    """Minimal concrete DomainEvent for publish() wiring tests."""

    aggregate_type: str = "SampleAggregate"


@pytest.fixture
def calls() -> list[str]:
    return []


@pytest.fixture
def bus_with_mocks(calls: list[str]) -> KafkaEventBus:
    """A real KafkaEventBus with its collaborators swapped for mocks."""
    config = KafkaEventBusConfig(bootstrap_servers="localhost:9092")
    bus = KafkaEventBus(config=config)

    mock_connection_manager = MagicMock()
    mock_connection_manager.connect = AsyncMock(
        side_effect=lambda: calls.append("connection.connect")
    )
    mock_connection_manager.disconnect = AsyncMock(
        side_effect=lambda: calls.append("connection.disconnect")
    )
    mock_connection_manager.is_connected = True
    mock_connection_manager.producer = MagicMock()
    mock_connection_manager.consumer = MagicMock()
    mock_connection_manager.metrics = None

    mock_consumer_loop = MagicMock()
    mock_consumer_loop.stop = AsyncMock(side_effect=lambda: calls.append("consumer_loop.stop"))
    mock_consumer_loop.is_consuming = False
    mock_consumer_loop._consume_task = None

    bus._connection_manager = mock_connection_manager
    bus._consumer_loop = mock_consumer_loop
    bus._publisher = MagicMock()
    bus._dlq_admin = MagicMock()
    return bus


@pytest.mark.asyncio
async def test_shutdown_ordering(bus_with_mocks: KafkaEventBus, calls: list[str]) -> None:
    """shutdown() signals _shutdown_event, drains background tasks, then disconnects."""
    bus = bus_with_mocks
    assert not bus._shutdown_event.is_set()
    drained: list[str] = []

    async def fake_drain(timeout: float | None = None) -> None:
        drained.append("drain_background")
        calls.append("drain_background")

    bus._drain_background = fake_drain  # type: ignore[method-assign]
    await bus.shutdown(timeout=1.0)
    assert bus._shutdown_event.is_set()
    assert calls == ["drain_background", "connection.disconnect"]


@pytest.mark.asyncio
async def test_shutdown_stops_consumer_loop_when_still_consuming(
    bus_with_mocks: KafkaEventBus, calls: list[str]
) -> None:
    """When still consuming at shutdown time, disconnect() stops consumer loop first."""
    bus = bus_with_mocks
    bus._consumer_loop.is_consuming = True

    async def fake_drain(timeout: float | None = None) -> None:
        calls.append("drain_background")

    bus._drain_background = fake_drain  # type: ignore[method-assign]
    await bus.shutdown(timeout=1.0)
    assert calls == ["drain_background", "consumer_loop.stop", "connection.disconnect"]


def test_stats_identity_across_collaborators() -> None:
    """Collaborators share the same KafkaEventBusStats reflected by get_stats_dict()."""
    config = KafkaEventBusConfig(bootstrap_servers="localhost:9092")
    bus = KafkaEventBus(config=config)

    assert bus._publisher._stats is bus._stats
    assert bus._consumer_loop._stats is bus._stats
    assert bus._dlq_admin._stats is bus._stats

    bus._publisher._stats.events_published += 3
    assert bus.get_stats_dict()["events_published"] == 3
    assert bus.stats.events_published == 3


@pytest.mark.asyncio
async def test_connect_wires_metrics_after_connection_established(
    monkeypatch: pytest.MonkeyPatch, bus_with_mocks: KafkaEventBus, calls: list[str]
) -> None:
    """connect() registers gauges only after connection manager reports connected."""
    bus = bus_with_mocks
    bus._meter = MagicMock()
    bus._connection_manager.metrics = MagicMock()

    mock_register_conn = MagicMock(
        side_effect=lambda *a, **k: calls.append("register_connection_gauge") or True
    )
    mock_register_lag = MagicMock(
        side_effect=lambda *a, **k: calls.append("register_consumer_lag_gauge") or True
    )
    monkeypatch.setattr(
        "eventsource.adapters.kafka.bus.register_connection_gauge", mock_register_conn
    )
    monkeypatch.setattr(
        "eventsource.adapters.kafka.bus.register_consumer_lag_gauge", mock_register_lag
    )

    await bus.connect()

    assert calls == [
        "connection.connect",
        "register_connection_gauge",
        "register_consumer_lag_gauge",
    ]
    mock_register_conn.assert_called_once()
    mock_register_lag.assert_called_once()


@pytest.mark.asyncio
async def test_connect_does_not_wire_metrics_when_connect_fails(
    bus_with_mocks: KafkaEventBus,
) -> None:
    """_wire_metrics must never fire as a side effect of a failed connect."""
    bus = bus_with_mocks
    bus._connection_manager.connect = AsyncMock()
    bus._connection_manager.is_connected = False

    wired: list[bool] = []
    bus._wire_metrics = lambda: wired.append(True)  # type: ignore[method-assign]
    await bus.connect()
    assert wired == []


@pytest.mark.asyncio
async def test_background_publish_tracked_and_drained(bus_with_mocks: KafkaEventBus) -> None:
    """publish(background=True) tracks background tasks and drains them on completion."""
    bus = bus_with_mocks
    bus._connection_manager.is_connected = True
    bus._connection_manager.producer = MagicMock()

    release = asyncio.Event()
    recorded_calls: list[tuple[list[Any], bool]] = []

    async def fake_publish_all(events: list[Any], background: bool) -> None:
        recorded_calls.append((events, background))
        await release.wait()

    bus._publisher.publish_all = fake_publish_all
    event = SampleEvent(aggregate_id=uuid4())

    await bus.publish([event], background=True)
    assert bus.get_background_task_count() == 1

    release.set()
    await bus._drain_background(timeout=1.0)
    assert bus.get_background_task_count() == 0
    assert recorded_calls == [([event], True)]


@pytest.mark.asyncio
async def test_background_publish_failure_does_not_raise(
    bus_with_mocks: KafkaEventBus,
) -> None:
    """A failure inside a background publish is logged, not raised to the caller."""
    bus = bus_with_mocks
    bus._connection_manager.is_connected = True
    bus._connection_manager.producer = MagicMock()

    async def failing_publish_all(events: list[Any], background: bool) -> None:
        raise RuntimeError("boom")

    bus._publisher.publish_all = failing_publish_all
    event = SampleEvent(aggregate_id=uuid4())

    await bus.publish([event], background=True)
    await bus._drain_background(timeout=1.0)
    assert bus.get_background_task_count() == 0
