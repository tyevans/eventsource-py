"""Unit tests for KafkaEventBus facade composition (Task 17).

These tests exercise the facade's wiring of its collaborators
(KafkaConnectionManager, KafkaConsumerLoop, KafkaPublisher, KafkaDLQAdmin)
entirely against mocks -- no broker required. Mirrors the fixture +
shared-call-log approach of ``tests/unit/bus/rabbitmq/test_facade_wiring.py``
(Task 9).

Covers:
- ``shutdown()`` follows its documented order: signal ``_shutdown_event``,
  drain background tasks, then disconnect (which -- per the current facade
  body, which wins over the brief's sketch -- stops the consumer loop only
  if still consuming, then disconnects the connection).
- The ``KafkaEventBusStats`` instance handed to publisher/consumer/dlq_admin
  is the same object ``get_stats_dict()`` reflects.
- ``connect()`` wires observable gauges (``register_connection_gauge`` /
  ``register_consumer_lag_gauge``) only after a successful connect, and
  never as a side effect of a failed connect (Task 13 deferred item).
- Background publishes are scheduled via ``_track_background`` so
  ``get_background_task_count()`` rises while in flight, the tracked task
  actually calls the publisher, and it drains to zero via
  ``_drain_background``. A background-publish failure is logged, not raised.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

pytest.importorskip("aiokafka", reason="aiokafka not installed")

from eventsource.adapters.kafka.bus import KafkaEventBus  # noqa: E402
from eventsource.adapters.kafka.config import KafkaEventBusConfig  # noqa: E402
from eventsource.events.base import DomainEvent  # noqa: E402


class SampleEvent(DomainEvent):
    """Minimal concrete DomainEvent for publish() wiring tests."""

    event_type: str = "SampleEvent"
    aggregate_type: str = "SampleAggregate"


@pytest.fixture
def calls() -> list[str]:
    return []


@pytest.fixture
def bus_with_mocks(calls: list[str]) -> KafkaEventBus:
    """A real KafkaEventBus with its collaborators swapped for mocks.

    Each mock's relevant methods append a tag to the shared ``calls`` log so
    tests can assert relative ordering across collaborators.
    """
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

    mock_publisher = MagicMock()
    mock_dlq_admin = MagicMock()

    bus._connection_manager = mock_connection_manager
    bus._consumer_loop = mock_consumer_loop
    bus._publisher = mock_publisher
    bus._dlq_admin = mock_dlq_admin

    return bus


@pytest.mark.asyncio
async def test_shutdown_ordering(bus_with_mocks: KafkaEventBus, calls: list[str]) -> None:
    """shutdown() signals _shutdown_event, drains background tasks, then
    disconnects -- which (since the mock reports not-consuming) skips
    stop_consuming and goes straight to connection.disconnect.
    """
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
    """When still consuming at shutdown time, disconnect() stops the
    consumer loop before disconnecting the connection.
    """
    bus = bus_with_mocks
    bus._consumer_loop.is_consuming = True

    async def fake_drain(timeout: float | None = None) -> None:
        calls.append("drain_background")

    bus._drain_background = fake_drain  # type: ignore[method-assign]

    await bus.shutdown(timeout=1.0)

    assert calls == ["drain_background", "consumer_loop.stop", "connection.disconnect"]


def test_stats_identity_across_collaborators() -> None:
    """The stats object handed to publisher/consumer_loop/dlq_admin at
    __init__ time is the SAME object get_stats_dict() reflects -- checked
    against the real (unmocked) collaborators, so this would fail if
    __init__ handed each collaborator its own fresh KafkaEventBusStats.
    """
    config = KafkaEventBusConfig(bootstrap_servers="localhost:9092")
    bus = KafkaEventBus(config=config)

    assert bus._publisher._stats is bus._stats
    assert bus._consumer_loop._stats is bus._stats
    assert bus._dlq_admin._stats is bus._stats

    # A collaborator-side mutation must be visible through the facade's
    # public accessor -- not just object identity.
    bus._publisher._stats.events_published += 3
    assert bus.get_stats_dict()["events_published"] == 3
    assert bus.stats.events_published == 3


@pytest.mark.asyncio
async def test_connect_wires_metrics_after_connection_established(
    monkeypatch: pytest.MonkeyPatch, bus_with_mocks: KafkaEventBus, calls: list[str]
) -> None:
    """connect() registers the connection/lag gauges only after the
    connection manager reports connected, i.e. after producer/consumer
    startup -- mock register_connection_gauge/register_consumer_lag_gauge
    where the facade actually references them: eventsource.adapters.kafka.bus.
    """
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
    """publish(background=True) schedules delivery via _track_background so
    get_background_task_count() rises while in flight and _drain_background
    waits for it to complete.
    """
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
    # Prove the tracked task actually did the work, not just that it was
    # scheduled and drained.
    assert recorded_calls == [([event], True)]


@pytest.mark.asyncio
async def test_background_publish_failure_does_not_raise(
    bus_with_mocks: KafkaEventBus,
) -> None:
    """A send/serialization failure inside a background publish is logged
    (via the tracked task's on-done handling), not raised to the caller --
    this pins the intentional error-surfacing semantics documented on
    publish().
    """
    bus = bus_with_mocks
    bus._connection_manager.is_connected = True
    bus._connection_manager.producer = MagicMock()

    async def failing_publish_all(events: list[Any], background: bool) -> None:
        raise RuntimeError("boom")

    bus._publisher.publish_all = failing_publish_all

    event = SampleEvent(aggregate_id=uuid4())

    # Must not raise, even though the underlying publish fails.
    await bus.publish([event], background=True)

    await bus._drain_background(timeout=1.0)
    assert bus.get_background_task_count() == 0
