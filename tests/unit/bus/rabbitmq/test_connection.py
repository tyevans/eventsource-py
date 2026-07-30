"""Unit tests for RabbitMQConnectionManager (bus god-class decomposition, Task 4)."""

from __future__ import annotations

from unittest import mock

import pytest

from eventsource.bus.rabbitmq.config import RabbitMQEventBusConfig
from eventsource.bus.rabbitmq.connection import RabbitMQConnectionManager
from eventsource.bus.rabbitmq.models import RabbitMQEventBusStats


@pytest.fixture
def manager() -> RabbitMQConnectionManager:
    return RabbitMQConnectionManager(
        config=RabbitMQEventBusConfig(rabbitmq_url="amqp://guest:guest@localhost:5672/"),
        stats=RabbitMQEventBusStats(),
    )


def _fake_connection() -> mock.AsyncMock:
    fake_channel = mock.AsyncMock()
    fake_channel.is_closed = False
    fake_channel.close_callbacks = mock.MagicMock()
    fake_conn = mock.AsyncMock()
    fake_conn.is_closed = False
    fake_conn.channel = mock.AsyncMock(return_value=fake_channel)
    fake_conn.reconnect_callbacks = mock.MagicMock()
    fake_conn.close_callbacks = mock.MagicMock()
    return fake_conn


@pytest.mark.asyncio
async def test_connect_sets_connected_and_channel(manager: RabbitMQConnectionManager) -> None:
    fake_conn = _fake_connection()
    with mock.patch(
        "eventsource.bus.rabbitmq.connection.aio_pika.connect_robust",
        new=mock.AsyncMock(return_value=fake_conn),
    ):
        await manager.connect()

    assert manager.is_connected
    assert manager.channel is fake_conn.channel.return_value
    assert manager.connection is fake_conn


@pytest.mark.asyncio
async def test_connect_is_idempotent_under_lock(manager: RabbitMQConnectionManager) -> None:
    fake_conn = _fake_connection()
    mock_connect_robust = mock.AsyncMock(return_value=fake_conn)
    with mock.patch(
        "eventsource.bus.rabbitmq.connection.aio_pika.connect_robust",
        new=mock_connect_robust,
    ):
        await manager.connect()
        await manager.connect()

    mock_connect_robust.assert_awaited_once()


def test_require_channel_raises_before_connect(manager: RabbitMQConnectionManager) -> None:
    with pytest.raises(RuntimeError):
        manager.require_channel()


@pytest.mark.asyncio
async def test_require_channel_returns_channel_after_connect(
    manager: RabbitMQConnectionManager,
) -> None:
    fake_conn = _fake_connection()
    with mock.patch(
        "eventsource.bus.rabbitmq.connection.aio_pika.connect_robust",
        new=mock.AsyncMock(return_value=fake_conn),
    ):
        await manager.connect()

    assert manager.require_channel() is manager.channel


@pytest.mark.asyncio
async def test_reconnect_callbacks_fire_in_order(manager: RabbitMQConnectionManager) -> None:
    calls: list[str] = []

    async def cb_a() -> None:
        calls.append("a")

    async def cb_b() -> None:
        calls.append("b")

    manager.on_reconnect(cb_a)
    manager.on_reconnect(cb_b)
    await manager._run_reconnect_callbacks()  # the internal hook the aio-pika callback awaits

    assert calls == ["a", "b"]


@pytest.mark.asyncio
async def test_on_reconnect_recreates_channel_and_runs_callbacks(
    manager: RabbitMQConnectionManager,
) -> None:
    calls: list[str] = []

    async def cb() -> None:
        calls.append("cb")

    manager.on_reconnect(cb)

    mock_connection = mock.AsyncMock()
    mock_connection.is_closed = False
    mock_channel = mock.AsyncMock()
    mock_channel.close_callbacks = mock.MagicMock()
    mock_connection.channel = mock.AsyncMock(return_value=mock_channel)
    manager._connection = mock_connection

    await manager._on_reconnect(mock_connection)

    assert calls == ["cb"]
    assert manager.is_connected
    assert manager.is_reconnecting is False
    assert manager.channel is mock_channel


@pytest.mark.asyncio
async def test_on_reconnect_sets_disconnected_on_failure(
    manager: RabbitMQConnectionManager,
) -> None:
    mock_connection = mock.AsyncMock()
    mock_connection.channel = mock.AsyncMock(side_effect=Exception("boom"))

    await manager._on_reconnect(mock_connection)

    assert manager.is_connected is False
    assert manager.is_reconnecting is False


@pytest.mark.asyncio
async def test_disconnect_clears_state_and_is_idempotent(
    manager: RabbitMQConnectionManager,
) -> None:
    fake_conn = _fake_connection()
    with mock.patch(
        "eventsource.bus.rabbitmq.connection.aio_pika.connect_robust",
        new=mock.AsyncMock(return_value=fake_conn),
    ):
        await manager.connect()

    await manager.disconnect()
    assert manager.is_connected is False
    assert manager.channel is None
    assert manager.connection is None

    # Safe to call twice
    await manager.disconnect()
    assert manager.is_connected is False


@pytest.mark.asyncio
async def test_force_disconnect_suppresses_errors_and_clears_state(
    manager: RabbitMQConnectionManager,
) -> None:
    fake_conn = _fake_connection()
    with mock.patch(
        "eventsource.bus.rabbitmq.connection.aio_pika.connect_robust",
        new=mock.AsyncMock(return_value=fake_conn),
    ):
        await manager.connect()

    manager._channel.close = mock.AsyncMock(side_effect=Exception("close failed"))  # type: ignore[union-attr]
    manager._connection.close = mock.AsyncMock(side_effect=Exception("close failed"))  # type: ignore[union-attr]

    await manager.force_disconnect()

    assert manager.is_connected is False
    assert manager.channel is None
    assert manager.connection is None


def test_sanitize_url_masks_credentials(manager: RabbitMQConnectionManager) -> None:
    assert manager._sanitize_url("amqp://user:pass@h:5672/") == "amqp://***:***@h:5672/"


def test_create_ssl_context_none_for_plaintext(manager: RabbitMQConnectionManager) -> None:
    assert manager._create_ssl_context() is None
