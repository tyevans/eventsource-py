"""RabbitMQ config repr redaction, plaintext transport and guest warnings (TASK-0480, TASK-0490)."""

from __future__ import annotations

import logging
from unittest import mock

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBusConfig
from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
from eventsource.adapters.rabbitmq.models import RabbitMQEventBusStats

SECRET = "Sup3rSecretPw"  # pragma: allowlist secret
SECRET_URL = f"amqp://svc:{SECRET}@broker.example.com:5672/"


def _fake_channel() -> mock.AsyncMock:
    channel = mock.AsyncMock(is_closed=False)
    channel.close_callbacks = mock.MagicMock()
    return channel


def _messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


def _all_record_text(caplog: pytest.LogCaptureFixture) -> str:
    parts: list[str] = []
    for record in caplog.records:
        parts.append(record.getMessage())
        parts.append(repr(record.__dict__))
    return "\n".join(parts)


class TestRepr:
    def test_repr_hides_password(self) -> None:
        assert SECRET not in repr(RabbitMQEventBusConfig(rabbitmq_url=SECRET_URL))

    def test_repr_keeps_host_and_redacts_userinfo(self) -> None:
        text = repr(RabbitMQEventBusConfig(rabbitmq_url=SECRET_URL))
        assert "broker.example.com:5672" in text
        assert "***:***@" in text


class TestPlaintextWarning:
    def test_amqp_to_remote_host_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            RabbitMQEventBusConfig(rabbitmq_url=SECRET_URL)
        assert any(
            "plaintext transport" in m and "broker.example.com" in m for m in _messages(caplog)
        )

    @pytest.mark.parametrize(
        "url",
        [
            "amqp://u:p@localhost:5672/",
            "amqp://u:p@127.0.0.1:5672/",
            "amqp://u:p@[::1]:5672/",
        ],
    )
    def test_amqp_to_loopback_does_not_warn(
        self, url: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            RabbitMQEventBusConfig(rabbitmq_url=url)
        assert not any("plaintext" in m for m in _messages(caplog))

    def test_amqps_to_remote_host_does_not_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            RabbitMQEventBusConfig(rabbitmq_url="amqps://u:p@broker.example.com:5671/")
        assert not any("plaintext" in m for m in _messages(caplog))


class TestGuestWarning:
    def test_default_url_is_kept_and_loopback_is_silent(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        with caplog.at_level(logging.WARNING):
            config = RabbitMQEventBusConfig()
        assert config.rabbitmq_url == "amqp://guest:guest@localhost:5672/"
        assert not any("guest" in m for m in _messages(caplog))

    def test_guest_on_remote_host_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            RabbitMQEventBusConfig(rabbitmq_url="amqps://guest:guest@broker.example.com:5671/")
        assert any("'guest' user" in m for m in _messages(caplog))

    async def test_connect_as_guest_to_remote_host_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = RabbitMQEventBusConfig(rabbitmq_url="amqps://guest:guest@broker.example.com:5671/")
        manager = RabbitMQConnectionManager(config=config, stats=RabbitMQEventBusStats())
        fake_conn = mock.AsyncMock(is_closed=False)
        fake_conn.channel = mock.AsyncMock(return_value=_fake_channel())
        fake_conn.reconnect_callbacks = mock.MagicMock()
        fake_conn.close_callbacks = mock.MagicMock()
        with (
            mock.patch(
                "eventsource.adapters.rabbitmq.connection.aio_pika.connect_robust",
                new=mock.AsyncMock(return_value=fake_conn),
            ),
            caplog.at_level(logging.WARNING),
        ):
            await manager.connect()
        assert any("'guest' user" in m for m in _messages(caplog))
        await manager.disconnect()


class TestConnectLogsRedact:
    async def test_connect_logs_never_contain_password(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = RabbitMQEventBusConfig(rabbitmq_url=SECRET_URL)
        manager = RabbitMQConnectionManager(config=config, stats=RabbitMQEventBusStats())
        fake_conn = mock.AsyncMock(is_closed=False)
        fake_conn.channel = mock.AsyncMock(return_value=_fake_channel())
        fake_conn.reconnect_callbacks = mock.MagicMock()
        fake_conn.close_callbacks = mock.MagicMock()
        with (
            mock.patch(
                "eventsource.adapters.rabbitmq.connection.aio_pika.connect_robust",
                new=mock.AsyncMock(return_value=fake_conn),
            ),
            caplog.at_level(logging.DEBUG),
        ):
            await manager.connect()
        assert "Connected to RabbitMQ" in caplog.text
        assert SECRET not in _all_record_text(caplog)
        await manager.disconnect()
