"""Broker connect-failure logs must not carry the broker URL or its password.

The fake broker client echoes the URL in its exception text, as some client
libraries do. The adapter must log without that text.
"""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusConfig
from eventsource.adapters.redis.bus import RedisEventBus
from eventsource.adapters.redis.config import RedisEventBusConfig

# Built at runtime so secret scanners do not flag a fake literal. The test only
# needs a distinctive value to search the logs for.
PASSWORD = "-".join(("ledger", "fixture", "value", "7f3a"))
AMQP_URL = f"amqp://ledger:{PASSWORD}@broker.internal:5672/"
REDIS_URL = f"redis://:{PASSWORD}@cache.internal:6379/0"


def _assert_password_absent(caplog: pytest.LogCaptureFixture) -> None:
    assert caplog.records, "expected a failure log record"
    for record in caplog.records:
        assert PASSWORD not in record.getMessage(), record.getMessage()
        assert PASSWORD not in repr(record.args)
        assert PASSWORD not in repr(record.__dict__)


async def test_rabbitmq_connect_failure_log_redacts_url(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="eventsource")
    config = RabbitMQEventBusConfig(rabbitmq_url=AMQP_URL)
    bus = RabbitMQEventBus(config=config)

    with (
        patch("eventsource.adapters.rabbitmq.connection.aio_pika") as aio_pika,
        pytest.raises(ConnectionError),
    ):
        aio_pika.connect_robust = AsyncMock(
            side_effect=ConnectionError(f"could not reach {AMQP_URL}")
        )
        await bus.connect()

    assert any("Failed to connect to RabbitMQ" in r.getMessage() for r in caplog.records)
    _assert_password_absent(caplog)


async def test_redis_connect_failure_log_redacts_url(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.DEBUG, logger="eventsource")
    bus = RedisEventBus(config=RedisEventBusConfig(redis_url=REDIS_URL))
    client_factory = MagicMock()
    client_factory.from_url = AsyncMock(side_effect=ConnectionError(f"bad url {REDIS_URL}"))

    with (
        patch(
            "eventsource.adapters.redis.bus_connection._get_aioredis",
            return_value=client_factory,
        ),
        pytest.raises(ConnectionError),
    ):
        await bus.connect()

    assert any("Failed to connect to Redis" in r.getMessage() for r in caplog.records)
    _assert_password_absent(caplog)
