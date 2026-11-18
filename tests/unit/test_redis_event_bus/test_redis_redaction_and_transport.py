"""Redis config repr, connect-log redaction and plaintext transport warning (TASK-0480, TASK-0490)."""

from __future__ import annotations

import logging
from unittest.mock import AsyncMock, patch

import pytest

from eventsource.adapters.redis.bus import RedisEventBus, RedisEventBusConfig

SECRET = "R3disSecretPw"  # pragma: allowlist secret
SECRET_URL = f"redis://:{SECRET}@cache.example.com:6379/0"


def _fake_client() -> AsyncMock:
    client = AsyncMock()
    client.ping = AsyncMock(return_value=True)
    client.xgroup_create = AsyncMock(return_value=True)
    return client


def _warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.levelno >= logging.WARNING]


class TestRedisConfigRepr:
    def test_repr_hides_password(self) -> None:
        assert SECRET not in repr(RedisEventBusConfig(redis_url=SECRET_URL))

    def test_repr_keeps_host(self) -> None:
        assert "cache.example.com:6379" in repr(RedisEventBusConfig(redis_url=SECRET_URL))


class TestRedisPlaintextWarning:
    def test_redis_to_remote_host_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            RedisEventBusConfig(redis_url=SECRET_URL)
        assert any(
            "plaintext transport" in m and "cache.example.com" in m for m in _warnings(caplog)
        )

    def test_default_loopback_url_does_not_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            RedisEventBusConfig()
        assert not any("plaintext" in m for m in _warnings(caplog))

    def test_rediss_to_remote_host_does_not_warn(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.WARNING):
            RedisEventBusConfig(redis_url="rediss://:pw@cache.example.com:6380/0")
        assert not any("plaintext" in m for m in _warnings(caplog))


class TestRedisConnectLogsRedact:
    async def test_connect_log_records_never_contain_password(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        bus = RedisEventBus(config=RedisEventBusConfig(redis_url=SECRET_URL, enable_tracing=False))
        client = _fake_client()
        with (
            patch("eventsource.adapters.redis.bus.REDIS_AVAILABLE", True),
            patch("eventsource.adapters.redis.bus.aioredis") as fake_module,
            caplog.at_level(logging.DEBUG),
        ):
            fake_module.from_url = AsyncMock(return_value=client)
            await bus.connect()
        assert any(r.getMessage() == "Connected to Redis" for r in caplog.records)
        for record in caplog.records:
            assert SECRET not in record.getMessage()
            assert SECRET not in repr(record.__dict__)
        await bus.disconnect()
