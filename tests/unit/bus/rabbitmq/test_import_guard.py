"""The optional-dependency guard must behave identically to the flat module."""

import importlib
import sys
from unittest import mock

import pytest


def _purge(prefix: str) -> None:
    for name in [m for m in sys.modules if m == prefix or m.startswith(prefix + ".")]:
        del sys.modules[name]


def test_rabbitmq_available_true_with_driver_installed() -> None:
    mod = importlib.import_module("eventsource.bus.rabbitmq")
    assert mod.RABBITMQ_AVAILABLE is True


def test_import_succeeds_and_flag_false_without_aio_pika() -> None:
    saved = {
        m: sys.modules[m]
        for m in list(sys.modules)
        if m == "aio_pika"
        or m.startswith("aio_pika.")
        or m == "eventsource.bus.rabbitmq"
        or m.startswith("eventsource.bus.rabbitmq.")
    }
    try:
        _purge("eventsource.bus.rabbitmq")
        _purge("aio_pika")
        with mock.patch.dict(sys.modules, {"aio_pika": None}):
            mod = importlib.import_module("eventsource.bus.rabbitmq")
            assert mod.RABBITMQ_AVAILABLE is False
            with pytest.raises(mod.RabbitMQNotAvailableError):
                mod.RabbitMQEventBus(mod.RabbitMQEventBusConfig(rabbitmq_url="amqp://x"))
    finally:
        _purge("eventsource.bus.rabbitmq")
        _purge("aio_pika")
        sys.modules.update(saved)
