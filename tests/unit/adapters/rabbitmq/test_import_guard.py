"""The optional-dependency guard must behave identically to the flat module."""

import importlib
import subprocess
import sys


def test_rabbitmq_available_true_with_driver_installed() -> None:
    mod = importlib.import_module("eventsource.adapters.rabbitmq")
    assert mod.RABBITMQ_AVAILABLE is True


def test_import_succeeds_and_flag_false_without_aio_pika() -> None:
    # Run in a fresh subprocess so the blocked-import scenario never touches
    # this test process's sys.modules -- mutating sys.modules in-process
    # (even with careful save/purge/restore) leaves stale bindings that can
    # desync from the real module objects other tests import, since the
    # package now spans several sibling modules (bus.py, config.py, models.py,
    # serialization.py) that must all resolve consistently.
    script = (
        "import sys\n"
        "sys.modules['aio_pika'] = None\n"
        "import eventsource.adapters.rabbitmq as mod\n"
        "assert mod.RABBITMQ_AVAILABLE is False, mod.RABBITMQ_AVAILABLE\n"
        "try:\n"
        "    mod.RabbitMQEventBus(mod.RabbitMQEventBusConfig(rabbitmq_url='amqp://x'))\n"
        "except mod.RabbitMQNotAvailableError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('RabbitMQNotAvailableError was not raised')\n"
        "print('OK')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "OK" in result.stdout
