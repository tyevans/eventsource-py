"""The optional-dependency guard must behave identically to the flat module."""

import importlib
import subprocess
import sys


def test_kafka_available_true_with_driver_installed() -> None:
    mod = importlib.import_module("eventsource.bus.kafka")
    assert mod.KAFKA_AVAILABLE is True


def test_import_succeeds_and_flag_false_without_aiokafka() -> None:
    # Run in a fresh subprocess so the blocked-import scenario never touches
    # this test process's sys.modules -- mutating sys.modules in-process
    # (even with careful save/purge/restore) leaves stale bindings that can
    # desync from the real module objects other tests import, since the
    # package now spans several sibling modules (bus.py and, in a later
    # refactor, connection.py) that must all resolve consistently.
    script = (
        "import sys\n"
        "sys.modules['aiokafka'] = None\n"
        "import eventsource.bus.kafka as mod\n"
        "assert mod.KAFKA_AVAILABLE is False, mod.KAFKA_AVAILABLE\n"
        "try:\n"
        "    mod.KafkaEventBus(mod.KafkaEventBusConfig())\n"
        "except mod.KafkaNotAvailableError:\n"
        "    pass\n"
        "else:\n"
        "    raise AssertionError('KafkaNotAvailableError was not raised')\n"
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
