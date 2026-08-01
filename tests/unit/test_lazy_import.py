"""PEP 562 lazy front door tests for `eventsource/__init__.py`.

`import eventsource` must not eagerly import any adapter or driver
(sqlalchemy, asyncpg, aiosqlite, redis, aiokafka, aio-pika). Each public
name is resolved on first attribute access via `__getattr__`. The
subprocess-based tests below need a fresh interpreter -- the pytest
process itself has already imported half the world by the time these
tests run, so `sys.modules` cannot be inspected in-process for this.
"""

import subprocess
import sys

import pytest

import eventsource


def test_bare_import_does_not_load_sqlalchemy_or_adapters() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import eventsource, sys\n"
            "assert 'sqlalchemy' not in sys.modules, 'sqlalchemy loaded eagerly'\n"
            "assert 'eventsource.adapters' not in sys.modules, 'adapters loaded eagerly'\n",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_accessing_postgresql_event_store_loads_sqlalchemy() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import eventsource, sys\n"
            "eventsource.PostgreSQLEventStore\n"
            "assert 'sqlalchemy' in sys.modules, 'lazy load did not import sqlalchemy'\n",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_every_all_name_resolves_via_getattr_and_appears_in_dir() -> None:
    d = dir(eventsource)
    for name in eventsource.__all__:
        getattr(eventsource, name)  # must not raise
        assert name in d, f"eventsource.{name} missing from dir(eventsource)"


def test_unknown_attribute_raises_attribute_error() -> None:
    name = "nonexistent_name"
    with pytest.raises(AttributeError):
        getattr(eventsource, name)


def test_lazy_mapping_covers_every_dunder_all_name() -> None:
    """Every name in __all__ (besides __version__) must have exactly one
    entry in the mechanically-built _LAZY table."""
    all_names = set(eventsource.__all__) - {"__version__"}
    lazy_names = set(eventsource._LAZY.keys())
    assert all_names <= lazy_names, all_names - lazy_names
