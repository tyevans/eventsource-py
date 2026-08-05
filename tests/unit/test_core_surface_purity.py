"""Importing the core surface must not drag in a database or broker driver.

`docs/core-surface.md` defines a Tier 0 boundary: the domain ring, the ports,
and the application ring resolve to stdlib and pydantic, so a future Tier 0
extraction stays cheap and a plain install stays light. The import-linter
contracts in `pyproject.toml` enforce that statically, over the import graph.

This asserts the same property *at runtime*, which is not redundant with the
static contract. import-linter reads `import` statements; it cannot see an
`importlib.import_module`, a driver a third-party package registers on import,
or a module the lazy `__getattr__` in `eventsource/__init__.py` resolves. Only
executing the import and looking at `sys.modules` covers those. This is the
generalization of the one-off check the outbox ring migration used:

    uv run python -c "import sys, eventsource.ports.outbox;
                      assert 'sqlalchemy' not in sys.modules"

The probe runs in a subprocess because the assertion is about a *clean*
interpreter: by the time pytest has collected this repo's suite, sqlalchemy is
long since in `sys.modules` and the check would pass vacuously in-process. One
subprocess covers every module -- it imports them one at a time and reports the
first that leaks, so attribution survives the batching.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap

# Modules a Tier 0 consumer imports. Package roots where the whole package is
# Tier 0, individual modules where the package is tiered per module.
CORE_SURFACE = [
    "eventsource.domain",
    "eventsource.domain.aggregate",
    "eventsource.domain.command",
    "eventsource.domain.decider",
    "eventsource.domain.decorators",
    "eventsource.domain.event",
    "eventsource.domain.event_registry",
    "eventsource.domain.exceptions",
    "eventsource.domain.types",
    "eventsource.ports",
    "eventsource.ports.checkpoints",
    "eventsource.ports.dlq",
    "eventsource.ports.handlers",
    "eventsource.ports.outbox",
    "eventsource.ports.readmodels",
    "eventsource.ports.snapshots",
    "eventsource.application",
    "eventsource.application.projections.base",
]

# Drivers whose absence *is* the Tier 0 property. sqlalchemy is a core
# dependency, so it is importable and a leak would go unnoticed; redis,
# asyncpg, aiosqlite, aiokafka and aio_pika sit behind extras and may not be
# installed at all -- "not in sys.modules" is the right assertion either way.
FORBIDDEN = ["sqlalchemy", "redis", "asyncpg", "aiosqlite", "aiokafka", "aio_pika"]

PROBE = textwrap.dedent("""
    import importlib
    import sys

    forbidden = {forbidden!r}

    def leaks():
        return sorted(
            name
            for name in forbidden
            if name in sys.modules or any(m.startswith(name + ".") for m in sys.modules)
        )

    for module in {modules!r}:
        importlib.import_module(module)
        found = leaks()
        if found:
            raise SystemExit(f"{{module}} pulled in: " + ", ".join(found))
""")


def _run_probe(modules: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-c", PROBE.format(modules=modules, forbidden=FORBIDDEN)],
        capture_output=True,
        text=True,
    )


def test_core_surface_pulls_in_no_driver() -> None:
    result = _run_probe(CORE_SURFACE)
    assert result.returncode == 0, (
        f"a core-surface module pulled in a driver it must stay free of: "
        f"{result.stdout.strip()}{result.stderr.strip()}"
    )


def test_the_probe_would_actually_fail() -> None:
    """Guard the guard: a probe that can never fail proves nothing.

    Recurring defect shape #3 -- a check whose failing branch is unreachable
    passes every test that does not assert on it. `adapters/_sql/engine.py` is
    sqlalchemy-backed by design, so the probe must reject it.
    """
    result = _run_probe(["eventsource.adapters._sql.engine"])
    assert result.returncode != 0
    assert "sqlalchemy" in result.stderr
