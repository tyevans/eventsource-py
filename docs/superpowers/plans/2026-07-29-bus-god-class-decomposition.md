# Broker Backend God-Class Decomposition Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Decompose `RabbitMQEventBus` (~3,570-line class) and `KafkaEventBus` (~2,195-line class) into backend subpackages of state-owning collaborators behind unchanged facades, shipped as 0.7.0.

**Architecture:** Each backend flat module becomes a package (`bus/rabbitmq/`, `bus/kafka/`) whose `__init__.py` re-exports everything the old module exported. Collaborators (ConnectionManager, Topology, Publisher, Consumer, DLQAdmin, plus pure serialization/death-header modules) own their state; the `*EventBus` class becomes a facade that composes them and keeps every public signature. Extraction is leaf-first: package conversion → pure modules → stateful collaborators → facade slim-down.

**Tech Stack:** Python 3.11+, pydantic v2, aio-pika (RabbitMQ), aiokafka (Kafka), pytest + pytest-asyncio, hypothesis, mutmut, uv.

**Spec:** `docs/superpowers/specs/2026-07-29-bus-god-class-decomposition-design.md` — read the spec section named in a task before implementing it.

## Global Constraints

- Target version **0.7.0**. Public breakage limited to exactly three Kafka items: remove `get_handlers_for_event`; deprecate `record_reconnection`/`record_rebalance` (warn + delegate shims, removal slated for 0.8.0); `KafkaRebalanceListener` moves to `bus/kafka/connection.py` (still re-exported from the package `__init__`).
- Every other public method/attribute on `RabbitMQEventBus` and `KafkaEventBus` keeps its **exact signature and behavior**.
- Collaborators are internal: **nothing new exported** from `src/eventsource/bus/__init__.py` or `src/eventsource/__init__.py`.
- Package `__init__.py` re-exports **everything the old flat module's `__all__` listed** — all existing imports keep working verbatim.
- The 9-test `EventBusConformanceSuite` subclasses and all public-API integration tests pass **unmodified**. Only tests that pin private internals may change.
- ADR 0011 semantics bit-for-bit: consume paths run ALL handlers, aggregate failures into `HandlerDispatchError`, withhold ack on failure; single-failure unwrap before DLQ error recording is preserved.
- Optional-dependency guard behavior identical, including `*_AVAILABLE = False` fallback when the driver is missing.
- Old flat modules `bus/rabbitmq.py` / `bus/kafka.py` are deleted in their conversion tasks; the packages take their import paths.
- Every task must leave `uv run mypy src/eventsource/ --config-file=pyproject.toml`, `uv run ruff check src/ tests/`, and the task's targeted unit tests green. mypy is strict — annotate everything.
- Test dirs contain `__init__.py` files (project convention) — create one in each new test directory.
- Async tests use `@pytest.mark.asyncio`. Integration tests for brokers carry `@pytest.mark.rabbitmq` / `@pytest.mark.kafka` markers and need Docker — the orchestrator runs those; implementers run unit tests only unless the task says otherwise.
- Commit format: `type: subject` (see `.claude/rules/commits.md`), e.g. `refactor: extract rabbitmq topology collaborator`.

---

### Task 1: Convert `bus/rabbitmq.py` to a package

**Files:**
- Move: `src/eventsource/bus/rabbitmq.py` → `src/eventsource/bus/rabbitmq/bus.py`
- Create: `src/eventsource/bus/rabbitmq/__init__.py`
- Create: `tests/unit/bus/rabbitmq/__init__.py`
- Test: `tests/unit/bus/rabbitmq/test_import_guard.py`

**Interfaces:**
- Consumes: existing flat module; its `__all__` (at `rabbitmq.py:4216-4228`) lists `BatchPublishError, DLQMessage, HealthCheckResult, OTEL_AVAILABLE, QueueInfo, RabbitMQEventBus, RabbitMQEventBusConfig, RabbitMQEventBusStats, RabbitMQNotAvailableError, RABBITMQ_AVAILABLE, ShutdownError`.
- Produces: package `eventsource.bus.rabbitmq` with identical import surface. All later Rabbit tasks split files out of `bus/rabbitmq/bus.py`.

- [ ] **Step 1: Move the module into a package**

```bash
mkdir -p src/eventsource/bus/rabbitmq
git mv src/eventsource/bus/rabbitmq.py src/eventsource/bus/rabbitmq/bus.py
```

- [ ] **Step 2: Write the package `__init__.py`**

```python
"""RabbitMQ event bus backend.

Public import surface is identical to the pre-0.7.0 flat module
``eventsource.bus.rabbitmq``. Internal collaborators live in sibling
modules and are not part of the public API.
"""

from eventsource.bus.rabbitmq.bus import (
    OTEL_AVAILABLE,
    RABBITMQ_AVAILABLE,
    BatchPublishError,
    DLQMessage,
    HealthCheckResult,
    QueueInfo,
    RabbitMQEventBus,
    RabbitMQEventBusConfig,
    RabbitMQEventBusStats,
    RabbitMQNotAvailableError,
    ShutdownError,
)

__all__ = [
    "BatchPublishError",
    "DLQMessage",
    "HealthCheckResult",
    "OTEL_AVAILABLE",
    "QueueInfo",
    "RabbitMQEventBus",
    "RabbitMQEventBusConfig",
    "RabbitMQEventBusStats",
    "RabbitMQNotAvailableError",
    "RABBITMQ_AVAILABLE",
    "ShutdownError",
]
```

Keep `__all__` in `bus.py` too for now (it moves out as later tasks strip the file).

- [ ] **Step 3: Write the failing import-guard test**

`tests/unit/bus/rabbitmq/test_import_guard.py` (create `tests/unit/bus/rabbitmq/__init__.py` as an empty file first):

```python
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
        if m == "aio_pika" or m.startswith("aio_pika.")
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
```

Note: `RabbitMQEventBusConfig` may require more constructor args — check its `__post_init__` (`bus.py:130-377`) and pass the minimal valid set. If constructing the config itself requires aio-pika (it should not — it is a plain dataclass), construct it before the patch.

- [ ] **Step 4: Run the new test and the existing unit suite**

```bash
uv run pytest tests/unit/bus/ -x -q
uv run pytest tests/unit/ -q
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run ruff check src/ tests/
```

Expected: all pass. If `mock.patch.dict` on `sys.modules` proves flaky with pytest plugins, an equivalent approach: insert a `importlib.abc.MetaPathFinder` that raises `ImportError` for `aio_pika` — either is acceptable; the assertion set must not change.

- [ ] **Step 5: Commit**

```bash
git add -A src/eventsource/bus/rabbitmq tests/unit/bus/rabbitmq
git commit -m "refactor: convert bus/rabbitmq.py to a package with identical import surface"
```

---

### Task 2: Extract `death_headers.py` (pure functions) with property tests

**Files:**
- Create: `src/eventsource/bus/rabbitmq/death_headers.py`
- Modify: `src/eventsource/bus/rabbitmq/bus.py` (remove the seven static method bodies, lines ~1570-1787; add aliases)
- Test: `tests/unit/bus/rabbitmq/test_death_headers.py`

**Interfaces:**
- Consumes: the seven `@staticmethod`s on `RabbitMQEventBus`: `get_death_count`, `get_first_death_queue`, `get_first_death_reason`, `get_first_death_exchange`, `get_original_routing_key`, `is_from_dlq`, `get_death_info` (`bus.py:1570-1787`). They are pure — they read message headers only.
- Produces: module-level functions in `death_headers.py` with the **same names and signatures** as the current staticmethods (copy each signature verbatim from `bus.py`, dropping only the implicit staticmethod context). `RabbitMQEventBus` keeps permanent static aliases. Task 7 (consumer) calls these functions directly.

- [ ] **Step 1: Write the test file first**

Port any existing death-header assertions from current tests (grep `get_death_count\|is_from_dlq\|get_death_info` under `tests/`) and add coverage for every function. Representative shape (adjust arg construction to the real signatures — they take a headers mapping or an `AbstractIncomingMessage`; mirror what `bus.py` does):

```python
"""Unit + property tests for the pure death-header functions."""

from hypothesis import given
from hypothesis import strategies as st

from eventsource.bus.rabbitmq import death_headers


def _headers_with_death(count: int, queue: str = "q1", reason: str = "rejected",
                        exchange: str = "ex", routing_keys: list[str] | None = None) -> dict:
    return {
        "x-death": [
            {
                "count": count,
                "queue": queue,
                "reason": reason,
                "exchange": exchange,
                "routing-keys": routing_keys or ["events.order.created"],
            }
        ]
    }


def test_death_count_zero_when_no_header() -> None:
    assert death_headers.get_death_count({}) == 0


def test_death_count_reads_first_entry() -> None:
    assert death_headers.get_death_count(_headers_with_death(3)) == 3


def test_is_from_dlq_false_without_header() -> None:
    assert death_headers.is_from_dlq({}) is False


@given(count=st.integers(min_value=0, max_value=10_000))
def test_death_count_roundtrips_any_count(count: int) -> None:
    assert death_headers.get_death_count(_headers_with_death(count)) == count


@given(queue=st.text(min_size=1, max_size=50))
def test_first_death_queue_returns_recorded_queue(queue: str) -> None:
    assert death_headers.get_first_death_queue(_headers_with_death(1, queue=queue)) == queue
```

Add one test per remaining function (`get_first_death_reason`, `get_first_death_exchange`, `get_original_routing_key`, `get_death_info`) asserting the happy path and the missing-header default. Match the real return types (`int`, `str | None`, `dict | None` — read the signatures).

- [ ] **Step 2: Run to verify failure** — `uv run pytest tests/unit/bus/rabbitmq/test_death_headers.py -q` → FAIL (module doesn't exist).

- [ ] **Step 3: Create `death_headers.py`** — move the seven bodies verbatim from `bus.py:1570-1787` into module functions (docstrings included). No behavior edits.

- [ ] **Step 4: Replace the staticmethods on the facade with permanent aliases**

In `bus.py`, delete the seven method bodies and add inside the class body:

```python
    # Permanent public aliases: the pure implementations live in death_headers.py.
    get_death_count = staticmethod(death_headers.get_death_count)
    get_first_death_queue = staticmethod(death_headers.get_first_death_queue)
    get_first_death_reason = staticmethod(death_headers.get_first_death_reason)
    get_first_death_exchange = staticmethod(death_headers.get_first_death_exchange)
    get_original_routing_key = staticmethod(death_headers.get_original_routing_key)
    is_from_dlq = staticmethod(death_headers.is_from_dlq)
    get_death_info = staticmethod(death_headers.get_death_info)
```

Update in-class callers (grep `self.get_death_count\|self.is_from_dlq` etc. in `bus.py`) — they keep working via the aliases; leave them as-is.

- [ ] **Step 5: Run tests, lint, mypy** — `uv run pytest tests/unit/bus/ -q && uv run mypy src/eventsource/ --config-file=pyproject.toml && uv run ruff check src/ tests/` → PASS.

- [ ] **Step 6: Commit** — `git commit -m "refactor: extract pure death-header functions to bus/rabbitmq/death_headers.py"`

---

### Task 3: Extract Rabbit `config.py`, `models.py`, `serialization.py`

**Files:**
- Create: `src/eventsource/bus/rabbitmq/config.py` (← `RabbitMQEventBusConfig`, `bus.py:130-377`)
- Create: `src/eventsource/bus/rabbitmq/models.py` (← `RabbitMQNotAvailableError` 110-128, `DLQMessage` 379-419, `RabbitMQEventBusStats` 420-495, `QueueInfo` 496-528, `HealthCheckResult` 529-578, `ShutdownError` 4153-4172, `BatchPublishError` 4174-4228)
- Create: `src/eventsource/bus/rabbitmq/serialization.py` (← `_get_event_field_default` 1990, `_get_routing_key` 2016, `_serialize_event` 2035, `_create_message` 2088, `_create_message_with_tracing` 2126, `_deserialize_event` 2170)
- Modify: `src/eventsource/bus/rabbitmq/bus.py`, `src/eventsource/bus/rabbitmq/__init__.py`
- Test: `tests/unit/bus/rabbitmq/test_serialization.py`; repoint the RabbitMQ half of `tests/unit/bus/test_serialization_properties.py`

**Interfaces:**
- Produces (`serialization.py` module functions — private methods become public module functions with explicit params instead of `self`):
  - `get_event_field_default(event_class: type[DomainEvent], field: str) -> Any`
  - `get_routing_key(event: DomainEvent, config: RabbitMQEventBusConfig) -> str`
  - `serialize_event(event: DomainEvent) -> bytes` (keep the exact current payload format — the Redis payload-is-authoritative property test guards the JSON shape)
  - `create_message(event: DomainEvent, config: RabbitMQEventBusConfig) -> Message`
  - `create_message_with_tracing(event: DomainEvent, config: RabbitMQEventBusConfig, tracer: Tracer | None, enable_tracing: bool) -> Message`
  - `deserialize_event(body: bytes, headers: Mapping[str, Any], resolve_event_class: Callable[[str], type[DomainEvent] | None]) -> DomainEvent`
  - Adjust these param lists to whatever the current bodies actually read off `self` (`self._config`, `self._tracer`, `self._enable_tracing`, `self._resolve_event_class`) — every `self.X` read becomes an explicit parameter; **no other logic changes**.
- Facade keeps thin private wrappers (`def _serialize_event(self, event): return serialization.serialize_event(event)`) so remaining `bus.py` call sites don't all change in this task; wrappers die naturally in Tasks 6-7.
- `config.py` / `models.py`: classes move verbatim; `bus.py` imports them; package `__init__.py` re-imports the public ones from their new homes instead of from `.bus`.

- [ ] **Step 1:** Move `RabbitMQEventBusConfig` to `config.py` and the dataclasses/exceptions to `models.py`, verbatim, carrying their imports. The aio-pika-dependent types used in signatures need the guard-safe import pattern: `models.py`/`config.py` must import **only** stdlib + eventsource types (check — the config and models should not need aio-pika; if a type hint needs it, use `TYPE_CHECKING`).
- [ ] **Step 2:** Update `bus.py` to import from the new modules; update `__init__.py` to re-export `RabbitMQEventBusConfig` from `.config` and the model classes from `.models`. Run `uv run pytest tests/unit/ -q` → PASS.
- [ ] **Step 3:** Write `tests/unit/bus/rabbitmq/test_serialization.py` — port the assertion style of `tests/unit/bus/test_serialization_properties.py` (Rabbit section) against the new module functions:

```python
import pytest
from hypothesis import given

from eventsource.bus.rabbitmq import serialization
from eventsource.bus.rabbitmq.config import RabbitMQEventBusConfig
# reuse the event strategies/fixtures test_serialization_properties.py uses


def test_roundtrip_preserves_event_identity(sample_event, config) -> None:
    body = serialization.serialize_event(sample_event)
    restored = serialization.deserialize_event(
        body, headers={}, resolve_event_class=lambda name: type(sample_event)
    )
    assert restored == sample_event
```

Drive hypothesis-based async bodies (if any) with `asyncio.run` from sync tests — `@given` on `async def` passes vacuously.

- [ ] **Step 4:** Move the six method bodies into `serialization.py` per the Interfaces block; add the thin facade wrappers. Repoint the Rabbit assertions in `tests/unit/bus/test_serialization_properties.py` at the module functions (same assertions, new call target).
- [ ] **Step 5:** `uv run pytest tests/unit/bus/ -q && uv run mypy src/eventsource/ --config-file=pyproject.toml && uv run ruff check src/ tests/` → PASS.
- [ ] **Step 6:** Commit — `git commit -m "refactor: extract rabbitmq config, models, and serialization modules"`

---

### Task 4: Extract `RabbitMQConnectionManager`

**Files:**
- Create: `src/eventsource/bus/rabbitmq/connection.py`
- Modify: `src/eventsource/bus/rabbitmq/bus.py` (`_create_ssl_context` 798, `connect` 891, `disconnect` 1006, `_sanitize_url` 1077, `_on_reconnect` 1092, `_on_connection_close` 1161, `_on_channel_close` 1205, `_force_disconnect` 4081 — bodies move; facade methods become delegations)
- Test: `tests/unit/bus/rabbitmq/test_connection.py`

**Interfaces:**
- Produces:

```python
class RabbitMQConnectionManager:
    def __init__(self, config: RabbitMQEventBusConfig, stats: RabbitMQEventBusStats) -> None: ...

    @property
    def is_connected(self) -> bool: ...
    @property
    def is_reconnecting(self) -> bool: ...
    @property
    def connection(self) -> AbstractRobustConnection | None: ...
    @property
    def channel(self) -> AbstractChannel | None: ...

    def require_channel(self) -> AbstractChannel:
        """Return the live channel or raise RuntimeError('Not connected...')
        with the same message the current code uses."""

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        """Register an async callback fired after a successful reconnect,
        in registration order."""

    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def force_disconnect(self) -> None: ...
```

- The manager owns `_connection`, `_channel`, `_connected`, `_reconnecting`, `_lock` (the connect lock), SSL context creation, URL sanitizing, and the aio-pika close/reconnect callbacks. The current `_on_reconnect` body does two things: re-establish channel state (stays here) and re-declare topology / resume consuming (becomes `for cb in self._reconnect_callbacks: await cb()`).
- Facade (`RabbitMQEventBus`) constructs the manager in `__init__`, keeps `connect()`/`disconnect()`/`is_connected` as delegations, and registers reconnect callbacks in Task 8/9 wiring. Until Tasks 5-8 land, the facade may still reach `self._connection_manager.channel` where it previously used `self._channel` — do a mechanical rename of those reads.

- [ ] **Step 1:** Write `tests/unit/bus/rabbitmq/test_connection.py` first — construct the manager with a config and stats, mock `aio_pika.connect_robust` (patch it inside `eventsource.bus.rabbitmq.connection`), and assert:
  - `connect()` sets `is_connected` and stores connection/channel from the mocked robust connection,
  - `connect()` is idempotent under the lock (call twice, one underlying connect),
  - `require_channel()` raises `RuntimeError` before connect,
  - `on_reconnect` callbacks fire in registration order when the reconnect handler runs (invoke the manager's internal reconnect handler directly with a mocked connection),
  - `disconnect()` clears state and is safe to call twice,
  - URL sanitizing masks credentials (assert `_sanitize_url("amqp://user:pass@h:5672/") == "amqp://user:***@h:5672/"` — match the current implementation's exact masking, read it first).

Full test skeleton:

```python
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


@pytest.mark.asyncio
async def test_connect_sets_connected_and_channel(manager) -> None:
    fake_channel = mock.AsyncMock()
    fake_conn = mock.AsyncMock()
    fake_conn.channel.return_value = fake_channel
    with mock.patch(
        "eventsource.bus.rabbitmq.connection.aio_pika.connect_robust",
        new=mock.AsyncMock(return_value=fake_conn),
    ):
        await manager.connect()
    assert manager.is_connected
    assert manager.channel is fake_channel


@pytest.mark.asyncio
async def test_reconnect_callbacks_fire_in_order(manager) -> None:
    calls: list[str] = []

    async def cb_a() -> None:
        calls.append("a")

    async def cb_b() -> None:
        calls.append("b")

    manager.on_reconnect(cb_a)
    manager.on_reconnect(cb_b)
    await manager._run_reconnect_callbacks()  # the internal hook the aio-pika callback awaits
    assert calls == ["a", "b"]
```

(Adapt mock plumbing to the real `connect` body — e.g. QoS setup, publisher confirms — after reading it. The assertions above are the contract; add mocks until they hold.)

- [ ] **Step 2:** Run → FAIL (module missing).
- [ ] **Step 3:** Create `connection.py`, moving the eight bodies. The reconnect handler calls `await self._run_reconnect_callbacks()` where the old body re-declared topology/resumed consuming; the old inline logic moves OUT (facade re-registers it as callbacks in this task — register two temporary closures in `RabbitMQEventBus.__init__` that call the still-facade-resident topology/consumer methods, so behavior is unchanged now and Tasks 5/7 swap the closure targets).
- [ ] **Step 4:** Facade: replace `_connection`/`_channel`/`_connected`/`_reconnecting` field uses with manager access (mechanical). Keep public `connect()`/`disconnect()` signatures delegating; `is_connected` property reads the manager.
- [ ] **Step 5:** `uv run pytest tests/unit/bus/ -q && uv run mypy src/eventsource/ --config-file=pyproject.toml && uv run ruff check src/ tests/` → PASS.
- [ ] **Step 6:** Commit — `git commit -m "refactor: extract RabbitMQConnectionManager with explicit reconnect hook"`

---

### Task 5: Extract `RabbitMQTopology`

**Files:**
- Create: `src/eventsource/bus/rabbitmq/topology.py`
- Modify: `src/eventsource/bus/rabbitmq/bus.py` (`_declare_exchange` 1247, `_declare_queue` 1293, `_bind_queue` 1330, `bind_event_type` 1389, `bind_routing_key` 1449, `_declare_dlq` 1489 move; facade keeps `bind_event_type`/`bind_routing_key` delegations)
- Test: `tests/unit/bus/rabbitmq/test_topology.py`
- Modify: `tests/integration/bus/test_rabbitmq.py` (assertions on `_exchange`/`_dlq_exchange`/`_consumer_queue`/`_dlq_queue` — repoint)

**Interfaces:**
- Produces:

```python
class RabbitMQTopology:
    def __init__(self, config: RabbitMQEventBusConfig, connection: RabbitMQConnectionManager) -> None: ...

    @property
    def exchange(self) -> AbstractExchange | None: ...
    @property
    def dlq_exchange(self) -> AbstractExchange | None: ...
    @property
    def consumer_queue(self) -> AbstractQueue | None: ...
    @property
    def dlq_queue(self) -> AbstractQueue | None: ...

    async def declare_all(self) -> None:
        """Declare exchange, consumer queue, bindings, and DLQ per config flags —
        exactly what connect() declares today, in the same order."""

    async def redeclare(self) -> None:
        """Reconnect path: re-declare everything (current _on_reconnect behavior)."""

    async def bind_event_type(self, event_type: type[DomainEvent]) -> None: ...
    async def bind_routing_key(self, routing_key: str) -> None: ...
```

- Consumes: `RabbitMQConnectionManager.require_channel()` (Task 4), `serialization.get_routing_key` if the bind path derives keys (check the current `bind_event_type` body).
- Facade wiring: `RabbitMQEventBus.__init__` creates `self._topology = RabbitMQTopology(config, self._connection_manager)`; `connect()` calls `await self._topology.declare_all()` after manager connect (same order as today); the reconnect closure from Task 4 now calls `self._topology.redeclare()`. Facade `bind_event_type`/`bind_routing_key` delegate with unchanged public signatures.

- [ ] **Step 1:** Write `tests/unit/bus/rabbitmq/test_topology.py` first: with a mocked connection manager (`require_channel` returns an `AsyncMock` channel whose `declare_exchange`/`declare_queue` return mocks), assert: `declare_all()` declares the main exchange with the configured name/type/durability; declares DLQ exchange+queue only when the config enables DLQ; `redeclare()` after `declare_all()` re-issues declarations (call count 2) and rebinding; `bind_event_type(OrderCreated)` binds the derived routing key; properties return the declared objects.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Move the six bodies into the class, swapping `self._channel` for `self._connection.require_channel()` and field writes to topology-owned fields. **Step 4:** Wire the facade per Interfaces; delete the moved fields from the facade.
- [ ] **Step 5:** Update `tests/integration/bus/test_rabbitmq.py`: every assertion on `bus._exchange`, `bus._dlq_exchange`, `bus._consumer_queue`, `bus._dlq_queue` becomes either (a) a public-surface assertion via `get_queue_info()` / `health_check()` where one exists, or (b) `bus._topology.exchange` etc. where the test genuinely verifies declaration happened. Prefer (a). `_config` assertions → `bus.config` (public property). Do not weaken what a test proves.
- [ ] **Step 6:** `uv run pytest tests/unit/bus/ -q` + mypy + ruff → PASS. (Orchestrator runs the rabbitmq integration suite after review.)
- [ ] **Step 7:** Commit — `git commit -m "refactor: extract RabbitMQTopology owning exchange and queue declaration"`

---

### Task 6: Extract `RabbitMQPublisher`

**Files:**
- Create: `src/eventsource/bus/rabbitmq/publisher.py`
- Modify: `src/eventsource/bus/rabbitmq/bus.py` (`_publish_single` 2341, `_publish_single_no_stats` 2443, `_publish_batch` 2471, `_publish_chunk_concurrent` 2588, `_publish_batch_concurrent` 2636, `_publish_batch_sequential` 2720 move; `publish` 2242 / `publish_batch` 2289 stay on the facade as thin orchestrators keeping `background` semantics + `_track_background`)
- Test: `tests/unit/bus/rabbitmq/test_publisher.py`

**Interfaces:**
- Produces:

```python
class RabbitMQPublisher:
    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
        topology: RabbitMQTopology,
        stats: RabbitMQEventBusStats,
        tracer: Tracer | None,
        enable_tracing: bool,
    ) -> None: ...

    async def publish_one(self, event: DomainEvent) -> None:
        """Current _publish_single body: build message via serialization.create_message_with_tracing,
        publish to topology.exchange with the derived routing key, update stats."""

    async def publish_batch(self, events: Sequence[DomainEvent]) -> dict[str, int]:
        """Current batch entry: dispatch to sequential/concurrent strategy per config,
        raising BatchPublishError exactly as today."""
```

  (Internal strategy methods `_publish_chunk_concurrent` etc. move as private methods of the publisher, bodies verbatim.)
- Facade: `publish(events, background=False)` keeps its `BaseEventBus`-conformant signature; durable path awaits `self._publisher.publish_one(e)` per event (or the existing loop structure — preserve current ordering/error behavior exactly); `background=True` keeps `self._track_background(...)` on the facade. `publish_batch` public signature unchanged, delegates.

- [ ] **Step 1:** Write `tests/unit/bus/rabbitmq/test_publisher.py` first: mocked connection/topology (exchange is an `AsyncMock`); assert `publish_one` publishes exactly one message to the exchange with the routing key from `serialization.get_routing_key` and increments `stats.events_published` (use the real stats field name); assert `publish_batch` with a failing exchange raises `BatchPublishError` carrying per-event errors; assert the sequential strategy preserves order (record call order for 5 events).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Create the publisher; move bodies; facade wires `self._publisher = RabbitMQPublisher(...)` in `__init__` and delegates. Remove the Task 3 serialization wrappers that only the moved code used.
- [ ] **Step 4:** `uv run pytest tests/unit/bus/ -q` + mypy + ruff → PASS. **Step 5:** Commit — `git commit -m "refactor: extract RabbitMQPublisher with batch strategies"`

---

### Task 7: Extract `RabbitMQConsumer` (consume loop + retry/DLQ write path)

**Files:**
- Create: `src/eventsource/bus/rabbitmq/consumer.py`
- Modify: `src/eventsource/bus/rabbitmq/bus.py` (`start_consuming` 2798, `stop_consuming` 2871, `start_consuming_in_background` 2886, `_process_message` 2910, `_dispatch_event` 3104, `_calculate_retry_delay` 1788, `_handle_failed_message` 1802, `_republish_for_retry` 1873, `_send_to_dlq` 1913, `_stop_consuming_gracefully` 3769, `_drain_in_flight` 3818 move)
- Test: `tests/unit/bus/rabbitmq/test_consumer.py`

**Interfaces:**
- Produces:

```python
class RabbitMQConsumer:
    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
        topology: RabbitMQTopology,
        stats: RabbitMQEventBusStats,
        retry_policy: RetryPolicy,
        handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]],
        resolve_event_class: Callable[[str], type[DomainEvent] | None],
        tracer: Tracer | None,
        enable_tracing: bool,
    ) -> None: ...

    @property
    def is_consuming(self) -> bool: ...

    async def start(self) -> None: ...          # current start_consuming body
    async def stop(self) -> None: ...           # current stop_consuming body
    def start_in_background(self) -> asyncio.Task[None]: ...
    async def resume_if_was_consuming(self) -> None:
        """Reconnect path: restart consuming iff it was active before the drop
        (current _was_consuming logic)."""
    async def stop_gracefully(self, timeout: float) -> None: ...
    async def drain_in_flight(self, timeout: float) -> None: ...
```

- The consumer owns `_consumer_task`, `_consuming`, `_was_consuming`. Dispatch calls `self._handlers_for(type(event))`; deserialization calls `serialization.deserialize_event(body, headers, self._resolve_event_class)`; retry republish publishes to `topology.exchange` (as `_republish_for_retry` does today); DLQ writes go to `topology.dlq_exchange`. Death-count reads use `death_headers.*` module functions.
- **ADR 0011 invariant (verify, don't redesign):** `_dispatch_event` runs ALL handlers, collects failures, raises `HandlerDispatchError(failures)`; `_process_message` treats that as processing failure (retry → DLQ, no ack); single-failure unwrap (`if isinstance(e, HandlerDispatchError) and len(e.failures) == 1: dlq_error = e.failures[0][1]` at ~`bus.py:3096`) moves verbatim.
- Facade: `start_consuming`/`stop_consuming`/`start_consuming_in_background`/`is_consuming` delegate with unchanged public signatures; construction passes `self._handlers_for` and `self._resolve_event_class` bound methods; the Task 4 reconnect closure now calls `self._consumer.resume_if_was_consuming`.

- [ ] **Step 1:** Write `tests/unit/bus/rabbitmq/test_consumer.py` first. Core cases (mock the incoming message: `AsyncMock` with `.body`, `.headers`, `.ack`, `.reject`, and a `process()` context manager matching how `_process_message` consumes it — read the body first):
  - all handlers run even when the first fails; failure aggregates and message is NOT acked;
  - handler success path acks;
  - failure below `retry_policy.max_retries` republishes for retry with incremented death/retry count;
  - failure at max retries goes to the DLQ exchange with error metadata; single-failure unwrap recorded;
  - `resume_if_was_consuming` restarts only if consuming was active before.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Create `consumer.py`, move the eleven bodies, swap `self._channel`→`connection.require_channel()`, `self._exchange`→`topology.exchange`, static death helpers→`death_headers.*`.
- [ ] **Step 4:** Wire the facade per Interfaces; swap the reconnect closure target. Delete moved fields/methods from `bus.py`.
- [ ] **Step 5:** `uv run pytest tests/unit/bus/ -q` + mypy + ruff → PASS. **Step 6:** Commit — `git commit -m "refactor: extract RabbitMQConsumer owning dispatch and retry/DLQ write path"`

---

### Task 8: Extract `RabbitMQDLQAdmin`

**Files:**
- Create: `src/eventsource/bus/rabbitmq/dlq.py`
- Modify: `src/eventsource/bus/rabbitmq/bus.py` (`get_dlq_messages` 3233, `get_dlq_message_count` 3351, `replay_dlq_message` 3413, `_replay_message` 3526, `purge_dlq` 3603 move; facade delegates)
- Test: `tests/unit/bus/rabbitmq/test_dlq_admin.py`

**Interfaces:**
- Produces:

```python
class RabbitMQDLQAdmin:
    def __init__(
        self,
        config: RabbitMQEventBusConfig,
        connection: RabbitMQConnectionManager,
        topology: RabbitMQTopology,
        stats: RabbitMQEventBusStats,
    ) -> None: ...

    async def get_messages(...) -> list[DLQMessage]: ...   # copy the exact public signatures
    async def get_message_count(self) -> int: ...          # from bus.py before moving
    async def replay_message(...) -> ...: ...
    async def purge(self) -> ...: ...
```

Copy each public signature verbatim from `bus.py` (defaults included) — the facade delegations must be signature-identical to today's methods.

- [ ] **Step 1:** Write `tests/unit/bus/rabbitmq/test_dlq_admin.py` first: mocked topology `dlq_queue` (an `AsyncMock` supporting the get/iterator pattern the current body uses — read `get_dlq_messages` first); assert message paging honors the limit; count returns the queue's declared message count; `purge` calls queue purge and returns its count; `replay_message` republishes to the main exchange and acks the DLQ message.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Move the five bodies; facade delegates (`async def get_dlq_messages(self, ...): return await self._dlq_admin.get_messages(...)`).
- [ ] **Step 4:** `uv run pytest tests/unit/bus/ -q` + mypy + ruff → PASS. **Step 5:** Commit — `git commit -m "refactor: extract RabbitMQDLQAdmin"`

---

### Task 9: Slim the Rabbit facade; shutdown/health composition; backfill wiring tests

**Files:**
- Modify: `src/eventsource/bus/rabbitmq/bus.py` (`shutdown` 3672, `health_check` 3934, `get_queue_info` 3850, stats accessors — reorganize in place)
- Modify: `src/eventsource/bus/rabbitmq/connection.py`, `topology.py` (add health-slice methods)
- Test: `tests/unit/bus/rabbitmq/test_facade_wiring.py`

**Interfaces:**
- `RabbitMQConnectionManager.health_slice() -> dict[str, Any]` — the connection/channel checks currently inline in `health_check`.
- `RabbitMQTopology.queue_health(queue_name: str) -> QueueInfo | None` — the passive queue checks (`get_queue_info` body moves here; the facade's public `get_queue_info` delegates).
- Facade `health_check()` assembles `HealthCheckResult` from the slices + stats — **the returned result must be field-for-field identical to today's** for the same underlying states.
- Facade `shutdown()` keeps its public signature and owns ordering: `consumer.stop_gracefully` → `consumer.drain_in_flight` → `self._drain_background()` → `connection.disconnect` (match the current body's order and timeout handling exactly — read it first; if the current order differs, the current order wins).
- After this task `bus.py` should be roughly: `__init__` (composition wiring), `publish`/`publish_batch` orchestrators, consume/stats/DLQ/health/shutdown delegations, static death-header aliases, `__aenter__`/`__aexit__`. Target ≤600 lines.

- [ ] **Step 1:** Write `tests/unit/bus/rabbitmq/test_facade_wiring.py` first (all against mocks — no broker):

```python
@pytest.mark.asyncio
async def test_reconnect_redeclares_topology_and_resumes_consumer(bus_with_mocks) -> None:
    """Fire the connection manager's reconnect callbacks; assert topology.redeclare
    and consumer.resume_if_was_consuming were awaited, in that order."""


@pytest.mark.asyncio
async def test_shutdown_ordering(bus_with_mocks) -> None:
    """shutdown() stops the consumer, drains in-flight, drains background tasks,
    then disconnects — assert relative call order via a shared call log."""


@pytest.mark.asyncio
async def test_health_check_composes_slices(bus_with_mocks) -> None:
    """health_check() returns HealthCheckResult reflecting mocked slice values."""


@pytest.mark.asyncio
async def test_stats_accumulate_across_collaborators(bus_with_mocks) -> None:
    """The stats object handed to publisher/consumer is the same object the
    facade's get_stats() returns (identity check + counter increment visible)."""
```

Build `bus_with_mocks` as a fixture: construct a real `RabbitMQEventBus`, then replace `_connection_manager`, `_topology`, `_consumer`, `_publisher`, `_dlq_admin` with mocks wired to a shared `calls: list[str]` log. Write the docstring-described assertions as real code.

- [ ] **Step 2:** Run → FAIL where composition doesn't exist yet. **Step 3:** Implement the health/queue-info slice moves and shutdown wiring. **Step 4:** All unit bus tests + mypy + ruff → PASS.
- [ ] **Step 5:** Verify facade size: `wc -l src/eventsource/bus/rabbitmq/*.py` — `bus.py` ≤ ~600 lines; no collaborator > ~900. If a moved cluster silently stayed in `bus.py`, move it.
- [ ] **Step 5b:** Tracing tests: the facade keeps `_tracer`/`_enable_tracing` attributes (they are constructor wiring inputs), so `tests/unit/bus/test_rabbitmq_tracing.py` / `test_eventbus_tracing_patterns.py` should pass unmodified — run them (`uv run pytest tests/unit/bus/test_rabbitmq_tracing.py tests/unit/bus/test_eventbus_tracing_patterns.py -q`). If one reaches an internal that moved, repoint it at the collaborator; keep the assertion identical.
- [ ] **Step 6:** Commit — `git commit -m "refactor: rabbitmq facade composes collaborators; backfill wiring tests"`

---

### Task 10: Convert `bus/kafka.py` to a package

**Files:**
- Move: `src/eventsource/bus/kafka.py` → `src/eventsource/bus/kafka/bus.py`
- Create: `src/eventsource/bus/kafka/__init__.py`
- Create: `tests/unit/bus/kafka/__init__.py`
- Test: `tests/unit/bus/kafka/test_import_guard.py`

**Interfaces:**
- Old module `__all__` (`kafka.py:3131-3141`): `DeserializationError, EventSerializer, KAFKA_AVAILABLE, KafkaEventBus, KafkaEventBusConfig, KafkaEventBusMetrics, KafkaEventBusStats, KafkaNotAvailableError, KafkaRebalanceListener`. The package `__init__` re-exports all of them (yes, including `KafkaRebalanceListener` — it stays importable from `eventsource.bus.kafka` even after it moves to `connection.py` in Task 13).

- [ ] **Step 1:** `mkdir -p src/eventsource/bus/kafka && git mv src/eventsource/bus/kafka.py src/eventsource/bus/kafka/bus.py`
- [ ] **Step 2:** Write `src/eventsource/bus/kafka/__init__.py` importing the nine names from `.bus` with the same `__all__` (mirror Task 1's structure exactly, Kafka names).
- [ ] **Step 3:** Write `tests/unit/bus/kafka/test_import_guard.py` — same structure as Task 1's guard test with `aiokafka` in place of `aio_pika`, asserting `KAFKA_AVAILABLE is False` and `KafkaEventBus(...)` raises `KafkaNotAvailableError` when the import is blocked.
- [ ] **Step 4:** `uv run pytest tests/unit/ -q` + mypy + ruff → PASS.
- [ ] **Step 5:** Commit — `git commit -m "refactor: convert bus/kafka.py to a package with identical import surface"`

---

### Task 11: Extract Kafka `config.py`, `models.py`, `serialization.py`

**Files:**
- Create: `src/eventsource/bus/kafka/config.py` (← `KafkaEventBusConfig`, `bus.py:409-786`, incl. `_validate_security_config`, `get_producer_config`, `get_consumer_config`, `_add_security_config`, `create_ssl_context`, `get_sanitized_config`)
- Create: `src/eventsource/bus/kafka/models.py` (← `KafkaNotAvailableError` 300-318, `DeserializationError` 320-332, `KafkaEventBusStats` 788-848)
- Create: `src/eventsource/bus/kafka/serialization.py` (← `EventSerializer` 334-407, verbatim)
- Modify: `src/eventsource/bus/kafka/bus.py`, `src/eventsource/bus/kafka/__init__.py`
- Test: repoint the Kafka assertions in `tests/unit/bus/test_serialization_properties.py` at `eventsource.bus.kafka.serialization.EventSerializer` (import-path change only; assertions unchanged)

- [ ] **Step 1:** Move the classes verbatim (guard-safe: these modules import stdlib + eventsource only; aiokafka-typed hints go under `TYPE_CHECKING`). Update `bus.py` imports and the package `__init__` re-export sources.
- [ ] **Step 2:** Repoint the serialization property tests' imports. `uv run pytest tests/unit/ -q` + mypy + ruff → PASS.
- [ ] **Step 3:** Commit — `git commit -m "refactor: extract kafka config, models, and serializer modules"`

---

### Task 12: Extract Kafka `metrics.py`; move gauge registration out of `connect()`

**Files:**
- Create: `src/eventsource/bus/kafka/metrics.py` (← `KafkaEventBusMetrics` 174-298, `_register_connection_gauge` 1360, `_register_consumer_lag_gauge` 1404)
- Modify: `src/eventsource/bus/kafka/bus.py`
- Test: `tests/unit/bus/kafka/test_metrics.py`; repoint `tests/integration/bus/test_kafka.py` assertions on `_metrics`/`_connection_gauge_registered`/`_lag_gauge_registered`

**Interfaces:**
- Produces (in `metrics.py`):

```python
class KafkaEventBusMetrics:  # moved verbatim
    ...

def register_connection_gauge(
    meter: Any, metrics: KafkaEventBusMetrics, is_connected: Callable[[], bool]
) -> bool:
    """Register the connection gauge; return True if registered (current
    _connection_gauge_registered semantics). No-op returning False when meter is None."""

def register_consumer_lag_gauge(
    meter: Any, metrics: KafkaEventBusMetrics, lag_supplier: Callable[[], Any]
) -> bool: ...
```

  Adjust the callable params to what the current gauge callbacks actually read off `self` — every `self.X` becomes an injected callable/value.
- Facade: `connect()` no longer registers gauges. `KafkaEventBus.connect()` completes, then calls a private `self._wire_metrics()` that invokes both registration functions and stores the returned flags (facade keeps `_connection_gauge_registered`/`_lag_gauge_registered` as plain bools if integration tests still read them — otherwise move the flags into the metrics module and repoint the tests; prefer repointing).

- [ ] **Step 1:** Write `tests/unit/bus/kafka/test_metrics.py` first: registration with a mock meter creates the observable gauge once and returns True; `meter=None` returns False and registers nothing; calling twice doesn't double-register (mirror current idempotence).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Move + rewire per Interfaces. **Step 4:** Repoint the integration-test private reads at the new home. **Step 5:** `uv run pytest tests/unit/ -q` + mypy + ruff → PASS. **Step 6:** Commit — `git commit -m "refactor: extract kafka metrics module; gauge registration out of connect()"`

---

### Task 13: Extract `KafkaConnectionManager`; move `KafkaRebalanceListener`; deprecate `record_*`

**Files:**
- Create: `src/eventsource/bus/kafka/connection.py`
- Modify: `src/eventsource/bus/kafka/bus.py` (`connect` 1090, `disconnect` 1160, `_cleanup_connections` 1181, `_reconnect_consumer` 2015, `_get_security_config` 2801 move; `KafkaRebalanceListener` 850-944 moves; `record_reconnection` 1308 / `record_rebalance` 1328 become deprecation shims)
- Modify: `src/eventsource/bus/kafka/__init__.py` (re-export `KafkaRebalanceListener` from `.connection`)
- Test: `tests/unit/bus/kafka/test_connection.py`

**Interfaces:**
- Produces:

```python
class KafkaConnectionManager:
    def __init__(
        self,
        config: KafkaEventBusConfig,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics,
    ) -> None: ...

    @property
    def producer(self) -> AIOKafkaProducer | None: ...
    @property
    def consumer(self) -> AIOKafkaConsumer | None: ...
    @property
    def is_connected(self) -> bool: ...

    def require_producer(self) -> AIOKafkaProducer: ...
    async def connect(self) -> None: ...
    async def disconnect(self) -> None: ...
    async def cleanup(self) -> None: ...
    async def reconnect_consumer(self) -> None: ...

    def record_reconnection(self) -> None: ...   # real logic (stats + metrics), moved from the facade
    def record_rebalance(self, ...) -> None: ...  # copy exact current signature
```

- `KafkaRebalanceListener` moves into `connection.py` and its constructor takes the `KafkaConnectionManager` (it currently back-references the bus solely to call `record_rebalance`/stats — verify with grep, then re-target). Public re-export path `eventsource.bus.kafka.KafkaRebalanceListener` still works.
- Facade shims (public API, deprecated):

```python
    def record_reconnection(self) -> None:
        """Deprecated since 0.7.0; scheduled for removal in 0.8.0."""
        warnings.warn(
            "KafkaEventBus.record_reconnection() is deprecated and will be "
            "removed in 0.8.0; it was only ever intended for internal use.",
            DeprecationWarning,
            stacklevel=2,
        )
        self._connection_manager.record_reconnection()
```

  (`record_rebalance` shim mirrors this, forwarding its args.)

- [ ] **Step 1:** Write `tests/unit/bus/kafka/test_connection.py` first: mocked `AIOKafkaProducer`/`AIOKafkaConsumer` classes patched inside `eventsource.bus.kafka.connection`; assert `connect()` starts the producer (and consumer when configured), `is_connected` flips, `disconnect` stops both and is double-call safe, `require_producer` raises before connect, `record_reconnection` increments the stats counter and metrics. Plus facade shim tests: `pytest.warns(DeprecationWarning)` on both `record_*` facade calls, and the call still lands on the manager.
- [ ] **Step 2:** Run → FAIL. **Step 3:** Move bodies; re-target the rebalance listener; wire facade (`self._connection_manager = KafkaConnectionManager(config, self._stats, self._metrics)`), delegations for `connect`/`disconnect`/`is_connected`.
- [ ] **Step 4:** `uv run pytest tests/unit/ -q` + mypy + ruff → PASS. **Step 5:** Commit — `git commit -m "refactor: extract KafkaConnectionManager; deprecate facade record_* methods"`

---

### Task 14: Extract `KafkaPublisher` (split-phase send/ack)

**Files:**
- Create: `src/eventsource/bus/kafka/publisher.py`
- Modify: `src/eventsource/bus/kafka/bus.py` (`_begin_publish_single_event` 1566, `_await_publish_ack` 1657, `_track_background_publish` 1724, `_get_partition_key` 1813, `_serialize_event` 1827, `_create_headers` 1838 move; `publish` 1484 stays as facade orchestrator)
- Test: `tests/unit/bus/kafka/test_publisher.py`

**Interfaces:**
- Produces:

```python
class KafkaPublisher:
    def __init__(
        self,
        config: KafkaEventBusConfig,
        connection: KafkaConnectionManager,
        serializer: EventSerializer,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics,
        tracer: Tracer | None,
        enable_tracing: bool,
    ) -> None: ...

    async def publish_all(self, events: Sequence[DomainEvent]) -> None:
        """Split-phase: sequentially hand each event to producer.send() (preserving
        per-aggregate ordering via the aggregate_id partition key), then gather acks.
        Current publish-path semantics exactly, including the aiokafka plain-Future
        handling (add_done_callback with cancelled/exception checks)."""
```

- Facade `publish(events, background=False)`: durable path `await self._publisher.publish_all(events)`; background path `self._track_background(self._publisher.publish_all(events))` — preserve exactly what the current facade does (read `publish` 1484 first; its structure wins over this description).

- [ ] **Step 1:** Write `tests/unit/bus/kafka/test_publisher.py` first: mock producer whose `send()` returns a fresh `asyncio.Future`; assert: partition key equals the event's `aggregate_id` bytes (match current derivation); two events for one aggregate are sent in order before acks are awaited; an ack future failing raises after all sends were handed off; headers carry the event-type name (match `_create_headers`).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Move bodies, wire facade. **Step 4:** `uv run pytest tests/unit/ -q` + mypy + ruff → PASS. **Step 5:** Commit — `git commit -m "refactor: extract KafkaPublisher with split-phase send/ack"`

---

### Task 15: Extract `KafkaConsumerLoop`; remove `get_handlers_for_event`

**Files:**
- Create: `src/eventsource/bus/kafka/consumer.py`
- Modify: `src/eventsource/bus/kafka/bus.py` (`start_consuming` 1903, `start_consuming_in_background` 2039, `stop_consuming` 1265, `_process_message` 2063, `_extract_trace_context` 2143, `_process_message_with_span` 2167, `_deserialize_message` 2282, `_get_header_value` 2308, `_get_retry_count` 2331, `_dispatch_to_handlers` 2348, `_handle_processing_error` 2539, `_republish_for_retry` 2606, `_calculate_retry_delay` 2664, `_send_to_dlq` 2679, `_create_dlq_headers` 2769 move; **delete** `get_handlers_for_event` 1874)
- Test: `tests/unit/bus/kafka/test_consumer.py`; repoint `test_serialization_properties.py` if it touches `_deserialize_message`

**Interfaces:**
- Produces:

```python
class KafkaConsumerLoop:
    def __init__(
        self,
        config: KafkaEventBusConfig,
        connection: KafkaConnectionManager,
        serializer: EventSerializer,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics,
        retry_policy: RetryPolicy,
        handlers_for: Callable[[type[DomainEvent]], tuple[HandlerAdapter, ...]],
        resolve_event_class: Callable[[str], type[DomainEvent] | None],
        tracer: Tracer | None,
        enable_tracing: bool,
        shutdown_event: asyncio.Event,
    ) -> None: ...

    @property
    def is_consuming(self) -> bool: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def start_in_background(self) -> asyncio.Task[None]: ...
```

- **ADR 0011 + class-keyed dispatch invariants:** `_dispatch_to_handlers` keeps `handlers_for(type(event))` keying (the 0.6.0 bug fix), runs ALL handlers, aggregates failures into `HandlerDispatchError`, no commit on failure; single-failure unwrap before DLQ error recording moves verbatim.
- `get_handlers_for_event` (a `DeprecationWarning` shim since 0.6.0) is **deleted** from the facade — grep `get_handlers_for_event` across `src/` and `tests/` and remove the stragglers (a test asserting the deprecation shim exists gets deleted too; note it in the commit body).
- Facade keeps `start_consuming`/`stop_consuming`/`start_consuming_in_background`/`is_consuming` delegations with unchanged public signatures; passes `self._handlers_for`/`self._resolve_event_class` bound methods.

- [ ] **Step 1:** Write `tests/unit/bus/kafka/test_consumer.py` first — mirror Task 7's dispatch cases against a mocked aiokafka consumer/message (`.value`, `.headers`, `.topic`, `.partition`, `.offset` — read `_process_message` for the shape): all-handlers-run isolation, `HandlerDispatchError` aggregation, no-commit-on-failure, retry republish below max, DLQ at max with error headers, class-keyed handler lookup (two event classes with the same name string must not cross-dispatch — construct via distinct registries).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Move the fifteen bodies; delete `get_handlers_for_event` + its references. **Step 4:** `uv run pytest tests/unit/ -q` + mypy + ruff → PASS. **Step 5:** Commit — `git commit -m "refactor!: extract KafkaConsumerLoop; remove deprecated get_handlers_for_event"`

---

### Task 16: Extract `KafkaDLQAdmin`

**Files:**
- Create: `src/eventsource/bus/kafka/dlq.py`
- Modify: `src/eventsource/bus/kafka/bus.py` (`get_dlq_messages` 2833, `replay_dlq_message` 2948, `get_dlq_message_count` 3082 move; facade delegates)
- Test: `tests/unit/bus/kafka/test_dlq_admin.py`

**Interfaces:**
- Produces:

```python
class KafkaDLQAdmin:
    def __init__(
        self,
        config: KafkaEventBusConfig,
        connection: KafkaConnectionManager,
        serializer: EventSerializer,
        stats: KafkaEventBusStats,
    ) -> None: ...

    async def get_messages(...) -> ...: ...      # copy exact public signatures from bus.py
    async def replay_message(...) -> ...: ...
    async def get_message_count(self) -> int: ...
```

- All three current methods build their own throwaway `AIOKafkaConsumer` with near-identical boilerplate — extract that into one private helper `async def _dlq_consumer(self) -> AsyncIterator[AIOKafkaConsumer]` (an `asynccontextmanager` that builds, starts, and always stops the consumer), and have the three methods use it. This is the one intentional internal dedup in this plan; observable behavior unchanged.
- Facade delegations signature-identical.

- [ ] **Step 1:** Write `tests/unit/bus/kafka/test_dlq_admin.py` first: patch `AIOKafkaConsumer` inside `eventsource.bus.kafka.dlq`; assert count sums end-minus-beginning offsets across partitions (mirror `bus.py:3110-3128`); `get_messages` honors its limit; replay sends to the main topic via `connection.require_producer()`; the throwaway consumer is always stopped (assert `stop` awaited even when the body raises — use a raising mock).
- [ ] **Step 2:** Run → FAIL. **Step 3:** Move + dedup per Interfaces. **Step 4:** `uv run pytest tests/unit/ -q` + mypy + ruff → PASS. **Step 5:** Commit — `git commit -m "refactor: extract KafkaDLQAdmin with shared throwaway-consumer helper"`

---

### Task 17: Slim the Kafka facade; backfill wiring tests

**Files:**
- Modify: `src/eventsource/bus/kafka/bus.py` (`shutdown` 1228, `get_stats_dict` 1278, `get_topic_info` 1286 reorganized; `_shutdown_event` ownership; composition wiring in `__init__`)
- Test: `tests/unit/bus/kafka/test_facade_wiring.py`

**Interfaces:**
- Facade `shutdown()` keeps its public signature and owns ordering: signal `_shutdown_event` → stop consumer loop → `self._drain_background()` → `connection.disconnect()` (read the current body first — its order wins over this description).
- After this task `bus.py` ≈ `__init__` wiring, `publish` orchestrator, delegations, deprecation shims, `__aenter__`/`__aexit__`, `get_topic_info`. Target ≤500 lines.

- [ ] **Step 1:** Write `tests/unit/bus/kafka/test_facade_wiring.py` first (mirror Task 9's fixture approach with mocked collaborators + shared call log): shutdown ordering; stats identity across collaborators (`get_stats_dict()` reflects a counter a collaborator incremented); metrics wiring happens after connect (mock `register_connection_gauge` in `eventsource.bus.kafka.bus` namespace, assert called after producer start); background publish goes through `_track_background` (assert `get_background_task_count()` rises then drains via `_drain_background`).
- [ ] **Step 2:** Run → FAIL where wiring is incomplete. **Step 3:** Finish composition + slim. **Step 4:** `wc -l src/eventsource/bus/kafka/*.py` sanity; `uv run pytest tests/unit/ -q` + mypy + ruff → PASS. Tracing tests pass unmodified (facade keeps `_tracer`/`_enable_tracing` wiring attributes); if one reaches a moved internal, repoint it, assertion identical.
- [ ] **Step 5:** Commit — `git commit -m "refactor: kafka facade composes collaborators; backfill wiring tests"`

---

### Task 18: Release chores — mutmut targets, 0.7.0, CHANGELOG, ADR 0019 renumber + ADR 0020

**Files:**
- Modify: `pyproject.toml` (version, mutmut `only_mutate`)
- Modify: `CHANGELOG.md`
- Move: `docs/adrs/0009-postgresql-advisory-locks.md` → `docs/adrs/0019-postgresql-advisory-locks.md`
- Create: `docs/adrs/0020-broker-backend-collaborator-decomposition.md`
- Modify: `docs/adrs/index.md`

- [ ] **Step 1: mutmut targets.** In `pyproject.toml` `[tool.mutmut]`, append to `only_mutate` (following the existing entry format): `src/eventsource/bus/rabbitmq/death_headers.py`, `src/eventsource/bus/rabbitmq/serialization.py`, `src/eventsource/bus/kafka/serialization.py`, `src/eventsource/bus/rabbitmq/consumer.py`, `src/eventsource/bus/kafka/consumer.py`, `src/eventsource/bus/memory.py` — and extend the paired test-selection config the existing entries use (registry/retry/base pattern) with the matching new test files (`tests/unit/bus/rabbitmq/…`, `tests/unit/bus/kafka/…`, `tests/unit/bus/test_memory.py`). Sanity: `uv run mutmut run --help` exits 0 (config parses); do NOT run the full mutation campaign in-task.
- [ ] **Step 2: version + changelog.** `version = "0.7.0"` in `pyproject.toml`. CHANGELOG 0.7.0 entry: Changed — RabbitMQ/Kafka backends decomposed into internal collaborator packages (imports unchanged); Removed — `KafkaEventBus.get_handlers_for_event` (deprecated 0.6.0); Deprecated — `KafkaEventBus.record_reconnection` / `record_rebalance` (removal 0.8.0). Follow the file's existing entry format.
- [ ] **Step 3: ADR renumber.** `git mv docs/adrs/0009-postgresql-advisory-locks.md docs/adrs/0019-postgresql-advisory-locks.md`; update the number in that file's title heading only (`# ADR 0009` → `# ADR 0019`) — body otherwise untouched; `grep -rn "0009" docs/ src/ README.md` and repoint any references to the advisory-locks ADR (leave subscription-coordination 0009 references alone); update `docs/adrs/index.md`.
- [ ] **Step 4: ADR 0020.** Create `docs/adrs/0020-broker-backend-collaborator-decomposition.md`:

```markdown
# ADR 0020: Broker Backend Collaborator Decomposition

## Status

Accepted (2026-07-29)

## Context

RabbitMQEventBus (~3,570-line class) and KafkaEventBus (~2,195-line class)
each mixed connection lifecycle, topology declaration, serialization,
publish paths, consume/dispatch, retry/DLQ write paths, DLQ administration,
health checks, and stats in one object. Cross-cutting methods
(health_check, _process_message, _dispatch_to_handlers) read state from
four or five responsibility clusters, and ~1,500 lines of logic were
covered only through Docker integration tests.

## Decision

Each broker backend is a package of internal, state-owning collaborators
composed by a facade that keeps the public API:

- ConnectionManager owns broker clients, connected/reconnecting flags, and
  an explicit `on_reconnect(callback)` hook; other collaborators request
  live clients at call time and never capture them at construction.
- RabbitMQTopology owns exchange/queue objects and declaration; Kafka has
  no topology collaborator (config-driven — the asymmetry is real).
- Publisher and Consumer own their paths; the Consumer owns the retry/DLQ
  write path and receives `handlers_for` / `resolve_event_class` callables
  so the SubscriptionRegistry stays on the facade (ADR 0013, ADR 0010).
- DLQAdmin owns get/count/replay(/purge) administration.
- Pure logic (RabbitMQ death-header introspection, serialization) lives in
  plain modules.
- Collaborators are internal: nothing new is exported from
  `eventsource.bus` or `eventsource`. Facades keep every public signature;
  the only 0.7.0 API changes are removing the already-deprecated
  `KafkaEventBus.get_handlers_for_event` and deprecating
  `record_reconnection`/`record_rebalance`.
- Shutdown/drain ordering and health-check assembly are facade-owned;
  collaborators expose stop/close primitives and health slices.

## Alternatives Considered

- **Mixin split**: files shrink but every mixin still reads shared `self`
  state — coupling unchanged, only harder to see. Rejected.
- **Function-module extraction**: logic moves to parameterized functions
  but all state stays on one class with 15-20 fields. Rejected.
- **Shared cross-backend contracts** (common DLQAdmin/consumer pipeline
  interfaces implemented by Redis too): deferred, not rejected — revisit
  once both per-backend decompositions have settled.

## Consequences

- Collaborators are unit-testable with mocked broker objects; reconnect
  and shutdown wiring have direct unit coverage instead of Docker-only.
- Delivery, retry, and error-isolation semantics (ADR 0007, ADR 0011) are
  unchanged; the conformance suite passes unmodified.
- Internal collaborator APIs may change freely between releases.
```

- [ ] **Step 5:** `uv run pytest tests/unit/ -q` + `uv run ruff check src/ tests/` + mypy → PASS. Docs build if a make target exists (`make help | grep -i docs`).
- [ ] **Step 6:** Commit — `git commit -m "chore: release chores for 0.7.0 — mutmut targets, changelog, ADR 0019 renumber, ADR 0020"`

---

## Post-plan verification (orchestrator, not a task)

- Full suite: `uv run pytest tests/unit/ -q`; broker integration via Docker: `uv run pytest -m "(kafka or rabbitmq) and not benchmark" --no-cov`; `make check` for CI parity.
- Grep the tree for stragglers: `grep -rn "bus/rabbitmq.py\|bus/kafka.py" docs/ README.md` (stale path references).
- Confirm `git log --follow` still tracks history through the `git mv`s.
