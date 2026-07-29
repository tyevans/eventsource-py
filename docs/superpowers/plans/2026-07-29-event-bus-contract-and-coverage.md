# Event Bus Contract and Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the four `EventBus` backends honor one explicit contract, verified by a conformance suite that actually runs, property tests, and mutation coverage.

**Architecture:** Extract the subscription registry, retry policy, and background-task tracking that all four backends currently duplicate into `SubscriptionRegistry`, `RetryPolicy`, and a `BaseEventBus` concrete base class. The `EventBus` ABC stays a pure interface. The existing but never-subclassed `EventBusConformanceSuite` is wired into all four backends and grows three tests covering the newly-explicit contract.

**Tech Stack:** Python 3.11+, pydantic v2, pytest + pytest-asyncio (auto mode), hypothesis, mutmut, ruff, mypy strict, Docker Compose for integration services.

**Spec:** `docs/superpowers/specs/2026-07-29-event-bus-contract-and-coverage-design.md`

## Global Constraints

- Every task's requirements implicitly include this section.
- Python 3.11+. Type annotations required everywhere; `uv run mypy src/eventsource/ --config-file=pyproject.toml` must pass in strict mode.
- `uv run ruff check src/ tests/ --fix` and `uv run ruff format src/ tests/` must pass.
- **Do NOT add `@pytest.mark.asyncio` to async tests.** `asyncio_mode` is auto in `pyproject.toml`; adding the decorator is an error in this codebase.
- Async tests are plain `async def test_...` functions.
- Public API changes must be re-exported from `src/eventsource/__init__.py` with an `__all__` entry.
- Optional backends are guarded with `try/except ImportError` and an `*_AVAILABLE` flag.
- Do not modify `py.typed` or any file under `migrations/` (append-only SQL).
- Commit messages: `<type>: <lowercase description>` — `feat:`, `fix:`, `refactor:`, `chore:`, `test:`, `docs:`.
- Never widen `[tool.mutmut] only_mutate` to a package or directory — file paths only. See `cosmic-ray/engine.toml` for the runtime rationale.
- Unit tests must not require Docker. Anything needing a real broker goes in `tests/integration/` behind its marker.

## File Structure

**Created:**

| File | Responsibility |
| --- | --- |
| `src/eventsource/bus/registry.py` | `SubscriptionRegistry` — the only new stateful unit. Thread-safe handler storage and lookup. |
| `src/eventsource/bus/retry.py` | `RetryPolicy` — pure exponential-backoff-with-jitter calculation. No I/O, no state. |
| `src/eventsource/bus/base.py` | `BaseEventBus` — composes the registry, owns background-task tracking and event-class resolution. |
| `src/eventsource/testing/recording.py` | `RecordingEventBus` — decorator capturing published events for tests. |
| `tests/unit/bus/test_registry.py` | Unit + property tests for `SubscriptionRegistry`. |
| `tests/unit/bus/test_retry.py` | Property tests for `RetryPolicy`. |
| `tests/unit/bus/test_serialization_properties.py` | Roundtrip property tests for each backend's serialize/deserialize pair. |
| `tests/unit/bus/test_error_isolation_properties.py` | Property tests for handler error isolation. |
| `tests/unit/bus/test_conformance.py` | `InMemoryEventBus` conformance subclass. |
| `tests/unit/testing/test_recording.py` | Unit tests for `RecordingEventBus`. |

**Modified:**

| File | Change |
| --- | --- |
| `pyproject.toml` | ruff pin + Markdown exclusion; mutmut targets; coverage floor. |
| `.github/workflows/ci.yml` | Install PEP 735 group; add broker job. |
| `docker-compose.test.yml` | Add `kafka` and `rabbitmq` services. |
| `src/eventsource/bus/interface.py` | Docstring corrections for `background` and thread safety. |
| `src/eventsource/bus/memory.py` | Inherit `BaseEventBus`; delete subscription + background-task code. |
| `src/eventsource/bus/redis.py` | Inherit `BaseEventBus`; honor `background`. |
| `src/eventsource/bus/kafka.py` | Inherit `BaseEventBus`; batch publish; `RetryPolicy`; drop name-keyed handlers. |
| `src/eventsource/bus/rabbitmq.py` | Inherit `BaseEventBus`; `RetryPolicy`. |
| `src/eventsource/testing/conformance.py` | Add `await_delivery` hook + 3 tests. |
| `src/eventsource/__init__.py` | Export new public names. |
| `tests/integration/bus/test_{redis,kafka,rabbitmq}.py` | Add conformance subclasses. |

---

### Task 1: Fix the three pre-existing CI failures

**Independently mergeable.** As of 2026-07-29 `lint`, `import-linter`, and `audit` fail on `main` and every PR. Nothing else in this plan can be verified until this lands.

**Files:**
- Modify: `pyproject.toml`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: nothing.
- Produces: a green CI baseline. No code symbols.

- [ ] **Step 1: Reproduce all three failures locally**

```bash
uv run ruff format --check .          # expect: FAIL on .claude/agents/*.md
uv run lint-imports                    # may pass locally if installed; CI lacks it
pip download pip-audit -d /dev/null 2>/dev/null || true
```

Then confirm the CI-side cause — the tools live in a PEP 735 group, which `pip install -e ".[dev,all]"` does not install:

```bash
grep -n -A12 '^\[dependency-groups\]' pyproject.toml
```

Expected: `import-linter` and `pip-audit` appear under `[dependency-groups] dev`, not under `[project.optional-dependencies]`.

- [ ] **Step 2: Pin ruff and exclude Markdown from formatting**

In `pyproject.toml`, change the ruff dependency in `[project.optional-dependencies]` from `"ruff>=0.14.8"` to:

```toml
    "ruff>=0.14.8,<0.15",
```

And under `[tool.ruff]` (currently `line-length` / `target-version`), add:

```toml
[tool.ruff]
line-length = 100
target-version = "py311"
# ruff >= 0.14 formats Python code blocks inside Markdown. The snippets in our
# agent and docs Markdown are illustrative and deliberately abbreviated
# (`...` bodies, partial classes); reformatting them serves no one and made
# `ruff format --check .` fail repo-wide. The upper pin above stops a future
# release from silently widening what `--check` covers again.
extend-exclude = ["*.md"]
```

- [ ] **Step 3: Verify the format check passes**

Run: `uv run ruff format --check .`
Expected: PASS, no files listed.

Also confirm `ruff check` is unaffected:

Run: `uv run ruff check src/ tests/`
Expected: PASS.

- [ ] **Step 4: Make CI install the PEP 735 dev group**

In `.github/workflows/ci.yml`, every job's `Install dependencies` step currently reads some variant of:

```yaml
      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install -e ".[dev,all]"
```

Add the group install to the `lint`, `type-check`, `import-linter`, and `audit` jobs (the `test` job too, since `pytest-randomly` and `pytest-timeout` live in the same group):

```yaml
      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install -e ".[dev,all]" --group dev
```

Prefer this over duplicating the tool list into `[project.optional-dependencies]`, which would leave two lists to drift apart.

- [ ] **Step 5: Verify the group installs the missing binaries**

```bash
python -m venv /tmp/ci-check && /tmp/ci-check/bin/pip install -q --upgrade pip
/tmp/ci-check/bin/pip install -q -e ".[dev,all]" --group dev
/tmp/ci-check/bin/lint-imports --help > /dev/null && echo "lint-imports OK"
/tmp/ci-check/bin/pip-audit --version > /dev/null && echo "pip-audit OK"
rm -rf /tmp/ci-check
```

Expected: both `OK` lines print.

If `pip` is too old to support `--group` (needs pip >= 25.1), the `pip install --upgrade pip` line above already handles it; confirm with `/tmp/ci-check/bin/pip --version`.

- [ ] **Step 6: Run the local guardrail suite**

Run: `uv run lint-imports`
Expected: PASS — all import contracts hold.

Run: `uv run pip-audit`
Expected: PASS, or a report of pre-existing advisories. If advisories appear, do NOT fix them in this task — note them and raise with the user. This task is about the tooling not running, not about vulnerabilities.

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml .github/workflows/ci.yml
git commit -m "fix: repair three pre-existing CI job failures

ruff >= 0.14 formats Python inside Markdown, so the unpinned ruff made
'ruff format --check .' fail on .claude/agents/*.md. Exclude Markdown and
pin below 0.15.

import-linter and pip-audit are declared in [dependency-groups] (PEP 735),
which 'pip install -e .[dev,all]' does not install, so both failed with
'command not found'. Install the group explicitly."
```

- [ ] **Step 8: Push and confirm CI is green**

```bash
git push
gh run list --limit 5
```

Expected: `lint`, `type-check`, `import-linter`, `audit`, and `test` all pass. Do not start Task 2 until this is confirmed green — the rest of the plan depends on a trustworthy signal.

---

### Task 2: Extract `SubscriptionRegistry`

Pure addition. Nothing consumes it yet.

**Files:**
- Create: `src/eventsource/bus/registry.py`
- Test: `tests/unit/bus/test_registry.py`

**Interfaces:**
- Consumes: `HandlerAdapter` from `eventsource.handlers.adapter`; `DomainEvent`; `FlexibleEventHandler`, `FlexibleEventSubscriber` from `eventsource.protocols`.
- Produces:
  ```python
  class SubscriptionRegistry:
      def add(self, event_type: type[DomainEvent], handler: Any) -> HandlerAdapter
      def remove(self, event_type: type[DomainEvent], handler: Any) -> bool
      def add_wildcard(self, handler: Any) -> HandlerAdapter
      def remove_wildcard(self, handler: Any) -> bool
      def add_subscriber(self, subscriber: FlexibleEventSubscriber) -> None
      def handlers_for(self, event_type: type[DomainEvent]) -> tuple[HandlerAdapter, ...]
      def clear(self) -> None
      def count(self, event_type: type[DomainEvent] | None = None) -> int
      def wildcard_count(self) -> int
  ```

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/bus/test_registry.py`:

```python
"""Unit and property tests for SubscriptionRegistry."""

from typing import Any

from hypothesis import given
from hypothesis import strategies as st

from eventsource.bus.registry import SubscriptionRegistry
from eventsource.events.base import DomainEvent


class RegistryEventA(DomainEvent):
    event_type: str = "RegistryEventA"
    aggregate_type: str = "Registry"


class RegistryEventB(DomainEvent):
    event_type: str = "RegistryEventB"
    aggregate_type: str = "Registry"


def _handler(event: DomainEvent) -> None:
    """A no-op handler used purely as an identity token."""


def test_add_then_handlers_for_returns_the_handler() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)

    handlers = registry.handlers_for(RegistryEventA)

    assert len(handlers) == 1
    assert handlers[0].original is _handler


def test_handlers_for_unknown_type_is_empty() -> None:
    registry = SubscriptionRegistry()

    assert registry.handlers_for(RegistryEventA) == ()


def test_remove_returns_true_when_present_false_when_absent() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)

    assert registry.remove(RegistryEventA, _handler) is True
    assert registry.remove(RegistryEventA, _handler) is False


def test_specific_handlers_precede_wildcard_handlers() -> None:
    registry = SubscriptionRegistry()

    def wildcard(event: DomainEvent) -> None: ...

    registry.add_wildcard(wildcard)
    registry.add(RegistryEventA, _handler)

    handlers = registry.handlers_for(RegistryEventA)

    assert [h.original for h in handlers] == [_handler, wildcard]


def test_wildcard_reaches_every_event_type() -> None:
    registry = SubscriptionRegistry()
    registry.add_wildcard(_handler)

    assert len(registry.handlers_for(RegistryEventA)) == 1
    assert len(registry.handlers_for(RegistryEventB)) == 1


def test_handlers_for_is_stable_across_calls_without_mutation() -> None:
    """The combined tuple is cached, so dispatch allocates nothing per event."""
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)

    first = registry.handlers_for(RegistryEventA)
    second = registry.handlers_for(RegistryEventA)

    assert first is second


def test_cache_is_invalidated_on_mutation() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    before = registry.handlers_for(RegistryEventA)

    def other(event: DomainEvent) -> None: ...

    registry.add(RegistryEventA, other)
    after = registry.handlers_for(RegistryEventA)

    assert before is not after
    assert len(after) == 2


def test_wildcard_mutation_invalidates_specific_type_cache() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    before = registry.handlers_for(RegistryEventA)

    def wildcard(event: DomainEvent) -> None: ...

    registry.add_wildcard(wildcard)
    after = registry.handlers_for(RegistryEventA)

    assert before is not after
    assert len(after) == 2


def test_count_and_wildcard_count() -> None:
    registry = SubscriptionRegistry()

    def other(event: DomainEvent) -> None: ...

    registry.add(RegistryEventA, _handler)
    registry.add(RegistryEventA, other)
    registry.add(RegistryEventB, _handler)
    registry.add_wildcard(_handler)

    assert registry.count() == 3
    assert registry.count(RegistryEventA) == 2
    assert registry.count(RegistryEventB) == 1
    assert registry.wildcard_count() == 1


def test_clear_removes_everything() -> None:
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    registry.add_wildcard(_handler)

    registry.clear()

    assert registry.count() == 0
    assert registry.wildcard_count() == 0


def test_add_subscriber_registers_every_declared_type() -> None:
    class Subscriber:
        def subscribed_to(self) -> list[type[DomainEvent]]:
            return [RegistryEventA, RegistryEventB]

        async def handle(self, event: DomainEvent) -> None: ...

    registry = SubscriptionRegistry()
    registry.add_subscriber(Subscriber())

    assert registry.count(RegistryEventA) == 1
    assert registry.count(RegistryEventB) == 1


def test_the_same_handler_may_subscribe_twice() -> None:
    """Duplicate registration is allowed; each remove strips one."""
    registry = SubscriptionRegistry()
    registry.add(RegistryEventA, _handler)
    registry.add(RegistryEventA, _handler)

    assert registry.count(RegistryEventA) == 2
    assert registry.remove(RegistryEventA, _handler) is True
    assert registry.count(RegistryEventA) == 1


# =============================================================================
# Property tests
# =============================================================================

# Each op is (kind, handler_index). kind: 0=add, 1=remove, 2=add_wc, 3=remove_wc
_OPS = st.lists(
    st.tuples(st.integers(min_value=0, max_value=3), st.integers(min_value=0, max_value=4)),
    max_size=40,
)


@given(ops=_OPS)
def test_counts_track_net_operations(ops: list[tuple[int, int]]) -> None:
    """count() and wildcard_count() always equal the net number of live handlers."""
    handlers: list[Any] = []
    for i in range(5):

        def make(idx: int) -> Any:
            def h(event: DomainEvent) -> None: ...

            h.__name__ = f"handler_{idx}"
            return h

        handlers.append(make(i))

    registry = SubscriptionRegistry()
    specific: list[Any] = []
    wildcard: list[Any] = []

    for kind, idx in ops:
        handler = handlers[idx]
        if kind == 0:
            registry.add(RegistryEventA, handler)
            specific.append(handler)
        elif kind == 1:
            expected = handler in specific
            assert registry.remove(RegistryEventA, handler) is expected
            if expected:
                specific.remove(handler)
        elif kind == 2:
            registry.add_wildcard(handler)
            wildcard.append(handler)
        else:
            expected = handler in wildcard
            assert registry.remove_wildcard(handler) is expected
            if expected:
                wildcard.remove(handler)

        assert registry.count(RegistryEventA) == len(specific)
        assert registry.wildcard_count() == len(wildcard)
        assert len(registry.handlers_for(RegistryEventA)) == len(specific) + len(wildcard)


@given(ops=_OPS)
def test_handlers_for_always_orders_specific_before_wildcard(
    ops: list[tuple[int, int]],
) -> None:
    handlers: list[Any] = []
    for i in range(5):

        def make(idx: int) -> Any:
            def h(event: DomainEvent) -> None: ...

            h.__name__ = f"handler_{idx}"
            return h

        handlers.append(make(i))

    registry = SubscriptionRegistry()
    specific_count = 0

    for kind, idx in ops:
        handler = handlers[idx]
        if kind == 0:
            registry.add(RegistryEventA, handler)
            specific_count += 1
        elif kind == 1:
            if registry.remove(RegistryEventA, handler):
                specific_count -= 1
        elif kind == 2:
            registry.add_wildcard(handler)
        else:
            registry.remove_wildcard(handler)

    combined = registry.handlers_for(RegistryEventA)
    assert len(combined) == registry.count(RegistryEventA) + registry.wildcard_count()
    # The first `specific_count` entries are the type-specific ones; the rest
    # are wildcards. Verified through the public API only.
    assert len(combined[:specific_count]) == specific_count
    assert len(combined[specific_count:]) == registry.wildcard_count()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/bus/test_registry.py -v --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'eventsource.bus.registry'`

- [ ] **Step 3: Write the implementation**

Create `src/eventsource/bus/registry.py`:

```python
"""Shared subscription registry for event bus implementations.

Every EventBus backend needs the same thing: a thread-safe mapping of event
type to handlers, plus a wildcard list. Before this module existed, all four
backends implemented it independently and inconsistently -- InMemory locked,
Redis did not, and Kafka keyed by event type *name* rather than by the class.
"""

from __future__ import annotations

import threading
from typing import Any

from eventsource.events.base import DomainEvent
from eventsource.handlers.adapter import HandlerAdapter
from eventsource.protocols import FlexibleEventSubscriber


class SubscriptionRegistry:
    """Thread-safe registry of event handlers, keyed by event class.

    Handlers are stored as immutable tuples and the combined
    ``specific + wildcard`` tuple for each event type is cached, so dispatch
    performs no allocation per event. Any mutation drops the cache.

    Thread Safety:
        All public methods are safe to call from any thread. Subscription
        changes are rare relative to dispatch, so a single ``RLock`` around
        mutations and reads is not a meaningful cost.

    Example:
        >>> registry = SubscriptionRegistry()
        >>> registry.add(OrderCreated, my_handler)
        >>> for adapter in registry.handlers_for(OrderCreated):
        ...     await adapter.handle(event)
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._specific: dict[type[DomainEvent], tuple[HandlerAdapter, ...]] = {}
        self._wildcard: tuple[HandlerAdapter, ...] = ()
        self._combined: dict[type[DomainEvent], tuple[HandlerAdapter, ...]] = {}

    def _invalidate(self) -> None:
        """Drop the combined-tuple cache. Caller must hold the lock."""
        self._combined = {}

    def add(self, event_type: type[DomainEvent], handler: Any) -> HandlerAdapter:
        """Register a handler for a specific event type.

        Registering the same handler twice is allowed; it will be invoked
        twice, and each ``remove`` strips one registration.

        Args:
            event_type: The event class to subscribe to.
            handler: Object with a ``handle()`` method, or a callable.

        Returns:
            The HandlerAdapter wrapping the handler, for logging by callers.
        """
        adapter = HandlerAdapter(handler)
        with self._lock:
            self._specific[event_type] = self._specific.get(event_type, ()) + (adapter,)
            self._invalidate()
        return adapter

    def remove(self, event_type: type[DomainEvent], handler: Any) -> bool:
        """Remove one registration of a handler for an event type.

        Compares against the raw handler via ``HandlerAdapter.__eq__``, so no
        throwaway adapter is constructed.

        Returns:
            True if a registration was found and removed, False otherwise.
        """
        with self._lock:
            adapters = self._specific.get(event_type, ())
            for i, adapter in enumerate(adapters):
                if adapter == handler:
                    remaining = adapters[:i] + adapters[i + 1 :]
                    if remaining:
                        self._specific[event_type] = remaining
                    else:
                        del self._specific[event_type]
                    self._invalidate()
                    return True
        return False

    def add_wildcard(self, handler: Any) -> HandlerAdapter:
        """Register a handler that receives every published event."""
        adapter = HandlerAdapter(handler)
        with self._lock:
            self._wildcard = self._wildcard + (adapter,)
            self._invalidate()
        return adapter

    def remove_wildcard(self, handler: Any) -> bool:
        """Remove one wildcard registration.

        Returns:
            True if a registration was found and removed, False otherwise.
        """
        with self._lock:
            for i, adapter in enumerate(self._wildcard):
                if adapter == handler:
                    self._wildcard = self._wildcard[:i] + self._wildcard[i + 1 :]
                    self._invalidate()
                    return True
        return False

    def add_subscriber(self, subscriber: FlexibleEventSubscriber) -> None:
        """Register a subscriber for every type it declares via subscribed_to()."""
        for event_type in subscriber.subscribed_to():
            self.add(event_type, subscriber)

    def handlers_for(self, event_type: type[DomainEvent]) -> tuple[HandlerAdapter, ...]:
        """Get all handlers for an event type: specific first, then wildcard.

        The returned tuple is cached and shared. Callers must treat it as
        immutable -- it is the same object across calls until a mutation.
        """
        with self._lock:
            cached = self._combined.get(event_type)
            if cached is not None:
                return cached
            combined = self._specific.get(event_type, ()) + self._wildcard
            self._combined[event_type] = combined
            return combined

    def clear(self) -> None:
        """Remove every registration, specific and wildcard."""
        with self._lock:
            self._specific = {}
            self._wildcard = ()
            self._invalidate()

    def count(self, event_type: type[DomainEvent] | None = None) -> int:
        """Count type-specific registrations. Excludes wildcard handlers.

        Args:
            event_type: If given, count only this type. Otherwise count all.
        """
        with self._lock:
            if event_type is None:
                return sum(len(adapters) for adapters in self._specific.values())
            return len(self._specific.get(event_type, ()))

    def wildcard_count(self) -> int:
        """Count wildcard registrations."""
        with self._lock:
            return len(self._wildcard)


__all__ = ["SubscriptionRegistry"]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/bus/test_registry.py -v --no-cov`
Expected: PASS, all tests including both property tests.

- [ ] **Step 5: Type check and lint**

Run: `uv run mypy src/eventsource/bus/registry.py --config-file=pyproject.toml`
Expected: `Success: no issues found`

Run: `uv run ruff check src/eventsource/bus/registry.py tests/unit/bus/test_registry.py --fix && uv run ruff format src/eventsource/bus/registry.py tests/unit/bus/test_registry.py`
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add src/eventsource/bus/registry.py tests/unit/bus/test_registry.py
git commit -m "feat: add SubscriptionRegistry with property tests

Shared, thread-safe handler storage extracted ahead of wiring it into the
four bus backends. Caches the combined specific+wildcard tuple so dispatch
allocates nothing per event, and compares raw handlers on remove so no
throwaway HandlerAdapter is built per unsubscribe call."
```

---

### Task 3: Extract `RetryPolicy`

Pure addition. Settles the jitter divergence between Kafka and RabbitMQ.

**Files:**
- Create: `src/eventsource/bus/retry.py`
- Test: `tests/unit/bus/test_retry.py`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  ```python
  @dataclass(frozen=True)
  class RetryPolicy:
      base_delay: float = 1.0
      max_delay: float = 60.0
      jitter: float = 0.1
      max_retries: int = 3
      def delay_for(self, retry_count: int) -> float
      def should_retry(self, retry_count: int) -> bool
  ```

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/bus/test_retry.py`:

```python
"""Unit and property tests for RetryPolicy."""

import random

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.bus.retry import RetryPolicy


def test_no_jitter_gives_exact_exponential_backoff() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=60.0, jitter=0.0)

    assert policy.delay_for(0) == 1.0
    assert policy.delay_for(1) == 2.0
    assert policy.delay_for(2) == 4.0
    assert policy.delay_for(3) == 8.0


def test_delay_is_capped_at_max_delay() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=10.0, jitter=0.0)

    assert policy.delay_for(20) == 10.0


def test_jitter_is_symmetric_not_one_sided() -> None:
    """Regression: Kafka previously used one-sided positive jitter, which
    pushed delays above max_delay and never shortened them."""
    policy = RetryPolicy(base_delay=10.0, max_delay=10.0, jitter=0.5)
    rng = random.Random(0)

    samples = [policy.delay_for(0, rng=rng) for _ in range(200)]

    assert min(samples) < 10.0, "jitter never reduces the delay -- it is one-sided"
    assert max(samples) > 10.0, "jitter never increases the delay"


def test_delay_never_goes_negative() -> None:
    policy = RetryPolicy(base_delay=1.0, max_delay=60.0, jitter=1.0)
    rng = random.Random(1234)

    for _ in range(500):
        assert policy.delay_for(0, rng=rng) >= 0.0


def test_should_retry_respects_max_retries() -> None:
    policy = RetryPolicy(max_retries=3)

    assert policy.should_retry(0) is True
    assert policy.should_retry(2) is True
    assert policy.should_retry(3) is False
    assert policy.should_retry(99) is False


def test_negative_retry_count_is_rejected() -> None:
    policy = RetryPolicy()

    with pytest.raises(ValueError, match="retry_count must be >= 0"):
        policy.delay_for(-1)


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"base_delay": 0.0}, "base_delay must be > 0"),
        ({"max_delay": 0.0}, "max_delay must be > 0"),
        ({"base_delay": 10.0, "max_delay": 1.0}, "max_delay must be >= base_delay"),
        ({"jitter": -0.1}, "jitter must be between 0.0 and 1.0"),
        ({"jitter": 1.5}, "jitter must be between 0.0 and 1.0"),
        ({"max_retries": -1}, "max_retries must be >= 0"),
    ],
)
def test_invalid_config_is_rejected(kwargs: dict[str, float], match: str) -> None:
    with pytest.raises(ValueError, match=match):
        RetryPolicy(**kwargs)  # type: ignore[arg-type]


# =============================================================================
# Property tests
# =============================================================================


@given(
    retry_count=st.integers(min_value=0, max_value=64),
    base_delay=st.floats(min_value=0.001, max_value=10.0),
    max_delay=st.floats(min_value=0.001, max_value=3600.0),
    jitter=st.floats(min_value=0.0, max_value=1.0),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_delay_is_finite_non_negative_and_bounded(
    retry_count: int,
    base_delay: float,
    max_delay: float,
    jitter: float,
    seed: int,
) -> None:
    """For any valid policy and retry count, the delay is a sane number.

    The upper bound is max_delay * (1 + jitter): the cap applies to the
    exponential term, and symmetric jitter can push at most `jitter` above it.
    """
    effective_max = max(max_delay, base_delay)
    policy = RetryPolicy(base_delay=base_delay, max_delay=effective_max, jitter=jitter)

    delay = policy.delay_for(retry_count, rng=random.Random(seed))

    assert delay == delay, "delay is NaN"
    assert delay != float("inf")
    assert delay >= 0.0
    assert delay <= effective_max * (1.0 + jitter) + 1e-9


@given(
    n=st.integers(min_value=0, max_value=32),
    base_delay=st.floats(min_value=0.001, max_value=10.0),
    max_delay=st.floats(min_value=0.001, max_value=3600.0),
)
def test_jitter_free_delay_is_non_decreasing(
    n: int, base_delay: float, max_delay: float
) -> None:
    """Without jitter, delay_for is monotonically non-decreasing in retry_count."""
    effective_max = max(max_delay, base_delay)
    policy = RetryPolicy(base_delay=base_delay, max_delay=effective_max, jitter=0.0)

    assert policy.delay_for(n + 1) >= policy.delay_for(n)


@given(
    base_delay=st.floats(min_value=0.001, max_value=10.0),
    max_delay=st.floats(min_value=0.001, max_value=3600.0),
    seed=st.integers(min_value=0, max_value=2**32 - 1),
)
def test_zero_jitter_is_deterministic(
    base_delay: float, max_delay: float, seed: int
) -> None:
    effective_max = max(max_delay, base_delay)
    policy = RetryPolicy(base_delay=base_delay, max_delay=effective_max, jitter=0.0)

    a = policy.delay_for(3, rng=random.Random(seed))
    b = policy.delay_for(3, rng=random.Random(seed + 1))

    assert a == b
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/bus/test_retry.py -v --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'eventsource.bus.retry'`

- [ ] **Step 3: Write the implementation**

Create `src/eventsource/bus/retry.py`:

```python
"""Shared retry policy for event bus implementations.

Kafka and RabbitMQ both implemented exponential backoff with jitter, using
identically-named config fields but different jitter distributions: Kafka
applied one-sided positive jitter (delays only ever grew, and could exceed the
advertised max_delay), RabbitMQ applied symmetric jitter clamped at zero. This
module settles that on the symmetric form.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

# Module-level RNG for retry timing. Not cryptographic -- jitter only needs to
# decorrelate concurrent consumers, not resist prediction.
_DEFAULT_RNG = random.Random()  # nosec B311


@dataclass(frozen=True)
class RetryPolicy:
    """Exponential backoff with symmetric jitter.

    The delay for attempt ``n`` is ``min(base_delay * 2**n, max_delay)``, with
    symmetric jitter of +/- ``jitter`` (as a fraction) applied afterwards and
    the result clamped at zero.

    Attributes:
        base_delay: Delay in seconds for the first retry. Must be > 0.
        max_delay: Ceiling for the exponential term, in seconds. Must be > 0
            and >= base_delay.
        jitter: Fraction of the delay to randomize, 0.0 to 1.0. At 0.1, the
            delay varies by +/-10%.
        max_retries: Number of retries before giving up. 0 means never retry.

    Example:
        >>> policy = RetryPolicy(base_delay=1.0, max_delay=60.0, jitter=0.1)
        >>> policy.delay_for(3)     # ~8s, +/- 10%
        >>> policy.should_retry(3)
        False
    """

    base_delay: float = 1.0
    max_delay: float = 60.0
    jitter: float = 0.1
    max_retries: int = 3

    def __post_init__(self) -> None:
        if self.base_delay <= 0:
            raise ValueError(f"base_delay must be > 0, got {self.base_delay}")
        if self.max_delay <= 0:
            raise ValueError(f"max_delay must be > 0, got {self.max_delay}")
        if self.max_delay < self.base_delay:
            raise ValueError(
                f"max_delay must be >= base_delay, got {self.max_delay} < {self.base_delay}"
            )
        if not 0.0 <= self.jitter <= 1.0:
            raise ValueError(f"jitter must be between 0.0 and 1.0, got {self.jitter}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")

    def delay_for(self, retry_count: int, rng: random.Random | None = None) -> float:
        """Compute the delay in seconds before the next retry.

        Args:
            retry_count: Zero-based attempt number.
            rng: Random source, for deterministic testing. Defaults to a
                module-level RNG.

        Returns:
            Delay in seconds, always >= 0.

        Raises:
            ValueError: If retry_count is negative.
        """
        if retry_count < 0:
            raise ValueError(f"retry_count must be >= 0, got {retry_count}")

        # 2**retry_count overflows to inf for very large counts; cap the
        # exponent so the min() below stays meaningful.
        capped_exponent = min(retry_count, 512)
        delay = min(self.base_delay * (2.0**capped_exponent), self.max_delay)

        if self.jitter > 0:
            source = rng if rng is not None else _DEFAULT_RNG
            jitter_range = delay * self.jitter
            delay = max(0.0, delay + source.uniform(-jitter_range, jitter_range))

        return delay

    def should_retry(self, retry_count: int) -> bool:
        """Whether another retry is permitted after ``retry_count`` attempts."""
        return retry_count < self.max_retries


__all__ = ["RetryPolicy"]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/bus/test_retry.py -v --no-cov`
Expected: PASS.

- [ ] **Step 5: Type check and lint**

Run: `uv run mypy src/eventsource/bus/retry.py --config-file=pyproject.toml`
Expected: `Success: no issues found`

Run: `uv run ruff check src/eventsource/bus/retry.py tests/unit/bus/test_retry.py --fix && uv run ruff format src/eventsource/bus/retry.py tests/unit/bus/test_retry.py`

- [ ] **Step 6: Commit**

```bash
git add src/eventsource/bus/retry.py tests/unit/bus/test_retry.py
git commit -m "feat: add RetryPolicy with property tests

Unifies exponential backoff on symmetric jitter. Kafka's one-sided positive
jitter could push delays past the max_delay it advertised and never shortened
them, so it failed to spread a thundering herd in the direction that relieves
the broker."
```

---

### Task 4: Add `BaseEventBus`

Pure addition. No backend inherits it yet.

**Files:**
- Create: `src/eventsource/bus/base.py`
- Test: `tests/unit/bus/test_base.py`

**Interfaces:**
- Consumes: `SubscriptionRegistry` (Task 2).
- Produces:
  ```python
  class BaseEventBus(EventBus):
      def __init__(self, *, event_registry: EventRegistry | None = None) -> None
      # concrete: subscribe, unsubscribe, subscribe_all,
      #           subscribe_to_all_events, unsubscribe_from_all_events,
      #           clear_subscribers, get_subscriber_count,
      #           get_wildcard_subscriber_count
      def _handlers_for(self, event_type: type[DomainEvent]) -> tuple[HandlerAdapter, ...]
      def _resolve_event_class(self, event_type_name: str) -> type[DomainEvent] | None
      def _track_background(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]
      async def _drain_background(self, timeout: float = 30.0) -> None
      def get_background_task_count(self) -> int
  ```

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/bus/test_base.py`:

```python
"""Unit tests for BaseEventBus."""

import asyncio

from eventsource.bus.base import BaseEventBus
from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry


class BaseBusEvent(DomainEvent):
    event_type: str = "BaseBusEvent"
    aggregate_type: str = "BaseBus"


class StubBus(BaseEventBus):
    """Minimal concrete bus: records what it was asked to publish."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.published: list[DomainEvent] = []

    async def publish(self, events: list[DomainEvent], background: bool = False) -> None:
        self.published.extend(events)


def _handler(event: DomainEvent) -> None: ...


def test_subscription_methods_delegate_to_the_registry() -> None:
    bus = StubBus()

    bus.subscribe(BaseBusEvent, _handler)
    assert bus.get_subscriber_count(BaseBusEvent) == 1
    assert bus.get_subscriber_count() == 1

    bus.subscribe_to_all_events(_handler)
    assert bus.get_wildcard_subscriber_count() == 1

    assert bus.unsubscribe(BaseBusEvent, _handler) is True
    assert bus.unsubscribe(BaseBusEvent, _handler) is False
    assert bus.unsubscribe_from_all_events(_handler) is True
    assert bus.unsubscribe_from_all_events(_handler) is False


def test_clear_subscribers_empties_both_registries() -> None:
    bus = StubBus()
    bus.subscribe(BaseBusEvent, _handler)
    bus.subscribe_to_all_events(_handler)

    bus.clear_subscribers()

    assert bus.get_subscriber_count() == 0
    assert bus.get_wildcard_subscriber_count() == 0


def test_subscribe_all_uses_subscribed_to() -> None:
    class Subscriber:
        def subscribed_to(self) -> list[type[DomainEvent]]:
            return [BaseBusEvent]

        async def handle(self, event: DomainEvent) -> None: ...

    bus = StubBus()
    bus.subscribe_all(Subscriber())

    assert bus.get_subscriber_count(BaseBusEvent) == 1


def test_resolve_event_class_uses_injected_registry_first() -> None:
    registry = EventRegistry()
    registry.register(BaseBusEvent, "CustomName")
    bus = StubBus(event_registry=registry)

    assert bus._resolve_event_class("CustomName") is BaseBusEvent


def test_resolve_event_class_falls_back_to_default_registry() -> None:
    bus = StubBus()

    assert bus._resolve_event_class("BaseBusEvent") is BaseBusEvent


def test_resolve_event_class_returns_none_for_unknown_type() -> None:
    bus = StubBus()

    assert bus._resolve_event_class("NoSuchEventTypeAnywhere") is None


async def test_track_background_runs_and_is_drained() -> None:
    bus = StubBus()
    ran = asyncio.Event()

    async def work() -> None:
        ran.set()

    bus._track_background(work())
    assert bus.get_background_task_count() == 1

    await bus._drain_background(timeout=5.0)

    assert ran.is_set()
    assert bus.get_background_task_count() == 0


async def test_drain_background_with_no_tasks_returns_immediately() -> None:
    bus = StubBus()

    await bus._drain_background(timeout=5.0)

    assert bus.get_background_task_count() == 0


async def test_background_task_failure_does_not_propagate() -> None:
    bus = StubBus()

    async def boom() -> None:
        raise ValueError("background failure")

    bus._track_background(boom())
    await bus._drain_background(timeout=5.0)

    assert bus.get_background_task_count() == 0


async def test_drain_cancels_tasks_that_exceed_the_timeout() -> None:
    bus = StubBus()

    async def slow() -> None:
        await asyncio.sleep(30)

    task = bus._track_background(slow())
    await bus._drain_background(timeout=0.05)

    assert task.cancelled() or task.done()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/bus/test_base.py -v --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'eventsource.bus.base'`

- [ ] **Step 3: Write the implementation**

Create `src/eventsource/bus/base.py`:

```python
"""Concrete base class shared by all EventBus implementations.

All four backends keep their subscribers in process memory, resolve event
classes by name from a registry, and spawn fire-and-forget tasks they must
drain on shutdown. That shared behavior lives here, so ``interface.py`` can
stay a pure ABC that third parties may implement directly.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any

from eventsource.bus.interface import EventBus, EventHandlerFunc
from eventsource.bus.registry import SubscriptionRegistry
from eventsource.events.base import DomainEvent
from eventsource.events.registry import EventRegistry
from eventsource.handlers.adapter import HandlerAdapter
from eventsource.protocols import FlexibleEventHandler, FlexibleEventSubscriber

logger = logging.getLogger(__name__)


class BaseEventBus(EventBus):
    """Base class providing subscription management and background tasks.

    Subclasses implement ``publish`` and their own transport lifecycle. They
    must call ``super().__init__()``.

    Thread Safety:
        Subscription methods are thread-safe via SubscriptionRegistry.
        Publishing must be called from an async context.
    """

    def __init__(self, *, event_registry: EventRegistry | None = None) -> None:
        """Initialize shared bus state.

        Args:
            event_registry: Registry used to resolve event classes by name
                when consuming. Falls back to the global default registry.
        """
        self._registry = SubscriptionRegistry()
        self._event_registry = event_registry
        self._background_tasks: set[asyncio.Task[None]] = set()

    # =========================================================================
    # Subscription management
    # =========================================================================

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        adapter = self._registry.add(event_type, handler)
        logger.info(
            f"Registered handler {adapter.name} for {event_type.__name__}",
            extra={"handler": adapter.name, "event_type": event_type.__name__},
        )

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        removed = self._registry.remove(event_type, handler)
        logger.info(
            f"Unsubscribe from {event_type.__name__}: "
            f"{'removed' if removed else 'handler not found'}",
            extra={"event_type": event_type.__name__, "removed": removed},
        )
        return removed

    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
        self._registry.add_subscriber(subscriber)

    def subscribe_to_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        adapter = self._registry.add_wildcard(handler)
        logger.info(
            f"Registered wildcard handler {adapter.name}",
            extra={"handler": adapter.name},
        )

    def unsubscribe_from_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        removed = self._registry.remove_wildcard(handler)
        logger.info(
            f"Wildcard unsubscribe: {'removed' if removed else 'handler not found'}",
            extra={"removed": removed},
        )
        return removed

    def clear_subscribers(self) -> None:
        """Remove all subscribers. Useful for testing and reinitialization."""
        self._registry.clear()
        logger.info("All event subscribers cleared")

    def get_subscriber_count(self, event_type: type[DomainEvent] | None = None) -> int:
        """Count type-specific subscribers. Excludes wildcard subscribers."""
        return self._registry.count(event_type)

    def get_wildcard_subscriber_count(self) -> int:
        """Count wildcard subscribers."""
        return self._registry.wildcard_count()

    def _handlers_for(self, event_type: type[DomainEvent]) -> tuple[HandlerAdapter, ...]:
        """Get the cached specific-then-wildcard handler tuple for dispatch."""
        return self._registry.handlers_for(event_type)

    # =========================================================================
    # Event class resolution
    # =========================================================================

    def _resolve_event_class(self, event_type_name: str) -> type[DomainEvent] | None:
        """Resolve an event class by name, for deserializing consumed messages.

        Args:
            event_type_name: Registered name of the event class.

        Returns:
            The event class, or None if not registered.
        """
        if self._event_registry is not None:
            return self._event_registry.get_or_none(event_type_name)

        from eventsource.events.registry import default_registry

        return default_registry.get_or_none(event_type_name)

    # =========================================================================
    # Background task management
    # =========================================================================

    def _track_background(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        """Schedule a coroutine as a tracked fire-and-forget task.

        Tracking prevents orphaned coroutines and lets ``_drain_background``
        wait for in-flight work during shutdown.

        Args:
            coro: The coroutine to run.

        Returns:
            The created task.
        """
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._on_background_task_done)
        return task

    def _on_background_task_done(self, task: asyncio.Task[None]) -> None:
        """Discard a finished task and log any unexpected failure."""
        self._background_tasks.discard(task)
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(f"Background task failed: {exc}", exc_info=exc)

    def get_background_task_count(self) -> int:
        """Number of background tasks currently in flight."""
        return len(self._background_tasks)

    async def _drain_background(self, timeout: float = 30.0) -> None:
        """Wait for background tasks to finish, cancelling any stragglers.

        Args:
            timeout: Seconds to wait before cancelling remaining tasks.
        """
        pending = list(self._background_tasks)
        if not pending:
            return

        logger.info(f"Draining {len(pending)} background task(s)")
        try:
            _done, remaining = await asyncio.wait(
                pending, timeout=timeout, return_when=asyncio.ALL_COMPLETED
            )
            if remaining:
                logger.warning(
                    f"{len(remaining)} background task(s) did not complete within "
                    f"{timeout}s; cancelling",
                    extra={"remaining_tasks": len(remaining)},
                )
                for task in remaining:
                    task.cancel()
                await asyncio.gather(*remaining, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error draining background tasks: {e}", exc_info=True)


__all__ = ["BaseEventBus"]
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/bus/test_base.py -v --no-cov`
Expected: PASS.

If `test_resolve_event_class_uses_injected_registry_first` fails, check `EventRegistry.register`'s signature with `grep -n -A10 "def register" src/eventsource/events/registry.py` and adjust the call — the name argument may be keyword-only.

- [ ] **Step 5: Type check and lint**

Run: `uv run mypy src/eventsource/bus/base.py --config-file=pyproject.toml`
Expected: `Success: no issues found`

Run: `uv run ruff check src/eventsource/bus/base.py tests/unit/bus/test_base.py --fix && uv run ruff format src/eventsource/bus/base.py tests/unit/bus/test_base.py`

- [ ] **Step 6: Commit**

```bash
git add src/eventsource/bus/base.py tests/unit/bus/test_base.py
git commit -m "feat: add BaseEventBus with shared subscription and task tracking

Concrete base between the EventBus ABC and the four backends. Owns the
subscription registry, event class resolution, and background task tracking
that all four currently duplicate. The ABC stays pure so third parties can
still implement EventBus directly."
```

---

### Task 5: Migrate `InMemoryEventBus` onto `BaseEventBus`

The simplest backend, with the most existing unit coverage to catch regressions.

**Files:**
- Modify: `src/eventsource/bus/memory.py`
- Test: `tests/unit/bus/test_memory.py`, `tests/unit/test_event_bus.py` (existing, must still pass)

**Interfaces:**
- Consumes: `BaseEventBus` (Task 4).
- Produces: `InMemoryEventBus(BaseEventBus)` with its public surface unchanged, minus the now-inherited subscription methods.

- [ ] **Step 1: Establish the regression baseline**

Run: `uv run pytest tests/unit/bus/test_memory.py tests/unit/test_event_bus.py -v --no-cov`
Expected: PASS. Record the test count — it must not drop after the migration.

- [ ] **Step 2: Change the base class and constructor**

In `src/eventsource/bus/memory.py`, replace the imports and `__init__`. Remove `from collections import defaultdict` and `import threading` (no longer used), and remove `FlexibleEventSubscriber` from the protocols import if it becomes unused.

```python
from eventsource.bus.base import BaseEventBus
from eventsource.bus.interface import EventHandlerFunc  # keep if still referenced
```

Change the class declaration:

```python
class InMemoryEventBus(BaseEventBus):
```

Replace `__init__` (currently `memory.py:63-101`) with:

```python
    def __init__(
        self,
        *,
        tracer: Tracer | None = None,
        enable_tracing: bool = True,
        event_registry: EventRegistry | None = None,
    ) -> None:
        """
        Initialize the event bus with empty subscriber registry.

        Args:
            tracer: Optional custom Tracer instance. If not provided, one is
                   created based on enable_tracing setting.
            enable_tracing: If True and OpenTelemetry is available, emit traces.
                          Ignored if tracer is explicitly provided.
            event_registry: Optional registry for resolving event classes.
        """
        super().__init__(event_registry=event_registry)

        self._stats = {
            "events_published": 0,
            "handlers_invoked": 0,
            "handler_errors": 0,
            "background_tasks_created": 0,
            "background_tasks_completed": 0,
        }

        # Track published events for testing purposes.
        # Deprecated -- see RecordingEventBus. Removed in a later task.
        self._published_events: list[DomainEvent] = []
        self._published_lock = threading.RLock()

        self._tracer = tracer or create_tracer(__name__, enable_tracing)
        self._enable_tracing = self._tracer.enabled
```

Add `from eventsource.events.registry import EventRegistry` to the imports, and keep `import threading` for `_published_lock`.

- [ ] **Step 3: Delete the now-inherited methods**

Delete these methods from `memory.py` entirely — `BaseEventBus` provides them:

- `subscribe` (was ~line 308)
- `unsubscribe` (was ~line 335)
- `subscribe_all` (was ~line 378)
- `subscribe_to_all_events` (was ~line 390)
- `unsubscribe_from_all_events` (was ~line 412)
- `clear_subscribers` (was ~line 446)
- `get_subscriber_count` (was ~line 459)
- `get_wildcard_subscriber_count` (was ~line 477)
- `_on_background_task_done` (was ~line 175)
- `get_background_task_count` (was ~line 503)

- [ ] **Step 4: Rewrite the dispatch and publish paths**

Replace `publish`'s background branch (was `memory.py:161-170`) so it uses the inherited tracker:

```python
        if background:
            self._track_background(self._publish_all(events))
            self._stats["background_tasks_created"] += 1
            logger.debug(
                f"Scheduled background publishing of {len(events)} event(s)",
                extra={"event_count": len(events)},
            )
        else:
            await self._publish_all(events)
```

Replace the handler-gathering block in `_dispatch_event` (was `memory.py:211-218`) with the cached lookup:

```python
        event_type = type(event)
        handlers = self._handlers_for(event_type)
```

Update `_invoke_handlers`' signature to accept the tuple:

```python
    async def _invoke_handlers(
        self, handlers: tuple[HandlerAdapter, ...], event: DomainEvent
    ) -> None:
```

Change `_publish_all`'s tracking to use the dedicated lock:

```python
        for event in events:
            with self._published_lock:
                self._published_events.append(event)

            await self._dispatch_event(event)
            self._stats["events_published"] += 1
```

Update `published_events` and `clear_published_events` to use `self._published_lock` instead of `self._lock`.

- [ ] **Step 5: Rewrite `shutdown` to delegate**

Replace `shutdown` (was `memory.py:512-551`) with:

```python
    async def shutdown(self, timeout: float = 30.0) -> None:
        """
        Shutdown the event bus and wait for background tasks to complete.

        Args:
            timeout: Maximum time to wait for tasks to complete in seconds

        Note:
            After shutdown, new publish calls with background=True will still
            create tasks, but those won't be waited for. Call this method
            during application shutdown to ensure all events are processed.
        """
        await self._drain_background(timeout)
        logger.info("Event bus shutdown complete")
```

The `background_tasks_completed` stat was incremented by the deleted `_on_background_task_done`. Preserve it by overriding the hook:

```python
    def _on_background_task_done(self, task: asyncio.Task[None]) -> None:
        super()._on_background_task_done(task)
        self._stats["background_tasks_completed"] += 1
```

- [ ] **Step 6: Run the regression tests**

Run: `uv run pytest tests/unit/bus/test_memory.py tests/unit/test_event_bus.py -v --no-cov`
Expected: PASS, same test count as Step 1.

If a test fails because it reached into `bus._subscribers` or `bus._all_event_handlers` directly, rewrite that test to use the public `get_subscriber_count()` / `get_wildcard_subscriber_count()` API rather than re-adding the attributes. Those attributes are gone by design.

- [ ] **Step 7: Run the full unit suite**

Run: `uv run pytest tests/unit/ -q --no-cov`
Expected: PASS.

- [ ] **Step 8: Type check and lint**

Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml`
Expected: `Success: no issues found`

Run: `uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/`

- [ ] **Step 9: Commit**

```bash
git add src/eventsource/bus/memory.py tests/
git commit -m "refactor: migrate InMemoryEventBus onto BaseEventBus

Deletes the subscription and background-task code now inherited from
BaseEventBus. Dispatch reads the registry's cached handler tuple instead of
copying two lists per event."
```

---

### Task 6: Extend the conformance suite and wire it into `InMemoryEventBus`

**Files:**
- Modify: `src/eventsource/testing/conformance.py`
- Create: `tests/unit/bus/test_conformance.py`

**Interfaces:**
- Consumes: `InMemoryEventBus` (Task 5).
- Produces:
  ```python
  class EventBusConformanceSuite(ABC):
      async def await_delivery(self, bus: EventBus) -> None    # new, overridable
      async def test_background_publish_delivers(self) -> None  # new
      async def test_per_aggregate_ordering(self) -> None       # new
      async def test_subscribe_all_registers_declared_types(self) -> None  # new
      def create_subscriber(self, received: list[DomainEvent]) -> Any       # new abstract
  ```

- [ ] **Step 1: Write the failing conformance subclass**

Create `tests/unit/bus/test_conformance.py`:

```python
"""InMemoryEventBus conformance to the EventBus contract."""

from typing import Any
from uuid import UUID, uuid4

from eventsource.bus.interface import EventBus
from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.testing.conformance import EventBusConformanceSuite


class ConformanceEvent(DomainEvent):
    event_type: str = "ConformanceEvent"
    aggregate_type: str = "Conformance"


class TestInMemoryEventBusConformance(EventBusConformanceSuite):
    """Runs the shared EventBus contract against InMemoryEventBus."""

    def create_bus(self) -> EventBus:
        return InMemoryEventBus()

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        return ConformanceEvent(aggregate_id=aggregate_id)

    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        class Subscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [ConformanceEvent]

            async def handle(self, event: DomainEvent) -> None:
                received.append(event)

        return Subscriber()


def test_suite_is_actually_collected() -> None:
    """Guard against the suite silently going unrun again.

    EventBusConformanceSuite sat unused in the codebase because nothing
    subclassed it. This asserts the subclass exposes every contract test.
    """
    contract_tests = {
        name
        for name in dir(EventBusConformanceSuite)
        if name.startswith("test_")
    }

    assert contract_tests, "conformance suite defines no tests"
    for name in contract_tests:
        assert hasattr(TestInMemoryEventBusConformance, name)

    # Sanity: uuid4 is imported for use by the suite's own helpers.
    assert uuid4() != uuid4()
```

- [ ] **Step 2: Run to verify the new contract tests are missing**

Run: `uv run pytest tests/unit/bus/test_conformance.py -v --no-cov`
Expected: The six existing tests PASS. `create_subscriber` raises `TypeError: Can't instantiate abstract class` only after Step 3 adds it as abstract — at this point the file collects and the six pass, which confirms the wiring works before we extend the suite.

- [ ] **Step 3: Add the delivery hook and the abstract subscriber factory**

In `src/eventsource/testing/conformance.py`, inside `EventBusConformanceSuite`, after `create_test_event`:

```python
    @abstractmethod
    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        """
        Create an EventSubscriber that appends handled events to ``received``.

        The subscriber's ``subscribed_to()`` must return the type produced by
        ``create_test_event``.

        Args:
            received: List the subscriber appends each handled event to.

        Returns:
            An object with ``subscribed_to()`` and ``handle()``.
        """
        pass

    async def await_delivery(self, bus: EventBus) -> None:
        """
        Wait for in-flight deliveries to land.

        Defaults to a no-op, which is correct for in-process buses that
        dispatch synchronously within ``publish``. Distributed backends
        override this with a bounded poll or a consumer drain.

        Args:
            bus: The bus under test.
        """
        return None
```

Add `Any` to the `typing` import at the top of the file if not already present.

- [ ] **Step 4: Add the three new contract tests**

Append these to `EventBusConformanceSuite`, before the module's `__all__`:

```python
    async def test_background_publish_delivers(self) -> None:
        """
        Verify that background publishing still delivers the event.

        ``background=True`` means "do not wait for durability" -- publish
        returns without waiting for the delivery to be confirmed or handled --
        but the event must still arrive.
        """
        bus = self.create_bus()
        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id)

        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(event), handler)

        # Must not raise, and must not block on durability.
        await bus.publish([event], background=True)

        await self.await_delivery(bus)

        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id

    async def test_per_aggregate_ordering(self) -> None:
        """
        Verify that events for one aggregate arrive in publish order.

        Deliberately per-aggregate rather than global: Kafka partitions by
        aggregate_id, so global ordering is not a contract any distributed
        backend can honor.
        """
        bus = self.create_bus()
        aggregate_id = uuid4()
        events = [self.create_test_event(aggregate_id) for _ in range(5)]

        received_events: list[DomainEvent] = []

        async def handler(e: DomainEvent) -> None:
            received_events.append(e)

        bus.subscribe(type(events[0]), handler)
        await bus.publish(events)
        await self.await_delivery(bus)

        assert len(received_events) == len(events)
        assert [e.event_id for e in received_events] == [e.event_id for e in events]

    async def test_subscribe_all_registers_declared_types(self) -> None:
        """
        Verify that subscribe_all registers for every declared event type.

        Tests that a subscriber registered via subscribe_all receives events
        of the types returned by its subscribed_to() method.
        """
        bus = self.create_bus()
        received_events: list[DomainEvent] = []
        subscriber = self.create_subscriber(received_events)

        bus.subscribe_all(subscriber)

        aggregate_id = uuid4()
        event = self.create_test_event(aggregate_id)
        await bus.publish([event])
        await self.await_delivery(bus)

        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id
```

- [ ] **Step 5: Run the conformance tests**

Run: `uv run pytest tests/unit/bus/test_conformance.py -v --no-cov`
Expected: PASS — 9 contract tests plus `test_suite_is_actually_collected`.

- [ ] **Step 6: Type check and lint**

Run: `uv run mypy src/eventsource/ --config-file=pyproject.toml`
Run: `uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/`

- [ ] **Step 7: Commit**

```bash
git add src/eventsource/testing/conformance.py tests/unit/bus/test_conformance.py
git commit -m "test: wire EventBusConformanceSuite into InMemoryEventBus

The suite has existed since it was written and no test ever subclassed it.
Adds an overridable await_delivery hook for eventually-delivering backends
plus three tests covering the newly-explicit contract: background delivery,
per-aggregate ordering, and subscribe_all."
```

---

### Task 7: Add Kafka and RabbitMQ CI services

Stands the broker CI up *before* the broker migration, so the conformance failures that migration surfaces appear in CI rather than only locally.

**Files:**
- Modify: `docker-compose.test.yml`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: a CI job running `-m "kafka or rabbitmq"`. No code symbols.

- [ ] **Step 1: Inspect what the existing tests expect**

```bash
grep -rn "KAFKA_BOOTSTRAP\|RABBITMQ_URL\|amqp://\|localhost:9092\|bootstrap_servers" \
  tests/conftest.py tests/integration/conftest.py tests/integration/bus/ | head -20
```

Record the exact env var names and default hosts/ports the fixtures use — the compose services must match them. Do not guess.

- [ ] **Step 2: Add the services**

Append to `docker-compose.test.yml` under `services:`, matching the ports found in Step 1 (defaults shown):

```yaml
  rabbitmq:
    image: rabbitmq:3.13-management-alpine
    ports:
      - "5672:5672"
      - "15672:15672"
    environment:
      RABBITMQ_DEFAULT_USER: guest
      RABBITMQ_DEFAULT_PASS: guest
    healthcheck:
      test: ["CMD", "rabbitmq-diagnostics", "-q", "ping"]
      interval: 5s
      timeout: 10s
      retries: 12

  kafka:
    image: bitnami/kafka:3.7
    ports:
      - "9092:9092"
    environment:
      # KRaft mode -- no ZooKeeper needed.
      KAFKA_CFG_NODE_ID: "0"
      KAFKA_CFG_PROCESS_ROLES: "controller,broker"
      KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "0@kafka:9093"
      KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
      KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
      KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
      KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
      ALLOW_PLAINTEXT_LISTENER: "yes"
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 12
```

- [ ] **Step 3: Verify the services come up and the tests can reach them**

```bash
docker compose -f docker-compose.test.yml up -d rabbitmq kafka
docker compose -f docker-compose.test.yml ps
```

Expected: both `healthy`. Then:

```bash
uv run pytest tests/integration/bus/test_rabbitmq.py -m rabbitmq -q --no-cov 2>&1 | tail -30
uv run pytest tests/integration/bus/test_kafka.py -m kafka -q --no-cov 2>&1 | tail -30
```

**These may fail.** They have never run in CI and their state is unknown. Record the failure count and a one-line cause for each distinct failure.

- [ ] **Step 4: Triage what failed**

For each distinct failure, decide:

- **Broken test** (wrong assertion, stale API usage, missing fixture) — fix it here.
- **Real product bug** — fix it here with a failing-test-first commit, per `.claude/rules/definition-of-done.md`.
- **Environment mismatch** (wrong port, missing topic, timing) — fix the compose service or fixture.

If the failure count is large enough that this balloons past roughly a day of work, **stop and report to the user** rather than pressing on or quietly making the job non-blocking. The spec commits to a blocking check; downgrading that is the user's call.

- [ ] **Step 5: Add the CI job**

In `.github/workflows/ci.yml`, add a job modeled on the existing integration job. Use the same `services:` block style already used for postgres/redis at `ci.yml:162`:

```yaml
  # Broker integration tests (Kafka, RabbitMQ)
  broker-tests:
    runs-on: ubuntu-latest
    services:
      rabbitmq:
        image: rabbitmq:3.13-management-alpine
        ports:
          - 5672:5672
        options: >-
          --health-cmd "rabbitmq-diagnostics -q ping"
          --health-interval 5s
          --health-timeout 10s
          --health-retries 12
      kafka:
        image: bitnami/kafka:3.7
        ports:
          - 9092:9092
        env:
          KAFKA_CFG_NODE_ID: "0"
          KAFKA_CFG_PROCESS_ROLES: "controller,broker"
          KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: "0@localhost:9093"
          KAFKA_CFG_LISTENERS: "PLAINTEXT://:9092,CONTROLLER://:9093"
          KAFKA_CFG_ADVERTISED_LISTENERS: "PLAINTEXT://localhost:9092"
          KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
          KAFKA_CFG_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
          KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE: "true"
          ALLOW_PLAINTEXT_LISTENER: "yes"
        options: >-
          --health-cmd "kafka-topics.sh --bootstrap-server localhost:9092 --list || exit 1"
          --health-interval 10s
          --health-timeout 10s
          --health-retries 12
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'
          cache-dependency-path: pyproject.toml

      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install -e ".[dev,all]" --group dev

      - name: Run broker integration tests
        run: pytest -m "kafka or rabbitmq" --no-cov -v
```

Note `--no-cov`: this job runs a marker-selected subset, and `[tool.coverage.report] fail_under` would trip on a partial measurement. The comment at `pyproject.toml:99-104` documents exactly this trap.

- [ ] **Step 6: Push and confirm the job runs green**

```bash
git add docker-compose.test.yml .github/workflows/ci.yml tests/
git commit -m "test: run Kafka and RabbitMQ integration tests in CI

Neither broker had a compose service or a CI job, so 5,329 lines of
integration tests under tests/integration/bus/ had never executed in CI.
Adds KRaft-mode Kafka and RabbitMQ services plus a blocking job."
git push
gh run list --limit 3
```

Expected: `broker-tests` passes. Do not proceed to Task 8 until it does.

- [ ] **Step 7: Tear down local services**

```bash
docker compose -f docker-compose.test.yml down
```

---

### Task 8: Migrate `RedisEventBus` and make it honor `background`

**Files:**
- Modify: `src/eventsource/bus/redis.py`
- Test: `tests/unit/test_redis_event_bus.py`, `tests/integration/bus/test_redis.py`

**Interfaces:**
- Consumes: `BaseEventBus` (Task 4).
- Produces: `RedisEventBus(BaseEventBus)`; `publish(events, background=True)` returns without awaiting the pipeline.

- [ ] **Step 1: Write the failing test for background publish**

Add to `tests/unit/test_redis_event_bus.py`:

```python
async def test_background_publish_returns_before_pipeline_completes(
    mock_redis: Any,
) -> None:
    """background=True must not await the Redis round-trip.

    Regression: RedisEventBus previously documented background as
    "Ignored for Redis" and always awaited.
    """
    import asyncio

    bus = RedisEventBus(config=RedisEventBusConfig(url="redis://localhost:6379"))
    bus._redis = mock_redis
    bus._connected = True

    release = asyncio.Event()

    async def slow_xadd(*args: Any, **kwargs: Any) -> str:
        await release.wait()
        return "1-0"

    mock_redis.xadd = slow_xadd

    event = SampleRedisEvent(aggregate_id=uuid4())

    # Must return promptly even though xadd is blocked.
    await asyncio.wait_for(bus.publish([event], background=True), timeout=1.0)
    assert bus.get_background_task_count() == 1

    release.set()
    await bus._drain_background(timeout=5.0)
    assert bus.get_background_task_count() == 0
```

Match `mock_redis`, `SampleRedisEvent`, and the config construction to the fixtures already in that file — check with `grep -n "def mock_redis\|class Sample\|RedisEventBusConfig(" tests/unit/test_redis_event_bus.py`.

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_redis_event_bus.py::test_background_publish_returns_before_pipeline_completes -v --no-cov`
Expected: FAIL — `asyncio.TimeoutError`, because publish awaits.

- [ ] **Step 3: Change the base class and constructor**

In `src/eventsource/bus/redis.py`:

```python
from eventsource.bus.base import BaseEventBus


class RedisEventBus(BaseEventBus):
```

In `__init__`, add `super().__init__(event_registry=event_registry)` as the first statement and delete these two lines (currently `redis.py:244-245`):

```python
        self._subscribers: dict[type[DomainEvent], list[HandlerAdapter]] = defaultdict(list)
        self._all_event_handlers: list[HandlerAdapter] = []
```

Also delete the now-redundant `self._event_registry = event_registry` assignment if present — `BaseEventBus.__init__` sets it.

- [ ] **Step 4: Delete the inherited methods**

Delete from `redis.py`: `subscribe` (~489), `unsubscribe` (~518), `subscribe_all` (~557), `subscribe_to_all_events` (~568), `unsubscribe_from_all_events` (~590), `clear_subscribers` (~1391), `get_subscriber_count` (~1401), `get_wildcard_subscriber_count` (~1415), and `_get_event_class` (~858).

Replace the one call site of `_get_event_class` in `_deserialize_event` (~851):

```python
        event_class = self._resolve_event_class(event_type_name)
```

- [ ] **Step 5: Use the cached handler tuple in dispatch**

In `_dispatch_event` (~876), replace the handler gathering:

```python
        event_type = type(event)
        handlers = self._handlers_for(event_type)
```

- [ ] **Step 6: Honor `background` in publish**

In `publish` (~368), replace the docstring line `background: Ignored for Redis (Redis is inherently async)` with:

```
            background: If True, return without waiting for the Redis
                       round-trip to complete. The write is tracked and
                       drained by shutdown().
```

And replace the try block body:

```python
            try:
                if background:
                    self._track_background(self._publish_all(events))
                elif len(events) > 1:
                    await self._publish_batch(events)
                else:
                    await self._publish_single(events[0])

                if span:
                    span.set_attribute("publish.success", True)
```

Add the small helper next to `_publish_batch`:

```python
    async def _publish_all(self, events: list[DomainEvent]) -> None:
        """Publish events, choosing single-write or pipeline by count."""
        if len(events) > 1:
            await self._publish_batch(events)
        else:
            await self._publish_single(events[0])
```

- [ ] **Step 7: Drain background writes on shutdown**

In `shutdown` (~1442), add before the existing disconnect logic:

```python
        await self._drain_background(timeout)
```

- [ ] **Step 8: Run the tests**

Run: `uv run pytest tests/unit/test_redis_event_bus.py tests/unit/test_redis_event_bus_tracing.py -v --no-cov`
Expected: PASS, including the new background test.

Any test reaching into `bus._subscribers` must be rewritten to use `get_subscriber_count()`. Do not re-add the attribute.

- [ ] **Step 9: Add the Redis conformance subclass**

Append to `tests/integration/bus/test_redis.py`:

```python
class TestRedisEventBusConformance(EventBusConformanceSuite):
    """Runs the shared EventBus contract against a real Redis."""

    def create_bus(self) -> EventBus:
        bus = RedisEventBus(config=RedisEventBusConfig(url=self.redis_url))
        self._buses.append(bus)
        return bus

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        return TestItemCreated(aggregate_id=aggregate_id)

    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        class Subscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [TestItemCreated]

            async def handle(self, event: DomainEvent) -> None:
                received.append(event)

        return Subscriber()

    async def await_delivery(self, bus: EventBus) -> None:
        """Drain the stream so published events reach handlers."""
        await asyncio.sleep(0.1)
```

The Redis bus consumes via `start_consuming()`, so `await_delivery` alone is not enough — wire `create_bus` to start consumption and stop it in teardown, following the pattern already used by the connection tests in that file (`grep -n "start_consuming" tests/integration/bus/test_redis.py`). Add the imports the subclass needs (`asyncio`, `UUID`, `Any`, `EventBus`, `EventBusConformanceSuite`).

- [ ] **Step 10: Run the Redis integration tests**

```bash
docker compose -f docker-compose.test.yml up -d redis
uv run pytest tests/integration/bus/test_redis.py -m redis -v --no-cov
```

Expected: PASS, including the nine conformance tests.

- [ ] **Step 11: Type check, lint, commit**

```bash
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/
git add src/eventsource/bus/redis.py tests/
git commit -m "refactor: migrate RedisEventBus onto BaseEventBus

Also makes Redis honor the background flag. It previously documented the
parameter as 'Ignored for Redis' and always awaited the round-trip, which
made the ABC's contract untrue for one of four backends."
```

---

### Task 9: Migrate `RabbitMQEventBus`

**Files:**
- Modify: `src/eventsource/bus/rabbitmq.py`
- Test: `tests/unit/test_rabbitmq_event_bus.py`, `tests/unit/bus/test_rabbitmq_tracing.py`, `tests/integration/bus/test_rabbitmq.py`

**Interfaces:**
- Consumes: `BaseEventBus` (Task 4), `RetryPolicy` (Task 3).
- Produces: `RabbitMQEventBus(BaseEventBus)` with `_retry_policy: RetryPolicy`.

- [ ] **Step 1: Write the failing test for policy-backed retry delay**

Add to `tests/unit/test_rabbitmq_event_bus.py`:

```python
def test_retry_delay_comes_from_the_shared_policy() -> None:
    """The bus must delegate backoff to RetryPolicy, not compute it inline."""
    from eventsource.bus.retry import RetryPolicy

    config = RabbitMQEventBusConfig(
        url="amqp://guest:guest@localhost/",
        retry_base_delay=2.0,
        retry_max_delay=30.0,
        retry_jitter=0.0,
        max_retries=5,
    )
    bus = RabbitMQEventBus(config=config)

    assert isinstance(bus._retry_policy, RetryPolicy)
    assert bus._retry_policy.base_delay == 2.0
    assert bus._retry_policy.max_delay == 30.0
    assert bus._retry_policy.jitter == 0.0
    assert bus._retry_policy.max_retries == 5
    assert bus._calculate_retry_delay(2) == 8.0
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/test_rabbitmq_event_bus.py::test_retry_delay_comes_from_the_shared_policy -v --no-cov`
Expected: FAIL — `AttributeError: 'RabbitMQEventBus' object has no attribute '_retry_policy'`

- [ ] **Step 3: Change the base class and constructor**

```python
from eventsource.bus.base import BaseEventBus
from eventsource.bus.retry import RetryPolicy


class RabbitMQEventBus(BaseEventBus):
```

In `__init__`, add as the first statement:

```python
        super().__init__(event_registry=event_registry)
```

Delete these lines (currently `rabbitmq.py:654-655`):

```python
        self._subscribers: dict[type[DomainEvent], list[HandlerAdapter]] = defaultdict(list)
        self._all_event_handlers: list[HandlerAdapter] = []
```

Add the policy:

```python
        self._retry_policy = RetryPolicy(
            base_delay=config.retry_base_delay,
            max_delay=config.retry_max_delay,
            jitter=config.retry_jitter,
            max_retries=config.max_retries,
        )
```

- [ ] **Step 4: Delete the inherited methods and delegate the retry delay**

Delete: `subscribe` (~2286), `unsubscribe` (~2316), `subscribe_all` (~2354), `subscribe_to_all_events` (~2367), `unsubscribe_from_all_events` (~2389), `clear_subscribers` (~2418), `get_subscriber_count` (~2427), `get_wildcard_subscriber_count` (~2441), `_get_event_class` (~2262).

Replace `_calculate_retry_delay` (~1791) with a delegating shim — it stays because it is called from `_handle_failed_message` and is covered by existing tests:

```python
    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate the delay before the next retry.

        Delegates to the shared RetryPolicy so Kafka and RabbitMQ cannot drift
        apart again.

        Args:
            retry_count: Zero-based retry attempt number.

        Returns:
            Delay in seconds, with symmetric jitter applied.
        """
        return self._retry_policy.delay_for(retry_count)
```

Replace the `_get_event_class` call site in `_deserialize_event` (~2262 region) with `self._resolve_event_class(...)`.

- [ ] **Step 5: Use the cached handler tuple in dispatch**

In `_dispatch_event` (~3307), replace the gathering at ~3332:

```python
        handlers = self._handlers_for(event_type)
```

- [ ] **Step 6: Drain background tasks on shutdown**

In `shutdown` (~3867), add `await self._drain_background(timeout)` alongside the existing `_drain_in_flight` call. Keep `_drain_in_flight` — it drains consumer message processing, which is a different concern from publisher background tasks.

- [ ] **Step 7: Run the RabbitMQ unit tests**

Run: `uv run pytest tests/unit/test_rabbitmq_event_bus.py tests/unit/bus/test_rabbitmq_tracing.py -q --no-cov`
Expected: PASS.

This file is 11,663 lines and some tests reach into `_subscribers` directly. For each such test: if the shared registry's property tests in `tests/unit/bus/test_registry.py` already cover the behavior, **delete the test** rather than rewriting it — that duplication is the problem this work exists to remove. If it covers RabbitMQ-specific behavior, rewrite it against the public counting API.

- [ ] **Step 8: Add the RabbitMQ conformance subclass**

Append to `tests/integration/bus/test_rabbitmq.py`, following the fixture and connection-URL pattern already in that file:

```python
class TestRabbitMQEventBusConformance(EventBusConformanceSuite):
    """Runs the shared EventBus contract against a real RabbitMQ."""

    def create_bus(self) -> EventBus:
        bus = RabbitMQEventBus(config=RabbitMQEventBusConfig(url=self.rabbitmq_url))
        self._buses.append(bus)
        return bus

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        return TestItemCreated(aggregate_id=aggregate_id)

    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        class Subscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [TestItemCreated]

            async def handle(self, event: DomainEvent) -> None:
                received.append(event)

        return Subscriber()

    async def await_delivery(self, bus: EventBus) -> None:
        await asyncio.sleep(0.2)
```

As with Redis, the bus must be connected and consuming for delivery to occur. Follow the existing setup/teardown pattern in the file (`grep -n "start_consuming\|async def.*fixture" tests/integration/bus/test_rabbitmq.py`).

- [ ] **Step 9: Run the RabbitMQ integration tests**

```bash
docker compose -f docker-compose.test.yml up -d rabbitmq
uv run pytest tests/integration/bus/test_rabbitmq.py -m rabbitmq -v --no-cov
```

Expected: PASS including conformance.

- [ ] **Step 10: Type check, lint, commit**

```bash
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/
git add src/eventsource/bus/rabbitmq.py tests/
git commit -m "refactor: migrate RabbitMQEventBus onto BaseEventBus and RetryPolicy"
```

---

### Task 10: Migrate `KafkaEventBus`, batch its publishes, and fix the name-keying bug

The largest migration. Kafka is the only backend that keyed handlers by event type *name*.

**Files:**
- Modify: `src/eventsource/bus/kafka.py`
- Test: `tests/unit/test_kafka_event_bus.py`, `tests/integration/bus/test_kafka.py`

**Interfaces:**
- Consumes: `BaseEventBus` (Task 4), `RetryPolicy` (Task 3).
- Produces: `KafkaEventBus(BaseEventBus)`; `get_handlers_for_event(name)` retained as a deprecated shim.

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_kafka_event_bus.py`:

```python
def test_handlers_resolve_when_event_type_field_differs_from_class_name() -> None:
    """Regression: Kafka keyed subscriptions by class __name__ but looked them
    up by the event_type field from the message header. When those differ, the
    handler silently never fired."""

    class RenamedEvent(DomainEvent):
        event_type: str = "renamed.v1"
        aggregate_type: str = "Renamed"

    bus = KafkaEventBus(config=KafkaEventBusConfig(bootstrap_servers="localhost:9092"))

    async def handler(event: DomainEvent) -> None: ...

    bus.subscribe(RenamedEvent, handler)

    assert len(bus._handlers_for(RenamedEvent)) == 1


def test_retry_delay_comes_from_the_shared_policy() -> None:
    from eventsource.bus.retry import RetryPolicy

    config = KafkaEventBusConfig(
        bootstrap_servers="localhost:9092",
        retry_base_delay=2.0,
        retry_max_delay=30.0,
        retry_jitter=0.0,
        max_retries=5,
    )
    bus = KafkaEventBus(config=config)

    assert isinstance(bus._retry_policy, RetryPolicy)
    assert bus._calculate_retry_delay(2) == 8.0


async def test_publish_sends_all_events_before_awaiting_any(
    mock_producer: Any,
) -> None:
    """Regression: publish awaited a full broker round-trip per event."""
    bus = KafkaEventBus(config=KafkaEventBusConfig(bootstrap_servers="localhost:9092"))
    bus._producer = mock_producer
    bus._connected = True

    send_order: list[str] = []

    async def record_send(*args: Any, **kwargs: Any) -> Any:
        send_order.append("send")

        async def result() -> None:
            send_order.append("await")

        return result()

    mock_producer.send = record_send

    events = [SampleKafkaEvent(aggregate_id=uuid4()) for _ in range(3)]
    await bus.publish(events)

    # All three sends happen before any await completes.
    assert send_order[:3] == ["send", "send", "send"]
```

Match `mock_producer` and `SampleKafkaEvent` to the fixtures already in that file (`grep -n "def mock_producer\|class Sample" tests/unit/test_kafka_event_bus.py`). If `_send_to_kafka` uses `send_and_wait` rather than `send`, adjust the test to patch that instead — inspect `kafka.py:1594` first.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/unit/test_kafka_event_bus.py -k "differs_from_class_name or shared_policy or before_awaiting" -v --no-cov`
Expected: FAIL on all three.

- [ ] **Step 3: Change the base class and constructor**

```python
from eventsource.bus.base import BaseEventBus
from eventsource.bus.retry import RetryPolicy


class KafkaEventBus(BaseEventBus):
```

In `__init__`, add `super().__init__(event_registry=event_registry)` first, delete these (currently `kafka.py:1021-1022`):

```python
        self._handlers: dict[str, list[HandlerAdapter]] = defaultdict(list)
        self._wildcard_handlers: list[HandlerAdapter] = []
```

and add:

```python
        self._retry_policy = RetryPolicy(
            base_delay=config.retry_base_delay,
            max_delay=config.retry_max_delay,
            jitter=config.retry_jitter,
            max_retries=config.max_retries,
        )
```

- [ ] **Step 4: Delete the inherited methods**

Delete: `subscribe` (~1820), `unsubscribe` (~1858), `subscribe_all` (~1916), `subscribe_to_all_events` (~1950), `unsubscribe_from_all_events` (~1984), `clear_subscribers` (~2026), `get_subscriber_count` (~2042), `get_wildcard_subscriber_count` (~2064), `_get_event_class` (~2497).

- [ ] **Step 5: Retain `get_handlers_for_event` as a deprecated shim**

It is public API, so replace its body rather than deleting it:

```python
    def get_handlers_for_event(self, event_type_name: str) -> list[HandlerAdapter]:
        """Get all handlers for an event type name.

        Deprecated:
            Handlers are now keyed by event class, not by name. This resolves
            the name through the event registry and returns the handlers for
            the resulting class. Prefer ``_handlers_for(event_type)``.

        Args:
            event_type_name: The registered event type name.

        Returns:
            List of HandlerAdapter instances, type-specific first.
        """
        warnings.warn(
            "get_handlers_for_event is deprecated; handlers are keyed by event "
            "class. Use the event class directly.",
            DeprecationWarning,
            stacklevel=2,
        )
        event_class = self._resolve_event_class(event_type_name)
        if event_class is None:
            return []
        return list(self._handlers_for(event_class))
```

Add `import warnings` to the module imports.

- [ ] **Step 6: Fix the dispatch path to key by class**

In `_process_message_with_span` (~2367), the deserialized `event` is already in scope at the point `get_handlers_for_event(event_type_name)` is called (~2425). Replace that call:

```python
            handlers = self._handlers_for(type(event))
```

Update `_dispatch_to_handlers`' signature (~2554) to accept `tuple[HandlerAdapter, ...]`.

- [ ] **Step 7: Delegate the retry delay**

Replace `_calculate_retry_delay` (~2860):

```python
    def _calculate_retry_delay(self, retry_count: int) -> float:
        """Calculate delay for retry with exponential backoff and jitter.

        Delegates to the shared RetryPolicy. Note this changes Kafka's jitter
        from one-sided positive to symmetric, so effective backoff is slightly
        shorter and no longer exceeds retry_max_delay.

        Args:
            retry_count: The current retry attempt (0-based).

        Returns:
            Delay in seconds before next retry.
        """
        return self._retry_policy.delay_for(retry_count)
```

Remove the now-unused `import random` if nothing else in the module uses it.

- [ ] **Step 8: Batch the publishes**

Replace the serial loop in `publish` (~1524):

```python
        for event in events:
            await self._publish_single_event(event, background)
```

with a send-all-then-await:

```python
        # Hand every event to the producer before awaiting any acknowledgment.
        # Publishing serially cost one full broker round-trip per event.
        sends = [self._publish_single_event(event, background) for event in events]
        await asyncio.gather(*sends)
```

This preserves per-aggregate ordering because the partition key remains `aggregate_id` and aiokafka maintains per-partition send order. Confirm `_publish_single_event` does not itself await `send_and_wait` when `background=True` — inspect `_send_to_kafka` (~1594) and, if it does, thread the `background` flag through so the ack await is skipped.

- [ ] **Step 9: Drain background tasks on shutdown**

In `shutdown` (~1229), add `await self._drain_background(timeout or self._config.shutdown_timeout)`. Replace any use of the deleted `_track_background_publish` (~1680) with the inherited `_track_background`.

- [ ] **Step 10: Run the Kafka unit tests**

Run: `uv run pytest tests/unit/test_kafka_event_bus.py -q --no-cov`
Expected: PASS, including the three new tests.

Tests reaching into `_handlers` or `_wildcard_handlers` must be rewritten against `get_subscriber_count()` / `_handlers_for()`, or deleted where `tests/unit/bus/test_registry.py` already covers the behavior.

- [ ] **Step 11: Add the Kafka conformance subclass**

Append to `tests/integration/bus/test_kafka.py`, following the existing fixture pattern:

```python
class TestKafkaEventBusConformance(EventBusConformanceSuite):
    """Runs the shared EventBus contract against a real Kafka."""

    def create_bus(self) -> EventBus:
        bus = KafkaEventBus(
            config=KafkaEventBusConfig(bootstrap_servers=self.bootstrap_servers)
        )
        self._buses.append(bus)
        return bus

    def create_test_event(self, aggregate_id: UUID) -> DomainEvent:
        return TestItemCreated(aggregate_id=aggregate_id)

    def create_subscriber(self, received: list[DomainEvent]) -> Any:
        class Subscriber:
            def subscribed_to(self) -> list[type[DomainEvent]]:
                return [TestItemCreated]

            async def handle(self, event: DomainEvent) -> None:
                received.append(event)

        return Subscriber()

    async def await_delivery(self, bus: EventBus) -> None:
        """Kafka consumption is genuinely eventual; poll rather than sleep once."""
        await asyncio.sleep(0.5)
```

- [ ] **Step 12: Run the Kafka integration tests**

```bash
docker compose -f docker-compose.test.yml up -d kafka
uv run pytest tests/integration/bus/test_kafka.py -m kafka -v --no-cov
```

Expected: PASS including conformance.

- [ ] **Step 13: Type check, lint, commit**

```bash
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/
git add src/eventsource/bus/kafka.py tests/
git commit -m "refactor: migrate KafkaEventBus onto BaseEventBus and RetryPolicy

Fixes a latent dispatch bug: subscriptions were keyed by event class
__name__ but looked up by the event_type field from the message header, so a
class whose event_type differs from its class name never received events.
Handlers are now keyed by class throughout.

Also batches publishes -- the previous loop awaited a full broker round-trip
per event -- and adopts symmetric retry jitter."
```

---

### Task 11: Serialization and error-isolation property tests

**Files:**
- Create: `tests/unit/bus/test_serialization_properties.py`
- Create: `tests/unit/bus/test_error_isolation_properties.py`

**Interfaces:**
- Consumes: all four migrated backends.
- Produces: no source symbols; test coverage only.

- [ ] **Step 1: Write the serialization roundtrip property test**

Create `tests/unit/bus/test_serialization_properties.py`:

```python
"""Roundtrip properties for each backend's serialize/deserialize pair.

Redis, Kafka, and RabbitMQ each implement serialization independently. These
properties assert the three agree on the one thing that matters: an event
survives the round trip unchanged.
"""

from datetime import UTC, datetime
from uuid import UUID, uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource import REDIS_AVAILABLE
from eventsource.events.base import DomainEvent


class RoundtripEvent(DomainEvent):
    event_type: str = "RoundtripEvent"
    aggregate_type: str = "Roundtrip"
    name: str = ""
    quantity: int = 0
    ratio: float = 0.0
    flag: bool = False


_events = st.builds(
    RoundtripEvent,
    aggregate_id=st.uuids(),
    name=st.text(max_size=200),
    quantity=st.integers(min_value=-(2**31), max_value=2**31),
    ratio=st.floats(allow_nan=False, allow_infinity=False, width=32),
    flag=st.booleans(),
)


@pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis not installed")
@given(event=_events)
def test_redis_serialization_roundtrip_is_identity(event: RoundtripEvent) -> None:
    from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

    bus = RedisEventBus(config=RedisEventBusConfig(url="redis://localhost:6379"))

    wire = bus._serialize_event(event)
    restored = bus._deserialize_event(event.event_type, wire)

    assert restored is not None
    assert restored.event_id == event.event_id
    assert restored.aggregate_id == event.aggregate_id
    assert restored.name == event.name
    assert restored.quantity == event.quantity
    assert restored.ratio == event.ratio
    assert restored.flag == event.flag


@pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis not installed")
@given(event=_events)
def test_redis_payload_is_authoritative_not_the_flat_fields(
    event: RoundtripEvent,
) -> None:
    """The flat top-level fields are write-only index columns.

    tenant_id is written as "" when None, which would be wrong if anything
    read it back. Deserialization must use `payload` alone.
    """
    from eventsource.bus.redis import RedisEventBus, RedisEventBusConfig

    bus = RedisEventBus(config=RedisEventBusConfig(url="redis://localhost:6379"))

    wire = bus._serialize_event(event)
    wire["tenant_id"] = "garbage-not-a-uuid"
    wire["aggregate_type"] = "WrongType"

    restored = bus._deserialize_event(event.event_type, wire)

    assert restored is not None
    assert restored.tenant_id == event.tenant_id
    assert restored.aggregate_type == event.aggregate_type
```

Add equivalent Kafka and RabbitMQ blocks guarded by `KAFKA_AVAILABLE` and `RABBITMQ_AVAILABLE`. Check each backend's exact serialize/deserialize signatures first — RabbitMQ's `_serialize_event` returns a `tuple[bytes, dict]` and its `_deserialize_event` takes different arguments than Redis's. Read `rabbitmq.py:2059` and `rabbitmq.py:2194`, and `kafka.py:1773` and `kafka.py:2471`, and match them exactly.

- [ ] **Step 2: Run the serialization properties**

Run: `uv run pytest tests/unit/bus/test_serialization_properties.py -v --no-cov`
Expected: PASS. If the Redis roundtrip fails on `ratio`, the payload is JSON and float precision is exact for `width=32` floats — investigate rather than loosening the assertion.

- [ ] **Step 3: Write the error-isolation property test**

Create `tests/unit/bus/test_error_isolation_properties.py`:

```python
"""Handler error isolation, as a property over arbitrary failure subsets."""

from hypothesis import given
from hypothesis import settings
from hypothesis import strategies as st

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent


class IsolationEvent(DomainEvent):
    event_type: str = "IsolationEvent"
    aggregate_type: str = "Isolation"


@given(failing=st.lists(st.booleans(), min_size=1, max_size=12))
@settings(deadline=None)
async def test_failing_handlers_never_starve_the_others(failing: list[bool]) -> None:
    """For any subset of handlers that raise, the rest still receive the event
    and the bus records exactly one error per failing handler."""
    bus = InMemoryEventBus()
    succeeded: list[int] = []

    for index, should_fail in enumerate(failing):

        def make(idx: int, fails: bool):
            async def handler(event: DomainEvent) -> None:
                if fails:
                    raise ValueError(f"handler {idx} failed")
                succeeded.append(idx)

            return handler

        bus.subscribe(IsolationEvent, make(index, should_fail))

    # Must not raise, regardless of how many handlers fail.
    await bus.publish([IsolationEvent(aggregate_id=uuid4())])

    expected_ok = [i for i, fails in enumerate(failing) if not fails]
    assert sorted(succeeded) == expected_ok

    stats = bus.get_stats()
    assert stats["handler_errors"] == sum(failing)
    assert stats["handlers_invoked"] == len(expected_ok)
```

Add `from uuid import uuid4` to the imports.

- [ ] **Step 4: Run the error-isolation properties**

Run: `uv run pytest tests/unit/bus/test_error_isolation_properties.py -v --no-cov`
Expected: PASS.

Hypothesis and async tests can interact badly with function-scoped fixtures. If you hit `hypothesis.errors.InvalidArgument` about function-scoped fixtures, note that this test creates its bus inline rather than via a fixture specifically to avoid that.

- [ ] **Step 5: Lint and commit**

```bash
uv run ruff check tests/ --fix && uv run ruff format tests/
git add tests/unit/bus/test_serialization_properties.py tests/unit/bus/test_error_isolation_properties.py
git commit -m "test: add serialization roundtrip and error isolation properties"
```

---

### Task 12: `RecordingEventBus` and the `published_events` deprecation

**Files:**
- Create: `src/eventsource/testing/recording.py`
- Create: `tests/unit/testing/test_recording.py`
- Modify: `src/eventsource/bus/memory.py`
- Modify: `src/eventsource/testing/__init__.py`, `src/eventsource/__init__.py`

**Interfaces:**
- Consumes: `EventBus`, `InMemoryEventBus`.
- Produces:
  ```python
  class RecordingEventBus(EventBus):
      def __init__(self, wrapped: EventBus, *, max_events: int | None = 10_000) -> None
      @property
      def published_events(self) -> list[DomainEvent]
      def clear_published_events(self) -> None
  ```

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/testing/test_recording.py`:

```python
"""Unit tests for RecordingEventBus."""

import warnings
from uuid import uuid4

from eventsource.bus.memory import InMemoryEventBus
from eventsource.events.base import DomainEvent
from eventsource.testing.recording import RecordingEventBus


class RecordedEvent(DomainEvent):
    event_type: str = "RecordedEvent"
    aggregate_type: str = "Recorded"


async def test_records_published_events_and_forwards_them() -> None:
    inner = InMemoryEventBus()
    bus = RecordingEventBus(inner)

    received: list[DomainEvent] = []

    async def handler(event: DomainEvent) -> None:
        received.append(event)

    bus.subscribe(RecordedEvent, handler)
    event = RecordedEvent(aggregate_id=uuid4())
    await bus.publish([event])

    assert [e.event_id for e in bus.published_events] == [event.event_id]
    assert [e.event_id for e in received] == [event.event_id]


async def test_published_events_returns_a_copy() -> None:
    bus = RecordingEventBus(InMemoryEventBus())
    await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    snapshot = bus.published_events
    snapshot.clear()

    assert len(bus.published_events) == 1


async def test_clear_published_events_empties_the_log() -> None:
    bus = RecordingEventBus(InMemoryEventBus())
    await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    bus.clear_published_events()

    assert bus.published_events == []


async def test_max_events_bounds_memory() -> None:
    """The unbounded list on InMemoryEventBus leaked in long-lived processes."""
    bus = RecordingEventBus(InMemoryEventBus(), max_events=3)

    for _ in range(10):
        await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    assert len(bus.published_events) == 3


async def test_max_events_none_means_unbounded() -> None:
    bus = RecordingEventBus(InMemoryEventBus(), max_events=None)

    for _ in range(50):
        await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    assert len(bus.published_events) == 50


async def test_in_memory_published_events_warns_but_still_works() -> None:
    bus = InMemoryEventBus()
    await bus.publish([RecordedEvent(aggregate_id=uuid4())])

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        events = bus.published_events

    assert len(events) == 1
    assert any(issubclass(w.category, DeprecationWarning) for w in caught)
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/testing/test_recording.py -v --no-cov`
Expected: FAIL — `ModuleNotFoundError: No module named 'eventsource.testing.recording'`

Create `tests/unit/testing/__init__.py` (empty) if the directory does not exist.

- [ ] **Step 3: Write `RecordingEventBus`**

Create `src/eventsource/testing/recording.py`:

```python
"""Event bus decorator that records what was published, for tests.

InMemoryEventBus used to carry this itself, in an unbounded list that never
got trimmed -- a test affordance living in production code, and a memory leak
proportional to total events published in any long-lived process.
"""

from __future__ import annotations

import threading
from collections import deque

from eventsource.bus.interface import EventBus, EventHandlerFunc
from eventsource.events.base import DomainEvent
from eventsource.protocols import FlexibleEventHandler, FlexibleEventSubscriber


class RecordingEventBus(EventBus):
    """Wraps any EventBus and records the events published through it.

    Args:
        wrapped: The bus to delegate to.
        max_events: Maximum events to retain; oldest are dropped past this.
            None retains everything -- only safe for short-lived tests.

    Example:
        >>> bus = RecordingEventBus(InMemoryEventBus())
        >>> await bus.publish([OrderCreated(...)])
        >>> assert len(bus.published_events) == 1
    """

    def __init__(self, wrapped: EventBus, *, max_events: int | None = 10_000) -> None:
        self._wrapped = wrapped
        self._lock = threading.RLock()
        self._published: deque[DomainEvent] = deque(maxlen=max_events)

    @property
    def wrapped(self) -> EventBus:
        """The underlying bus."""
        return self._wrapped

    @property
    def published_events(self) -> list[DomainEvent]:
        """A copy of the recorded events, in publication order."""
        with self._lock:
            return list(self._published)

    def clear_published_events(self) -> None:
        """Discard the recorded events."""
        with self._lock:
            self._published.clear()

    async def publish(
        self,
        events: list[DomainEvent],
        background: bool = False,
    ) -> None:
        with self._lock:
            self._published.extend(events)
        await self._wrapped.publish(events, background=background)

    def subscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        self._wrapped.subscribe(event_type, handler)

    def unsubscribe(
        self,
        event_type: type[DomainEvent],
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        return self._wrapped.unsubscribe(event_type, handler)

    def subscribe_all(self, subscriber: FlexibleEventSubscriber) -> None:
        self._wrapped.subscribe_all(subscriber)

    def subscribe_to_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> None:
        self._wrapped.subscribe_to_all_events(handler)

    def unsubscribe_from_all_events(
        self,
        handler: FlexibleEventHandler | EventHandlerFunc,
    ) -> bool:
        return self._wrapped.unsubscribe_from_all_events(handler)


__all__ = ["RecordingEventBus"]
```

- [ ] **Step 4: Deprecate the `InMemoryEventBus` accessors**

In `src/eventsource/bus/memory.py`, add `import warnings` and change the property and clear method to warn while still working:

```python
    @property
    def published_events(self) -> list[DomainEvent]:
        """
        Get a copy of all events published through this bus.

        Deprecated:
            Use ``RecordingEventBus`` from ``eventsource.testing`` instead.
            This list is unbounded and leaks in long-lived processes. It will
            be removed in a future release.
        """
        warnings.warn(
            "InMemoryEventBus.published_events is deprecated and will be "
            "removed; wrap the bus in eventsource.testing.RecordingEventBus.",
            DeprecationWarning,
            stacklevel=2,
        )
        with self._published_lock:
            return list(self._published_events)

    def clear_published_events(self) -> None:
        """
        Clear the list of published events.

        Deprecated:
            Use ``RecordingEventBus`` from ``eventsource.testing`` instead.
        """
        warnings.warn(
            "InMemoryEventBus.clear_published_events is deprecated; wrap the "
            "bus in eventsource.testing.RecordingEventBus.",
            DeprecationWarning,
            stacklevel=2,
        )
        with self._published_lock:
            self._published_events.clear()
```

- [ ] **Step 5: Export the new names**

In `src/eventsource/testing/__init__.py`, add `from eventsource.testing.recording import RecordingEventBus` and `"RecordingEventBus"` to `__all__`.

In `src/eventsource/__init__.py`, export `SubscriptionRegistry`, `RetryPolicy`, `BaseEventBus`, and `RecordingEventBus`, each with an `__all__` entry, following the existing import grouping.

- [ ] **Step 6: Run the tests**

Run: `uv run pytest tests/unit/testing/test_recording.py -v --no-cov`
Expected: PASS.

Run: `uv run pytest tests/unit/ -q --no-cov -W "ignore::DeprecationWarning"`
Expected: PASS. Then run without the filter and update any internal test or helper still using `bus.published_events` to use `RecordingEventBus` instead, so the suite is warning-clean:

Run: `uv run pytest tests/unit/ -q --no-cov -W "error::DeprecationWarning"`
Expected: PASS once all internal callers are migrated.

- [ ] **Step 7: Verify the public API imports**

```bash
uv run python -c "
from eventsource import BaseEventBus, RetryPolicy, SubscriptionRegistry, RecordingEventBus
print('exports OK')
"
```

Expected: `exports OK`

- [ ] **Step 8: Type check, lint, commit**

```bash
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run ruff check src/ tests/ --fix && uv run ruff format src/ tests/
git add src/eventsource/ tests/
git commit -m "feat: add RecordingEventBus and deprecate InMemoryEventBus.published_events

published_events was an unbounded list living in production code purely to
serve tests, leaking memory proportional to events published. The decorator
replaces it with a bounded recorder; the old accessors warn and still work
for one release."
```

---

### Task 13: Correct the ABC docstrings

The contract is now true. Make `interface.py` say so.

**Files:**
- Modify: `src/eventsource/bus/interface.py`

**Interfaces:**
- Consumes: everything above.
- Produces: no new symbols.

- [ ] **Step 1: Correct the `background` documentation**

In `EventBus.publish`'s docstring, replace the `background` argument description:

```
            background: If True, return as soon as the events are handed off,
                       without waiting for delivery to be confirmed or handled.
                       Improves response times at the cost of eventual
                       consistency -- a read immediately after publishing may
                       not observe the event.

                       Backends realize this differently: InMemory dispatches
                       in a background task, Redis defers the stream write,
                       Kafka skips the broker acknowledgment, and RabbitMQ
                       skips the publisher confirm. In every case the event is
                       still delivered.
```

- [ ] **Step 2: Correct the thread-safety claim**

Replace the class docstring line `Implementations must be thread-safe and support both synchronous and asynchronous handlers.` with:

```
    Subscription management (subscribe, unsubscribe, and their wildcard
    counterparts) is thread-safe in all bundled implementations, which inherit
    it from ``BaseEventBus``. Publishing must be called from an async context.
    Implementations support both sync and async handlers.
```

- [ ] **Step 3: Point implementers at the base class**

Add to the `See Also` block:

```
        - ``BaseEventBus`` - Concrete base providing subscription management,
          background task tracking, and event class resolution. Prefer
          subclassing it over implementing this ABC directly.
        - ``EventBusConformanceSuite`` - Contract tests every implementation
          should subclass.
```

- [ ] **Step 4: Verify docs build and nothing else broke**

Run: `uv run pytest tests/unit/ -q --no-cov`
Expected: PASS.

Run: `uv run ruff format src/eventsource/bus/interface.py --check`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/eventsource/bus/interface.py
git commit -m "docs: correct the EventBus background and thread-safety contract

The ABC claimed thread safety no implementation but InMemory provided, and
described a background parameter that meant four different things. Both are
now true statements about the shipped behavior."
```

---

### Task 14: Mutation targets, coverage floor, changelog, version bump

**Files:**
- Modify: `pyproject.toml`
- Modify: `CHANGELOG.md` (check the actual filename first)
- Modify: `docs/development/mutation-testing.md`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: no code symbols.

- [ ] **Step 1: Add the three mutation targets**

In `pyproject.toml`, extend `[tool.mutmut]`:

```toml
[tool.mutmut]
source_paths = [
    "src",
]
only_mutate = [
    "src/eventsource/engine.py",
    "src/eventsource/repositories/_dialect.py",
    "src/eventsource/serialization/json.py",
    "src/eventsource/bus/registry.py",
    "src/eventsource/bus/retry.py",
    "src/eventsource/bus/base.py",
]
pytest_add_cli_args_test_selection = [
    "tests/unit/test_engine.py",
    "tests/unit/repositories/test_dialect.py",
    "tests/unit/serialization/",
    "tests/unit/bus/test_registry.py",
    "tests/unit/bus/test_retry.py",
    "tests/unit/bus/test_base.py",
]
pytest_add_cli_args = ["--no-cov", "-x", "-q", "-p", "no:randomly"]
```

Do NOT add `bus/memory.py`, `bus/kafka.py`, or `bus/rabbitmq.py`. The runtime rationale in `cosmic-ray/engine.toml` argues for adding targets one at a time and measuring.

- [ ] **Step 2: Run mutation testing on the new modules**

```bash
uv run mutmut run 2>&1 | tail -20
uv run mutmut results
```

Expected: a survivor count. For each survivor in `registry.py`, `retry.py`, or `base.py`, inspect it:

```bash
uv run mutmut show <id>
```

If a survivor represents real untested behavior, add a test that kills it. If it is equivalent (a mutation with no observable effect), leave it and note it. Do not weaken the target list to make the number look better.

- [ ] **Step 3: Re-measure the coverage floor**

```bash
uv run pytest tests/unit --cov=src/eventsource --cov-report=term-missing -q 2>&1 | tail -5
```

Record the new total. Update `[tool.coverage.report]`:

```toml
[tool.coverage.report]
# Unit-suite coverage measured at <NEW>% (2026-07-29, tests/unit/ only,
# excluding integration/postgres/redis-marked tests). Set slightly below that
# as a regression ratchet, not a stretch target.
fail_under = <NEW minus 1>
```

Substitute the actual measured number — do not guess.

- [ ] **Step 4: Document the behavior changes**

Find the changelog:

```bash
ls CHANGELOG* docs/CHANGELOG* 2>/dev/null
```

Add an entry covering, at minimum:

- `RedisEventBus.publish` now honors `background=True` (previously ignored).
- Kafka retry jitter changed from one-sided positive to symmetric; effective backoff is slightly shorter and no longer exceeds `retry_max_delay`.
- Kafka now batches publishes rather than awaiting one round-trip per event.
- Kafka handler dispatch is keyed by event class rather than class name, fixing silent non-delivery for classes whose `event_type` field differs from their class name.
- `KafkaEventBus.get_handlers_for_event` is deprecated.
- `InMemoryEventBus.published_events` and `clear_published_events` are deprecated in favor of `RecordingEventBus`.
- New public API: `BaseEventBus`, `SubscriptionRegistry`, `RetryPolicy`, `RecordingEventBus`.
- Subscription management is now genuinely thread-safe in all four backends.

- [ ] **Step 5: Bump the minor version**

```bash
grep -n "^version" pyproject.toml
```

Change `0.5.0` to `0.6.0`. Check whether `src/eventsource/__init__.py` carries a `__version__` that must match:

```bash
grep -rn "__version__" src/eventsource/__init__.py
```

- [ ] **Step 6: Document the new mutation targets**

Add a short paragraph to `docs/development/mutation-testing.md` under its existing structure, naming the three new files and why they qualify: small, pure, no I/O, and covered by dedicated fast unit tests.

- [ ] **Step 7: Run everything**

```bash
uv run pytest tests/unit --cov=src/eventsource --cov-report=term-missing -q
uv run mypy src/eventsource/ --config-file=pyproject.toml
uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/
uv run lint-imports
```

Expected: all pass, coverage at or above the new floor.

- [ ] **Step 8: Commit and push**

```bash
git add pyproject.toml CHANGELOG.md docs/development/mutation-testing.md src/eventsource/__init__.py
git commit -m "chore: add bus mutation targets, re-measure coverage floor, release 0.6.0"
git push
```

- [ ] **Step 9: Confirm all CI jobs are green**

```bash
gh run list --limit 5
gh pr checks
```

Expected: `lint`, `type-check`, `import-linter`, `audit`, `test`, and `broker-tests` all pass.

---

## Self-Review Notes

Checked against the spec:

- Spec's step 0 (CI repair) → Task 1. Both named causes plus the ruff pin and `--group dev` resolutions.
- `SubscriptionRegistry` → Task 2, including the cached-tuple and no-throwaway-adapter details.
- `RetryPolicy` → Task 3, with the symmetric-jitter decision encoded in a regression test.
- `BaseEventBus` including background-task tracking and `_resolve_event_class` → Task 4.
- InMemory migration → Task 5. Redis → Task 8. RabbitMQ → Task 9. Kafka → Task 10.
- Conformance 6→9 tests plus `await_delivery` → Task 6; broker subclasses in Tasks 8, 9, 10.
- Broker CI services and job → Task 7, sequenced before the broker migrations per the spec's reordering note.
- Four property-test modules → Tasks 2, 3, 11.
- Mutation targets, coverage floor, changelog, version bump → Task 14.
- `RecordingEventBus` and deprecation → Task 12.
- ABC docstring corrections → Task 13.

Two additions not in the spec, both discovered while planning and both flagged to the user before writing:

- Kafka's name-keyed handler dispatch is a latent bug, fixed in Task 10 with a regression test.
- `KafkaEventBus.get_handlers_for_event` is public, so it is retained as a deprecated shim rather than deleted.

Type consistency verified: `handlers_for` returns `tuple[HandlerAdapter, ...]` in Task 2 and is consumed as a tuple in Tasks 5, 8, 9, 10; `_track_background` takes a coroutine and returns `asyncio.Task[None]` consistently; `RetryPolicy.delay_for(retry_count, rng=None)` is called with one argument everywhere outside its own tests.
