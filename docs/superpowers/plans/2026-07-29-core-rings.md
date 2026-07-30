# Core Rings (Clean Architecture Sub-project 1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the entities ring (`domain/`) and boundary ports (`ports/`) with opaque positions and five segregated store ports, port the memory/sqlite/postgresql backends as adapters, and keep the entire existing suite green through a legacy compatibility layer.

**Architecture:** Clean Architecture rings per `docs/adrs/0019-clean-architecture-store-ports.md` and spec `docs/superpowers/specs/2026-07-29-core-rings-design.md`. Ports are structural `Protocol`s; adapters satisfy them without inheritance; conformance suites (including hypothesis property suites) are the compliance check. Old `stores/` surface survives via a `LegacyStoreAdapter` wrapper until sub-project 2.

**Tech Stack:** Python 3.11+, pydantic v2, sqlalchemy async (aiosqlite/asyncpg), pytest + pytest-asyncio, hypothesis, mutmut + cosmic-ray.

## Global Constraints

- `domain/` and `ports/` import only stdlib + pydantic (+ each other, inward only). No sqlalchemy, no driver imports, no imports from `adapters/`, `stores/`, `repositories/`.
- Migration SQL files under `src/eventsource/migrations/` are untouched. Same schema, same queries (exception: the postgres feed query gains a safe-horizon predicate using the `xmin` system column — no DDL).
- Stream versions are 1-based event counts; absent stream = version 0. No ±1 translation anywhere.
- `NotImplementedError` never appears in an adapter. Unsupported capability = unimplemented port.
- Feed resumption is exclusive (`> from_position`); the feed must never permanently skip a committed event (no-skip guarantee).
- Existing unit + integration suites must pass unmodified at every task boundary from Task 11 on; before that, they pass because nothing they import has changed.
- All code passes `uv run ruff check`, `uv run ruff format --check`, `uv run mypy src/eventsource/ --config-file=pyproject.toml` (strict).
- Commits follow `.claude/rules/commits.md` (`type: lowercase description`).
- Run only targeted tests inside tasks (`uv run pytest <path> -v`); the orchestrator runs `make check` at the end.
- New public names export from `eventsource/__init__.py` with `__all__` entries; the canonical `ExpectedVersion` at top level is the **new VO** (legacy int-constants class stays reachable only as `eventsource.stores.interface.ExpectedVersion`).

---

### Task 1: StreamId value object (`domain/`)

**Files:**
- Create: `src/eventsource/domain/__init__.py`
- Create: `src/eventsource/domain/stream_id.py`
- Test: `tests/unit/domain/test_stream_id.py` (create dir with `__init__.py` if the suite convention requires none, match `tests/unit/` existing style)

**Interfaces:**
- Produces: `StreamId(aggregate_id: UUID, category: str)` frozen; `StreamId.render() -> str` (`"{aggregate_id}:{category}"`); `StreamId.parse(raw: str) -> StreamId`; `CATEGORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")`; invalid category raises `ValueError`.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/domain/test_stream_id.py
import re
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.domain import StreamId

CATEGORY_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-"


class TestStreamId:
    def test_render_matches_legacy_wire_format(self) -> None:
        aid = uuid4()
        assert StreamId(aggregate_id=aid, category="Order").render() == f"{aid}:Order"

    def test_parse_round_trip(self) -> None:
        sid = StreamId(aggregate_id=uuid4(), category="Order.v2")
        assert StreamId.parse(sid.render()) == sid

    def test_category_with_colon_rejected(self) -> None:
        with pytest.raises(ValueError):
            StreamId(aggregate_id=uuid4(), category="Order:evil")

    def test_empty_category_rejected(self) -> None:
        with pytest.raises(ValueError):
            StreamId(aggregate_id=uuid4(), category="")

    def test_frozen(self) -> None:
        sid = StreamId(aggregate_id=uuid4(), category="Order")
        with pytest.raises(Exception):
            sid.category = "Other"  # type: ignore[misc]

    @given(category=st.text(alphabet=CATEGORY_ALPHABET, min_size=1, max_size=64))
    def test_valid_categories_round_trip(self, category: str) -> None:
        sid = StreamId(aggregate_id=uuid4(), category=category)
        assert StreamId.parse(sid.render()) == sid

    @given(category=st.text(min_size=1, max_size=64))
    def test_fuzzed_categories_never_corrupt_wire_format(self, category: str) -> None:
        try:
            sid = StreamId(aggregate_id=uuid4(), category=category)
        except ValueError:
            return  # rejected is fine; corrupted is not
        assert StreamId.parse(sid.render()) == sid
```

- [ ] **Step 2: Run tests, verify failure**

Run: `uv run pytest tests/unit/domain/test_stream_id.py -v`
Expected: FAIL / collection error — `eventsource.domain` does not exist.

- [ ] **Step 3: Implement**

```python
# src/eventsource/domain/stream_id.py
"""Stream identity value object."""

import re
from dataclasses import dataclass
from uuid import UUID

CATEGORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")


@dataclass(frozen=True, slots=True)
class StreamId:
    """Identity of an event stream: aggregate id + category.

    ``category`` is today's aggregate type. The rendered wire format is
    ``"{aggregate_id}:{category}"`` — the delimiter is why ``:`` is banned
    from categories.
    """

    aggregate_id: UUID
    category: str

    def __post_init__(self) -> None:
        if not CATEGORY_PATTERN.match(self.category):
            raise ValueError(
                f"invalid stream category {self.category!r}: "
                "must match [A-Za-z0-9_.-]+"
            )

    def render(self) -> str:
        return f"{self.aggregate_id}:{self.category}"

    @classmethod
    def parse(cls, raw: str) -> "StreamId":
        aggregate_id, sep, category = raw.partition(":")
        if not sep:
            raise ValueError(f"not a stream id: {raw!r}")
        return cls(aggregate_id=UUID(aggregate_id), category=category)
```

```python
# src/eventsource/domain/__init__.py
"""Entities ring. Pure: stdlib + pydantic only.

TRANSITION: DomainEvent/EventRegistry still live in eventsource.events and
count as this ring until sub-project 3 moves them.
"""

from eventsource.domain.stream_id import CATEGORY_PATTERN, StreamId

__all__ = ["CATEGORY_PATTERN", "StreamId"]
```

- [ ] **Step 4: Run tests, verify pass**

Run: `uv run pytest tests/unit/domain/test_stream_id.py -v`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add src/eventsource/domain tests/unit/domain
git commit -m "feat: add streamid value object in domain ring"
```

---

### Task 2: Position value object and exceptions (`ports/positions.py`)

**Files:**
- Create: `src/eventsource/ports/__init__.py` (exports grow over Tasks 2-5)
- Create: `src/eventsource/ports/positions.py`
- Modify: `src/eventsource/exceptions.py` (append three exceptions)
- Test: `tests/unit/ports/test_position.py`

**Interfaces:**
- Produces:
  - `Position(store_id: str, key: tuple[int | str, ...])` frozen; totally ordered within a `store_id`; `to_str() -> str`; `Position.from_str(raw: str) -> Position`.
  - Ordering across store_ids raises `PositionForeignError`; `__eq__` across store_ids returns `False` (never raises).
  - `PositionDecodeError(EventSourceError)`, `PositionForeignError(EventSourceError)`, `DuplicateEventError(EventSourceError)` in `eventsource.exceptions` (check the actual base class name in `exceptions.py` — use the same base every other exception uses).
- Serialization format: compact JSON `{"s": store_id, "k": [<key elements>]}` — internal detail, but the tests pin round-trip, not the format.

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/ports/test_position.py
import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.exceptions import PositionDecodeError, PositionForeignError
from eventsource.ports import Position

keys = st.lists(
    st.one_of(st.integers(min_value=0, max_value=2**62), st.text(max_size=32)),
    min_size=1,
    max_size=3,
).map(tuple)


class TestPositionOrdering:
    def test_same_store_orders_by_key(self) -> None:
        assert Position("pg:a", (1,)) < Position("pg:a", (2,))

    def test_foreign_store_ordering_raises(self) -> None:
        with pytest.raises(PositionForeignError):
            _ = Position("pg:a", (1,)) < Position("pg:b", (1,))

    def test_foreign_store_equality_is_false_not_error(self) -> None:
        assert Position("pg:a", (1,)) != Position("pg:b", (1,))

    def test_equality_with_none_and_other_types(self) -> None:
        assert Position("pg:a", (1,)) != None  # noqa: E711
        assert Position("pg:a", (1,)) != 1
        assert Position("pg:a", (1,)) in {Position("pg:a", (1,))}

    @given(a=keys, b=keys, c=keys)
    def test_ordering_laws_within_store(self, a, b, c) -> None:
        pa, pb, pc = (Position("s", k) for k in (a, b, c))
        assert (pa < pb) == (not (pb < pa or pa == pb))  # trichotomy-ish
        if pa < pb and pb < pc:
            assert pa < pc  # transitivity


class TestPositionSerialization:
    @given(store_id=st.text(min_size=1, max_size=32), key=keys)
    def test_round_trip(self, store_id: str, key) -> None:
        p = Position(store_id, key)
        assert Position.from_str(p.to_str()) == p

    @given(garbage=st.text(max_size=64))
    def test_from_str_garbage_raises_decode_error(self, garbage: str) -> None:
        try:
            p = Position.from_str(garbage)
        except PositionDecodeError:
            return
        # If it decoded, it must round-trip (accidentally-valid JSON is ok)
        assert Position.from_str(p.to_str()) == p

    def test_bare_int_is_not_valid_here(self) -> None:
        # Legacy bare-int checkpoints are decoded by the SQL codec (Task 6),
        # never by Position.from_str itself (no store_id to attach).
        with pytest.raises(PositionDecodeError):
            Position.from_str("12345")
```

- [ ] **Step 2: Run tests, verify failure**

Run: `uv run pytest tests/unit/ports/test_position.py -v`
Expected: FAIL — `eventsource.ports` does not exist.

- [ ] **Step 3: Implement**

First append to `src/eventsource/exceptions.py` (match the module's existing base-class and docstring style — read it first):

```python
class DuplicateEventError(EventSourceError):
    """An event with this event_id already exists in the store."""


class PositionDecodeError(EventSourceError):
    """A persisted position string could not be decoded."""


class PositionForeignError(EventSourceError):
    """Positions from different stores were compared for order."""
```

```python
# src/eventsource/ports/positions.py
"""Position and ExpectedVersion value objects for the store ports."""

import json
from dataclasses import dataclass
from typing import Any

from eventsource.exceptions import PositionDecodeError, PositionForeignError


@dataclass(frozen=True, slots=True)
class Position:
    """Opaque, ordered, serializable global-feed position token.

    Totally ordered within one store; ordering across stores raises
    PositionForeignError; equality across stores is False. Consumers
    compare and persist — never arithmetic.
    """

    store_id: str
    key: tuple[int | str, ...]

    def _check_comparable(self, other: "Position") -> None:
        if self.store_id != other.store_id:
            raise PositionForeignError(
                f"cannot order positions from {self.store_id!r} "
                f"and {other.store_id!r}"
            )

    def __lt__(self, other: object) -> bool:
        if not isinstance(other, Position):
            return NotImplemented
        self._check_comparable(other)
        return self.key < other.key

    def __le__(self, other: object) -> bool:
        if not isinstance(other, Position):
            return NotImplemented
        self._check_comparable(other)
        return self.key <= other.key

    def __gt__(self, other: object) -> bool:
        if not isinstance(other, Position):
            return NotImplemented
        self._check_comparable(other)
        return self.key > other.key

    def __ge__(self, other: object) -> bool:
        if not isinstance(other, Position):
            return NotImplemented
        self._check_comparable(other)
        return self.key >= other.key

    def to_str(self) -> str:
        return json.dumps(
            {"s": self.store_id, "k": list(self.key)}, separators=(",", ":")
        )

    @classmethod
    def from_str(cls, raw: str) -> "Position":
        try:
            data: Any = json.loads(raw)
            store_id = data["s"]
            key = data["k"]
            if not isinstance(store_id, str) or not isinstance(key, list):
                raise TypeError
            if not all(isinstance(e, (int, str)) and not isinstance(e, bool) for e in key):
                raise TypeError
        except (json.JSONDecodeError, TypeError, KeyError) as exc:
            raise PositionDecodeError(f"not a position: {raw!r}") from exc
        return cls(store_id=store_id, key=tuple(key))
```

Note: `@dataclass(frozen=True)` generates `__eq__` comparing `(store_id, key)` — foreign stores compare unequal without raising, which is exactly the contract. Do not pass `order=True` (it would generate raising-free ordering); the explicit dunders above are the point.

```python
# src/eventsource/ports/__init__.py
"""Boundary ports (Clean Architecture output ports). Depends on domain only."""

from eventsource.ports.positions import Position

__all__ = ["Position"]
```

- [ ] **Step 4: Run tests, verify pass**

Run: `uv run pytest tests/unit/ports/test_position.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/eventsource/ports src/eventsource/exceptions.py tests/unit/ports
git commit -m "feat: add opaque position value object and port exceptions"
```

---

### Task 3: ExpectedVersion value object

**Files:**
- Modify: `src/eventsource/ports/positions.py` (append), `src/eventsource/ports/__init__.py`
- Test: `tests/unit/ports/test_expected_version.py`

**Interfaces:**
- Produces: `ExpectedVersion` frozen VO with constructors `any_()`, `no_stream()`, `stream_exists()`, `exact(n: int)`; readable `kind: str` in `{"any","no_stream","stream_exists","exact"}` and `version: int | None` (set only for `exact`). `exact(n)` requires `n >= 0` else `ValueError`. NB: the constructor is `any_` (trailing underscore — `any` shadows the builtin; adapters and the legacy wrapper must use this exact name).

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/ports/test_expected_version.py
import pytest

from eventsource.ports import ExpectedVersion


class TestExpectedVersion:
    def test_four_modes(self) -> None:
        assert ExpectedVersion.any_().kind == "any"
        assert ExpectedVersion.no_stream().kind == "no_stream"
        assert ExpectedVersion.stream_exists().kind == "stream_exists"
        ev = ExpectedVersion.exact(3)
        assert (ev.kind, ev.version) == ("exact", 3)

    def test_exact_negative_rejected(self) -> None:
        with pytest.raises(ValueError):
            ExpectedVersion.exact(-1)

    def test_equality(self) -> None:
        assert ExpectedVersion.exact(2) == ExpectedVersion.exact(2)
        assert ExpectedVersion.any_() != ExpectedVersion.no_stream()

    def test_frozen(self) -> None:
        with pytest.raises(Exception):
            ExpectedVersion.any_().kind = "exact"  # type: ignore[misc]
```

- [ ] **Step 2: Run, verify failure** — `uv run pytest tests/unit/ports/test_expected_version.py -v` → ImportError.

- [ ] **Step 3: Implement** (append to `positions.py`; export from `ports/__init__.py`)

```python
@dataclass(frozen=True, slots=True)
class ExpectedVersion:
    """Optimistic-concurrency expectation for append.

    Versions are 1-based event counts; an absent stream has version 0.
    ``exact(n)`` means "the stream currently has exactly n events".
    """

    kind: str
    version: int | None = None

    @classmethod
    def any_(cls) -> "ExpectedVersion":
        return cls(kind="any")

    @classmethod
    def no_stream(cls) -> "ExpectedVersion":
        return cls(kind="no_stream")

    @classmethod
    def stream_exists(cls) -> "ExpectedVersion":
        return cls(kind="stream_exists")

    @classmethod
    def exact(cls, version: int) -> "ExpectedVersion":
        if version < 0:
            raise ValueError(f"exact version must be >= 0, got {version}")
        return cls(kind="exact", version=version)
```

- [ ] **Step 4: Run, verify pass.**

- [ ] **Step 5: Commit** — `git commit -m "feat: add expectedversion value object"`

---

### Task 4: Envelope and read-option value objects (`ports/envelopes.py`)

**Files:**
- Create: `src/eventsource/ports/envelopes.py`
- Modify: `src/eventsource/ports/__init__.py`
- Test: `tests/unit/ports/test_envelopes.py`

**Interfaces:**
- Produces (all frozen dataclasses, slots):
  - `ReadDirection` — reuse by importing the existing enum: `from eventsource.stores.interface import ReadDirection`? **No** — that would point ports at an adapter-ring module. Define `ReadDirection(Enum): FORWARD, BACKWARD` here; Task 13's wrapper maps the old enum to this one by name.
  - `EventEnvelope(event: DomainEvent, stream_id: StreamId, stream_version: int, position: Position | None, stored_at: datetime)`
  - `AppendResult(stream: StreamId, new_version: int, position: Position | None)`
  - `StreamReadOptions(direction: ReadDirection = FORWARD, from_version: int | None = None, to_version: int | None = None, limit: int | None = None)`
  - `FeedReadOptions(tenant_id: UUID | None = None, limit: int | None = None)`
  - `CategoryReadOptions(tenant_id: UUID | None = None, from_timestamp: datetime | None = None, limit: int | None = None)`
- Consumes: `StreamId` (Task 1), `Position` (Task 2), `DomainEvent` (existing, `eventsource.events.base` — import as `from eventsource.events import DomainEvent` matching current internal style).

- [ ] **Step 1: Write the failing tests**

```python
# tests/unit/ports/test_envelopes.py
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.domain import StreamId
from eventsource.events import DomainEvent
from eventsource.ports import (
    AppendResult,
    EventEnvelope,
    FeedReadOptions,
    Position,
    ReadDirection,
    StreamReadOptions,
)


class ThingHappened(DomainEvent):
    pass


def make_envelope(position: Position | None) -> EventEnvelope:
    return EventEnvelope(
        event=ThingHappened(aggregate_id=uuid4()),
        stream_id=StreamId(aggregate_id=uuid4(), category="Thing"),
        stream_version=1,
        position=position,
        stored_at=datetime.now(UTC),
    )


class TestEnvelopes:
    def test_position_may_be_none_for_feedless_stores(self) -> None:
        assert make_envelope(None).position is None

    def test_envelope_frozen(self) -> None:
        env = make_envelope(None)
        with pytest.raises(Exception):
            env.stream_version = 2  # type: ignore[misc]

    def test_append_result_has_no_conflict_flags(self) -> None:
        result = AppendResult(
            stream=StreamId(aggregate_id=uuid4(), category="Thing"),
            new_version=1,
            position=None,
        )
        assert not hasattr(result, "success")
        assert not hasattr(result, "conflict")

    def test_option_defaults(self) -> None:
        assert StreamReadOptions().direction is ReadDirection.FORWARD
        assert FeedReadOptions().tenant_id is None
```

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement** `envelopes.py` exactly per the Interfaces block above (frozen slotted dataclasses, `ReadDirection(Enum)` with `FORWARD = "forward"` / `BACKWARD = "backward"`), export all names from `ports/__init__.py`. `ThingHappened` in the test relies on `DomainEvent`'s existing defaults — check `eventsource/events/base.py` for required fields (`aggregate_id` is required; `event_type` auto-derives) and adjust the test event construction to the actual minimal constructor if it differs.

- [ ] **Step 4: Run, verify pass. Also run `uv run mypy src/eventsource/ports/ --config-file=pyproject.toml`** — clean.

- [ ] **Step 5: Commit** — `git commit -m "feat: add envelope, appendresult and read-option value objects"`

---

### Task 5: The five store port protocols (`ports/store.py`) + snapshot/bus port re-homes

**Files:**
- Create: `src/eventsource/ports/store.py`
- Create: `src/eventsource/ports/snapshots.py` (re-home: `from eventsource.snapshots.interface import Snapshot, SnapshotStore` re-exported — the snapshot contract is unchanged by the spec, so ports/snapshots.py is an alias module until sub-project 3 physically moves it; add `# TRANSITION` marker)
- Create: `src/eventsource/ports/bus.py` (re-home `EventPublisher` the same way: re-export from `eventsource.stores.interface` with `# TRANSITION`)
- Modify: `src/eventsource/ports/__init__.py`
- Test: `tests/unit/ports/test_store_ports.py`

**Interfaces:**
- Produces (all `typing.Protocol`, all methods async or returning `AsyncIterator`):

```python
class EventAppender(Protocol):
    max_append_batch: int | None
    async def append(self, stream: StreamId, events: Sequence[DomainEvent],
                     expected: ExpectedVersion) -> AppendResult: ...

class StreamReader(Protocol):
    def read_stream(self, stream: StreamId,
                    options: StreamReadOptions | None = None) -> AsyncIterator[EventEnvelope]: ...
    async def get_stream_version(self, stream: StreamId) -> int: ...

class EventLookup(Protocol):
    async def event_exists(self, event_id: UUID) -> bool: ...

class GlobalEventFeed(Protocol):
    def read_all(self, from_position: Position | None = None,
                 options: FeedReadOptions | None = None) -> AsyncIterator[EventEnvelope]: ...
    async def current_position(self) -> Position | None: ...

class CategoryQuery(Protocol):
    def read_category(self, category: str,
                      options: CategoryReadOptions | None = None) -> AsyncIterator[EventEnvelope]: ...

class FullEventStore(EventAppender, StreamReader, EventLookup,
                     GlobalEventFeed, CategoryQuery, Protocol): ...
```

- Also produces: `async def collect(it: AsyncIterator[EventEnvelope]) -> list[EventEnvelope]` helper in `ports/store.py`.
- None of the five is `@runtime_checkable` initially — add it only when a consumer needs isinstance (rules file).

- [ ] **Step 1: Write the failing test** — structural conformance is checked by assigning a minimal stub to each protocol-typed variable; mypy is the real gate, the runtime test just imports and exercises `collect`:

```python
# tests/unit/ports/test_store_ports.py
import pytest

from eventsource.ports import collect
from eventsource.ports.store import (
    CategoryQuery,
    EventAppender,
    EventLookup,
    FullEventStore,
    GlobalEventFeed,
    StreamReader,
)


async def _agen(items):
    for item in items:
        yield item


@pytest.mark.asyncio
async def test_collect_drains_iterator() -> None:
    assert await collect(_agen([1, 2, 3])) == [1, 2, 3]


def test_ports_are_importable_and_distinct() -> None:
    ports = {EventAppender, StreamReader, EventLookup, GlobalEventFeed, CategoryQuery}
    assert len(ports) == 5
    assert issubclass(FullEventStore, EventAppender)
```

(Match the project's asyncio test style — check whether existing tests use `@pytest.mark.asyncio` or asyncio_mode=auto in pyproject, and follow it.)

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement** `store.py` with the exact signatures above plus `collect`; create the two `# TRANSITION` re-home modules; export everything from `ports/__init__.py`.

- [ ] **Step 4: Run test + `uv run mypy src/eventsource/ports/`** — pass, clean.

- [ ] **Step 5: Verify ring purity** — `grep -rn "sqlalchemy\|asyncpg\|aiosqlite" src/eventsource/ports/ src/eventsource/domain/` must return nothing (the two TRANSITION re-homes import from `stores.interface`/`snapshots.interface` which are sqlalchemy-free interface modules — verify that with `python -c "import ast, sys; ..."` or simply `grep -n "import" src/eventsource/snapshots/interface.py`).

- [ ] **Step 6: Commit** — `git commit -m "feat: add five store port protocols"`

---

### Task 6: SQL position codec + `_sql` helper package (`adapters/_sql/`)

**Files:**
- Create: `src/eventsource/adapters/__init__.py` (empty docstring module)
- Create: `src/eventsource/adapters/_sql/__init__.py`
- Create: `src/eventsource/adapters/_sql/positions.py`
- Create: `src/eventsource/adapters/_sql/dialect.py` — move the full contents of `src/eventsource/repositories/_dialect.py` here; rewrite `src/eventsource/repositories/_dialect.py` as `# TRANSITION` re-export (`from eventsource.adapters._sql.dialect import *  # noqa: F401,F403` plus explicit `__all__` matching the original)
- Test: `tests/unit/adapters/test_sql_position_codec.py`

**Interfaces:**
- Produces: `IntPositionCodec(store_id: str)` with `encode(value: int) -> Position` (`Position(store_id, (value,))`), `decode(raw: str) -> Position` accepting (a) `Position.to_str` output with matching store_id — foreign store_id raises `PositionForeignError`; (b) **legacy bare-int strings** (`"12345"`) → `Position(store_id, (12345,))`; anything else raises `PositionDecodeError`. `value_of(position: Position) -> int` (foreign store raises `PositionForeignError`; non-int key raises `PositionDecodeError`).
- Consumes: `Position`, exceptions (Task 2).

- [ ] **Step 1: Failing tests**

```python
# tests/unit/adapters/test_sql_position_codec.py
import pytest

from eventsource.adapters._sql.positions import IntPositionCodec
from eventsource.exceptions import PositionDecodeError, PositionForeignError
from eventsource.ports import Position


class TestIntPositionCodec:
    def test_encode_decode_round_trip(self) -> None:
        codec = IntPositionCodec(store_id="pg:main")
        pos = codec.encode(42)
        assert codec.decode(pos.to_str()) == pos
        assert codec.value_of(pos) == 42

    def test_legacy_bare_int_checkpoint_decodes(self) -> None:
        codec = IntPositionCodec(store_id="pg:main")
        assert codec.decode("12345") == Position("pg:main", (12345,))

    def test_foreign_store_decode_raises(self) -> None:
        codec = IntPositionCodec(store_id="pg:main")
        foreign = Position("sqlite:other", (1,)).to_str()
        with pytest.raises(PositionForeignError):
            codec.decode(foreign)

    def test_garbage_raises_decode_error(self) -> None:
        with pytest.raises(PositionDecodeError):
            IntPositionCodec(store_id="pg:main").decode("not-a-position")

    def test_dialect_reexport_intact(self) -> None:
        from eventsource.repositories._dialect import Dialect  # old path
        from eventsource.adapters._sql.dialect import Dialect as NewDialect
        assert Dialect is NewDialect
```

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement**

```python
# src/eventsource/adapters/_sql/positions.py
"""Int-backed position codec shared by the SQL-family adapters."""

from dataclasses import dataclass

from eventsource.exceptions import PositionDecodeError, PositionForeignError
from eventsource.ports import Position


@dataclass(frozen=True, slots=True)
class IntPositionCodec:
    store_id: str

    def encode(self, value: int) -> Position:
        return Position(store_id=self.store_id, key=(value,))

    def decode(self, raw: str) -> Position:
        if raw.isdigit():  # legacy bare-int checkpoint value
            return self.encode(int(raw))
        position = Position.from_str(raw)
        if position.store_id != self.store_id:
            raise PositionForeignError(
                f"position belongs to {position.store_id!r}, "
                f"this store is {self.store_id!r}"
            )
        return position

    def value_of(self, position: Position) -> int:
        if position.store_id != self.store_id:
            raise PositionForeignError(
                f"position belongs to {position.store_id!r}, "
                f"this store is {self.store_id!r}"
            )
        if len(position.key) != 1 or not isinstance(position.key[0], int):
            raise PositionDecodeError(f"not an int-backed position: {position!r}")
        return position.key[0]
```

Move `_dialect.py` content verbatim (git mv preferred, then create the re-export at the old path).

- [ ] **Step 4: Run new tests AND the existing repository tests** (`uv run pytest tests/unit/ -k "dialect or checkpoint or dlq or outbox" -v`) — all pass (the re-export must be perfectly transparent).

- [ ] **Step 5: Commit** — `git commit -m "feat: add sql position codec and re-home dialect helpers"`

---

### Task 7: Memory adapter (`adapters/memory/store.py`)

**Files:**
- Create: `src/eventsource/adapters/memory/__init__.py`, `src/eventsource/adapters/memory/store.py`
- Test: `tests/unit/adapters/test_memory_store.py` (direct tests only — conformance suites arrive Task 8/9 and take over)

**Interfaces:**
- Produces: `MemoryEventStore(store_id: str = "memory")` implementing all five ports. Constructor takes optional `event_registry` like the current `InMemoryEventStore` (mirror its signature). Internals: `_events: list[EventEnvelope]` (global order = list order, position = 1-based list index via `IntPositionCodec`), `_streams: dict[str, list[int]]` (render()ed StreamId → indexes), `_event_ids: set[UUID]`, `asyncio.Lock` serializing appends. `max_append_batch = None`.
- Port source: `src/eventsource/stores/in_memory.py` — reuse its expected-version dispatch logic, retyped. Mapping:
  - old `append_events(aggregate_id, aggregate_type, events, expected_version:int)` → `append(stream, events, expected)`; sentinel ints → `ExpectedVersion` kinds (`-1`/ANY→`any_`, `0`... check actual constants in `stores/interface.py`: `ANY`, `NO_STREAM`, `STREAM_EXISTS` values — map by name, not number).
  - old `get_events` → `collect(read_stream(...))`
  - old `event_exists` → `event_exists`
  - old `read_all(options)` → `read_all(from_position, options)` with exclusive resumption
  - old `get_events_by_type(aggregate_type, ...)` → `read_category(category, ...)`
  - old `get_global_position` → `current_position` (`None` when empty)
- Key contract points the tests below pin: duplicate event_id → `DuplicateEventError` (whole batch rejected, atomically); empty batch → `ValueError`; version = 1-based count; conflict → `OptimisticLockError` (existing exception — reuse it).

- [ ] **Step 1: Failing tests** (representative core; conformance suites deepen coverage next task)

```python
# tests/unit/adapters/test_memory_store.py
from uuid import uuid4

import pytest

from eventsource.adapters.memory import MemoryEventStore
from eventsource.domain import StreamId
from eventsource.events import DomainEvent
from eventsource.exceptions import DuplicateEventError, OptimisticLockError
from eventsource.ports import ExpectedVersion, collect


class ThingHappened(DomainEvent):
    pass


def sid() -> StreamId:
    return StreamId(aggregate_id=uuid4(), category="Thing")


@pytest.fixture
def store() -> MemoryEventStore:
    return MemoryEventStore()


class TestAppend:
    async def test_append_returns_one_based_version(self, store) -> None:
        stream = sid()
        result = await store.append(
            stream, [ThingHappened(aggregate_id=stream.aggregate_id)],
            ExpectedVersion.no_stream(),
        )
        assert result.new_version == 1
        assert await store.get_stream_version(stream) == 1

    async def test_absent_stream_version_zero(self, store) -> None:
        assert await store.get_stream_version(sid()) == 0

    async def test_exact_conflict_raises(self, store) -> None:
        stream = sid()
        await store.append(stream, [ThingHappened(aggregate_id=stream.aggregate_id)],
                           ExpectedVersion.no_stream())
        with pytest.raises(OptimisticLockError):
            await store.append(stream, [ThingHappened(aggregate_id=stream.aggregate_id)],
                               ExpectedVersion.exact(0))

    async def test_duplicate_event_id_rejected_atomically(self, store) -> None:
        stream = sid()
        event = ThingHappened(aggregate_id=stream.aggregate_id)
        await store.append(stream, [event], ExpectedVersion.no_stream())
        fresh = ThingHappened(aggregate_id=stream.aggregate_id)
        with pytest.raises(DuplicateEventError):
            await store.append(stream, [fresh, event], ExpectedVersion.exact(1))
        assert await store.get_stream_version(stream) == 1  # atomic: fresh not written

    async def test_empty_batch_rejected(self, store) -> None:
        with pytest.raises(ValueError):
            await store.append(sid(), [], ExpectedVersion.any_())


class TestFeed:
    async def test_exclusive_resumption(self, store) -> None:
        stream = sid()
        await store.append(stream,
                           [ThingHappened(aggregate_id=stream.aggregate_id) for _ in range(3)],
                           ExpectedVersion.no_stream())
        first_two = [env async for env in store.read_all()][:2]
        resumed = [env async for env in store.read_all(from_position=first_two[-1].position)]
        assert len(resumed) == 1

    async def test_current_position_none_when_empty(self, store) -> None:
        assert await store.current_position() is None
```

- [ ] **Step 2: Run, verify failure.**

- [ ] **Step 3: Implement** `MemoryEventStore` per the Interfaces mapping. The expected-version check, written once here and reused as the reference for both SQL adapters:

```python
def _check_expected(self, current: int, expected: ExpectedVersion, stream: StreamId) -> None:
    if expected.kind == "any":
        return
    if expected.kind == "no_stream" and current != 0:
        raise OptimisticLockError(...)   # mirror the fields the existing exception carries
    if expected.kind == "stream_exists" and current == 0:
        raise OptimisticLockError(...)
    if expected.kind == "exact" and current != expected.version:
        raise OptimisticLockError(...)
```

(Open `src/eventsource/exceptions.py` for `OptimisticLockError`'s actual constructor — it carries aggregate/expected/actual fields today; populate them faithfully, `aggregate_id=stream.aggregate_id`.)

- [ ] **Step 4: Run, verify pass. mypy clean on `src/eventsource/adapters/`.**

- [ ] **Step 5: Commit** — `git commit -m "feat: add memory event store adapter on the new ports"`

---

### Task 8: Conformance package — Appender, StreamReader, EventLookup suites

**Files:**
- Create: `src/eventsource/testing/conformance_ports/__init__.py`, `appender.py`, `stream_reader.py`, `event_lookup.py`
  (`testing/conformance.py` — the legacy EventStore/EventBus suites — stays untouched; the new package sits beside it and replaces it in sub-project 3.)
- Test: `tests/unit/adapters/test_memory_conformance.py`

**Interfaces:**
- Produces: `AppenderConformance`, `StreamReaderConformance`, `EventLookupConformance` — ABC test mixins in the style of the existing `EventStoreConformanceSuite` (read `src/eventsource/testing/conformance.py:34-56` for the fixture convention and copy it: an abstract `store` fixture returning the adapter under test). Each suite's cases (write real test methods for every bullet):
  - Appender: 1-based versions; absent=0 is reader's job but `no_stream` append to fresh stream succeeds; each of the four `ExpectedVersion` modes accepted/conflicting per contract; `DuplicateEventError` on duplicate id, atomically; empty batch `ValueError`; `AppendResult.position` non-None here (these adapters implement the feed) and strictly increasing across appends.
  - StreamReader: read returns exactly appended events in order; `from_version`/`to_version`/`limit`/`BACKWARD` honored; envelopes carry correct `stream_version` sequence 1..N.
  - EventLookup: exists after append; not before; unknown UUID False.
- Consumes: everything from Tasks 1-7.

- [ ] **Step 1: Write `tests/unit/adapters/test_memory_conformance.py` first** (the failing consumer):

```python
from collections.abc import AsyncIterator

import pytest

from eventsource.adapters.memory import MemoryEventStore
from eventsource.testing.conformance_ports import (
    AppenderConformance,
    EventLookupConformance,
    StreamReaderConformance,
)


class TestMemoryAppender(AppenderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryEventStore]:
        yield MemoryEventStore()


class TestMemoryStreamReader(StreamReaderConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryEventStore]:
        yield MemoryEventStore()


class TestMemoryEventLookup(EventLookupConformance):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MemoryEventStore]:
        yield MemoryEventStore()
```

- [ ] **Step 2: Run, verify failure** (package missing).

- [ ] **Step 3: Implement the three suites.** Every case listed in Interfaces gets a real test method — no "etc". Suites define their own tiny `DomainEvent` subclass and stream factory (module-level, like the existing conformance module does). Keep suites sqlalchemy-free (they will be part of the Tier 0 surface eventually — import only from `eventsource.ports`, `eventsource.domain`, `eventsource.events`, `eventsource.exceptions`, pytest).

- [ ] **Step 4: Run, verify all conformance cases pass against memory.**

- [ ] **Step 5: Commit** — `git commit -m "test: add appender, streamreader and eventlookup conformance suites"`

---

### Task 9: Conformance — GlobalFeed + CategoryQuery suites, feed-less partitioned store

**Files:**
- Create: `src/eventsource/testing/conformance_ports/feed.py`, `category.py`
- Create: `src/eventsource/testing/partitioned_memory.py` — `PartitionedMemoryStore`: implements Appender/StreamReader/EventLookup/CategoryQuery but **not** GlobalEventFeed; internally a dict of per-stream lists with no global order; envelopes carry `position=None`. ~80 lines, reuses `_check_expected` logic pattern from Task 7 (copy it — do not import from the adapter; testing must not depend on a specific adapter).
- Modify: `tests/unit/adapters/test_memory_conformance.py` (add feed + category suite subclasses for `MemoryEventStore`)
- Test: `tests/unit/testing/test_partitioned_store.py`

**Interfaces:**
- Produces:
  - `GlobalFeedConformance` cases: full read in position order; exclusive resumption (resume from position of event k yields exactly events k+1..N); `from_position=None` = from start; positions strictly increase; `current_position()` None when empty / equals last envelope's position otherwise; tenant filter honored when `FeedReadOptions(tenant_id=...)` (events for other tenants excluded — construct `TenantDomainEvent`s; read `src/eventsource/multitenancy` for the event type to use); resumed-from-persisted-string round-trip (`Position.from_str(p.to_str())` resumes identically).
  - `CategoryQueryConformance` cases: only the named category returned; `from_timestamp` filter; tenant filter; stored_at ordering.
  - `PartitionedMemoryStore` direct tests: envelopes have `position=None`; it does not expose `read_all`/`current_position` attributes (`assert not hasattr(...)`).

- [ ] **Step 1: Failing consumers first** (extend memory conformance module + partitioned store test), **Step 2: verify failure, Step 3: implement suites + partitioned store, Step 4: verify pass, Step 5: commit** `test: add feed and category conformance suites with feed-less proving store`. Same TDD rhythm as Task 8; every case in Interfaces becomes a real method.

---

### Task 10: Hypothesis stateful machine + sync façade

**Files:**
- Create: `src/eventsource/testing/sync_facade.py` — `SyncStoreFacade(store, loop_factory=asyncio.new_event_loop)`: sync wrappers `append`, `read_stream` (returns list), `get_stream_version`, `read_all` (returns list), `current_position`, `event_exists`, each driving the async store via a private event loop (mirror the pattern in `src/eventsource/sync/adapter.py` — read it; reuse its loop-management approach, but target the ports, not the old ABC).
- Create: `src/eventsource/testing/conformance_ports/stateful.py` — `StoreStateMachine(RuleBasedStateMachine)` base with an abstract `make_store()` hook.
- Test: `tests/unit/adapters/test_memory_stateful.py`

**Interfaces:**
- Produces: `StoreStateMachine` rules (real code in the plan-executor's implementation, signatures here):
  - `append_new_stream(n_events)` — appends to a fresh stream with `no_stream()`; model records list.
  - `append_existing(stream_index, n_events)` — appends with `exact(model_version)`; model appends.
  - `append_stale(stream_index)` — appends with `exact(model_version - 1)` when version ≥ 1; expects `OptimisticLockError`; model unchanged.
  - `check_stream(stream_index)` — invariant: store stream == model list (event ids, order); version == len.
  - `check_feed()` — invariant: feed event ids == flattened model in feed order; positions strictly increase.
  - Machine bounds: max 8 streams, max 5 events/step (keep runs fast; hypothesis profile `max_examples=25` for CI — set via `settings` on the concrete class).

```python
# tests/unit/adapters/test_memory_stateful.py
from hypothesis import settings

from eventsource.adapters.memory import MemoryEventStore
from eventsource.testing.conformance_ports.stateful import StoreStateMachine
from eventsource.testing.sync_facade import SyncStoreFacade


class MemoryStateMachine(StoreStateMachine):
    def make_store(self) -> SyncStoreFacade:
        return SyncStoreFacade(MemoryEventStore())


TestMemoryStateful = MemoryStateMachine.TestCase
TestMemoryStateful.settings = settings(max_examples=25, deadline=None)
```

- [ ] Steps: failing test → implement façade → implement machine → pass → commit `test: add hypothesis stateful conformance machine with sync facade`. Watch for the known gotcha (project memory): pytest-randomly reseeds hypothesis — if flakiness appears, pin `derandomize=True` in the settings rather than fighting the seed.

Additionally (same task, spec Testing §property 4): `tests/unit/serialization/test_event_round_trip_property.py` — a hypothesis test generating events with arbitrary payload field values (`st.dictionaries` of json-safe scalars applied to a test `DomainEvent` subclass with a `payload: dict[str, Any]` field), asserting `registry`-mediated serialize→deserialize round-trip equality via the existing `EventSourceJSONEncoder` path (read `src/eventsource/serialization/` for the canonical encode/decode entry points and use those, not raw `json`).

---

### Task 11: SQLite adapter

**Files:**
- Create: `src/eventsource/adapters/sqlite/__init__.py`, `src/eventsource/adapters/sqlite/store.py`
- Test: `tests/unit/adapters/test_sqlite_conformance.py` (sqlite tests run without Docker — mirror whichever marker/fixture pattern `tests/` currently uses for the sqlite store; find it with `grep -rn "sqlite" tests/unit tests/integration -l` and follow it)

**Interfaces:**
- Produces: `SQLiteEventStore` implementing all five ports + `max_append_batch = None`, `store_id` defaulting to the database path (`f"sqlite:{path}"`). Port of `src/eventsource/stores/sqlite.py` (~same SQL, same tables) with the Task 7 mapping table applied; positions minted via `IntPositionCodec`; `DuplicateEventError` raised by catching the unique-violation `IntegrityError` on `event_id` (sqlite reports it in `e.args[0]` — match on the constraint, mirroring how the current sqlite store detects version conflicts); optional-dependency guard (`AIOSQLITE_AVAILABLE`) preserved exactly as the current module does it.
- No safe-horizon work: sqlite writers are serialized (single-writer WAL) — trivially no-skip. Add a code comment stating that constraint.
- Conformance test module subclasses ALL suites incl. stateful (sqlite variant) and feed/category.

- [ ] Steps: conformance test module first (fails: module missing) → port the store → all suites pass → `uv run pytest tests/ -k sqlite -v` (old suite still green — untouched paths) → commit `feat: add sqlite event store adapter on the new ports`.

---

### Task 12: PostgreSQL adapter with safe-horizon feed

**Files:**
- Create: `src/eventsource/adapters/postgresql/__init__.py`, `src/eventsource/adapters/postgresql/store.py`
- Test: `tests/integration/adapters/test_postgresql_conformance.py` (marker `postgres`, Docker services per `docker-compose.test.yml` — copy the engine/session fixture pattern from the existing postgres store integration tests, `grep -rn "postgres" tests/integration -l`)
- Test: `tests/integration/adapters/test_postgresql_no_skip.py`

**Interfaces:**
- Produces: `PostgreSQLEventStore` implementing all five ports; `store_id` default `f"pg:{database}"` (derive from the engine URL; must be stable across restarts — never random). Port of `src/eventsource/stores/postgresql.py` per the Task 7 mapping. `DuplicateEventError` from the `event_id` unique violation (distinguish from the `(aggregate_id, aggregate_type, version)` conflict constraint by constraint name in the `IntegrityError` — the current module already parses that error for `OptimisticLockError`; extend the same parsing).
- **Safe-horizon feed** — the one genuinely new query. `read_all` uses:

```sql
SELECT ... FROM events
WHERE global_position > :from_position
  AND xmin::text::bigint < pg_snapshot_xmin(pg_current_snapshot())::text::bigint
ORDER BY global_position ASC
LIMIT :limit
```

  Rationale comment to include verbatim: rows whose inserting transaction is not yet definitely-committed (xmin ≥ snapshot xmin) are deferred to a later poll — the sequence commits out of order, and reading past a still-uncommitted lower position would skip it forever once the reader resumes from higher up. Uses the `xmin` system column: no DDL, schema untouched. Caveat: epoch comparison is not wraparound-proof in the ancient-xid regime; acceptable for now, revisit if a `xid8` column is ever added.
- `current_position()` must apply the same horizon (`SELECT max(global_position) FROM events WHERE xmin... < ...`), else a checkpoint could be minted beyond the horizon and skip on resume.

**No-skip integration test** (the mutation-killer for the horizon predicate):

```python
# tests/integration/adapters/test_postgresql_no_skip.py  (marker: postgres)
# Two concurrent writers: writer A begins a transaction, inserts (acquiring
# global_position N), and parks pre-commit on an asyncio.Event; writer B then
# inserts and commits (position N+1). Reader reads the feed: must NOT see N+1
# while N is uncommitted (horizon holds it back). Release A, commit; reader
# resumes from its checkpoint: sees N then N+1. Assert no event lost, order
# by position. Drive writer A with a raw asyncpg/sqlalchemy connection held
# open across an explicit begin(); park with anyio/asyncio primitives, and
# put a hard timeout (asyncio.wait_for, 10s) around the whole test.
```

Write this as a real test — the comment above is the specification of its choreography; every step in it becomes code.

- [ ] Steps: conformance module + no-skip test first (fail) → port store + horizon → `docker compose -f docker-compose.test.yml up -d` → `uv run pytest tests/integration/adapters -m postgres -v` pass → existing postgres suite still green (`uv run pytest tests/ -m postgres -v`) → commit `feat: add postgresql adapter with safe-horizon global feed`.

---

### Task 13: Legacy compatibility wrapper (`stores/legacy.py`)

**Files:**
- Create: `src/eventsource/stores/legacy.py` — `LegacyStoreAdapter(EventStore)` (the old ABC) wrapping any `FullEventStore`.
- Modify: `src/eventsource/stores/__init__.py` — construct the three legacy store classes as thin subclasses that build the new adapter and wrap it: `InMemoryEventStore`, `SQLiteEventStore`, `PostgreSQLEventStore` keep their existing constructor signatures and behavior by delegating to `LegacyStoreAdapter(MemoryEventStore(...))` etc. **Only if** their existing test suites pass unmodified this way; where a legacy test reaches into implementation internals (e.g. private attributes), keep the original class untouched instead and note it for sub-project 2 — the wrapper is still exercised by the conformance suite below.
- Test: `tests/unit/stores/test_legacy_adapter.py`

**Interfaces:**
- Consumes: everything.
- Produces: `LegacyStoreAdapter` translating per the spec's Compatibility Layer section:
  - `append_events(aggregate_id, aggregate_type, events, expected_version:int)` → `append(StreamId(aggregate_id, aggregate_type), events, _expected_from_int(expected_version))` where `_expected_from_int` maps the old class constants **by name** (`ExpectedVersion.ANY → any_()`, `NO_STREAM → no_stream()`, `STREAM_EXISTS → stream_exists()`, `n >= 0 → exact(n)`), and reconstructs the old `AppendResult` shape (`success=True, new_version, global_position=codec.value_of(position) if position else 0, conflict=False`); an `OptimisticLockError` is **re-raised** (the old stores raise too — verify against `stores/in_memory.py` behavior and mirror it exactly; if any path returned `conflicted()` results instead, mirror that path).
  - `get_events` → collected `read_stream` rebuilt into `EventStream` (old VO); `StoredEvent` fields from `EventEnvelope` (`global_position=0` when `position is None` — the old fabrication, preserved only inside the legacy surface).
  - `event_exists` → `event_exists`; `get_stream_version` → identity; `get_events_by_type(aggregate_type, ...)` → `read_category`; `read_all(options)` → `read_all(from_position=codec.encode(options.from_position) ...)` mapping old int options; `get_global_position` → `codec.value_of(await current_position())` or 0 when None.
  - Old `ReadDirection` enum values map to new by `.name`.
- The wrapper's own tests pin: every sentinel mapping; envelope↔storedevent conversion both ways; exception passthrough; int position round-trip through the codec including `get_global_position` on an empty store == 0.

- [ ] Steps: failing wrapper tests → implement → pass → **run the full existing unit suite** `uv run pytest tests/unit -x -q` (must be green — this is the task's real gate) → commit `feat: add legacy store adapter preserving the old eventstore surface`.

---

### Task 14: Snapshot adapters, top-level exports, gate wiring

**Files:**
- Create: `src/eventsource/adapters/memory/snapshots.py`, `adapters/sqlite/snapshots.py`, `adapters/postgresql/snapshots.py` — re-home the three snapshot stores (`git mv` contents from `snapshots/in_memory.py`, `snapshots/sqlite.py`, `snapshots/postgresql.py`; old modules become `# TRANSITION` re-exports; contract unchanged, so existing snapshot tests are the gate).
- Create: `src/eventsource/testing/conformance_ports/snapshots.py` — `SnapshotConformance` (save/get-latest/delete round-trip; version filtering; per the unchanged `SnapshotStore` interface — port the relevant cases from any existing snapshot tests into suite form) + wire into the three adapters' conformance test modules.
- Modify: `src/eventsource/__init__.py` — export `StreamId`, `Position`, `ExpectedVersion` (the VO), `EventEnvelope`, `AppendResult`, `StreamReadOptions`, `FeedReadOptions`, `CategoryReadOptions`, `ReadDirection` (new one), the five protocols, `FullEventStore`, `MemoryEventStore`, `SQLiteEventStore` + `PostgreSQLEventStore` (adapter classes, under those names only if no collision with legacy exports — otherwise export as `eventsource.adapters.sqlite.SQLiteEventStore` path-only and note it), `DuplicateEventError`, `PositionDecodeError`, `PositionForeignError`. Update `__all__`. **Do not remove any existing export.**
- Modify: `pyproject.toml` — add `src/eventsource/domain`, `src/eventsource/ports`, `src/eventsource/adapters` to the mutmut/cosmic-ray paths (find the existing tool config blocks and extend `paths_to_mutate`/module lists in the same style); exclude `stores/legacy.py` and all `# TRANSITION` re-export modules.
- Modify: `CHANGELOG.md` (top, unreleased section, matching existing entry style): new ports/VO surface added, old surface preserved via legacy wrapper, feed no-skip guarantee on postgres.
- Test: `tests/unit/test_public_api.py` — if a public-API test exists, extend it; otherwise add one asserting every name above imports from `eventsource` and appears in `__all__`.

- [ ] Steps: failing export test → wire exports → snapshot re-home with existing snapshot tests green → conformance suite added and green for all three adapters → `uv run ruff check src/ tests/ && uv run mypy src/eventsource/ --config-file=pyproject.toml` clean → commit `feat: export core rings surface and wire snapshot adapters`.

---

## Final Gate (orchestrator, not a task)

- `make check` — full CI parity (lint, mypy, import-linter, bandit/pip-audit, unit suite).
- `docker compose -f docker-compose.test.yml up -d && uv run pytest tests/integration -v` — integration green.
- Mutation spot-run on the new modules (`uv run mutmut run --paths-to-mutate src/eventsource/ports/positions.py` or the cosmic-ray equivalent per ADR 0008) — priority targets from the spec: resumption `>`, horizon predicate, `ExpectedVersion` dispatch, `Position.__lt__`, tenant `WHERE`.
- Verify ring purity: `grep -rn "sqlalchemy\|asyncpg\|aiosqlite\|redis\|aiokafka\|aio_pika" src/eventsource/ports src/eventsource/domain` → empty.
```
