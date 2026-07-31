# Command Objects, DeciderAggregate & Dogfooding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship `DomainCommand`, `DeciderAggregate`, `CommandRejectedError`, `create_event(command=)`, and `DeciderScenario`, then adopt the decider as the library's primary showcased style across examples, tutorials, fixtures, and bench.

**Architecture:** Commands are a new entities-ring package (`commands/`) beside `events/`. `DeciderAggregate` subclasses `AggregateRoot` in `aggregates/decider.py`, delegating to three abstract static methods (`initial_state`/`decide`/`evolve`) and stamping version + provenance in one `model_copy` per event inside `execute()`. The BDD harness gains a synchronous `DeciderScenario`. Spec: `docs/superpowers/specs/2026-07-30-decider-command-dogfood-design.md`.

**Tech Stack:** Python 3.11+, pydantic v2, pytest, mypy strict, ruff, import-linter, mkdocs.

## Global Constraints

- Entities ring purity: `commands/` may import only stdlib, pydantic, and other entities-ring modules (`events/`, `types.py`, `exceptions.py`). Never sqlalchemy or any adapter.
- All models frozen: `model_config = ConfigDict(frozen=True)` on `DomainCommand`.
- `decide`/`evolve`/`initial_state` are synchronous pure functions — no `self`, no I/O, no version arithmetic.
- Stamping precedence: fields in `event.model_fields_set` are NEVER overwritten. Tenant resolution order: command's explicit `tenant_id` → tenant context → leave untouched.
- Commands are never persisted: no registry, no `@register_command`, no serialization module, no bus.
- Every code task: `uv run mypy src/eventsource/ --config-file=pyproject.toml` and `uv run ruff check src/ tests/` must pass before commit.
- Commit style: `<type>: <lowercase description>` per `.claude/rules/commits.md`, each ending with `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.
- Docs tasks: `uv run mkdocs build --strict` must pass.
- Work lands on branch `decider-command-dogfood` in three PRs: Tasks 1–8 (PR 1), 9–13 (PR 2), 14–15 (PR 3).

## File Structure

| File | Responsibility |
|---|---|
| `src/eventsource/commands/__init__.py` (new) | Re-export `DomainCommand` |
| `src/eventsource/commands/base.py` (new) | `DomainCommand` model + `caused_by` |
| `src/eventsource/exceptions.py` (modify) | Add `CommandRejectedError` |
| `src/eventsource/aggregates/decider.py` (new) | `DeciderAggregate` |
| `src/eventsource/aggregates/base.py` (modify) | `create_event(command=)` provenance |
| `src/eventsource/aggregates/__init__.py` (modify) | Export `DeciderAggregate` |
| `src/eventsource/__init__.py` (modify) | Public exports |
| `pyproject.toml` (modify) | import-linter: add `eventsource.commands` to Tier 0 contract |
| `src/eventsource/testing/bdd.py` (modify) | `DeciderScenario` |
| `src/eventsource/testing/__init__.py` (modify) | Export `DeciderScenario` |
| `examples/basic_usage.py` (rewrite) | Decider-style bank account (flagship) |
| `examples/imperative_example.py` (new) | Imperative style reference (moved code) |
| `tests/fixtures/aggregates.py` (modify) | `OrderAggregate` → decider; keep `CounterAggregate`/`DeclarativeCounterAggregate` |
| `bench/core/domain.py` (modify) | Add `BenchDeciderCounter` |
| `docs/adrs/0022-command-objects-and-decider-style.md` (new) | ADR-0022 |
| `docs/adrs/index.md`, `mkdocs.yml` (modify) | ADR nav entries |
| `docs/explanation/decider-pattern.md`, `docs/explanation/aggregate-styles.md`, `docs/tutorials/03-first-aggregate.md`, `README.md` (modify) | Dogfood docs wave |
| `tests/unit/commands/test_domain_command.py` (new) | Command contract tests |
| `tests/unit/aggregates/test_decider_aggregate.py` (new) | Decider contract tests |
| `tests/unit/testing/test_decider_scenario.py` (new) | Scenario tests |

Note on PR boundaries vs the spec: moving the imperative example and updating `aggregate-styles.md` pointers happens in **Task 6 (PR 1)**, not PR 2 — converting `basic_usage.py` in PR 1 would otherwise leave `aggregate-styles.md` pointing at code that no longer exists there. Docs stay truthful at every merge point.

---

### Task 1: `DomainCommand`

**Files:**
- Create: `src/eventsource/commands/base.py`, `src/eventsource/commands/__init__.py`
- Test: `tests/unit/commands/test_domain_command.py` (and empty `tests/unit/commands/__init__.py` if sibling dirs have one — mirror `tests/unit/`'s existing convention)

**Interfaces:**
- Produces: `DomainCommand` with fields `command_id: UUID`, `issued_at: datetime`, `correlation_id: UUID`, `actor_id: str | None`, `tenant_id: UUID | None`; method `caused_by(event: DomainEvent) -> Self`. Later tasks import it as `from eventsource.commands import DomainCommand`.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for the DomainCommand base model."""

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from eventsource.commands import DomainCommand
from eventsource.events.base import DomainEvent


class OpenAccount(DomainCommand):
    owner_name: str


class SomethingHappened(DomainEvent):
    event_type: str = "SomethingHappened"
    aggregate_type: str = "Thing"


class TestDomainCommandDefaults:
    def test_command_id_generated(self) -> None:
        cmd = OpenAccount(owner_name="alice")
        assert isinstance(cmd.command_id, UUID)

    def test_two_commands_get_distinct_ids(self) -> None:
        a, b = OpenAccount(owner_name="a"), OpenAccount(owner_name="b")
        assert a.command_id != b.command_id
        assert a.correlation_id != b.correlation_id

    def test_issued_at_is_utc_aware(self) -> None:
        cmd = OpenAccount(owner_name="alice")
        assert cmd.issued_at.tzinfo is not None

    def test_actor_and_tenant_default_none(self) -> None:
        cmd = OpenAccount(owner_name="alice")
        assert cmd.actor_id is None
        assert cmd.tenant_id is None


class TestDomainCommandImmutability:
    def test_frozen(self) -> None:
        cmd = OpenAccount(owner_name="alice")
        with pytest.raises(ValidationError):
            cmd.owner_name = "bob"  # type: ignore[misc]


class TestCausedBy:
    def test_caused_by_copies_correlation_only(self) -> None:
        event = SomethingHappened(aggregate_id=uuid4(), aggregate_version=1)
        cmd = OpenAccount(owner_name="alice").caused_by(event)
        assert cmd.correlation_id == event.correlation_id
        assert cmd.owner_name == "alice"

    def test_caused_by_returns_new_instance(self) -> None:
        original = OpenAccount(owner_name="alice")
        event = SomethingHappened(aggregate_id=uuid4(), aggregate_version=1)
        chained = original.caused_by(event)
        assert chained is not original
        assert original.correlation_id != event.correlation_id


class TestMatchSupport:
    def test_class_pattern_with_keyword_capture(self) -> None:
        cmd: DomainCommand = OpenAccount(owner_name="alice")
        match cmd:
            case OpenAccount(owner_name=name):
                assert name == "alice"
            case _:
                pytest.fail("pattern did not match")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/unit/commands/ -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'eventsource.commands'`

- [ ] **Step 3: Implement**

`src/eventsource/commands/base.py`:

```python
"""
Command base model for CQRS-style command handling.

Commands are immutable intents. Unlike events they are never persisted:
a rejected command leaves no trace in the event store by design. There is
no command registry, no serialization support, and no command bus — see
ADR-0022 for the rationale and non-goals.

Subclassing DomainCommand is opt-in. ``DeciderAggregate.execute()`` and
``AggregateRoot.create_event(command=...)`` accept any object as a command;
a DomainCommand additionally gets provenance stamped onto the events it
produces (causation_id, correlation_id, actor_id, tenant_id).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import TYPE_CHECKING, Self
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field

if TYPE_CHECKING:
    from eventsource.events.base import DomainEvent


class DomainCommand(BaseModel):
    """
    Base class for immutable command objects.

    Attributes:
        command_id: Unique identity of this command instance. Becomes the
            causation_id of every event the command produces.
        issued_at: When the command was issued (UTC).
        correlation_id: Workflow chain identifier. Fresh commands start a
            new chain; saga-issued commands should use caused_by() to
            continue an existing one.
        actor_id: Optional identifier of who issued the command.
        tenant_id: Optional tenant. When unset, stamping falls back to the
            tenant context (see DeciderAggregate.execute / create_event).
    """

    model_config = ConfigDict(frozen=True)

    command_id: UUID = Field(default_factory=uuid4)
    issued_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    correlation_id: UUID = Field(default_factory=uuid4)
    actor_id: str | None = Field(default=None)
    tenant_id: UUID | None = Field(default=None)

    def caused_by(self, event: DomainEvent) -> Self:
        """
        Return a copy of this command that continues the event's workflow.

        Copies only the event's correlation_id. Commands deliberately have
        no causation_id field — event -> command -> event linkage within a
        workflow is by correlation (ADR-0022).
        """
        return self.model_copy(update={"correlation_id": event.correlation_id})
```

`src/eventsource/commands/__init__.py`:

```python
"""Command objects for CQRS-style command handling."""

from eventsource.commands.base import DomainCommand

__all__ = ["DomainCommand"]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/unit/commands/ -v`
Expected: all PASS

- [ ] **Step 5: Lint, type-check, commit**

Run: `uv run ruff check src/ tests/ && uv run ruff format src/eventsource/commands/ tests/unit/commands/ && uv run mypy src/eventsource/ --config-file=pyproject.toml`

```bash
git add src/eventsource/commands/ tests/unit/commands/
git commit -m "feat: add DomainCommand base model"
```

---

### Task 2: `CommandRejectedError`

**Files:**
- Modify: `src/eventsource/exceptions.py` (append after the last exception class)
- Test: `tests/unit/commands/test_domain_command.py` (append a class)

**Interfaces:**
- Produces: `CommandRejectedError(EventSourceError)` with `__init__(self, message: str, command: object | None = None)` and attribute `command`.

- [ ] **Step 1: Write the failing test** (append to `tests/unit/commands/test_domain_command.py`)

```python
class TestCommandRejectedError:
    def test_is_eventsource_error_and_carries_command(self) -> None:
        from eventsource.exceptions import CommandRejectedError, EventSourceError

        cmd = OpenAccount(owner_name="alice")
        err = CommandRejectedError("account already open", command=cmd)
        assert isinstance(err, EventSourceError)
        assert err.command is cmd
        assert "already open" in str(err)

    def test_command_defaults_to_none(self) -> None:
        from eventsource.exceptions import CommandRejectedError

        assert CommandRejectedError("no").command is None
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/commands/ -v -k Rejected`
Expected: FAIL — `ImportError: cannot import name 'CommandRejectedError'`

- [ ] **Step 3: Implement** (append to `src/eventsource/exceptions.py`)

```python
class CommandRejectedError(EventSourceError):
    """
    A command was rejected by domain logic.

    Raising this from ``decide()`` (or a command method) is a convention,
    not a requirement — any exception may be used. It gives application
    code one catchable type meaning "the domain said no" as distinct from
    a bug.

    Attributes:
        command: The rejected command object, when provided.
    """

    def __init__(self, message: str, command: object | None = None) -> None:
        self.command = command
        super().__init__(message)
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/commands/ -v`
Expected: all PASS

- [ ] **Step 5: Lint, type-check, commit**

Run: `uv run ruff check src/ tests/ && uv run mypy src/eventsource/ --config-file=pyproject.toml`

```bash
git add src/eventsource/exceptions.py tests/unit/commands/test_domain_command.py
git commit -m "feat: add CommandRejectedError exception"
```

---

### Task 3: `DeciderAggregate`

**Files:**
- Create: `src/eventsource/aggregates/decider.py`
- Modify: `src/eventsource/aggregates/__init__.py` (add export — read the file first and mirror its existing import/`__all__` style)
- Test: `tests/unit/aggregates/test_decider_aggregate.py` (create dir/`__init__.py` mirroring `tests/unit/` sibling convention if needed)

**Interfaces:**
- Consumes: `DomainCommand` (Task 1), `CommandRejectedError` (Task 2), `AggregateRoot` (`apply_event`, `get_next_version`, `_get_tenant_from_context`, `aggregate_type`, `_state`).
- Produces: `DeciderAggregate(AggregateRoot[TState])` with abstract static methods `initial_state(aggregate_id: UUID) -> TState`, `decide(command: Any, state: TState) -> list[DomainEvent]`, `evolve(state: TState, event: DomainEvent) -> TState`; method `execute(command: object) -> list[DomainEvent]`; property `state -> TState` (never None). Later tasks import as `from eventsource import DeciderAggregate` (after Task 5) or `from eventsource.aggregates.decider import DeciderAggregate`.

- [ ] **Step 1: Write the failing tests**

```python
"""Tests for DeciderAggregate."""

from uuid import UUID, uuid4

import pytest
from pydantic import BaseModel

from eventsource.aggregates.decider import DeciderAggregate
from eventsource.commands import DomainCommand
from eventsource.events.base import DomainEvent
from eventsource.exceptions import CommandRejectedError


class AccountOpened(DomainEvent):
    event_type: str = "AccountOpened"
    aggregate_type: str = "Account"
    owner: str


class MoneyDeposited(DomainEvent):
    event_type: str = "MoneyDeposited"
    aggregate_type: str = "Account"
    amount: float


class OpenAccount(DomainCommand):
    owner: str


class DepositMoney(DomainCommand):
    amount: float


class AccountState(BaseModel):
    account_id: UUID
    owner: str | None = None
    balance: float = 0.0
    is_open: bool = False


class Account(DeciderAggregate[AccountState]):
    aggregate_type = "Account"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> AccountState:
        return AccountState(account_id=aggregate_id)

    @staticmethod
    def decide(command: object, state: AccountState) -> list[DomainEvent]:
        match command, state:
            case OpenAccount(owner=owner), AccountState(is_open=False):
                return [AccountOpened(aggregate_id=state.account_id, owner=owner)]
            case OpenAccount(), _:
                raise CommandRejectedError("account already open", command=command)
            case DepositMoney(amount=amount), AccountState(is_open=True):
                return [MoneyDeposited(aggregate_id=state.account_id, amount=amount)]
            case DepositMoney(), _:
                raise CommandRejectedError("account not open", command=command)
            case _:
                raise CommandRejectedError(f"unknown command: {command!r}", command=command)

    @staticmethod
    def evolve(state: AccountState, event: DomainEvent) -> AccountState:
        match event:
            case AccountOpened(owner=owner):
                return state.model_copy(update={"owner": owner, "is_open": True})
            case MoneyDeposited(amount=amount):
                return state.model_copy(update={"balance": state.balance + amount})
            case _:
                return state


class TestEagerState:
    def test_state_is_initial_before_any_event(self) -> None:
        acct = Account(uuid4())
        assert acct.state.is_open is False
        assert acct.version == 0

    def test_first_command_accepted(self) -> None:
        acct = Account(uuid4())
        acct.execute(OpenAccount(owner="alice"))
        assert acct.state.is_open is True


class TestExecuteStamping:
    def test_version_and_type_stamped(self) -> None:
        acct = Account(uuid4())
        events = acct.execute(OpenAccount(owner="alice"))
        assert [e.aggregate_version for e in events] == [1]
        assert events[0].aggregate_type == "Account"
        assert acct.version == 1
        assert acct.uncommitted_events == events

    def test_provenance_from_domain_command(self) -> None:
        acct = Account(uuid4())
        cmd = OpenAccount(owner="alice", actor_id="user-1", tenant_id=uuid4())
        (event,) = acct.execute(cmd)
        assert event.causation_id == cmd.command_id
        assert event.correlation_id == cmd.correlation_id
        assert event.actor_id == "user-1"
        assert event.tenant_id == cmd.tenant_id

    def test_plain_object_command_gets_version_but_no_provenance(self) -> None:
        from dataclasses import dataclass

        @dataclass(frozen=True)
        class PlainOpen:
            owner: str

        class PlainAccount(Account):
            @staticmethod
            def decide(command: object, state: AccountState) -> list[DomainEvent]:
                match command:
                    case PlainOpen(owner=owner):
                        return [AccountOpened(aggregate_id=state.account_id, owner=owner)]
                    case _:
                        return Account.decide(command, state)

        acct = PlainAccount(uuid4())
        (event,) = acct.execute(PlainOpen(owner="alice"))
        assert event.aggregate_version == 1
        assert event.causation_id is None

    def test_explicit_fields_win_over_stamping(self) -> None:
        explicit_correlation = uuid4()

        class ExplicitAccount(Account):
            @staticmethod
            def decide(command: object, state: AccountState) -> list[DomainEvent]:
                return [
                    AccountOpened(
                        aggregate_id=state.account_id,
                        owner="alice",
                        correlation_id=explicit_correlation,
                    )
                ]

        acct = ExplicitAccount(uuid4())
        (event,) = acct.execute(OpenAccount(owner="alice"))
        assert event.correlation_id == explicit_correlation
        assert event.causation_id is not None  # not explicit -> still stamped

    def test_multi_event_versions_sequential(self) -> None:
        class DoubleAccount(Account):
            @staticmethod
            def decide(command: object, state: AccountState) -> list[DomainEvent]:
                return [
                    AccountOpened(aggregate_id=state.account_id, owner="a"),
                    MoneyDeposited(aggregate_id=state.account_id, amount=1.0),
                ]

        acct = DoubleAccount(uuid4())
        events = acct.execute(OpenAccount(owner="a"))
        assert [e.aggregate_version for e in events] == [1, 2]
        assert acct.version == 2


class TestRejectionAtomicity:
    def test_rejection_leaves_aggregate_untouched(self) -> None:
        acct = Account(uuid4())
        with pytest.raises(CommandRejectedError, match="not open"):
            acct.execute(DepositMoney(amount=5.0))
        assert acct.version == 0
        assert acct.uncommitted_events == []
        assert acct.state.balance == 0.0


class TestReplayEquivalence:
    def test_load_from_history_equals_folding_evolve(self) -> None:
        agg_id = uuid4()
        acct = Account(agg_id)
        acct.execute(OpenAccount(owner="alice"))
        acct.execute(DepositMoney(amount=25.0))
        history = acct.uncommitted_events

        replayed = Account(agg_id)
        replayed.load_from_history(history)

        folded = Account.initial_state(agg_id)
        for event in history:
            folded = Account.evolve(folded, event)

        assert replayed.state == folded == acct.state
        assert replayed.version == 2


class TestSnapshotRoundTrip:
    def test_serialize_and_restore(self) -> None:
        acct = Account(uuid4())
        acct.execute(OpenAccount(owner="alice"))
        snapshot = acct._serialize_state()

        restored = Account(acct.aggregate_id)
        restored._restore_from_snapshot(snapshot, version=1)
        assert restored.state == acct.state
        assert restored.version == 1
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/aggregates/test_decider_aggregate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'eventsource.aggregates.decider'`

- [ ] **Step 3: Implement** `src/eventsource/aggregates/decider.py`

```python
"""
DeciderAggregate: the decider pattern as a first-class aggregate style.

The domain is three pure functions — initial_state, decide, evolve — and
this class is the imperative shell that adapts them to the AggregateRoot
machinery (repositories, snapshots, replay). See ADR-0022 and
docs/explanation/decider-pattern.md.
"""

from __future__ import annotations

from abc import abstractmethod
from typing import Any
from uuid import UUID

from eventsource.aggregates.base import AggregateRoot, TState
from eventsource.commands.base import DomainCommand
from eventsource.events.base import DomainEvent


class DeciderAggregate(AggregateRoot[TState]):
    """
    Aggregate style built from pure ``decide``/``evolve`` functions.

    Subclasses implement three static methods and set ``aggregate_type``;
    everything else — replay, snapshots, version validation, repository
    integration — is inherited from AggregateRoot.

    Unlike AggregateRoot, state is eagerly initialized: ``state`` is never
    None on this class, so ``decide`` always has a real state to match on.

    Contract note: ``DomainEvent.aggregate_id`` is a required field, so
    ``decide`` must set it. ``initial_state(aggregate_id)`` receives the id
    precisely so state carries the aggregate's identity for decide to use.
    """

    def __init__(self, aggregate_id: UUID) -> None:
        super().__init__(aggregate_id)
        self._state = self.initial_state(aggregate_id)

    @staticmethod
    @abstractmethod
    def initial_state(aggregate_id: UUID) -> TState:  # type: ignore[misc]
        """Return the state of an aggregate before any event has occurred."""

    @staticmethod
    @abstractmethod
    def decide(command: Any, state: TState) -> list[DomainEvent]:  # type: ignore[misc]
        """Given current state, return the events a command produces, or raise."""

    @staticmethod
    @abstractmethod
    def evolve(state: TState, event: DomainEvent) -> TState:  # type: ignore[misc]
        """Return the next state after an event. Should be total (case _: return state)."""

    @property
    def state(self) -> TState:
        """Current state. Never None: eagerly initialized from initial_state()."""
        assert self._state is not None  # established in __init__, maintained by _apply
        return self._state

    def _get_initial_state(self) -> TState:
        return self.initial_state(self.aggregate_id)

    def _apply(self, event: DomainEvent) -> None:
        base = self._state if self._state is not None else self.initial_state(self.aggregate_id)
        self._state = self.evolve(base, event)

    def execute(self, command: object) -> list[DomainEvent]:
        """
        Run decide(), stamp each produced event, and apply it.

        decide() completes before any event is applied, so a rejection
        leaves the aggregate fully untouched. Returns the stamped events.

        Stamping (one model_copy per event; fields decide() set explicitly
        are never overwritten — detected via model_fields_set):
        - always: aggregate_version, aggregate_type
        - when command is a DomainCommand: causation_id (command_id),
          correlation_id, actor_id, tenant_id (command value, else tenant
          context, else untouched)
        """
        events = self.decide(command, self.state)
        applied: list[DomainEvent] = []
        for event in events:
            stamped = self._stamp(event, command)
            self.apply_event(stamped, is_new=True)
            applied.append(stamped)
        return applied

    def _stamp(self, event: DomainEvent, command: object) -> DomainEvent:
        fields_set = event.model_fields_set
        updates: dict[str, Any] = {}
        if "aggregate_version" not in fields_set:
            updates["aggregate_version"] = self.get_next_version()
        if "aggregate_type" not in fields_set:
            updates["aggregate_type"] = self.aggregate_type
        if isinstance(command, DomainCommand):
            if "causation_id" not in fields_set:
                updates["causation_id"] = command.command_id
            if "correlation_id" not in fields_set:
                updates["correlation_id"] = command.correlation_id
            if "actor_id" not in fields_set and command.actor_id is not None:
                updates["actor_id"] = command.actor_id
            if "tenant_id" not in fields_set:
                tenant = (
                    command.tenant_id
                    if command.tenant_id is not None
                    else self._get_tenant_from_context()
                )
                if tenant is not None:
                    updates["tenant_id"] = tenant
        if not updates:
            return event
        return event.model_copy(update=updates)
```

Implementation notes for this step:
- `TState` is imported from `aggregates/base.py` — check its definition there first; if the TypeVar is not exported under that name, import whatever name base.py uses (do not define a second TypeVar).
- If mypy rejects the `state` property override or the abstract staticmethods, resolve with the narrowest possible `# type: ignore[...]` code and a comment — do not loosen base-class types.
- `get_next_version()` must be called inside the per-event loop (versions increment as each event applies), never hoisted.

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/aggregates/test_decider_aggregate.py -v`
Expected: all PASS

- [ ] **Step 5: Export from the aggregates package**

Read `src/eventsource/aggregates/__init__.py` and add `DeciderAggregate` following its existing import/`__all__` pattern.

- [ ] **Step 6: Lint, type-check, run the aggregate suite, commit**

Run: `uv run ruff check src/ tests/ && uv run mypy src/eventsource/ --config-file=pyproject.toml && uv run pytest tests/unit/aggregates/ -v`

```bash
git add src/eventsource/aggregates/ tests/unit/aggregates/
git commit -m "feat: add DeciderAggregate with provenance stamping"
```

---

### Task 4: `create_event(command=)` for the other styles

**Files:**
- Modify: `src/eventsource/aggregates/base.py` (`create_event`, currently around lines 380–450)
- Test: `tests/unit/aggregates/test_decider_aggregate.py` (append)

**Interfaces:**
- Consumes: `DomainCommand` (Task 1).
- Produces: `create_event(self, event_class, *, command: object | None = None, **kwargs)` — same provenance stamping as `DeciderAggregate._stamp`, precedence: explicit kwargs > command fields > tenant context > auto fields.

- [ ] **Step 1: Write the failing test** (append to `tests/unit/aggregates/test_decider_aggregate.py`)

```python
class TestCreateEventCommandProvenance:
    def test_create_event_stamps_provenance_from_command(self) -> None:
        from eventsource.aggregates.base import AggregateRoot

        class ImperativeAccount(AggregateRoot[AccountState]):
            aggregate_type = "Account"

            def _get_initial_state(self) -> AccountState:
                return AccountState(account_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                if isinstance(event, AccountOpened):
                    self._state = AccountState(
                        account_id=self.aggregate_id, owner=event.owner, is_open=True
                    )

            def open(self, command: OpenAccount) -> None:
                self.create_event(AccountOpened, command=command, owner=command.owner)

        acct = ImperativeAccount(uuid4())
        cmd = OpenAccount(owner="alice", actor_id="user-1")
        acct.open(cmd)
        (event,) = acct.uncommitted_events
        assert event.causation_id == cmd.command_id
        assert event.correlation_id == cmd.correlation_id
        assert event.actor_id == "user-1"

    def test_explicit_kwargs_beat_command_fields(self) -> None:
        from eventsource.aggregates.base import AggregateRoot

        explicit = uuid4()

        class ImperativeAccount(AggregateRoot[AccountState]):
            aggregate_type = "Account"

            def _get_initial_state(self) -> AccountState:
                return AccountState(account_id=self.aggregate_id)

            def _apply(self, event: DomainEvent) -> None:
                pass

        acct = ImperativeAccount(uuid4())
        cmd = OpenAccount(owner="alice")
        event = acct.create_event(
            AccountOpened, command=cmd, owner="alice", correlation_id=explicit
        )
        assert event.correlation_id == explicit
        assert event.causation_id == cmd.command_id
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/aggregates/test_decider_aggregate.py -v -k CreateEvent`
Expected: FAIL — `TypeError: create_event() got an unexpected keyword argument 'command'` (or provenance assertions fail)

- [ ] **Step 3: Implement**

In `create_event`, change the signature to `def create_event(self, event_class: type[TEvent], *, command: object | None = None, **kwargs: Any) -> TEvent:` and insert a command-provenance block **after** the existing tenant-context block and **before** `event_kwargs.update(kwargs)` (so precedence is: kwargs > command > context > auto):

```python
        # Optionally auto-populate provenance from a command object
        if isinstance(command, DomainCommand):
            event_kwargs["causation_id"] = command.command_id
            event_kwargs["correlation_id"] = command.correlation_id
            if command.actor_id is not None:
                event_kwargs["actor_id"] = command.actor_id
            if command.tenant_id is not None:
                event_kwargs["tenant_id"] = command.tenant_id

        # User kwargs override auto-populated values  (existing line)
        event_kwargs.update(kwargs)
```

Import `DomainCommand` at the top of `base.py` (`from eventsource.commands.base import DomainCommand`) — a plain import, not TYPE_CHECKING, since it's used in `isinstance`. Update the `create_event` docstring's auto-populated list to mention `command=`. Also mention the keyword in the docstring example.

- [ ] **Step 4: Run to verify it passes, plus the whole unit suite for regressions**

Run: `uv run pytest tests/unit/ -q`
Expected: all PASS (no existing caller passes `command`, the parameter is keyword-only)

- [ ] **Step 5: Lint, type-check, commit**

```bash
git add src/eventsource/aggregates/base.py tests/unit/aggregates/test_decider_aggregate.py
git commit -m "feat: create_event accepts command= for provenance stamping"
```

---

### Task 5: Public exports and import-linter contract

**Files:**
- Modify: `src/eventsource/__init__.py`, `pyproject.toml`
- Test: `tests/unit/commands/test_domain_command.py` (append)

**Interfaces:**
- Produces: `from eventsource import DomainCommand, DeciderAggregate, CommandRejectedError` works.

- [ ] **Step 1: Write the failing test** (append)

```python
class TestPublicExports:
    def test_top_level_imports(self) -> None:
        from eventsource import CommandRejectedError, DeciderAggregate, DomainCommand

        assert DomainCommand is not None
        assert DeciderAggregate is not None
        assert CommandRejectedError is not None

    def test_in_all(self) -> None:
        import eventsource

        for name in ("DomainCommand", "DeciderAggregate", "CommandRejectedError"):
            assert name in eventsource.__all__
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/commands/ -v -k Exports`
Expected: FAIL — ImportError

- [ ] **Step 3: Implement**

In `src/eventsource/__init__.py`: add `from eventsource.commands import DomainCommand` near the events imports, `DeciderAggregate` to the existing `from eventsource.aggregates.base import ...`-adjacent imports (import from `eventsource.aggregates.decider`), and `CommandRejectedError` to the existing exceptions import block. Add all three to `__all__` in the appropriate alphabetical/sectioned position (read the file; it groups `__all__` by category — put `DomainCommand` near `DomainEvent`, `DeciderAggregate` near `DeclarativeAggregate`, `CommandRejectedError` with the other exceptions).

In `pyproject.toml`: find the import-linter contract named `"Tier 0 modules must not import sqlalchemy"` and add `"eventsource.commands",` to its `source_modules` list (alongside the events/serialization entries).

- [ ] **Step 4: Run tests + import-linter**

Run: `uv run pytest tests/unit/commands/ -v && uv run lint-imports 2>/dev/null || pre-commit run import-linter --all-files`
Expected: PASS (if neither import-linter invocation works standalone, `make check` in Task 8 covers it)

- [ ] **Step 5: Commit**

```bash
git add src/eventsource/__init__.py pyproject.toml tests/unit/commands/test_domain_command.py
git commit -m "feat: export DomainCommand, DeciderAggregate, CommandRejectedError"
```

---

### Task 6: Convert `examples/basic_usage.py` (smoke test) + move imperative reference

**Files:**
- Create: `examples/imperative_example.py` (receives the current `BankAccountAggregate` code)
- Rewrite: `examples/basic_usage.py` (decider style)
- Modify: `docs/explanation/aggregate-styles.md` (update file-path references)

**Interfaces:**
- Consumes: `DomainCommand`, `DeciderAggregate`, `CommandRejectedError` from `eventsource` (Task 5).

- [ ] **Step 1: Move the imperative example.** Copy `examples/basic_usage.py` to `examples/imperative_example.py` verbatim, then edit only its module docstring: title "Imperative Aggregate Example", first paragraph noting it demonstrates the hand-written `_apply` style on `AggregateRoot` and pointing to `basic_usage.py` for the primary (decider) style and `docs/explanation/aggregate-styles.md` for the comparison. Keep any `--8<--` snippet markers exactly as they are (docs may include them by path — Step 4 verifies).

- [ ] **Step 2: Rewrite `examples/basic_usage.py`** in the decider style. Keep the same events (`AccountOpened`, `MoneyDeposited`, `MoneyWithdrawn`), same `BankAccountState`, same `main()` flow (open → deposit → withdraw → save → reload), same `--8<-- [start:events]`/`[end:events]` marker names for any section that keeps the same meaning. The aggregate section becomes:

```python
# =============================================================================
# Step 2: Define Commands
# =============================================================================
# Commands are immutable intents. Unlike events, they may be rejected -- and a
# rejected command leaves no trace in the event store.


class OpenAccount(DomainCommand):
    """Request to open a new bank account."""

    owner_name: str
    initial_balance: float = 0.0


class DepositMoney(DomainCommand):
    """Request to deposit money into an account."""

    amount: float


class WithdrawMoney(DomainCommand):
    """Request to withdraw money from an account."""

    amount: float


# =============================================================================
# Step 3: The domain as pure functions
# =============================================================================
# decide: command + state -> events (or a rejection).
# evolve: state + event -> next state.
# Both are pure -- no I/O, no versions, testable with plain asserts.


class BankAccountAggregate(DeciderAggregate[BankAccountState]):
    """Bank account in the decider style."""

    aggregate_type = "BankAccount"

    @staticmethod
    def initial_state(aggregate_id: UUID) -> BankAccountState:
        return BankAccountState(account_id=aggregate_id)

    @staticmethod
    def decide(command: object, state: BankAccountState) -> list[DomainEvent]:
        match command, state:
            case OpenAccount(owner_name=name, initial_balance=balance), BankAccountState(is_open=False):
                return [
                    AccountOpened(
                        aggregate_id=state.account_id,
                        owner_name=name,
                        initial_balance=balance,
                    )
                ]
            case OpenAccount(), _:
                raise CommandRejectedError("account is already open", command=command)
            case DepositMoney(amount=amount), BankAccountState(is_open=True):
                if amount <= 0:
                    raise CommandRejectedError("deposit must be positive", command=command)
                return [MoneyDeposited(aggregate_id=state.account_id, amount=amount)]
            case WithdrawMoney(amount=amount), BankAccountState(is_open=True):
                if amount <= 0:
                    raise CommandRejectedError("withdrawal must be positive", command=command)
                if amount > state.balance:
                    raise CommandRejectedError("insufficient funds", command=command)
                return [MoneyWithdrawn(aggregate_id=state.account_id, amount=amount)]
            case (DepositMoney() | WithdrawMoney()), _:
                raise CommandRejectedError("account is not open", command=command)
            case _:
                raise CommandRejectedError(f"unknown command: {command!r}", command=command)

    @staticmethod
    def evolve(state: BankAccountState, event: DomainEvent) -> BankAccountState:
        match event:
            case AccountOpened(owner_name=name, initial_balance=balance):
                return state.model_copy(
                    update={"owner_name": name, "balance": balance, "is_open": True}
                )
            case MoneyDeposited(amount=amount):
                return state.model_copy(update={"balance": state.balance + amount})
            case MoneyWithdrawn(amount=amount):
                return state.model_copy(update={"balance": state.balance - amount})
            case _:
                return state
```

Update the top imports to `from eventsource import (AggregateRepository, DeciderAggregate, DomainCommand, CommandRejectedError, DomainEvent, InMemoryEventStore, register_event)`, adjust `main()` call sites from `account.open(...)`/`account.deposit(...)` to `account.execute(OpenAccount(owner_name=..., initial_balance=...))` etc., and update the module docstring to say this is the decider style with a pointer to `imperative_example.py` and `aggregate_example.py` for the other styles. Match the existing file's comment density and section-banner formatting throughout. If `BankAccountState` uses a different id field name than `account_id`, keep the existing name.

- [ ] **Step 3: Run both examples**

Run: `uv run python examples/basic_usage.py && uv run python examples/imperative_example.py`
Expected: both run to completion; `basic_usage.py` output preserves the original example's printed balances.

- [ ] **Step 4: Update doc references and verify docs build.** `grep -rn "basic_usage" docs/ mkdocs.yml` — for each hit describing the *imperative* bank account (notably `docs/explanation/aggregate-styles.md`), change the path to `examples/imperative_example.py`. Then: `uv run mkdocs build --strict`
Expected: build passes (this also catches any `--8<--` snippet include that moved).

- [ ] **Step 5: Lint and commit**

Run: `uv run ruff check examples/ && uv run ruff format examples/`

```bash
git add examples/ docs/explanation/aggregate-styles.md
git commit -m "docs: basic_usage example adopts decider style, imperative moves to imperative_example"
```

---

### Task 7: ADR-0022

**Files:**
- Create: `docs/adrs/0022-command-objects-and-decider-style.md`
- Modify: `docs/adrs/index.md` (add row/entry mirroring existing format), `mkdocs.yml` (add nav entry under ADRs, mirroring the ADR-0001 line format)

- [ ] **Step 1: Write the ADR.** Read `docs/adrs/0001-async-first-design.md` first and mirror its section structure exactly (Status/Context/Decision/Consequences). Content:

```markdown
# ADR-0022: Command Objects and the Decider Aggregate Style

## Status

Accepted (2026-07-30)

## Context

The library shipped two aggregate styles (hand-written `_apply` on
`AggregateRoot`; `@handles` on `DeclarativeAggregate`). The decider pattern —
the domain as pure `decide`/`evolve` functions — was documented in
`docs/explanation/decider-pattern.md` as a userland recipe with two structural
problems: `AggregateRoot._state` is `None` until the first event (a naive
decider rejects its first command), and version stamping required a manual
`model_copy` per event. Separately, `DomainEvent` carries
`correlation_id`/`causation_id`/`actor_id`/`tenant_id` but nothing originated
the chain: `with_causation()` links event→event only. In CQRS the originator
of an event chain is the command. Benchmarks (2026-07-30) showed the decider's
overhead is ~1.5x on the command path (~8 µs/order, dominated by pydantic event
construction paid by all styles) and ~1.06x on replay: maintainability, not
performance, decides.

## Decision

1. **`DomainCommand`** (entities ring, `commands/` beside `events/`): frozen
   pydantic model with `command_id`, `issued_at`, `correlation_id`,
   `actor_id`, `tenant_id`. Commands are **never persisted** — a rejected
   command leaves no trace. No registry, no serialization, no command bus.
   Commands have no `causation_id` field; `caused_by(event)` copies the
   event's `correlation_id` so saga-issued commands continue the workflow
   chain by correlation.
2. **`DeciderAggregate`** subclasses `AggregateRoot`: abstract static
   `initial_state`/`decide`/`evolve`; eager state initialization (state is
   never `None`); `execute(command)` runs `decide` to completion, then stamps
   each event with one `model_copy` — always `aggregate_version` and
   `aggregate_type`; for `DomainCommand`s also `causation_id=command_id`,
   `correlation_id`, `actor_id`, `tenant_id`. Precedence: fields in
   `event.model_fields_set` are never overwritten. Tenant resolution:
   command value → tenant context → untouched. Rejections are atomic: no
   version bump, no uncommitted events.
3. **Structural typing, opt-in provenance**: `decide`/`execute`/`create_event`
   accept any object as a command; `isinstance(command, DomainCommand)` is
   what unlocks provenance. `create_event(command=...)` gives the imperative
   and declarative styles identical stamping.
4. **`CommandRejectedError`** is the conventional (not required) rejection
   type: one catchable exception meaning "the domain said no".
5. **The decider is the primary showcased style**: examples, tutorials, and
   general-purpose fixtures lead with it; imperative and declarative each
   keep one worked reference example.

## Consequences

- Every event can be traced to the command that caused it and the actor who
  issued it; `causation_id` references a `command_id` that is resolvable only
  if the application logs its commands, but it still groups the events of one
  command and marks them command-caused.
- The command path pays one `model_copy` per event (~1–2 µs); replay is
  unaffected. Rejected alternatives: an event-spec `decide` contract (faster
  but non-standard signature, awkward assertions) and version-at-append
  (ADR-scale churn to save ~2 µs/event).
- ADR-0001 (async-first) stands: `decide`/`evolve` are sync pure functions
  like `_apply`; I/O boundaries remain async. ADR-0012 stands: commands have
  no registry by design. ADR-0018 stands: this ADR documents the
  command-then-context tenant resolution order as an extension. ADR-0019
  stands: commands are entities-ring; no port changes.
```

- [ ] **Step 2: Wire navigation.** Add the ADR to `docs/adrs/index.md` (mirror existing row format) and to the `ADRs:` section of `mkdocs.yml` nav as `- "ADR-0022: Command Objects & Decider Style": adrs/0022-command-objects-and-decider-style.md`.

- [ ] **Step 3: Verify docs build**

Run: `uv run mkdocs build --strict`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add docs/adrs/ mkdocs.yml
git commit -m "docs: ADR-0022 command objects and decider aggregate style"
```

---

### Task 8: PR 1 — full gate and draft PR

- [ ] **Step 1: Run the full CI-parity gate**

Run: `make check`
Expected: lint, mypy, import-linter, bandit/pip-audit, unit suite all green. Fix anything that fails before proceeding (if a fix is non-trivial, stop and report rather than force it).

- [ ] **Step 2: Push and open draft PR**

```bash
git push -u origin decider-command-dogfood
gh pr create --draft --title "feat: DomainCommand, DeciderAggregate, and command provenance" \
  --body "PR 1 of 3 for docs/superpowers/specs/2026-07-30-decider-command-dogfood-design.md: commands package, DeciderAggregate, CommandRejectedError, create_event(command=), exports, ADR-0022, basic_usage converted as smoke test. PR 2: dogfood wave. PR 3: DeciderScenario BDD harness.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

### Task 9: Fixtures conversion (PR 2 begins)

**Files:**
- Modify: `tests/fixtures/aggregates.py`, `tests/fixtures/__init__.py` (if it re-exports)

**Interfaces:**
- Consumes: `DeciderAggregate`, `DomainCommand`, `CommandRejectedError`.
- Produces: `OrderAggregate` reimplemented as a `DeciderAggregate` with commands `CreateOrder(customer_id: UUID)`, `AddOrderItem(item_name: str, price: float)`, `ShipOrder(tracking_number: str)`; same public command-method signatures preserved as thin wrappers so existing tests pass unmodified.

- [ ] **Step 1: Survey usage.** `grep -rn "OrderAggregate" tests/ | grep -v fixtures` — list every test touching it and which methods/attributes they use (`create`, `add_item`, `ship`, `state`, `version`, `uncommitted_events`).

- [ ] **Step 2: Reimplement `OrderAggregate`** as `DeciderAggregate[OrderState]`: add the three command models (as `DomainCommand` subclasses, module-level), `initial_state`/`decide`/`evolve` static methods reproducing the current `_apply` + command-method business rules exactly (read the current implementation first; port each guard clause into a `decide` case, each state mutation into an `evolve` case). Keep the existing methods `create(customer_id)`, `add_item(item_name, price)`, `ship(tracking_number)` as one-line wrappers, e.g. `def create(self, customer_id: UUID) -> None: self.execute(CreateOrder(customer_id=customer_id))` — existing tests must not change. If a current guard raises `ValueError`, keep raising `ValueError` from `decide` for that case (behavior-preserving beats convention here); note any such case in the commit message.
- **Do NOT touch** `CounterAggregate` (it exists to test `AggregateRoot` itself) or `DeclarativeCounterAggregate` (tests `@handles` dispatch).

- [ ] **Step 3: Run the whole unit suite**

Run: `uv run pytest tests/unit/ -q`
Expected: all PASS with zero test-file modifications. If a test fails, the port changed behavior — fix the fixture, not the test.

- [ ] **Step 4: Lint, type-check, commit**

```bash
git add tests/fixtures/
git commit -m "test: OrderAggregate fixture adopts decider style"
```

---

### Task 10: Tutorial 03 leads with the decider

**Files:**
- Modify: `docs/tutorials/03-first-aggregate.md`; survey `docs/tutorials/*.md` and `docs/getting-started.md` for aggregate-style assumptions

- [ ] **Step 1: Rewrite `03-first-aggregate.md`.** Keep the same Order domain (created/shipped/cancelled) and the same pedagogical beats (state model → events → aggregate → business rule → replay → other styles), but the aggregate section teaches `DeciderAggregate`: commands as `DomainCommand` values, `decide` as the business-rule home (with a `CommandRejectedError` rejection walkthrough), `evolve` as the fold, `execute()` at the call site, and `load_from_history()` unchanged. The closing "same aggregate, declarative style" section becomes "other styles", pointing to `imperative_example.py`, `aggregate_example.py`, and `docs/explanation/aggregate-styles.md`. Every code block in the rewritten tutorial must be runnable: build the tutorial's final file in `$CLAUDE_JOB_DIR/tmp` (or a scratch dir) by concatenating the blocks in order and run it with `uv run python` before committing.

- [ ] **Step 2: Sweep the other tutorials.** `grep -rln "AggregateRoot\|create_event\|_apply" docs/tutorials/ docs/getting-started.md` — in files other than 03, do NOT restyle; only fix statements the new positioning makes false (e.g., "the two styles" → "the three styles", pointers to 03's declarative ending). Small surgical edits.

- [ ] **Step 3: Verify docs build**

Run: `uv run mkdocs build --strict`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add docs/tutorials/ docs/getting-started.md
git commit -m "docs: first-aggregate tutorial teaches the decider style first"
```

---

### Task 11: Bench decider variant

**Files:**
- Modify: `bench/core/domain.py`; survey `bench/` for where `BenchCounter` is instantiated to decide wiring

**Interfaces:**
- Produces: `BenchDeciderCounter(DeciderAggregate[BenchCounterState])` with the same increment semantics as `BenchCounter`, plus command `BenchIncrement(amount: int)`.

- [ ] **Step 1: Add the decider variant** to `bench/core/domain.py` (read the existing `BenchCounter` first; mirror its event and state exactly):

```python
class BenchIncrement(DomainCommand):
    amount: int = 1


class BenchDeciderCounter(DeciderAggregate[BenchCounterState]):
    aggregate_type = BenchCounter.aggregate_type

    @staticmethod
    def initial_state(aggregate_id: UUID) -> BenchCounterState:
        return BenchCounterState()

    @staticmethod
    def decide(command: object, state: BenchCounterState) -> list[DomainEvent]:
        match command:
            case BenchIncrement(amount=amount):
                return [BenchCounterIncremented(aggregate_id=..., amount=amount)]
            case _:
                raise CommandRejectedError(f"unknown command: {command!r}", command=command)

    @staticmethod
    def evolve(state: BenchCounterState, event: DomainEvent) -> BenchCounterState:
        match event:
            case BenchCounterIncremented(amount=amount):
                return state.model_copy(update={"count": state.count + amount})
            case _:
                return state
```

Adapt field names to the real `BenchCounterState`/`BenchCounterIncremented` definitions (the `...` for `aggregate_id` must come from state — if `BenchCounterState` has no id field, add one defaulting from `initial_state(aggregate_id)`, mirroring how the examples carry identity). Then `grep -rn "BenchCounter" bench/` and wire `BenchDeciderCounter` into whichever scenario/matrix module instantiates aggregates so both variants run — follow the existing registration pattern; if the harness has no aggregate-variant axis, add the decider as an additional scenario rather than restructuring the matrix.

- [ ] **Step 2: Smoke-run the bench harness's fastest path**

Run: whatever `make bench`'s smallest invocation is (check `Makefile` / `bench/README` — e.g. a `--quick` or single-scenario flag). If the bench requires Docker services that aren't running, verify importability instead: `uv run python -c "from bench.core.domain import BenchDeciderCounter, BenchIncrement"`
Expected: no import errors; harness lists/runs the new variant.

- [ ] **Step 3: Lint and commit**

```bash
git add bench/
git commit -m "feat: bench decider counter variant for style comparison"
```

---

### Task 12: Docs wave — decider-pattern.md, README touch

**Files:**
- Modify: `docs/explanation/decider-pattern.md`, `docs/explanation/aggregate-styles.md`, `README.md`

- [ ] **Step 1: Update `decider-pattern.md`.** The "imperative shell" section now shows `DeciderAggregate` (subclass with three static methods; no hand-rolled `decider_state` property — that gotcha is solved by eager initialization, which the doc should state). Move the old userland shell into a closing "How the shell works underneath" appendix, framed as what `DeciderAggregate` does for you. Replace the "There is currently no first-class `Decider` abstraction…" closing paragraph with a short section on `DomainCommand` provenance (causation/correlation/actor/tenant stamping, precedence) linking to ADR-0022. Keep the benchmark section as-is (numbers are unchanged; they measured the same stamping design).

- [ ] **Step 2: Update `aggregate-styles.md`.** The intro's third-style pointer paragraph (added in PR #79) now says the decider ships as `DeciderAggregate` and is the primary showcased style; keep the deep-dive delegated to `decider-pattern.md`.

- [ ] **Step 3: README touch.** In the decider section (landed in PR #80): change the shell code block to the `DeciderAggregate` subclass form (drop the hand-written `decider_state`/`_apply`/`execute` — show the three static methods), make commands subclass `DomainCommand`, and change rejections to `CommandRejectedError`. Keep the imperative Quick Start first, untouched. Re-verify both README variants run: extract both python blocks, compose as in prior PRs (swap `Order` class, convert the two command calls), run both, and require identical output matching the inline comments.

- [ ] **Step 4: Verify docs build**

Run: `uv run mkdocs build --strict`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add docs/explanation/ README.md
git commit -m "docs: feature DeciderAggregate across decider docs and readme"
```

---

### Task 13: PR 2 — full gate and draft PR

- [ ] **Step 1: Run `make check`** — all green.

- [ ] **Step 2: Push and open draft PR** (if PR 1 is not yet merged, this continues the same branch and the PR body should say so; if PR 1 merged, rebase onto main first)

```bash
git push
gh pr create --draft --title "docs: dogfood the decider style across examples, tutorials, fixtures, bench" \
  --body "PR 2 of 3 for the decider/command spec: fixtures OrderAggregate, tutorial 03, bench variant, decider docs + readme adopt DeciderAggregate.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

### Task 14: `DeciderScenario` (PR 3)

**Files:**
- Modify: `src/eventsource/testing/bdd.py` (append), `src/eventsource/testing/__init__.py` (export)
- Test: `tests/unit/testing/test_decider_scenario.py` (mirror `tests/unit/` dir conventions)

**Interfaces:**
- Consumes: `DeciderAggregate` (Task 3), `CommandRejectedError` (Task 2).
- Produces: `DeciderScenario` — constructor `(aggregate_class=None, *, decide=None, evolve=None, initial_state=None, aggregate_id: UUID | None = None)`; chainable `given(*events) -> Self`, `when(command) -> Self`, `then_events(*event_types) -> Self`, `then_rejected(exc_type=CommandRejectedError, match: str | None = None) -> Self`; property `events -> list[DomainEvent]`.

- [ ] **Step 1: Write the failing tests** — reuse the `Account` decider from Task 3's test module by importing it: `from tests.unit.aggregates.test_decider_aggregate import Account, AccountOpened, DepositMoney, MoneyDeposited, OpenAccount` (if `tests/` is not importable as a package in this repo's pytest config, copy the ~60-line Account definition into this test file instead — check how other test modules share fixtures first; prefer whatever `tests/fixtures/` convention exists).

```python
"""Tests for DeciderScenario."""

from uuid import uuid4

import pytest

from eventsource.exceptions import CommandRejectedError
from eventsource.testing import DeciderScenario


class TestGivenWhenThen:
    def test_then_events_asserts_types_in_order(self) -> None:
        agg_id = uuid4()
        (
            DeciderScenario(Account, aggregate_id=agg_id)
            .given(AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner="alice"))
            .when(DepositMoney(amount=5.0))
            .then_events(MoneyDeposited)
        )

    def test_then_events_fails_on_wrong_type(self) -> None:
        agg_id = uuid4()
        scenario = (
            DeciderScenario(Account, aggregate_id=agg_id)
            .given(AccountOpened(aggregate_id=agg_id, aggregate_version=1, owner="alice"))
            .when(DepositMoney(amount=5.0))
        )
        with pytest.raises(AssertionError):
            scenario.then_events(AccountOpened)

    def test_then_rejected_default_type_and_match(self) -> None:
        (
            DeciderScenario(Account)
            .when(DepositMoney(amount=5.0))
            .then_rejected(match="not open")
        )

    def test_then_rejected_accepts_custom_exception(self) -> None:
        (
            DeciderScenario(Account)
            .when(DepositMoney(amount=5.0))
            .then_rejected(CommandRejectedError)
        )

    def test_then_events_reports_unexpected_rejection(self) -> None:
        scenario = DeciderScenario(Account).when(DepositMoney(amount=5.0))
        with pytest.raises(AssertionError, match="rejected"):
            scenario.then_events(MoneyDeposited)

    def test_then_rejected_fails_when_events_produced(self) -> None:
        scenario = DeciderScenario(Account).when(OpenAccount(owner="alice"))
        with pytest.raises(AssertionError):
            scenario.then_rejected()

    def test_events_property_exposes_produced_events(self) -> None:
        scenario = DeciderScenario(Account).when(OpenAccount(owner="alice"))
        assert len(scenario.events) == 1
        assert isinstance(scenario.events[0], AccountOpened)

    def test_three_function_form(self) -> None:
        (
            DeciderScenario(
                decide=Account.decide,
                evolve=Account.evolve,
                initial_state=Account.initial_state,
            )
            .when(OpenAccount(owner="alice"))
            .then_events(AccountOpened)
        )

    def test_when_before_then_required(self) -> None:
        with pytest.raises(AssertionError, match="when"):
            DeciderScenario(Account).then_events(AccountOpened)
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/unit/testing/test_decider_scenario.py -v`
Expected: FAIL — ImportError on `DeciderScenario`

- [ ] **Step 3: Implement** (append to `src/eventsource/testing/bdd.py`)

```python
class DeciderScenario:
    """
    Synchronous given/when/then harness for decider-style domains.

    Works with a DeciderAggregate subclass or the three functions directly.
    No store, no event loop, no fixtures: ``given`` folds events through
    ``evolve`` from ``initial_state``, ``when`` runs ``decide`` capturing
    events or the raised exception, ``then_*`` assert.

    Example:
        >>> (DeciderScenario(OrderAggregate)
        ...     .given(OrderCreated(aggregate_id=oid, aggregate_version=1, ...))
        ...     .when(ShipOrder(tracking_number="T"))
        ...     .then_events(OrderShipped))
    """

    def __init__(
        self,
        aggregate_class: type[Any] | None = None,
        *,
        decide: Callable[[Any, Any], list[DomainEvent]] | None = None,
        evolve: Callable[[Any, DomainEvent], Any] | None = None,
        initial_state: Callable[[UUID], Any] | None = None,
        aggregate_id: UUID | None = None,
    ) -> None:
        if aggregate_class is not None:
            decide = aggregate_class.decide
            evolve = aggregate_class.evolve
            initial_state = aggregate_class.initial_state
        if decide is None or evolve is None or initial_state is None:
            raise TypeError(
                "DeciderScenario needs an aggregate class or all of "
                "decide=, evolve=, initial_state="
            )
        self._decide = decide
        self._evolve = evolve
        self._aggregate_id = aggregate_id if aggregate_id is not None else uuid4()
        self._state = initial_state(self._aggregate_id)
        self._events: list[DomainEvent] | None = None
        self._error: BaseException | None = None

    @property
    def events(self) -> list[DomainEvent]:
        """Events produced by when(); empty before when() or on rejection."""
        return list(self._events) if self._events is not None else []

    def given(self, *events: DomainEvent) -> "DeciderScenario":
        """Fold prior events into state via evolve."""
        for event in events:
            self._state = self._evolve(self._state, event)
        return self

    def when(self, command: object) -> "DeciderScenario":
        """Run decide, capturing produced events or the raised exception."""
        try:
            self._events = list(self._decide(command, self._state))
        except Exception as exc:  # noqa: BLE001 - the exception IS the result
            self._error = exc
        return self

    def then_events(self, *event_types: type[DomainEvent]) -> "DeciderScenario":
        """Assert the command produced exactly these event types, in order."""
        assert self._events is not None or self._error is not None, (
            "call when() before then_events()"
        )
        if self._error is not None:
            raise AssertionError(
                f"expected events {[t.__name__ for t in event_types]} but the "
                f"command was rejected: {self._error!r}"
            )
        assert self._events is not None
        actual = [type(e).__name__ for e in self._events]
        expected = [t.__name__ for t in event_types]
        assert actual == expected, f"expected events {expected}, got {actual}"
        return self

    def then_rejected(
        self,
        exc_type: type[BaseException] = CommandRejectedError,
        match: str | None = None,
    ) -> "DeciderScenario":
        """Assert the command was rejected with exc_type (default CommandRejectedError)."""
        assert self._events is not None or self._error is not None, (
            "call when() before then_rejected()"
        )
        if self._error is None:
            raise AssertionError(
                f"expected rejection but command produced {self.events!r}"
            )
        assert isinstance(self._error, exc_type), (
            f"expected {exc_type.__name__}, got {type(self._error).__name__}: {self._error}"
        )
        if match is not None:
            import re

            assert re.search(match, str(self._error)), (
                f"rejection message {str(self._error)!r} does not match {match!r}"
            )
        return self
```

Add the needed imports at the top of `bdd.py` (`uuid4`, `CommandRejectedError` from `eventsource.exceptions`) following its existing import layout. Export `DeciderScenario` from `src/eventsource/testing/__init__.py` mirroring how the other bdd helpers are exported (check its `__all__`).

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/unit/testing/ -v`
Expected: all PASS

- [ ] **Step 5: Lint, type-check, commit**

```bash
git add src/eventsource/testing/ tests/unit/testing/
git commit -m "feat: DeciderScenario given/when/then harness"
```

---

### Task 15: PR 3 — testing docs, full gate, draft PR

- [ ] **Step 1: Document the harness.** Add a "Testing deciders" subsection to whichever doc covers the BDD helpers (`grep -rln "given_events\|InMemoryTestHarness" docs/` — likely `docs/api/testing.md` and/or tutorial 08) with the `DeciderScenario` example from Task 14's docstring, adapted to the doc's running example. `uv run mkdocs build --strict` passes.

- [ ] **Step 2: Run `make check`** — all green.

- [ ] **Step 3: Push and open draft PR**

```bash
git push
gh pr create --draft --title "feat: DeciderScenario BDD harness for decider-style domains" \
  --body "PR 3 of 3 for the decider/command spec: synchronous given/when/then scenario harness, exported from eventsource.testing, with docs.

🤖 Generated with [Claude Code](https://claude.com/claude-code)"
```

---

## Self-Review Notes (completed)

- **Spec coverage:** DomainCommand (T1), CommandRejectedError (T2), DeciderAggregate + stamping + atomicity + replay/snapshot (T3), create_event(command=) (T4), exports + import-linter (T5), basic_usage smoke + imperative move + aggregate-styles pointers (T6, pulled forward from PR 2 — rationale in File Structure note), ADR-0022 + stands-verdicts (T7), fixtures (T9), tutorials (T10), bench (T11), decider docs + README (T12), DeciderScenario (T14) + testing docs (T15). Gates per PR (T8/T13/T15).
- **Known judgment calls for implementers:** fixture `OrderAggregate` keeps `ValueError` where current behavior uses it (behavior-preserving); `CounterAggregate`/`DeclarativeCounterAggregate` intentionally NOT converted (they are the subject under test for their styles).
- **Type consistency check:** `execute(command: object)`, `decide(command: Any, ...)` — the looseness is deliberate (structural typing); `DeciderScenario.then_rejected` default `CommandRejectedError` matches T2's class name; `caused_by` copies `correlation_id` only (matches spec and ADR text).
