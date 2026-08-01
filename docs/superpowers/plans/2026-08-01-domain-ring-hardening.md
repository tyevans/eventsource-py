# Domain Ring Hardening Wave Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the two reproduced domain-ring bugs (parent `FieldInfo` corruption, `clear_tenant_context` leak), remove fragile patterns (silent handler collisions, typo-swallowing events, bare assert, exception MI traps), consolidate duplicated logic (handler discovery, event-type derivation, provenance stamping), and move 13 infrastructure exception classes from `domain/exceptions.py` to a new `ports/exceptions.py`.

**Architecture:** All changes stay within the ring rules: domain remains stdlib+pydantic only; the exception move goes *outward* (domain → ports), which the layers contract permits. Single source of truth is established for event-type naming (`DomainEvent.event_type_name()`), handler discovery (`decorators.discover_handlers()`), and provenance stamping (`AggregateRoot._provenance_updates()`).

**Tech Stack:** Python 3.13, pydantic v2, pytest, uv, import-linter, mypy strict.

## Global Constraints

- Branch `domain-hardening-wave` off `origin/main` (commit 9acb6db, includes 0.9.0). PR targets `main`. **Never self-merge.**
- NO-SHIMS standing policy: moved/renamed public names get no deprecation shims; breaking changes are changelog-flagged (`**BREAKING:**`) and ADR-recorded. Pre-1.0, no-external-consumers rule per ADRs 0025-0034.
- Every task: `uv run ruff check src tests && uv run mypy src/eventsource/` must pass before commit. Full gate at the end is `make check`.
- Commit style: `<type>: <lowercase description>` (fix/feat/refactor/test/docs/chore).
- TDD: write the failing test, see it fail, implement, see it pass, commit.
- mypy strict: all new functions fully annotated.
- ADR bodies are immutable — amend via Status pointers + new ADRs (0041, 0042 — current highest is 0040).
- Tests for domain live in `tests/unit/domain/`; run targeted tests with `uv run pytest tests/unit/domain/ -x -q` (subagents run targeted tests only; orchestrator owns the full suite).

---

### Task 1: Unified event-type derivation (fixes parent-FieldInfo corruption bug)

**Files:**
- Modify: `src/eventsource/domain/event.py:145-229` (`__init_subclass__`, `_ensure_event_type`, new classmethod)
- Modify: `src/eventsource/domain/event_registry.py:130-154` (`_resolve_event_type`)
- Test: `tests/unit/domain/test_event_type_auto.py` (add), `tests/unit/domain/test_event_registry.py` (add)

**Interfaces:**
- Produces: `DomainEvent.event_type_name() -> str` classmethod — the single source of truth for an event class's wire name. Later tasks and the registry rely on it.

**Background (the bug):** `DomainEvent.__init_subclass__` currently runs *before* pydantic builds the subclass's own `model_fields`, so `cls.model_fields["event_type"]` resolves to the **parent's** dict and `field_info.default = cls.__name__` (event.py:193) mutates the parent's shared `FieldInfo`. Defining `class Child(Parent)` flips `Parent.model_fields["event_type"].default` to `"Child"`. Instances are unaffected (their core schema is already frozen), but `EventRegistry._resolve_event_type` reads `model_fields.default`, so registering `Parent` after `Child` exists files it under `"Child"` — wire value `"Parent"` then fails deserialization lookup.

- [ ] **Step 1: Setup — create the branch**

```bash
git fetch origin main
git checkout -b domain-hardening-wave origin/main
```

- [ ] **Step 2: Write the failing tests**

Append to `tests/unit/domain/test_event_type_auto.py`:

```python
from uuid import uuid4

from eventsource.domain.event import DomainEvent
from eventsource.domain.event_registry import EventRegistry


class TestSubclassingDoesNotCorruptParent:
    def test_parent_field_default_survives_subclassing(self) -> None:
        class ParentEvent(DomainEvent):
            aggregate_type: str = "P"

        class ChildEvent(ParentEvent):
            pass

        assert ParentEvent.event_type_name() == "ParentEvent"
        assert ChildEvent.event_type_name() == "ChildEvent"
        event = ParentEvent(aggregate_id=uuid4())
        assert event.event_type == "ParentEvent"

    def test_domainevent_base_default_untouched(self) -> None:
        class AnythingEvent(DomainEvent):
            aggregate_type: str = "A"

        assert DomainEvent.model_fields["event_type"].default == ""

    def test_registry_key_matches_wire_value_after_subclassing(self) -> None:
        registry = EventRegistry()

        class OrderPlaced(DomainEvent):
            aggregate_type: str = "Order"

        class OrderPlacedSpecial(OrderPlaced):
            pass

        # Register parent AFTER child exists — the corruption trigger.
        registry.register(OrderPlaced)
        registry.register(OrderPlacedSpecial)
        assert registry.get("OrderPlaced") is OrderPlaced
        assert registry.get("OrderPlacedSpecial") is OrderPlacedSpecial

    def test_explicit_wire_name_inherited_by_subclass(self) -> None:
        class LegacyEvent(DomainEvent):
            event_type: str = "legacy.v1"
            suppress_event_type_warning = True
            aggregate_type: str = "L"

        class LegacyChildEvent(LegacyEvent):
            pass

        # Inherited explicit default is what instances serialize, so the
        # registry must agree (registering both then raises Duplicate).
        assert LegacyEvent.event_type_name() == "legacy.v1"
        assert LegacyChildEvent.event_type_name() == "legacy.v1"
```

- [ ] **Step 3: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_event_type_auto.py -x -q`
Expected: FAIL — `AttributeError: ... has no attribute 'event_type_name'`

- [ ] **Step 4: Implement**

In `src/eventsource/domain/event.py`:

(a) Delete the `elif` branch of `__init_subclass__` (current lines 186-193, the `field_info.default = cls.__name__` mutation). Keep the explicit-type mismatch warning above it untouched.

(b) Add the classmethod right after `__init_subclass__`:

```python
    @classmethod
    def event_type_name(cls) -> str:
        """
        Canonical wire name for this event class.

        Resolution: a non-empty explicit ``event_type`` field default
        (declared on this class or inherited from a parent's explicit
        declaration), else the class name. Single source of truth shared
        by instance construction (_ensure_event_type) and EventRegistry —
        do not derive the name from ``model_fields`` anywhere else.

        Safe to call only after class creation completes (i.e. never from
        ``__init_subclass__``, where ``cls.model_fields`` still belongs to
        the parent).
        """
        field_info = cls.model_fields.get("event_type")
        if field_info is not None and isinstance(field_info.default, str) and field_info.default:
            return field_info.default
        return cls.__name__
```

(c) Simplify `_ensure_event_type`'s body (keep decorator and docstring, trim the stale default-mutation references from the docstring):

```python
        if isinstance(data, dict):
            if not data.get("event_type"):
                data = dict(data)  # Copy to avoid mutating caller input
                data["event_type"] = cls.event_type_name()
        return data
```

In `src/eventsource/domain/event_registry.py`, replace the body of `_resolve_event_type` (keep signature and docstring, update the resolution-order note):

```python
        if event_type is not None:
            return event_type
        return event_class.event_type_name()
```

- [ ] **Step 5: Run tests**

Run: `uv run pytest tests/unit/domain/test_event_type_auto.py tests/unit/domain/test_event_registry.py tests/unit/domain/test_domain_event.py -q`
Expected: PASS. If an existing test asserted `Child.model_fields["event_type"].default == "Child"`, update it to assert `event_type_name()` instead — the FieldInfo mutation is gone by design.

- [ ] **Step 6: Lint, typecheck, commit**

```bash
uv run ruff check src tests && uv run mypy src/eventsource/
git add src/eventsource/domain/event.py src/eventsource/domain/event_registry.py tests/unit/domain/test_event_type_auto.py
git commit -m "fix: stop __init_subclass__ mutating parent FieldInfo; unify event-type derivation on event_type_name()"
```

---

### Task 2: `clear_tenant_context()` becomes a real clear

**Files:**
- Modify: `src/eventsource/domain/tenant_context.py:283-309`
- Test: `tests/unit/domain/test_tenant_context.py` (add)

**Interfaces:**
- Consumes: nothing from other tasks.
- Produces: new semantics — `clear_tenant_context()` empties `_token_stack` and invalidates all outstanding `TenantContextToken`s in the current context.

**Background (the bug):** the current implementation calls `tenant_context.set(None)` without touching `_token_stack`. Any enclosing `tenant_scope` exit (or manual `reset_tenant_context`) then resets the contextvar to its pre-set value — resurrecting the tenant that was supposedly cleared. A hard clear must kill the stack so later resets fail loudly instead of silently restoring a tenant.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/domain/test_tenant_context.py`:

```python
import pytest
from uuid import uuid4

from eventsource.domain.exceptions import TenantContextResetError
from eventsource.domain.tenant_context import (
    clear_tenant_context,
    get_current_tenant,
    reset_tenant_context,
    set_current_tenant,
    tenant_scope_sync,
)


class TestClearIsFinal:
    def test_clear_invalidates_outstanding_tokens(self) -> None:
        token = set_current_tenant(uuid4())
        clear_tenant_context()
        assert get_current_tenant() is None
        with pytest.raises(TenantContextResetError):
            reset_tenant_context(token)
        assert get_current_tenant() is None

    def test_clear_inside_scope_raises_on_scope_exit(self) -> None:
        # Clearing inside a scope is a discipline violation; the scope's
        # exit must fail loudly rather than silently resurrect the tenant.
        with pytest.raises(TenantContextResetError):
            with tenant_scope_sync(uuid4()):
                clear_tenant_context()
        assert get_current_tenant() is None
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_tenant_context.py -q -k TestClearIsFinal`
Expected: FAIL — no `TenantContextResetError` raised (old behavior silently resurrects).

- [ ] **Step 3: Implement**

Replace the body of `clear_tenant_context()` and rewrite its docstring:

```python
def clear_tenant_context() -> None:
    """
    Unconditionally clear the tenant context for the current execution
    context, invalidating ALL outstanding TenantContextTokens.

    This is the hard-reset escape hatch for request/task boundaries
    ("no tenant may survive past this point"). After calling it,
    get_current_tenant() returns None and any reset_tenant_context()
    call with a previously issued token raises TenantContextResetError
    — including the implicit reset performed when an enclosing
    tenant_scope()/tenant_scope_sync() exits. Never call this inside a
    tenant scope unless you want that scope's exit to fail loudly.

    Example:
        >>> from uuid import uuid4
        >>> from eventsource import (
        ...     set_current_tenant,
        ...     clear_tenant_context,
        ...     get_current_tenant,
        ... )
        >>> set_current_tenant(uuid4())  # doctest: +ELLIPSIS
        <...TenantContextToken...>
        >>> clear_tenant_context()
        >>> assert get_current_tenant() is None
    """
    logger.debug("Tenant context cleared")
    tenant_context.set(None)
    _token_stack.set(())
```

(Match the existing `_token_stack` element type — it stores tuples; setting the empty tuple `()` is correct. Adjust the doctest to whatever `set_current_tenant`'s repr actually prints, or drop the doctest line if the module's other examples don't execute under pytest.)

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/unit/domain/test_tenant_context.py tests/unit/domain/test_tenant_context_properties.py -q`
Expected: PASS. If a property test encoded the old resurrect behavior, update it — cite this task in the test's comment.

- [ ] **Step 5: Lint, typecheck, commit**

```bash
uv run ruff check src tests && uv run mypy src/eventsource/
git add src/eventsource/domain/tenant_context.py tests/unit/domain/test_tenant_context.py
git commit -m "fix: clear_tenant_context invalidates token stack so scopes cannot resurrect a cleared tenant"
```

---

### Task 3: Consolidated handler discovery with duplicate detection and aggregate-side validation

**Files:**
- Modify: `src/eventsource/domain/decorators.py` (add `discover_handlers`)
- Modify: `src/eventsource/domain/exceptions.py` (add `DuplicateHandlerError`; add `reason` kwarg to `HandlerSignatureError.__init__` at line ~538)
- Modify: `src/eventsource/domain/aggregate.py:142,691-705` (move `_event_handlers` to `DeclarativeAggregate`, rewrite `__init_subclass__`)
- Modify: `src/eventsource/application/projections/handlers.py:~140-181` (`HandlerRegistry._discover_handlers` consumes the shared helper)
- Test: `tests/unit/domain/test_decorators.py`, `tests/unit/domain/test_aggregate_root.py`

**Interfaces:**
- Produces: `discover_handlers(owner: type) -> dict[type[DomainEvent], str]` in `eventsource.domain.decorators`; `DuplicateHandlerError(owner_name: str, event_type: type, first_handler: str, second_handler: str)` in `eventsource.domain.exceptions`; `HandlerSignatureError(..., reason: str | None = None)`.
- Consumes: nothing from earlier tasks.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/domain/test_decorators.py`:

```python
import pytest
from uuid import uuid4

from eventsource.domain.aggregate import DeclarativeAggregate
from eventsource.domain.decorators import discover_handlers, handles
from eventsource.domain.event import DomainEvent
from eventsource.domain.exceptions import DuplicateHandlerError, HandlerSignatureError


class ThingHappened(DomainEvent):
    aggregate_type: str = "Thing"


class TestDiscoverHandlers:
    def test_discovers_decorated_methods(self) -> None:
        class Owner:
            @handles(ThingHappened)
            def _on_thing(self, event: ThingHappened) -> None: ...

        assert discover_handlers(Owner) == {ThingHappened: "_on_thing"}

    def test_duplicate_handlers_for_same_event_raise(self) -> None:
        with pytest.raises(DuplicateHandlerError) as exc_info:
            class BadOwner:
                @handles(ThingHappened)
                def _a_handler(self, event: ThingHappened) -> None: ...

                @handles(ThingHappened)
                def _b_handler(self, event: ThingHappened) -> None: ...

            discover_handlers(BadOwner)
        assert "ThingHappened" in str(exc_info.value)


class TestDeclarativeAggregateValidation:
    def test_duplicate_handles_raises_at_class_definition(self) -> None:
        with pytest.raises(DuplicateHandlerError):
            class DupAggregate(DeclarativeAggregate[dict]):
                aggregate_type = "Dup"

                @handles(ThingHappened)
                def _one(self, event: ThingHappened) -> None: ...

                @handles(ThingHappened)
                def _two(self, event: ThingHappened) -> None: ...

    def test_async_handler_raises_handler_signature_error(self) -> None:
        with pytest.raises(HandlerSignatureError):
            class AsyncAggregate(DeclarativeAggregate[dict]):
                aggregate_type = "Async"

                @handles(ThingHappened)
                async def _on_thing(self, event: ThingHappened) -> None: ...

    def test_wrong_param_count_raises_handler_signature_error(self) -> None:
        with pytest.raises(HandlerSignatureError):
            class FatAggregate(DeclarativeAggregate[dict]):
                aggregate_type = "Fat"

                @handles(ThingHappened)
                def _on_thing(self, context: object, event: ThingHappened) -> None: ...

    def test_base_aggregate_root_has_no_shared_mutable_registry(self) -> None:
        from eventsource.domain.aggregate import AggregateRoot

        assert "_event_handlers" not in AggregateRoot.__dict__
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_decorators.py -x -q`
Expected: FAIL — `ImportError: cannot import name 'discover_handlers'`

- [ ] **Step 3: Implement `discover_handlers` and `DuplicateHandlerError`**

In `src/eventsource/domain/exceptions.py`, after `HandlerDispatchError` (line ~230):

```python
class DuplicateHandlerError(EventSourceError):
    """
    Raised when two @handles methods in one class claim the same event type.

    Without this check, discovery order (alphabetical via dir()) silently
    picks one handler and drops the other's state mutation.
    """

    def __init__(
        self,
        owner_name: str,
        event_type: type,
        first_handler: str,
        second_handler: str,
    ) -> None:
        self.owner_name = owner_name
        self.event_type = event_type
        self.first_handler = first_handler
        self.second_handler = second_handler
        super().__init__(
            f"{owner_name} declares multiple handlers for "
            f"{event_type.__name__}: '{first_handler}' and '{second_handler}'. "
            f"Each event type may have exactly one @handles method per class."
        )
```

In `HandlerSignatureError.__init__` (line ~538), add the optional `reason` kwarg after `is_async_required`; when provided, use it instead of the param-count message:

```python
        reason: str | None = None,
    ) -> None:
        ...
        if reason is not None:
            message = (
                f"Handler '{handler_name}' in {owner_name} is invalid for "
                f"@handles({event_name}): {reason}"
            )
        else:
            message = (
                ... existing message unchanged ...
            )
```

In `src/eventsource/domain/decorators.py`, after `is_event_handler` (and add `from eventsource.domain.exceptions import DuplicateHandlerError` to imports; extend `__all__`):

```python
def discover_handlers(owner: type) -> dict[type[DomainEvent], str]:
    """
    Scan a class for @handles-decorated methods.

    The shared discovery core used by DeclarativeAggregate (domain ring)
    and HandlerRegistry (application ring). Returns a mapping of event
    type to method name.

    Raises:
        DuplicateHandlerError: If two methods claim the same event type.
    """
    handlers: dict[type[DomainEvent], str] = {}
    for name in dir(owner):
        try:
            attr = getattr(owner, name)
        except AttributeError:
            continue
        event_type = get_handled_event_type(attr)
        if event_type is None or not isinstance(event_type, type):
            continue
        existing = handlers.get(event_type)
        if existing is not None and existing != name:
            raise DuplicateHandlerError(
                owner_name=owner.__name__,
                event_type=event_type,
                first_handler=existing,
                second_handler=name,
            )
        handlers[event_type] = name
    return handlers
```

- [ ] **Step 4: Rewire `DeclarativeAggregate` and remove the shared class-level dict**

In `src/eventsource/domain/aggregate.py`:

(a) Delete line 142 (`_event_handlers: dict[type[DomainEvent], str] = {}`) from `AggregateRoot`.

(b) In `DeclarativeAggregate` (class body, next to `unregistered_event_handling`), declare:

```python
    # Per-subclass handler registry, rebuilt by __init_subclass__.
    _event_handlers: ClassVar[dict[type[DomainEvent], str]] = {}
```

(c) Replace `__init_subclass__` (lines 691-705) with:

```python
    def __init_subclass__(cls, **kwargs: object) -> None:
        """Discover and validate @handles methods for each subclass."""
        super().__init_subclass__(**kwargs)
        cls._event_handlers = discover_handlers(cls)
        for event_type, name in cls._event_handlers.items():
            method = getattr(cls, name)
            if inspect.iscoroutinefunction(method):
                raise HandlerSignatureError(
                    handler_name=name,
                    owner_name=cls.__name__,
                    event_type=event_type,
                    param_count=1,
                    is_async_required=False,
                    reason=(
                        "aggregate event handlers run synchronously during "
                        "replay; remove 'async'"
                    ),
                )
            try:
                params = list(inspect.signature(method).parameters.values())
            except (ValueError, TypeError):
                continue
            param_count = len(params) - 1  # exclude self (unbound function)
            if param_count != 1:
                raise HandlerSignatureError(
                    handler_name=name,
                    owner_name=cls.__name__,
                    event_type=event_type,
                    param_count=param_count,
                    is_async_required=False,
                )
```

Add `import inspect` and the `discover_handlers` / `HandlerSignatureError` imports at the top of aggregate.py (intra-domain + stdlib only — ring-safe).

(d) `grep -rn "_event_handlers" src tests` — `tests/unit/test_fixtures.py` and `tests/unit/domain/test_aggregate_root.py` reference it; if any test pokes `AggregateRoot._event_handlers` directly, retarget it to a `DeclarativeAggregate` subclass.

- [ ] **Step 5: Consume the helper in `HandlerRegistry`**

In `src/eventsource/application/projections/handlers.py`, rewrite the discovery loop inside `_discover_handlers` to iterate `discover_handlers(type(owner)).items()` (import from `eventsource.domain.decorators` — application→domain is ring-legal) and then build each `HandlerInfo` exactly as today (`inspect.iscoroutinefunction`, signature param count, `self._handlers[event_type] = handler_info`, debug log). Keep `_validate_handlers` unchanged — async/context validation stays application-side. Projections gain duplicate detection for free; the old inline `dir()` scan is deleted.

- [ ] **Step 6: Run tests**

Run: `uv run pytest tests/unit/domain/ tests/unit/application/ -q`
Expected: PASS (existing aggregate + projection suites confirm no behavior change beyond the new errors).

- [ ] **Step 7: Lint, typecheck, commit**

```bash
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src/eventsource tests
git commit -m "feat: shared discover_handlers with duplicate detection; validate aggregate handler signatures at class init"
```

---

### Task 4: Strictness — `extra="forbid"` and no bare assert

**Files:**
- Modify: `src/eventsource/domain/event.py:78` (model_config)
- Modify: `src/eventsource/domain/decider.py:56-60` (state property)
- Test: `tests/unit/domain/test_domain_event.py`, `tests/unit/domain/test_decider_aggregate.py`

**Interfaces:** none produced; independent of other tasks.

- [ ] **Step 1: Write the failing tests**

In `tests/unit/domain/test_domain_event.py`:

```python
import pytest
from uuid import uuid4

from pydantic import ValidationError

from eventsource.domain.event import DomainEvent


class TestExtraForbid:
    def test_unknown_field_raises_validation_error(self) -> None:
        class PaymentTaken(DomainEvent):
            aggregate_type: str = "Payment"
            amount: int

        with pytest.raises(ValidationError):
            PaymentTaken(aggregate_id=uuid4(), amount=5, amonut=7)  # typo must not be swallowed
```

In `tests/unit/domain/test_decider_aggregate.py`:

```python
class TestStateInvariant:
    def test_none_state_raises_even_without_assertions(self) -> None:
        class BrokenDecider(DeciderAggregate[dict]):
            aggregate_type = "Broken"

            @staticmethod
            def initial_state(aggregate_id: UUID) -> dict:
                return None  # type: ignore[return-value]  # deliberate contract violation

            @staticmethod
            def decide(command: object, state: dict) -> list[DomainEvent]:
                return []

            @staticmethod
            def evolve(state: dict, event: DomainEvent) -> dict:
                return state

        agg = BrokenDecider(uuid4())
        with pytest.raises(RuntimeError, match="initial_state"):
            _ = agg.state
```

(Reuse the file's existing imports for `DeciderAggregate`, `DomainEvent`, `UUID`, `uuid4`, `pytest`.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_domain_event.py tests/unit/domain/test_decider_aggregate.py -q -k "TestExtraForbid or TestStateInvariant"`
Expected: FAIL — no ValidationError (extra ignored); AssertionError instead of RuntimeError.

- [ ] **Step 3: Implement**

`event.py:78`:

```python
    model_config = ConfigDict(frozen=True, allow_inf_nan=False, extra="forbid")
```

`decider.py` state property:

```python
    @property
    def state(self) -> TState:
        """Current state. Never None: eagerly initialized from initial_state()."""
        if self._state is None:
            raise RuntimeError(
                f"{type(self).__name__} has no state: initial_state() must "
                f"return a non-None state (established in __init__, "
                f"maintained by _apply)."
            )
        return self._state
```

- [ ] **Step 4: Run the domain + application + adapters unit suites** — `extra="forbid"` can flush out in-tree callers passing stray kwargs:

Run: `uv run pytest tests/unit -q`
Expected: PASS. If a test constructs events with unknown fields, that is the defect this change exists to catch — fix the construction site (usually a typo or data meant for `metadata`), never relax the config.

- [ ] **Step 5: Lint, typecheck, commit**

```bash
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src/eventsource tests
git commit -m "feat: forbid unknown fields on DomainEvent; replace bare assert in DeciderAggregate.state"
```

---

### Task 5: Exception hygiene — drop builtin MI bases, drop redundant `__str__`s

**Files:**
- Modify: `src/eventsource/domain/exceptions.py:473,491,511` (bases), `:267-427` (SnapshotError family `__str__`s)
- Test: `tests/unit/domain/test_exceptions_home.py` (or the file's existing exception-hierarchy test module)

**Interfaces:** none produced. Breaking: `except KeyError`/`except ValueError` no longer catches these three.

- [ ] **Step 1: Write the failing tests**

```python
from eventsource.domain.exceptions import (
    DuplicateEventTypeError,
    EventTypeNotFoundError,
    HandlerSignatureError,
)


class TestNoBuiltinBases:
    def test_registry_errors_are_not_builtin_lookup_errors(self) -> None:
        assert not issubclass(EventTypeNotFoundError, KeyError)
        assert not issubclass(DuplicateEventTypeError, ValueError)
        assert not issubclass(HandlerSignatureError, ValueError)

    def test_not_found_message_is_not_requoted(self) -> None:
        err = EventTypeNotFoundError("OrderCreated", ["A", "B"])
        assert not str(err).startswith("'")  # KeyError.__str__ used to re-quote
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_exceptions_home.py -q -k TestNoBuiltinBases`
Expected: FAIL on the issubclass assertions.

- [ ] **Step 3: Implement**

(a) Change the three class declarations to single inheritance: `class EventTypeNotFoundError(EventSourceError):`, `class DuplicateEventTypeError(EventSourceError):`, `class HandlerSignatureError(EventSourceError):`.

(b) Delete the `def __str__(self) -> str: return self.message` overrides from `SnapshotDeserializationError`, `SnapshotSchemaVersionError`, and `SnapshotNotFoundError` — each already calls `super().__init__(self.message)`, so `Exception.__str__` returns the identical string. Keep the field-based `__repr__` overrides and the `self.message` attributes.

(c) Audit every in-tree `except ValueError`/`except KeyError` that wraps registry/handler calls: inspect the hits at `application/subscriptions/shutdown.py:615`, `application/subscriptions/coordination.py:857,882,910,938`, `adapters/kafka/consumer.py:580`, `adapters/_sql/dialect.py:47`, `application/projections/coordinator.py:123,144,532`, plus any others from `grep -rn "except KeyError\|except ValueError" src/eventsource --include="*.py"`. For each: if the try block can raise one of the three rebased exceptions and relied on the builtin base, change it to catch the specific eventsource exception. Expected: none do (they guard `list.remove`, int parsing, etc.) — record the audit result in the commit message.

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/unit -q`
Expected: PASS.

- [ ] **Step 5: Lint, typecheck, commit**

```bash
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src/eventsource tests
git commit -m "feat: registry and handler exceptions no longer subclass KeyError/ValueError; drop redundant SnapshotError __str__ overrides"
```

---

### Task 6: One provenance-stamping implementation

**Files:**
- Modify: `src/eventsource/domain/aggregate.py:434-464` (`create_event`, new `_provenance_updates`)
- Modify: `src/eventsource/domain/decider.py:69-115` (`execute` docstring, `_stamp`)
- Test: `tests/unit/domain/test_create_event.py`, `tests/unit/domain/test_decider_aggregate.py`

**Interfaces:**
- Produces: `AggregateRoot._provenance_updates(command: object, explicitly_set: Collection[str]) -> dict[str, Any]`.

**Semantics decision (approved):** the ambient tenant-context fallback applies **unconditionally** — for any command type, tenant precedence is explicit > `DomainCommand.tenant_id` > ambient context. The decider previously skipped the ambient fallback for non-`DomainCommand` commands; it now gains it. Causation/correlation/actor still come only from `DomainCommand`.

- [ ] **Step 1: Write the failing test**

In `tests/unit/domain/test_decider_aggregate.py` (reuse the file's existing decider fixture aggregate; the one below is the minimal shape):

```python
class TestAmbientTenantStamping:
    def test_plain_command_gets_ambient_tenant(self) -> None:
        from eventsource.domain.tenant_context import tenant_scope_sync

        tenant = uuid4()

        @dataclass
        class PlainShip:  # deliberately NOT a DomainCommand
            order_id: UUID

        class ShipDecider(DeciderAggregate[dict]):
            aggregate_type = "Order"

            @staticmethod
            def initial_state(aggregate_id: UUID) -> dict:
                return {"id": aggregate_id}

            @staticmethod
            def decide(command: object, state: dict) -> list[DomainEvent]:
                return [Shipped(aggregate_id=state["id"])]

            @staticmethod
            def evolve(state: dict, event: DomainEvent) -> dict:
                return state

        class Shipped(DomainEvent):
            aggregate_type: str = "Order"

        agg = ShipDecider(uuid4())
        with tenant_scope_sync(tenant):
            events = agg.execute(PlainShip(order_id=agg.aggregate_id))
        assert events[0].tenant_id == tenant
```

(Define `Shipped` above `ShipDecider` in the actual test file so the name resolves; add `from dataclasses import dataclass` to imports.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_decider_aggregate.py -q -k TestAmbientTenantStamping`
Expected: FAIL — `events[0].tenant_id is None` (old `_stamp` skips ambient fallback for non-DomainCommand).

- [ ] **Step 3: Implement**

In `AggregateRoot` (aggregate.py, place directly above `create_event`; add `from collections.abc import Collection` to imports if absent):

```python
    def _provenance_updates(
        self,
        command: object,
        explicitly_set: Collection[str],
    ) -> dict[str, Any]:
        """
        Shared stamping semantics for create_event() and DeciderAggregate._stamp().

        Fields listed in explicitly_set are never overwritten. Tenant
        precedence: explicit > DomainCommand.tenant_id > ambient tenant
        context (unconditional fallback regardless of command type).
        Causation/correlation/actor come only from a DomainCommand.
        """
        updates: dict[str, Any] = {}
        if isinstance(command, DomainCommand):
            if "causation_id" not in explicitly_set:
                updates["causation_id"] = command.command_id
            if "correlation_id" not in explicitly_set:
                updates["correlation_id"] = command.correlation_id
            if "actor_id" not in explicitly_set and command.actor_id is not None:
                updates["actor_id"] = command.actor_id
        if "tenant_id" not in explicitly_set:
            tenant: UUID | None = None
            if isinstance(command, DomainCommand) and command.tenant_id is not None:
                tenant = command.tenant_id
            if tenant is None:
                tenant = self._get_tenant_from_context()
            if tenant is not None:
                updates["tenant_id"] = tenant
        return updates
```

Rewrite the body of `create_event` between the auto-populated dict and event construction (delete current lines 442-455):

```python
        event_kwargs: dict[str, Any] = {
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "aggregate_version": self.get_next_version(),
        }
        event_kwargs.update(self._provenance_updates(command, kwargs.keys()))
        # User kwargs override auto-populated values
        event_kwargs.update(kwargs)
```

Rewrite `DeciderAggregate._stamp`:

```python
    def _stamp(self, event: DomainEvent, command: object) -> DomainEvent:
        fields_set = event.model_fields_set
        updates: dict[str, Any] = {}
        if "aggregate_version" not in fields_set:
            updates["aggregate_version"] = self.get_next_version()
        if "aggregate_type" not in fields_set:
            updates["aggregate_type"] = self.aggregate_type
        updates.update(self._provenance_updates(command, fields_set))
        if not updates:
            return event
        return event.model_copy(update=updates)
```

Update the `execute` docstring's stamping description: tenant fallback is no longer conditional on `DomainCommand` ("tenant_id: command value if DomainCommand, else ambient tenant context, else untouched — for every command type").

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/unit/domain/test_create_event.py tests/unit/domain/test_decider_aggregate.py tests/unit/aggregates -q`
Expected: PASS — create_event behavior is unchanged by construction (same precedence, same outputs); the only behavior delta is the decider's new ambient fallback.

- [ ] **Step 5: Lint, typecheck, commit**

```bash
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src/eventsource tests
git commit -m "refactor: single _provenance_updates helper; decider gains unconditional ambient tenant fallback"
```

---

### Task 7: Move 13 infrastructure exceptions to `ports/exceptions.py`

**Files:**
- Create: `src/eventsource/ports/exceptions.py`
- Modify: `src/eventsource/domain/exceptions.py` (delete moved classes), `src/eventsource/domain/__init__.py` (drop moved exports), `src/eventsource/ports/__init__.py` (add exports), `src/eventsource/__init__.py` (`_LAZY` map repoints)
- Modify: every import site found by grep (~29 across application/, adapters/, ports/, testing/, tests/)
- Test: `tests/unit/domain/test_exceptions_home.py`, new `tests/unit/ports/test_exceptions_home.py`

**Interfaces:**
- Produces: `eventsource.ports.exceptions` containing exactly: `CheckpointError`, `PositionDecodeError`, `PositionForeignError`, `LockAcquisitionError`, `LockNotHeldError`, `SubscriptionError`, `SubscriptionConfigError`, `SubscriptionStateError`, `SubscriptionAlreadyExistsError`, `CheckpointNotFoundError`, `EventStoreConnectionError`, `EventBusConnectionError`, `TransitionError`. All keep their exact current bases (`SubscriptionError(EventSourceError)` etc. — `EventSourceError` is imported from `eventsource.domain.exceptions`, a ring-legal ports→domain import).
- **Stays in domain** (deliberate): `EventStoreError`, `EventBusError` (broad categories raised across rings), `HandlerSignatureError` + `DuplicateHandlerError` (raised by domain `decorators.py`/`aggregate.py` since Task 3), `SnapshotError` family (raised by domain `aggregate.py` snapshot restore), all tenant/aggregate/event errors.

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/ports/test_exceptions_home.py`:

```python
"""Infrastructure exception taxonomy lives in the ports ring (ADR 0041)."""

from eventsource.domain.exceptions import EventSourceError
from eventsource.ports.exceptions import (
    CheckpointError,
    CheckpointNotFoundError,
    EventBusConnectionError,
    EventStoreConnectionError,
    LockAcquisitionError,
    LockNotHeldError,
    PositionDecodeError,
    PositionForeignError,
    SubscriptionAlreadyExistsError,
    SubscriptionConfigError,
    SubscriptionError,
    SubscriptionStateError,
    TransitionError,
)


class TestPortsExceptionsHome:
    def test_all_rooted_in_eventsource_error(self) -> None:
        for exc in (
            CheckpointError, CheckpointNotFoundError, EventBusConnectionError,
            EventStoreConnectionError, LockAcquisitionError, LockNotHeldError,
            PositionDecodeError, PositionForeignError, SubscriptionAlreadyExistsError,
            SubscriptionConfigError, SubscriptionError, SubscriptionStateError,
            TransitionError,
        ):
            assert issubclass(exc, EventSourceError)

    def test_domain_module_no_longer_exports_them(self) -> None:
        from eventsource.domain import exceptions as domain_exceptions

        for name in (
            "CheckpointError", "LockAcquisitionError", "LockNotHeldError",
            "PositionDecodeError", "PositionForeignError", "SubscriptionError",
        ):
            assert not hasattr(domain_exceptions, name)

    def test_top_level_reexports_still_work(self) -> None:
        from eventsource import CheckpointError as TopLevel  # noqa: F401
```

(Before writing the last test, check which of the 13 are in the top-level `__all__` via `grep -n '"CheckpointError"\|"LockAcquisitionError"' src/eventsource/__init__.py` — assert re-export only for names that are actually top-level public today. Do not add new top-level names.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/ports/test_exceptions_home.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'eventsource.ports.exceptions'`

- [ ] **Step 3: Move the classes**

(a) Create `src/eventsource/ports/exceptions.py` with this header, then cut-paste the 13 class definitions **verbatim** from `domain/exceptions.py` (lines 88-92 CheckpointError, 236-247 Position*, 430-471 Lock*, 578-637 Subscription family):

```python
"""
Infrastructure exception taxonomy (ADR 0041).

These error types describe failures of the *port* contracts — stores,
buses, locks, positions, checkpoints, subscriptions — not domain
concepts. They are rooted in EventSourceError so `except
EventSourceError` still catches everything, but they live in the ports
ring: importable by application and adapters (which raise them) without
polluting the entities ring.
"""

from __future__ import annotations

from eventsource.domain.exceptions import EventSourceError

__all__ = [
    "CheckpointError",
    "CheckpointNotFoundError",
    "EventBusConnectionError",
    "EventStoreConnectionError",
    "LockAcquisitionError",
    "LockNotHeldError",
    "PositionDecodeError",
    "PositionForeignError",
    "SubscriptionAlreadyExistsError",
    "SubscriptionConfigError",
    "SubscriptionError",
    "SubscriptionStateError",
    "TransitionError",
]
```

Bring along any `UUID`/`typing` imports the moved `__init__` bodies need. Delete the same classes (and the now-orphaned "Subscription exceptions" section banner) from `domain/exceptions.py`.

(b) `src/eventsource/domain/__init__.py`: remove `CheckpointError`, `LockAcquisitionError`, `LockNotHeldError`, `PositionDecodeError`, `PositionForeignError` from the import block and `__all__`.

(c) `src/eventsource/ports/__init__.py`: mirror however that file currently exposes its modules (inspect it first); add the 13 names following the existing pattern and `__all__` convention.

(d) `src/eventsource/__init__.py`: in `_LAZY`, repoint every moved name that appears there from `"eventsource.domain.exceptions"` to `"eventsource.ports.exceptions"`. Top-level `__all__` is unchanged.

- [ ] **Step 4: Update all import sites**

```bash
grep -rln "from eventsource.domain.exceptions import" src tests docs | xargs grep -ln "CheckpointError\|LockAcquisitionError\|LockNotHeldError\|PositionDecodeError\|PositionForeignError\|SubscriptionError\|SubscriptionConfigError\|SubscriptionStateError\|SubscriptionAlreadyExistsError\|CheckpointNotFoundError\|EventStoreConnectionError\|EventBusConnectionError\|TransitionError"
```

For each file: split the import — moved names from `eventsource.ports.exceptions`, remaining names stay on `eventsource.domain.exceptions`. Watch for `import eventsource.domain.exceptions as ...` attribute-style uses (`grep -rn "domain.exceptions\." src tests`). Update `tests/unit/domain/test_exceptions_home.py` to drop the moved names from its domain-side assertions. Docs snippets found by the grep get the new path too.

- [ ] **Step 5: Verify layering and run the suite**

```bash
uv run lint-imports
uv run pytest tests/unit -q
```

Expected: import-linter passes (ports→domain is inward; adapters/application→ports is inward). Unit suite passes.

- [ ] **Step 6: Lint, typecheck, commit**

```bash
uv run ruff check src tests && uv run mypy src/eventsource/
git add -A src tests docs
git commit -m "refactor: move 13 infrastructure exceptions from domain to ports ring (ADR 0041)"
```

---

### Task 8: Sync `domain/__init__` exported surface

**Files:**
- Modify: `src/eventsource/domain/__init__.py`
- Test: `tests/unit/domain/test_exceptions_home.py` (extend)

**Interfaces:**
- Consumes: Task 3 (`DuplicateHandlerError` exists), Task 7 (moved names already gone).

After Task 7, the classes remaining in `domain/exceptions.py` but missing from `domain/__init__.__all__` are: `HandlerSignatureError`, `DuplicateHandlerError` (new), `TenantContextNotSetError`, `TenantContextResetError`, `TenantMismatchError`. The three tenant errors are already public via the top-level package sourced straight from `domain.exceptions` — the ring facade must agree with reality.

- [ ] **Step 1: Write the failing test**

```python
class TestDomainFacadeComplete:
    def test_every_public_domain_exception_is_exported(self) -> None:
        import eventsource.domain as domain
        from eventsource.domain import exceptions as ex

        public = {
            name for name in dir(ex)
            if isinstance(getattr(ex, name), type)
            and issubclass(getattr(ex, name), Exception)
            and not name.startswith("_")
            and getattr(ex, name).__module__ == "eventsource.domain.exceptions"
        }
        exported = set(domain.__all__)
        missing = public - exported
        assert not missing, f"domain/__init__ is missing: {sorted(missing)}"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/domain/test_exceptions_home.py -q -k TestDomainFacadeComplete`
Expected: FAIL listing the five names.

- [ ] **Step 3: Implement** — add the five names to the import block and `__all__` (keep the list's existing alphabetical-with-constants-first ordering).

- [ ] **Step 4: Run tests**

Run: `uv run pytest tests/unit/domain -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
uv run ruff check src tests && uv run mypy src/eventsource/
git add src/eventsource/domain/__init__.py tests/unit/domain/test_exceptions_home.py
git commit -m "fix: export all public domain exceptions from the ring facade"
```

---

### Task 9: ADRs 0041 + 0042, architecture rules amendment, changelog, docs sync

**Files:**
- Create: `docs/adrs/0041-infrastructure-exceptions-to-ports.md`, `docs/adrs/0042-domain-event-strictness.md`
- Modify: `docs/adrs/index.md`, `docs/adrs/0030-*.md` and `docs/adrs/0032-*.md` (Status pointers), `.claude/rules/architecture.md`, `CHANGELOG.md`, `mkdocs.yml` (nav — required for the two new ADR pages), any docs pages referencing changed behavior.

**Interfaces:** consumes the outcomes of Tasks 1-8; no code.

- [ ] **Step 1: Write ADR 0041** (`0041-infrastructure-exceptions-to-ports.md`), following the existing ADR template in `docs/adrs/` (read 0040 for the format). Content requirements: Context — Tier-0 became the universal error taxonomy because it is the only ring every ring may import; ~⅓ of `domain/exceptions.py` had no domain meaning. Decision — the 13 named classes move to `ports/exceptions.py`, rooted in `EventSourceError`; no shims (standing pre-1.0 rule). Consequences — `from eventsource.domain.exceptions import LockAcquisitionError` breaks; top-level re-exports unchanged; domain keeps broad categories (`EventStoreError`, `EventBusError`) and everything domain code itself raises. ADR Impact — ADR 0030 (settled exceptions.py contents) **amended**; ADR 0032 (subscription exceptions merged into domain) **amended**; add "Amended by ADR 0041" to both ADRs' Status sections.

- [ ] **Step 2: Write ADR 0042** (`0042-domain-event-strictness.md`). Decision covers, one section each: (1) `event_type_name()` as single derivation source, `__init_subclass__` no longer mutates `FieldInfo` (bug record); (2) `extra="forbid"` on `DomainEvent`; (3) registry/handler exceptions drop builtin `KeyError`/`ValueError` bases; (4) `clear_tenant_context()` hard-clear semantics; (5) duplicate `@handles` detection + aggregate signature validation at class-definition time; (6) unified provenance stamping with unconditional ambient-tenant fallback. ADR Impact — ADR 0033 (event/registry settlement) **stands** (locations unchanged, behavior hardened); ADR 0038 (multitenancy dissolution) **amended** (clear semantics changed) — add Status pointer to 0038. ADR 0022 (decider) **amended** (stamping semantics) — add Status pointer.

- [ ] **Step 3: Update `docs/adrs/index.md`** with both new rows, and **amend `.claude/rules/architecture.md`**: in the entities-ring bullet, exceptions.py is no longer "the full exception hierarchy, including the SnapshotError family and the lock exceptions" — reword to "the domain exception hierarchy (EventSourceError root, aggregate/event/snapshot/tenant errors); infrastructure error types live in `ports/exceptions.py` (ADR 0041)". In the ports bullet, add `ports/exceptions.py` as settled.

- [ ] **Step 4: CHANGELOG.md** under `## [Unreleased]`:

```markdown
### Fixed
- **`DomainEvent.__init_subclass__` no longer mutates the parent class's shared `event_type` FieldInfo.** Subclassing a concrete event corrupted the parent's registry key: `register_event(Parent)` after `class Child(Parent)` filed Parent under "Child", making stored `"Parent"` events undeserializable (or raising a spurious `DuplicateEventTypeError`). Event-type derivation is now unified on the new `DomainEvent.event_type_name()` classmethod, used by both instance construction and `EventRegistry`.
- **`clear_tenant_context()` now actually clears.** Previously it left the token stack intact, so any enclosing `tenant_scope()` exit silently resurrected the "cleared" tenant — a cross-tenant leakage vector. It now invalidates all outstanding tokens in the current context; a subsequent `reset_tenant_context()` (including a scope exit) raises `TenantContextResetError` instead of restoring a stale tenant.
- Two `@handles` methods for the same event type in one class now raise the new `DuplicateHandlerError` at class-definition time instead of silently dropping one handler (discovery order used to decide the winner alphabetically).

### Changed
- **BREAKING: `DomainEvent` now uses `extra="forbid"`.** Unknown constructor kwargs (typically typos) raise `pydantic.ValidationError` instead of being silently dropped and persisting an event with missing data. Arbitrary payload data belongs in the `metadata` field.
- **BREAKING: `EventTypeNotFoundError`, `DuplicateEventTypeError`, and `HandlerSignatureError` no longer subclass `KeyError`/`ValueError`.** `except KeyError`/`except ValueError` will no longer catch them; catch the specific type or `EventSourceError`. Their `str()` output is no longer re-quoted by `KeyError.__str__`.
- **BREAKING: 13 infrastructure exceptions moved from `eventsource.domain.exceptions` to `eventsource.ports.exceptions`** (ADR 0041, no shims): `CheckpointError`, `CheckpointNotFoundError`, `EventBusConnectionError`, `EventStoreConnectionError`, `LockAcquisitionError`, `LockNotHeldError`, `PositionDecodeError`, `PositionForeignError`, `SubscriptionError`, `SubscriptionConfigError`, `SubscriptionStateError`, `SubscriptionAlreadyExistsError`, `TransitionError`. Top-level `from eventsource import ...` re-exports are unchanged.
- `DeciderAggregate` stamping now applies the ambient tenant-context fallback for every command type, not only `DomainCommand` (unified with `create_event()` semantics via the shared `_provenance_updates()` helper).
- `DeclarativeAggregate` validates handler signatures at class-definition time: async handlers and wrong parameter counts raise `HandlerSignatureError` (previously only projections validated).
- `DeciderAggregate.state` raises `RuntimeError` on a `None` state instead of using a bare `assert` (which `python -O` strips).

### Added
- `DomainEvent.event_type_name()` classmethod — the canonical wire name for an event class.
- `eventsource.domain.decorators.discover_handlers()` — shared @handles discovery used by aggregates and projections.
- `DuplicateHandlerError` exception; `HandlerSignatureError` gains an optional `reason` parameter.
- `domain/__init__` now exports `HandlerSignatureError`, `DuplicateHandlerError`, and the three tenant-context exceptions (surface sync).
```

- [ ] **Step 5: Docs sweep** — `grep -rln "clear_tenant_context\|domain.exceptions import\|extra fields\|LockAcquisitionError\|SubscriptionError" docs/` and update every page that shows old behavior or old import paths. Add both ADR pages to `mkdocs.yml` nav (nav omissions are NOT caught by strict build — check `docs-nav-completeness` requirement). Then `uv run mkdocs build --strict` must pass.

- [ ] **Step 6: Commit**

```bash
git add docs .claude/rules/architecture.md CHANGELOG.md mkdocs.yml
git commit -m "docs: ADR 0041/0042, architecture rules amendment, changelog for domain hardening wave"
```

---

### Task 10: Full gate and PR

- [ ] **Step 1: Run CI parity**

Run: `make check`
Expected: ruff, mypy strict, import-linter, and the full test suite all pass. Fix anything that fails before proceeding (orchestrator owns full-suite failures — do not push red).

- [ ] **Step 2: Push and open the PR (never self-merge)**

```bash
git push -u origin domain-hardening-wave
gh pr create --base main --title "Domain ring hardening wave (ADRs 0041-0042)" --body "$(cat <<'EOF'
## Summary
- Fixes two reproduced bugs: parent `FieldInfo` corruption in `DomainEvent.__init_subclass__` (registry-key corruption -> undeserializable events) and `clear_tenant_context()` failing to invalidate the token stack (cross-tenant leakage vector).
- Consolidates three duplicated mechanisms: event-type derivation (`event_type_name()`), handler discovery (`discover_handlers()` + `DuplicateHandlerError`), provenance stamping (`_provenance_updates()`).
- Hardening: `extra="forbid"` on DomainEvent, no builtin KeyError/ValueError exception bases, assert->raise in decider, aggregate handler signature validation.
- Moves 13 infrastructure exceptions to `ports/exceptions.py` (ADR 0041). Breaking changes flagged in CHANGELOG; no shims per standing policy.

Source analysis: `.fractal/report.md` domain-ring audit (2026-08-01).

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Report** — summarize what shipped, link the PR, list the breaking changes, and note follow-ups deliberately left out: `types.py` deprecation decision, `event_version` upcasting ADR, `application/migration/exceptions.py` decomposition (1533 lines with live `CircuitBreaker`/`ErrorHandler` behavior inside).
