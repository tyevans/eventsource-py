# Fractal findings: src/eventsource/domain deep analysis (2026-08-01)

**Goal**: identify insecure patterns, poor architecture, consolidation/decomposition opportunities.
**Shape**: 3 Level-1 handlers (behavior core / event model & tenancy / exceptions & surface), 3 follow-up lookups resolved inline. Stopped at goal-satisfied.

## Confirmed defects (fix-worthy)

1. **`DomainEvent.__init_subclass__` mutates the PARENT's shared `FieldInfo`** — `cls.model_fields["event_type"]` at `__init_subclass__` time resolves to the parent's dict; setting `.default = cls.__name__` corrupts the parent's default. Reproduced: subclassing a concrete event makes `EventRegistry._resolve_event_type(Parent)` return the child's name → registration under wrong key → wire-value lookup fails on deserialization, or spurious `DuplicateEventTypeError`. src/eventsource/domain/event.py:186-193, event_registry.py:146-154. Store adapters resolve classes only via the registry map (sqlite/store.py:491, postgresql/store.py:652), so a corrupted key = unreadable events.
2. **`clear_tenant_context()` is silently undone** — sets contextvar to None without touching `_token_stack`; exiting any enclosing scope resurrects the cleared tenant. Cross-tenant leakage vector in the module that exists to prevent it. tenant_context.py:283-309. Reproduced.
3. **Silent `@handles` collision** — two handlers for one event type: alphabetically-last method name wins, no error. aggregate.py:697-702.
4. **`create_event(**kwargs)` swallows typos** — `DomainEvent` config lacks `extra="forbid"`; misspelled field silently dropped, event persisted missing data. aggregate.py:436-461, event.py:78.
5. **Bare `assert` guards `DeciderAggregate.state`** — stripped under `python -O`. decider.py:59.
6. **`_event_handlers = {}` class-level mutable on `AggregateRoot`** — direct subclasses share the base dict process-wide. aggregate.py:142.
7. **Multiple-inheritance exception trap** — `EventTypeNotFoundError(EventSourceError, KeyError)` etc.: `except ValueError`/`KeyError` swallows domain errors; `KeyError.__str__` re-quotes messages. exceptions.py:473,491,511.

## Consolidation opportunities

- **Handler discovery duplicated** between `DeclarativeAggregate.__init_subclass__` (no validation) and `application/projections/handlers.py:HandlerRegistry` (full validation). decorators.py docs promise `HandlerSignatureError` at class init — never fires for aggregates. Lift discovery+validation into domain/decorators.py; both consume it. Fixes defect 3 too.
- **Event-type-name derivation duplicated** — event.py `__init_subclass__` vs registry `_resolve_event_type` reading `model_fields`. One classmethod accessor fixes defect 1 and removes the duplication.
- **Provenance stamping duplicated with divergent semantics** — `create_event` (unconditional tenant-context fallback) vs decider `_stamp` (only for `DomainCommand`). aggregate.py:442-455 vs decider.py:98-112.
- **`SnapshotError` family hand-rolls `__str__`/`__repr__`** ×3, unlike rest of file. exceptions.py:244-427.

## Decomposition verdicts

- **aggregate.py (850 loc): do NOT split** — 546 lines are docstrings; ~300 loc, 2 cohesive classes.
- **exceptions.py (758 loc): real decomposition target** — ~10 of 34 classes are infrastructure (EventStore/EventBus connection, Lock*, Position*, Checkpoint*, Subscription*) living in domain only because Tier-0 is universally importable. Natural home: `ports/exceptions.py`. Cost: 29 direct import sites, public API (NO-SHIMS policy makes this a breaking change → needs ADR + probably 1.0 boundary).
- **types.py: dead weight** — zero internal consumers; `TenantId = UUID | None` contradicts `TenantDomainEvent.tenant_id: UUID`. Public path, so deprecate rather than delete.
- **command.py, stream_id.py: earn their modules** (import cycle / 15 consumers respectively). Tenancy-in-domain is ADR-0038-settled.

## Surface drift

- 12 exception classes missing from `domain/__init__.__all__`; 3 (tenant errors) ARE public via top-level `eventsource/__init__.py` importing `domain.exceptions` directly. Facade not load-bearing: 108 internal imports hit submodules vs 18 via the facade.

## Clean bills of health

- Layering: domain imports = stdlib + pydantic + intra-domain only; enforced by import-linter contracts (pyproject.toml:299-333).
- No injection surfaces: registry is explicit-registration dict lookup under RLock; no eval/importlib/dynamic names; stream_id regex `\Z`-anchored.
- No secrets/SQL/connection detail in any exception message.

## Gap (ADR-worthy, not a defect)

- `event_version` field is decorative — no upcaster/migration hook anywhere; project policy is "additive event types", so either document the field as reserved or write the upcasting ADR.

## Adjacent finding (out of scope, flagged)

- `application/migration/exceptions.py` is 1533 lines and contains live behavior (`CircuitBreaker`, `ErrorHandler`, `RetryConfig`) masquerading as an exceptions module — structurally worse than anything in domain/.
