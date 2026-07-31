# Design: Command Objects, DeciderAggregate, and Dogfooding the Decider Style

**Date:** 2026-07-30
**Status:** Approved (brainstormed with Ty; see PR #79/#80 for the docs groundwork)
**Target release:** 0.8.0

## Context

PRs #79 and #80 documented the decider pattern (pure `decide`/`evolve` functions
behind a thin `AggregateRoot` shell) and made it the README's featured alternative.
Benchmarks from that work: ~1.5x on the command path (~8 µs/order absolute, invisible
next to event construction at ~11 µs/event and millisecond-scale store appends),
~1.06x wash on replay. Conclusion reached: maintainability, not performance, is the
deciding factor.

Two gaps remain. First, the userland shell has a real footgun — `AggregateRoot._state`
is `None` until the first event and nothing calls `_get_initial_state()`, so a naive
decider rejects its first command — plus per-shell version-stamping boilerplate.
Second, `DomainEvent` carries `correlation_id`/`causation_id`/`actor_id`/`tenant_id`
but nothing *originates* that chain: `with_causation()` only links event→event. In
CQRS the originator is the command.

This design ships the pattern first-class and adopts it as the library's **primary
showcased style** — dogfooded across examples, tutorials, test fixtures, the BDD
harness, and the benchmark domain.

## Decisions (made during brainstorm)

1. **Positioning: primary style.** Tutorials, examples, and general-purpose fixtures
   lead with the decider. Imperative and declarative each keep one worked reference
   example and remain fully documented in `docs/explanation/aggregate-styles.md`.
2. **`decide` returns events; the shell stamps.** The classic literature signature
   `decide(command, state) -> list[DomainEvent]`, matching the README and explanation
   doc. The provenance stamp subsumes the version re-stamp in a single `model_copy`
   per event. The faster event-spec contract (1.29x) was considered and rejected:
   non-standard signature, awkward test assertions, and the perf delta is noise.
3. **Staging: three PRs.** Core feature → dogfood wave → BDD harness (details below).

## Component 1: `DomainCommand` (entities ring)

New package `src/eventsource/commands/` beside `events/` — pure (stdlib + pydantic
only); the import-linter entities-ring contract is extended to include it.

```python
class DomainCommand(BaseModel):
    model_config = ConfigDict(frozen=True)

    command_id: UUID = Field(default_factory=uuid4)
    issued_at: datetime  # default_factory: timezone-aware utcnow, same idiom as DomainEvent.occurred_at
    correlation_id: UUID = Field(default_factory=uuid4)
    actor_id: str | None = None
    tenant_id: UUID | None = None
```

- `command_id` becomes the `causation_id` of every event the command produces. Since
  commands are never persisted, that id is resolvable only if the application logs its
  commands — but even unresolved it still groups all events produced by one command
  and marks them as command-caused. This is the standard CQRS convention.
- `correlation_id` starts a workflow chain. Commands have **no `causation_id` field**;
  `DomainCommand.caused_by(event)` returns a copy carrying the event's
  `correlation_id` only, so saga-issued commands keep the workflow chain intact
  (event → command → event linkage within a workflow is by correlation).
- **Opt-in and structurally typed**: `decide`/`execute` accept *any* object. Plain
  dataclasses and `BaseModel`s (both current README examples) keep working; subclassing
  `DomainCommand` is what unlocks provenance stamping, detected via `isinstance`.
- **Non-goals**: no `@register_command`, no serialization registry, no command bus, no
  command persistence. Commands are never stored — a rejected command leaves no trace
  in the event store by design.

## Component 2: `DeciderAggregate` (aggregates)

```python
class DeciderAggregate(AggregateRoot[TState]):
    @staticmethod
    @abstractmethod
    def initial_state(aggregate_id: UUID) -> TState: ...

    @staticmethod
    @abstractmethod
    def decide(command: Any, state: TState) -> list[DomainEvent]: ...

    @staticmethod
    @abstractmethod
    def evolve(state: TState, event: DomainEvent) -> TState: ...
```

Base-class behavior:

- `__init__` eagerly sets `_state = initial_state(aggregate_id)`. This deletes the
  None-state footgun. On this subclass `state` is typed `TState` (never `None`).
- `_apply(event)` delegates to `evolve`; `_get_initial_state()` to `initial_state`.
  Replay, snapshots, version validation, and repository integration are inherited
  from `AggregateRoot` unchanged.
- `execute(command) -> list[DomainEvent]` runs `decide(command, self.state)`, then for
  each returned event performs **one** `model_copy` stamping:
  - always: `aggregate_version` (from `get_next_version()`), `aggregate_type`;
  - when `isinstance(command, DomainCommand)`: `causation_id=command.command_id`,
    `correlation_id=command.correlation_id`, `actor_id`, and `tenant_id` (resolution
    order: command's explicit value → tenant context → leave untouched);
  - **precedence: explicit fields win** — a field `decide` set explicitly is never
    overwritten by stamping, detected via `event.model_fields_set` (pydantic tracks
    which fields were passed at construction vs defaulted).
  Each stamped event goes through `apply_event(event, is_new=True)`. The stamped
  events are returned, mirroring `create_event`.
- Failure atomicity: `decide` completes before any `apply_event`, so a rejection (or
  any exception) leaves the aggregate untouched — no version bump, no uncommitted
  events, no partial application of a multi-event return.

Static methods (not free functions passed to a factory) keep the three functions
importable and testable standalone while giving the repository a normal class to
construct.

Note on `aggregate_id`: `DomainEvent.aggregate_id` is a required field, so `decide`
must set it — which it can, because `initial_state(aggregate_id)` receives the id
precisely so state carries the aggregate's identity (e.g. `OrderState.order_id`).
This is a documented contract requirement of the pattern, not something the shell
can stamp after the fact.

## Component 3: Provenance for the other styles

`create_event(...)` gains an optional `command=` keyword. When passed a
`DomainCommand`, the same provenance fields are stamped with the same precedence
rules. Imperative and declarative aggregates get identical audit-trail benefits
without adopting the decider.

## Component 4: `CommandRejectedError`

Added to `exceptions.py`: `CommandRejectedError(EventSourceError)` with a message and
an optional `command` attribute. It is a **convention, not a requirement** — `decide`
may raise anything (existing `ValueError` examples remain legal). All dogfood surfaces
raise it, and application code gets one catchable type meaning "the domain said no"
as distinct from a bug.

## Component 5: BDD harness extension (`testing/bdd.py`)

`DeciderScenario` — synchronous, no store, no event loop:

```python
(DeciderScenario(OrderAggregate)          # or (decide, evolve, initial_state)
    .given(OrderPlaced(...))              # folds via evolve from initial_state
    .when(ShipOrder(tracking_number="T")) # runs decide, captures events or exception
    .then_events(OrderShipped)            # assert produced event types/fields
    # or .then_rejected(CommandRejectedError, match="must be placed"))
```

`then_rejected()` defaults to `CommandRejectedError` but accepts any exception type.
Reuses the existing bdd.py assertion vocabulary so both harnesses feel like one
family.

## Dogfood conversion map (PR 2 unless noted)

| Surface | Change |
|---|---|
| `examples/basic_usage.py` | Converts to decider + `DomainCommand` (bank account as commands-as-values). |
| `examples/imperative_example.py` (new) | Receives the current imperative `BankAccountAggregate` — the imperative style reference survives here. |
| `examples/aggregate_example.py` | Stays declarative — the `@handles` style reference. |
| `docs/explanation/aggregate-styles.md` | File-path references updated to `imperative_example.py`; gains pointer to the shipped `DeciderAggregate`. |
| `docs/explanation/decider-pattern.md` | Drops the "no first-class abstraction" caveat; features the shipped class; userland-shell section becomes a "how it works underneath" appendix. |
| Tutorial `03-first-aggregate.md` | Teaches decider first; closes with an "other styles" pointer. Other tutorials update mechanically. |
| `tests/fixtures/aggregates.py` | General-purpose counter fixtures convert to `DeciderAggregate` (whole unit suite exercises the feature). Fixtures that specifically test `AggregateRoot`/`DeclarativeAggregate` behavior stay on their styles. |
| `bench/core/domain.py` | Gains a decider variant so imperative-vs-decider becomes a maintained benchmark in the matrix harness. |
| `README.md` | Light touch: decider section adopts `DomainCommand` (bulk already landed in PR #80). |

One converted example (`basic_usage.py`) is built **inside PR 1** as an ergonomics
smoke test, then the remainder of the wave lands in PR 2.

## Testing strategy

- `DomainCommand` contract: field defaults, frozenness, `caused_by` correlation chain.
- `DeciderAggregate` contract: eager initial state; stamping (version always;
  provenance only for `DomainCommand`; explicit-fields-win precedence; tenant
  resolution order); plain-dataclass commands work; repository and snapshot
  round-trips.
- Replay-equivalence property: rebuilding via `load_from_history` from
  `execute`-produced events equals folding `evolve` directly.
- `create_event(command=)` stamping parity with `execute`.
- `DeciderScenario`: given/when/then paths, rejection default and custom exception.
- Dogfood proof: converted fixtures keep the existing unit suite green.
- Gates: mypy strict, ruff, import-linter (with `commands/` added to the entities
  contract), `make check` CI parity.

## ADR Impact (per definition-of-done)

- **New ADR-0021: Command objects and the decider aggregate style.** Records the
  `DomainCommand` contract, provenance stamping semantics and precedence, tenant
  resolution order, decider-as-primary-style, and non-goals (no command bus, no
  command persistence/registry, structural-typing opt-in).
- ADR-0001 (async-first): **stands** — `decide`/`evolve` are sync pure functions,
  same as `_apply` today; all I/O boundaries remain async.
- ADR-0012 (event-type auto-derivation): **stands** — commands deliberately have no
  registry; nothing about event registration changes.
- ADR-0018 (tenant isolation): **stands** — ADR-0021 documents the command-then-
  context tenant resolution order as an extension, not a change.
- ADR-0019 (clean architecture store ports): **stands** — commands live in the
  entities ring; no port changes.

## Staging plan

1. **PR 1 — core feature:** `commands/` package, `DeciderAggregate`,
   `CommandRejectedError`, `create_event(command=)`, ADR-0021, unit tests, public
   exports (`DomainCommand`, `DeciderAggregate`, `CommandRejectedError` from
   `eventsource`), `basic_usage.py` conversion as smoke test, import-linter update.
2. **PR 2 — dogfood wave:** remaining conversion map (examples, tutorials, fixtures,
   bench, doc updates).
3. **PR 3 — BDD harness:** `DeciderScenario` in `testing/bdd.py`, exported from
   `eventsource.testing`, with tests and a testing-guide docs update.

## Out of scope

- Command bus / async command handling (own ADR if ever wanted).
- Command persistence, serialization registry, or `@register_command`.
- Event-spec `decide` contract (rejected; revisit only if a real profile demands it).
- Version-at-append redesign (rejected: ADR-scale churn to save ~2 µs/event).
