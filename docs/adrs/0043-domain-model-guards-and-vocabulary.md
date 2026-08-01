# 0043. Domain Model Guards, Vocabulary, and the Decider-First Teaching Layer

Six related changes from the DDD teaching-layer wave: `aggregate_type`
becomes required on aggregates and category-validated on events, closing
two silent-corruption paths; `DeclarativeAggregate` stops silently
skipping unhandled events by default; `domain/types.py` sheds its
position aliases and threads plain-`UUID` identity aliases through real
signatures; `DeciderAggregate` gains an optional, PEP-696-defaulted
second type parameter for typed command dispatch; the documentation
front doors are rewritten decider-first to match what ADR 0022 already
decided; and the Python floor rises to 3.13 to use PEP 696 natively.
None of these relocate a module; all six change what the domain ring
accepts, or how it is taught.

## Status

**Accepted.** Implemented across tasks 1–7 of the DDD teaching-layer
wave.

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0022](0022-command-objects-and-decider-style.md) | Stands — `DeciderAggregate`'s structural typing for `execute()`/`decide()` is unaffected; Decision §4 below adds an optional, backward-compatible second type parameter without touching the structural-typing design. ADR 0022 §5 (decider as the primary showcased style) is what Decision §5 below brings the documentation into compliance with. |
| [0030](0030-top-level-module-ring-consolidation.md) | Amended — `domain/types.py`'s settled contents change (Decision §3 below); the module's location inside `domain/`, which ADR 0030 decided, is unaffected. |
| [0042](0042-domain-event-strictness.md) | Stands — this ADR's guards are additive to ADR 0042's hardening wave; neither touches the other's decisions. |

ADR 0030's Status section carries an "Amended by ADR 0043" pointer.

## Context

Two more silent-corruption paths remained in the entities ring after
ADR 0042's hardening wave, both in the same shape as the six that ADR
0042 closed: a caller-visible failure that used to be a quiet default
instead.

1. `AggregateRoot.aggregate_type` carried a fallback default of
   `"Unknown"`. A concrete aggregate subclass that forgot to declare
   `aggregate_type` did not fail at class definition or at
   construction — it silently began writing to and reading from a
   `"Unknown"`-typed stream, indistinguishable at a glance from a
   correctly-typed one until a query for the real category came back
   empty.
2. `DomainEvent.aggregate_type` was an unconstrained `str`. Nothing
   stopped a caller from passing a value containing `:` or other
   `StreamId`-delimiter characters; the corruption did not surface at
   event construction, where the mistake was made, but later, whenever
   the value was rendered into a stream identifier — a `StreamId`
   confused about where the category ends and the aggregate id begins.

A third, related gap was behavioral rather than structural.
`DeclarativeAggregate.unregistered_event_handling` defaulted to
`"ignore"`: an aggregate replaying an event it had no `@handles` method
for silently skipped it, with no error and no log. On the write model —
where a command handler reasons over the aggregate's current state to
decide whether a command is valid — a silently incomplete state is a
correctness bug wearing the shape of a working aggregate. Projections
have a different failure calculus (a missing handler there is more
often a deliberately partial read model) and were not touched.

Separately, `domain/types.py` — settled onto `domain/` by ADR 0030 —
still carried `Version`, `StreamPosition`, and `GlobalPosition`, three
names describing positions in the global feed. ADR 0019 had already
made positions **opaque adapter-owned tokens** (`ports/positions.py`);
these three aliases were vocabulary for a design ADR 0019 superseded,
kept alive only by inertia. The five identity aliases that remained
(`AggregateId`, `EventId`, `TenantId`, `CorrelationId`, `CausationId`)
were published as their own names but not actually used in
`DomainEvent`'s or `DomainCommand`'s field annotations, which still
spelled `UUID` directly — the published vocabulary and the real
signatures had drifted apart.

`DeciderAggregate[TState]`'s single type parameter left `decide()`'s
`command: TCommand` parameter typed as whatever the base class's
`TCommand` bound resolved to — effectively untyped for a caller with
more than one command class, since mypy could not narrow which command
subclass a given `decide()` branch was handling. A caller could not opt
into a command union the type checker would flag as non-exhaustive.

Finally, the teaching layer had drifted from ADR 0022 §5's own
decision: getting-started.md, the docs index, and the aggregate-styles
guide still led with the declarative style, not the decider — the style
ADR 0022 named as primary was not the style newcomers actually saw
first. A related inconsistency sat in the same pages: the quickstart
recommended hand-declaring `event_type`, contradicting
`.claude/rules/architecture.md`'s "never declare it by hand" rule for
the auto-derivation ADR 0012 established.

## Decision

### 1. `aggregate_type` required on aggregates; category-validated on events

`AggregateRoot.aggregate_type` becomes a `ClassVar[str]` with no
default. `AggregateRoot.__init__` checks `getattr(type(self),
"aggregate_type", None)` and raises the new `AggregateTypeNotSetError`
if a concrete subclass never set it — the failure moves from "silently
wrong stream category, discovered later" to "cannot construct the
aggregate at all." `DomainEvent.aggregate_type` gains a
`model_validator(mode="after")` that checks the value against
`CATEGORY_PATTERN` (`domain/stream_id.py`'s compiled regex, reused
directly so event validation accepts exactly what `StreamId` does) and
raises `pydantic.ValidationError` on a value that would corrupt a
`StreamId` render.

The validator is a `model_validator(mode="after")`, not a
`field_validator` and not pydantic's `validate_default=True`. Both
alternatives were benchmarked against the after-validator: `
validate_default=True` re-runs the field's validator on every
construction whether or not the caller supplied a value, which
benchmarked at roughly a 15% per-construction cost increase on
`DomainEvent` — a base class instantiated on every event append in the
library. An after-validator runs exactly once per construction, checks
the already-assigned instance attribute, and costs nothing extra when
the value is valid, which is the overwhelmingly common case.

### 2. `DeclarativeAggregate.unregistered_event_handling` defaults to `"error"`

The `ClassVar[UnregisteredEventHandling]` default flips from `"ignore"`
to `"error"`. Replaying an event with no matching `@handles` method now
raises `UnhandledEventError` unless the subclass explicitly opts down
with `unregistered_event_handling = "ignore"` or `"warn"`. The
projections-side knob (`DeclarativeProjection`'s own handling of
unregistered events) is untouched — the asymmetry is deliberate: a
write-model aggregate reasoning over incomplete state is a correctness
defect, a read-model projection with a narrower interest than the full
event stream is often working as designed.

`create_event()`'s `aggregate_version` override parameter — previously
documented as ordinary usage — is reframed in its docstring as an
escape hatch: normal callers let the aggregate compute its own next
version, and passing an explicit override is for the narrow cases
(replay tooling, migration scripts) where a caller genuinely knows
better than the aggregate's own bookkeeping.

### 3. `domain/types.py` reshaped to real vocabulary

`Version`, `StreamPosition`, and `GlobalPosition` are deleted from
`domain/types.py`. Positions remain exactly where ADR 0019 put them:
opaque, adapter-owned tokens behind `ports/positions.py`; `domain/`
never had authority over their representation and the three deleted
aliases were vocabulary for a pre-ADR-0019 design that ADR 0030's
relocation carried forward without re-examining. The five identity
aliases (`AggregateId`, `EventId`, `TenantId`, `CorrelationId`,
`CausationId`) are now plain `UUID` aliases — optionality is expressed
on the referencing field (e.g. `causation_id: CausationId | None`), not
baked into the identity type itself — and are threaded through
`DomainEvent`'s and `DomainCommand`'s actual field annotations in place
of bare `UUID`, so the published vocabulary and the real signatures
agree for the first time.

### 4. `DeciderAggregate[TState, TCommand]` with a PEP-696-defaulted second parameter

`DeciderAggregate` becomes
`DeciderAggregate(AggregateRoot[TState], Generic[TState, TCommand])`
with `TCommand = TypeVar("TCommand", default=object)` — native PEP 696,
not a compatibility shim. Parameter order is `[TState, TCommand]`,
state first, matching `AggregateRoot[TState]`'s existing single
parameter and keeping the defaulted parameter last per PEP 696's own
convention. A caller who only ever subscripted `DeciderAggregate
[MyState]` keeps working unchanged — `TCommand` resolves to `object`
and `decide()`'s `command` parameter is typed exactly as permissively
as it always was. A caller who subscripts `DeciderAggregate[MyState,
MyCommandUnion]` gets `decide()` and `execute()` typed against that
union, so mypy can flag a command variant the aggregate's `decide()`
never branches on. This adds typed dispatch without touching ADR
0022's structural-typing design: `decide()` and `execute()`'s shapes
are unchanged, only their generic parameterization gained a second,
optional axis.

### 5. Teaching layer realigned decider-first

`getting-started.md`, `index.md`'s concepts tour, `
explanation/aggregate-styles.md`, and tutorial 08 are rewritten to lead
with `DeciderAggregate` + `DomainCommand` + `CommandRejectedError`,
bringing the documentation into compliance with what ADR 0022 §5
already decided and never retracted — the decider was always the
intended primary style, the docs simply had not caught up.
`aggregate-styles.md` keeps exactly one worked example per legacy style
(`DeclarativeAggregate`, raw `AggregateRoot`) alongside a typed
two-parameter decider example, and both pages that discuss
`DeciderAggregate` note that `TCommand` is optional. The quickstart's
`event_type` guidance is corrected to match the architecture rule:
auto-derived by default, hand-declared only for a versioned wire name
(e.g. `OrderCreated.v2`) that must diverge from the class name.

### 6. Python floor raised to 3.13

`requires-python` becomes `">=3.13"` (was `>=3.11`); the CI matrix,
mypy `python_version`, and ruff `target-version` all move to 3.13. This
is what makes Decision §4's `TypeVar(..., default=...)` native rather
than routed through `typing_extensions`. The project targets one modern
floor instead of carrying a compatibility import, decided with no
external users to migrate — the same standing rule ADR 0030's Context
invokes for its own no-shim policy.

The version bump also activated three ruff `UP0xx` modernization rules
that hadn't fired under the 3.11 target: `UP046` (generic class should
use PEP 695 type-parameter syntax instead of `Generic[...]`
subclassing), `UP047` (generic function should use PEP 695 type
parameters), and `UP040` (type alias should use the `type` statement
instead of `TypeAlias`). Fifty-seven other, unrelated `UP0xx`
autofixes — safe, mechanical modernizations with no structural
implication — were applied directly. The three structural rules above
are staged as ignores in `pyproject.toml`'s `[tool.ruff.lint]` block,
with a comment recording the deferral: adopting PEP 695's type-
parameter syntax project-wide (including its auto-variance inference,
which differs from `Generic[...]`'s explicit variance) is a distinct,
larger change than raising the interpreter floor, and is a named
follow-up rather than something to fold into this wave silently.

## Consequences

### Positive

- An aggregate that forgets `aggregate_type` fails at construction with
  a named exception instead of writing to a stream silently mistyped as
  `"Unknown"`.
- An event whose `aggregate_type` would corrupt a `StreamId` fails at
  construction, at the call site that made the mistake, instead of at
  an unrelated stream-render call site much later.
- A `DeclarativeAggregate` replaying an event with no matching handler
  now fails loudly by default, closing the same class of "the write
  model quietly reasoned over incomplete state" defect ADR 0042 closed
  for duplicate handlers.
- `domain/types.py`'s five identity aliases now describe what
  `DomainEvent`/`DomainCommand` actually declare; there is no second,
  drifted vocabulary to keep in sync by hand.
- `DeciderAggregate[TState, TCommand]` lets a caller with more than one
  command type get exhaustiveness checking from mypy, without changing
  anything about how a single-command decider is written or typed.
- The teaching layer's decider-first framing matches what ADR 0022 §5
  already decided; a newcomer now sees the primary style first instead
  of encountering it as one of three options with no signal about
  which one the project recommends.

### Negative

- **BREAKING (four separate changes):** removing the `aggregate_type`
  default; validating `DomainEvent.aggregate_type` as a stream
  category; flipping `unregistered_event_handling`'s default; and
  raising the Python floor to 3.13. Each requires an explicit migration
  step for anyone depending on the old behavior — no transition window,
  per the project's pre-1.0 standing rule (ADR 0025, ADR 0030).
- `domain/types.py` deleting `Version`, `StreamPosition`, and
  `GlobalPosition` breaks any code importing them directly; the
  replacement is `ports/positions.py`'s opaque token, which is not a
  drop-in rename — a caller doing arithmetic on the deleted aliases (a
  pattern ADR 0019 already prohibited for positions) has no direct
  replacement and must restructure around the opaque-token contract.
- The `UP046`/`UP047`/`UP040` ruff ignores are a deliberate, temporary
  gap in the modernization the 3.13 floor otherwise enables: the
  codebase's generic classes and type aliases do not yet use PEP 695
  syntax, and a reviewer scanning for "why hasn't this been
  modernized" needs to find this ADR's Decision §6, not rediscover the
  staging decision from the bare ignore list.
- `DeciderAggregate`'s parameter order (`[TState, TCommand]`) means a
  caller who wants to specify `TCommand` without a fully-generic
  `TState` cannot omit the first parameter — this is standard PEP 696
  behavior for a defaulted trailing parameter, not a library-specific
  restriction, but it is a constraint worth naming since the two
  parameters read as independent axes.

## Alternatives Considered

**Validate `DomainEvent.aggregate_type` with `field_validator` or
`validate_default=True` instead of an after-validator.** Rejected:
`validate_default=True` re-runs on every construction regardless of
whether the caller supplied a value, benchmarked at roughly 15% extra
per-construction cost on a base class instantiated for every event in
the system. A `field_validator` runs before the model's other fields
are necessarily settled and would need its own category-pattern
duplication; the `model_validator(mode="after")` runs once, after the
instance is fully assigned, at the same cost as any single-field check.

**Give `unregistered_event_handling` a different default for aggregates
than the codebase's existing `"error"`/`"ignore"`/`"warn"` vocabulary,
e.g. a fourth "strict" mode.** Rejected: the three-value vocabulary
already existed and was already used by callers who opted down
explicitly; introducing a fourth value to distinguish "the new stricter
default" from "the pre-existing opt-in `'error'`" would be a
distinction without a behavioral difference — both mean the same thing,
raise on an unhandled event.

**Keep `TCommand` mandatory on `DeciderAggregate` rather than
PEP-696-defaulted.** Rejected: every existing `DeciderAggregate[MyState]`
subscription in the codebase and in downstream code would require a
mechanical edit to add a second parameter for no behavioral gain in the
common single-command case. A defaulted parameter gets the same typed-
dispatch benefit for multi-command callers while leaving the dominant,
simpler case untouched — the same reasoning PEP 696 itself was written
to formalize.

**Defer the Python 3.13 floor and route PEP 696 through
`typing_extensions.TypeVar` instead.** Rejected: the project has no
external users to preserve a compatibility window for (the same
standing rule behind every no-shim ADR since 0025), and a compatibility
import for a feature the floor bump makes native is exactly the kind of
carrying cost the project's clean-break policy exists to avoid taking
on for a hypothetical consumer.

## References

- `src/eventsource/domain/aggregate.py` — `AggregateRoot.aggregate_type`,
  `AggregateTypeNotSetError` construction-time check,
  `DeclarativeAggregate.unregistered_event_handling`
- `src/eventsource/domain/event.py` — `DomainEvent.aggregate_type`'s
  `model_validator(mode="after")`, `CATEGORY_PATTERN` reuse
- `src/eventsource/domain/stream_id.py` — `CATEGORY_PATTERN`
- `src/eventsource/domain/exceptions.py` — `AggregateTypeNotSetError`
- `src/eventsource/domain/types.py` — the five identity aliases
- `src/eventsource/ports/positions.py` — the opaque position token ADR
  0019 established, unaffected by this ADR
- `src/eventsource/domain/decider.py` — `DeciderAggregate[TState,
  TCommand]`, `TCommand = TypeVar("TCommand", default=object)`
- `pyproject.toml` — `requires-python = ">=3.13"`, the staged
  `UP046`/`UP047`/`UP040` ruff ignores and their deferral comment
- `docs/getting-started.md`, `docs/index.md`,
  `docs/explanation/aggregate-styles.md` — the decider-first teaching
  layer
- [ADR 0019](0019-clean-architecture-store-ports.md) — the opaque-token
  position design this ADR's deleted aliases had drifted from
- [ADR 0022](0022-command-objects-and-decider-style.md) — the decider
  pattern and its §5 documentation stance this ADR's teaching-layer
  changes bring the docs into compliance with
- [ADR 0030](0030-top-level-module-ring-consolidation.md) — the
  `domain/types.py` relocation this ADR amends the contents of
- [ADR 0042](0042-domain-event-strictness.md) — the sibling hardening
  wave this ADR's guards extend

## Related

- `CHANGELOG.md` — `[Unreleased]` entries for all six changes
- `.claude/rules/architecture.md` — the entities-ring alias list and
  `aggregate_type` requirement, updated for this ADR
