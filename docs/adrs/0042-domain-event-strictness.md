# 0042. Domain Event and Handler Strictness

Six related hardening changes to the entities ring, all shipped together
in the domain-ring hardening wave: a single source of truth for an
event's wire name, `extra="forbid"` on `DomainEvent`, single-inheritance
registry/handler exceptions, hard-clear tenant-context semantics,
class-definition-time handler validation, and unified provenance
stamping. None changes where a class lives; all six change what the
domain ring now refuses to let through silently.

## Status

**Accepted.** Implemented across tasks 1–6 and 8 of the domain-ring
hardening wave.

**ADR Impact**

| ADR | Status |
| --- | --- |
| [0033](0033-events-handlers-internal-ring-migration.md) | Stands — `events/` and `handlers/`'s locations inside `domain/` are unchanged; this ADR hardens behavior at those same locations, not their placement. |
| [0038](0038-multitenancy-dissolution.md) | Amended — `clear_tenant_context()`'s semantics change (Decision §4 below); location from ADR 0038 unaffected. |
| [0022](0022-command-objects-and-decider-style.md) | Amended — `DeciderAggregate`'s provenance-stamping semantics change (Decision §6 below); the command-object design and decider pattern ADR 0022 introduced are unaffected. |

ADR 0033's and ADR 0038's and ADR 0022's Status sections each carry an
"Amended by ADR 0042" pointer.

## Context

Six independent defects and gaps accumulated in the entities ring as it
grew past the point any one person was tracking every call site by hand:

1. `DomainEvent.__init_subclass__` derived a subclass's wire name by
   reading and rewriting the *parent* class's `event_type` `FieldInfo` in
   place — a shared, mutable object. Subclassing a concrete event
   corrupted the parent: `class Child(Parent): ...` silently changed what
   `EventRegistry.register_event(Parent)` filed `Parent` under, so a
   previously stored `"Parent"` event could become undeserializable, or
   registering both classes could raise a spurious
   `DuplicateEventTypeError` depending on registration order. Nothing
   caught this because no test constructed a event subclass hierarchy and
   then re-registered the parent.
2. `DomainEvent` accepted arbitrary extra constructor keyword arguments
   and silently dropped them (pydantic's default `extra="ignore"`
   behavior on a frozen model). A typo'd field name in event construction
   persisted an event missing the data the caller thought it was
   recording, with no error at any point in the pipeline.
3. `EventTypeNotFoundError` and `DuplicateEventTypeError` also subclassed
   `KeyError` and `ValueError` respectively, and `HandlerSignatureError`
   also subclassed `ValueError` — multiple inheritance chosen, at the
   time each was introduced, so an `except KeyError` or `except
   ValueError` written before the library had an `EventSourceError` tree
   would keep working. This meant `str(exc)` for `EventTypeNotFoundError`
   went through `KeyError.__str__`, which re-quotes its argument,
   producing a message with embedded quote characters no caller asked
   for, and meant `except ValueError` clauses written for unrelated
   reasons silently caught library-specific failures they were never
   designed to handle.
4. `clear_tenant_context()` called `tenant_context.set(None)` but never
   touched the module-level token stack `set_current_tenant()` and
   `tenant_scope()` push onto. An enclosing `tenant_scope()`'s exit still
   held a live token from before the clear, and exiting that scope called
   `tenant_context.reset(token)`, which resurrected the tenant the caller
   had just tried to clear — a cross-tenant leakage vector in exactly the
   shape a multi-tenant deployment cannot afford: code that believed it
   had guaranteed an empty context for the rest of a request in fact had
   not.
5. Two `@handles` methods for the same event type in one
   `DeclarativeAggregate` or `DeclarativeProjection` subclass silently
   dropped one handler — discovery order (effectively alphabetical by
   method name) decided the winner with no error, no warning, and no way
   for the class author to know a handler they wrote was never called.
   Separately, handler signature validation (async-ness, parameter count)
   ran only inside projections, not aggregates — a `@handles` method on a
   `DeclarativeAggregate` with the wrong shape failed at dispatch time
   with a confusing `TypeError`, not at class-definition time with a
   clear message.
6. Provenance stamping (attaching tenant/caused-by metadata to emitted
   events) had two different code paths: `create_event()`'s helper
   applied an ambient tenant-context fallback unconditionally, but
   `DeciderAggregate`'s stamping only applied that fallback when the
   incoming command was a `DomainCommand` — a command type that happened
   not to be one silently skipped the fallback, producing inconsistent
   provenance depending on a caller's command base class rather than on
   whether an ambient tenant context existed.

## Decision

### 1. `event_type_name()` as the single derivation source

`DomainEvent.event_type_name()` is a new classmethod that computes the
wire name for a class without mutating anything. `__init_subclass__` and
`EventRegistry` both call it instead of each deriving (and, in
`__init_subclass__`'s case, side-effecting) their own answer.
`__init_subclass__` no longer touches the parent class's `FieldInfo` at
all — the corruption in Context item 1 is closed by removing the mutation
entirely, not by making it safer.

### 2. `extra="forbid"` on `DomainEvent`

`DomainEvent`'s pydantic `model_config` sets `extra="forbid"`. An unknown
constructor kwarg now raises `pydantic.ValidationError` at construction
time instead of being silently dropped. **Breaking:** any caller passing
a field name that does not exist on the event class (typo or otherwise)
now fails loudly. Arbitrary payload data that does not fit a named field
belongs in the existing `metadata` field, which remains permissive.

### 3. Registry and handler exceptions drop builtin bases

`EventTypeNotFoundError` no longer subclasses `KeyError`;
`DuplicateEventTypeError` and `HandlerSignatureError` no longer subclass
`ValueError`. All three now subclass only `EventSourceError`. **Breaking:**
`except KeyError` or `except ValueError` clauses written against these
three types no longer catch them — catch the specific type or
`EventSourceError` instead. `str()` output for `EventTypeNotFoundError` is
no longer re-quoted by `KeyError.__str__`.

### 4. `clear_tenant_context()` hard-clear semantics

`clear_tenant_context()` now empties the module-level token stack in
addition to setting the context variable to `None`. A subsequent
`reset_tenant_context()` call — including one triggered by an enclosing
`tenant_scope()`'s exit — finds no valid token to restore and raises the
new `TenantContextResetError` rather than silently resurrecting a
pre-clear tenant. This is the fix for the leakage vector in Context item
4: "cleared" now means cleared for the rest of the current context, full
stop, and any code that still tries to reset onto invalidated state gets
an explicit error instead of silent data corruption.

### 5. Duplicate-handler detection and class-definition-time signature validation

`domain/decorators.py` gains `discover_handlers()`, a single
implementation of `@handles` discovery shared by both
`DeclarativeAggregate` and `DeclarativeProjection` (previously each
implemented its own walk). It raises the new `DuplicateHandlerError` if
two methods on the same class both claim the same event type — at class
*definition* time for `DeclarativeAggregate`, and at instance
*construction* time for `DeclarativeProjection` (which discovers handlers
in `__init__`, not `__init_subclass__`).
`HandlerSignatureError` gains an optional `reason` keyword so both
call sites can describe what shape check failed (wrong parameter count,
handler declared `async` where sync is required, etc.). `
DeclarativeAggregate` now validates every discovered handler's signature
at class-definition time the same way `DeclarativeProjection` already
did, so a malformed `@handles` method on an aggregate fails fast with a
clear message instead of surfacing as a `TypeError` the first time an
event happens to dispatch to it. The `_event_handlers` ClassVar moves off
`AggregateRoot` (which never needed it as a base-class default) onto
`DeclarativeAggregate`, where handler discovery actually happens.

### 6. Unified provenance stamping with unconditional ambient-tenant fallback

A shared `_provenance_updates()` helper computes the tenant/caused-by
metadata to stamp onto an emitted event, and both `create_event()` and
`DeciderAggregate`'s command-handling path call it. The ambient
tenant-context fallback — "if the command didn't carry an explicit
tenant, but a `tenant_scope()` is active, use that" — now applies
unconditionally for every command type the decider handles, not only
`DomainCommand` subclasses. This closes the inconsistency in Context item
6: provenance stamping no longer depends on which command base class a
caller happened to use.

## Consequences

### Positive

- The `FieldInfo`-corruption bug (Context item 1) cannot recur structurally
  — there is exactly one place that computes an event's wire name, and it
  has no side effects.
- A typo'd event field is now a loud `ValidationError` at construction
  time instead of silently persisted data loss (Context item 2).
- `except KeyError` / `except ValueError` clauses that happened to catch
  library exceptions by accident are forced to become explicit
  (`except EventSourceError` or the specific type), which is a one-time
  cost that prevents a broader class of accidental-catch bugs going
  forward.
- Cross-tenant leakage through a stale token reset (Context item 4) is
  closed, and the failure mode when it would have occurred is now a raised
  `TenantContextResetError` instead of silent tenant resurrection.
- A duplicate `@handles` method is now a definition-time error with a
  specific message naming the collision, not a silently dropped handler
  discovered only by noticing an event was never processed.
- Provenance stamping behavior no longer depends on incidental command
  base-class choice.

### Negative

- **BREAKING (three separate changes):** `extra="forbid"` on
  `DomainEvent`; `EventTypeNotFoundError`/`DuplicateEventTypeError`/
  `HandlerSignatureError` no longer multiple-inherit from builtins; and
  any code that relied on "last `@handles` wins" for a duplicate handler
  now gets `DuplicateHandlerError` at class-definition time for aggregates,
  or at instance construction time for projections, instead of quietly
  picking one. All three require an explicit migration
  step for anyone who was depending on the old, looser behavior — no
  transition window, per the project's pre-1.0 standing rule.
- `DeclarativeAggregate` subclasses with previously-unnoticed handler
  signature defects (wrong parameter count, wrongly-async handlers) that
  were tolerated because dispatch happened to work by accident will now
  fail at class-definition time. This is deliberate — surfacing that
  failure earlier and with a clear message — but it means a subclass that
  imported cleanly before this change may now raise `HandlerSignatureError`
  at import time.
- `extra="forbid"` applies to `from_dict()` deserialization as well as
  fresh construction, since `from_dict()` is `model_validate()`. A stored
  event carrying a field no longer declared on its event class — one
  written by an older version of the class, or corrupted in storage — now
  fails replay with `ValidationError` instead of silently dropping the
  extra field. This is accepted deliberately: the project's schema policy
  is additive-only via new event types (never widen or narrow an existing
  one), so under that policy an unknown field on a stored event can only
  indicate a bug or out-of-band tampering, not a legitimate schema
  evolution that replay should tolerate.

## Alternatives Considered

**Fix the `FieldInfo` mutation bug by copying the `FieldInfo` before
mutating it, rather than removing the mutation.** Rejected: the mutation
itself was never necessary — `event_type_name()` can be computed on
demand from the class's own attributes without writing anything back.
Copy-before-mutate would have fixed the corruption but kept a stateful
side effect in `__init_subclass__` for no reason.

**Make `extra="forbid"` opt-in via a subclass flag rather than the
default for all `DomainEvent` subclasses.** Rejected: an opt-in flag
means every existing event class silently keeps the old, unsafe default
until someone remembers to add the flag — exactly the kind of
easy-to-forget migration step the project's no-shim, clean-break
philosophy exists to avoid perpetuating.

**Keep `KeyError`/`ValueError` as secondary bases on the three registry
exceptions for backward compatibility.** Rejected: multiple inheritance
from a builtin was the direct cause of the re-quoting bug in Context item
3, and keeping it "for compatibility" pre-1.0, with no external
consumers, trades a real correctness defect for a hypothetical
convenience nobody is using.

## References

- `src/eventsource/domain/event.py` — `event_type_name()`,
  `__init_subclass__`, `extra="forbid"`
- `src/eventsource/domain/exceptions.py` — `EventTypeNotFoundError`,
  `DuplicateEventTypeError`, `HandlerSignatureError`,
  `DuplicateHandlerError`
- `src/eventsource/domain/decorators.py` — `discover_handlers()`
- `src/eventsource/domain/tenant_context.py` — `clear_tenant_context()`
- `src/eventsource/domain/decider.py` — `_provenance_updates()`
- [ADR 0033](0033-events-handlers-internal-ring-migration.md) — the
  location this ADR hardens behavior at, without changing
- [ADR 0038](0038-multitenancy-dissolution.md) — the tenant-context
  module this ADR amends the semantics of
- [ADR 0022](0022-command-objects-and-decider-style.md) — the decider
  pattern this ADR amends the stamping semantics of

## Related

- `CHANGELOG.md` — `[Unreleased]` entries for all six changes
- `docs/guides/error-handling.md`, `docs/api/exceptions.md` — updated for
  the single-inheritance exception change
- `docs/api/multitenancy.md`, `docs/guides/multi-tenant.md` — updated in
  the final-review fix wave to describe the hard-clear semantics (they
  previously still documented the pre-fix restore-on-clear behavior);
  this ADR is the durable record of the decision
