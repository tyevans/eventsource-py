# 0033. Events, Handlers, and Internal Ring Migration

`events/`, `handlers/`, and `_internal/` were the last top-level packages
`.claude/rules/architecture.md`'s ring map still carried on a "during
transition" list — `events/` under entities (ring 1), `handlers/` under use
cases (ring 2), and `_internal/` as an unranked shared-utility package with
no ring assignment at all. This ADR dissolves all three: their seven modules
land on `domain/`, `application/`, and `adapters/_bus/` by the same
Dependency Rule test ADR 0025, ADR 0026, ADR 0029, ADR 0030, ADR 0031, and
ADR 0032 have already applied to every other pre-ring package, and
`eventsource/events/`, `eventsource/handlers/`, and `eventsource/_internal/`
are deleted outright.

## Status

**Accepted.** Implemented in `src/eventsource/domain/event.py`,
`src/eventsource/domain/event_registry.py`, `src/eventsource/domain/decorators.py`,
`src/eventsource/domain/exceptions.py` (three merged exceptions),
`src/eventsource/application/projections/handlers.py`,
`src/eventsource/application/background_tasks.py`, and
`src/eventsource/adapters/_bus/handler_adapter.py`. `src/eventsource/events/`,
`src/eventsource/handlers/`, and `src/eventsource/_internal/` no longer
exist; `import eventsource.events`, `import eventsource.handlers`, and
`import eventsource._internal` all raise `ModuleNotFoundError`. No shim, no
deprecation window — the standing rule ADR 0025/0026/0030 already applied to
every prior retirement. The top-level `eventsource/__init__.py` barrel's
`__all__` is byte-identical to before this migration.

## Decision Table

| Old module | New home | Ring | Rationale |
|---|---|---|---|
| `events/base.py` (`DomainEvent`) | `domain/event.py` | domain | Pure pydantic entity — the same "frozen model, no I/O" shape every other domain-ring type already has. |
| `events/registry.py` (`EventRegistry`, `register_event`, `default_registry`, `get_event_class`, `get_event_class_or_none`, `is_event_registered`, `list_registered_events`) | `domain/event_registry.py` | domain | Pure stdlib (`threading.RLock`) plus domain types — no I/O, no adapter contact. |
| `EventTypeNotFoundError`, `DuplicateEventTypeError` | `domain/exceptions.py` | domain | Full-hierarchy rule (ADR 0030): every core exception lives in one place. Rebased onto `EventSourceError`, `KeyError` and `ValueError` mixins retained respectively — widening-only, the same pattern ADR 0029 and ADR 0032 already applied. |
| `handlers/decorators.py` (`@handles`, `get_handled_event_type`, `is_event_handler`) | `domain/decorators.py` | domain | `DeclarativeAggregate.__init_subclass__` (`domain/aggregate.py`) discovers the `_handles_event_type` attribute directly — the attribute's contract is owned by its innermost consumer, not by a package shared with the projection-facing registry. |
| `handlers/registry.py` (`HandlerRegistry`, `HandlerInfo`, `UnregisteredEventHandling`) | `application/projections/handlers.py` | application | Extracted from `DeclarativeProjection` under ADR 0013; its sole `src/` importer is `application/projections/base.py`. |
| `HandlerSignatureError` | `domain/exceptions.py` | domain | Full-hierarchy rule; `ValueError` mixin retained, widening-only. |
| `handlers/adapter.py` (`HandlerAdapter`, `AsyncHandlerFunc`, `get_handler_name`) | `adapters/_bus/handler_adapter.py` | adapters | Every importer is a bus adapter (`adapters/_bus/base.py`, `adapters/_bus/registry.py`, and the Kafka/RabbitMQ/Redis/memory bus backends) — `HandlerAdapter` normalizes a caller-supplied handler to `await adapter.handle(event)` for dispatch, which is bus plumbing, not use-case orchestration. |
| `_internal/background_tasks.py` (`BackgroundTaskManager`) | `application/background_tasks.py` | application | See below. |

## Context

`docs/core-surface.md` and `.claude/rules/architecture.md`'s ring map
carried `events/` under "Entities (`domain/` — during transition also
`events/`)" and `handlers/` under "Use cases (`application/` — during
transition `migration/`, `handlers/`)" since the rings were first drawn.
Both were the last modules on their respective transitional lists once ADR
0032 retired `subscriptions/` — `application/`'s list shrank to `migration/`
and `handlers/`, and `domain/`'s shrank to `events/` alone. `_internal/`
never appeared on the ring map at all: it shipped as a standalone top-level
package (ADR 0030's own Context section names it, in passing, as the
reason `events/base.py` sits beneath `ports/handlers.py` as a hard import
floor) holding one module, `background_tasks.py`, with no ring assignment
and no import-linter contract of its own.

`events/base.py`'s `DomainEvent` is the load-bearing extraction floor
`docs/core-surface.md` already documents: `ports/handlers.py`,
`handlers/decorators.py`, and (pre-ADR-0031) `bus/interface.py` all carry a
plain module-level `from eventsource.events.base import DomainEvent`, not
guarded by `TYPE_CHECKING`, so every one of those modules pulls in pydantic
transitively. That floor does not move with this ADR — `domain/event.py` is
exactly as load-bearing a floor as `events/base.py` was, just renamed.
`events/registry.py`'s only `eventsource` import was already
`TYPE_CHECKING`-only (`docs/core-surface.md` notes this as the one module
that breaks the module-level-coupling pattern), so its move introduces no
new dependency-rule question.

`handlers/decorators.py` was the one module in the "use cases" transitional
list whose sole consumer lived in `domain/`, not `application/`:
`DeclarativeAggregate.__init_subclass__` (`domain/aggregate.py:716`) scans
`dir(cls)` for the `_handles_event_type` marker `@handles` sets, entirely
independent of `HandlerRegistry`. `handlers/registry.py`, by contrast, is
used by nothing but `application/projections/base.py` — it is the
`HandlerRegistry`/`HandlerInfo` pair ADR 0013 extracted out of
`DeclarativeProjection` as a collaborator. Leaving `@handles` and
`HandlerRegistry` in one package obscured that they have different
consuming rings; splitting them onto `domain/decorators.py` and
`application/projections/handlers.py` makes each module's ring assignment
match its actual importer instead of an accident of original packaging.

`handlers/adapter.py`'s `HandlerAdapter` had a third, disjoint consumer set:
every one of `adapters/_bus/base.py`, `adapters/_bus/registry.py`, and the
Kafka/RabbitMQ/Redis/memory bus backends imports it to normalize a
caller-supplied handler (an object with `.handle()`, or a plain sync/async
callable) into one awaitable shape before dispatch. Nothing in
`application/` imports it. Its old home also carried a compatibility
re-export block pointing back at `ports/handlers.py` for `AsyncEventHandler`
and `SyncEventHandler` — a shim this ADR drops per the no-shims rule; those
two names' canonical home was already `ports/handlers.py` since ADR 0030,
and the re-export bought nothing but a second import path for the same
names.

`_internal/background_tasks.py`'s `BackgroundTaskManager` had two
consumers on opposite sides of the ring boundary: `application/aggregates/`
uses it for background snapshot scheduling (one of the two concrete
`SnapshotScheduler` implementations ADR 0021 introduced), and
`adapters/_bus/base.py` uses it to track and drain in-flight background
publish tasks on shutdown. Neither consumer depends on the other, and
`BackgroundTaskManager` itself is pure `asyncio` task-set bookkeeping with
no store, bus, or driver contact of its own — it does not need a Protocol
in front of it, only a ring assignment.

## Why `BackgroundTaskManager` lands in `application/`, not a shared package

The Dependency Rule is directional, not symmetric: an outer ring
(`adapters/`) may depend inward on an inner ring (`application/`), but never
the reverse. A utility two rings both use therefore has exactly one
dependency-rule-compatible home — the innermost of its consumers — because
placing it at the outer ring would force the inner ring to import outward
to reach it, and placing it in a third, ring-less package (what `_internal/`
already was) recreates the "unranked shared utility" problem this ADR
exists to close. `application/` is `BackgroundTaskManager`'s innermost
consumer, so `application/background_tasks.py` is the only placement that
lets both `application/aggregates/` (an intra-ring import) and
`adapters/_bus/base.py` (an outward-to-inward import, which is exactly the
direction adapters are permitted to depend) reach it without a contract
exception. No code inside `BackgroundTaskManager` changed — placement was
the entire fix.

## Consequences

### Positive

- The ring-1 "during transition" list in `.claude/rules/architecture.md`
  loses `events/`; the ring-2 list loses `handlers/` and keeps only
  `migration/`. `_internal/` — which never had a ring assignment to lose —
  stops existing as an unranked exception to the ring map entirely.
- `domain/decorators.py` and `application/projections/handlers.py` each now
  sit in the ring their actual (and only) consumer occupies, rather than
  sharing a package whose name matched neither.
- `import-linter`'s contracts gain explicit coverage for
  `eventsource.adapters._bus` importing `handler_adapter` — previously
  `handlers/adapter.py` sat outside any ring-boundary contract at all.
- Tier 0 (stdlib + pydantic only) is unchanged in membership, just renamed:
  `domain/event.py` and `domain/event_registry.py` replace `events/base.py`
  and `events/registry.py` as the floor beneath `ports/handlers.py`.

### Negative

- `import eventsource.events`, `import eventsource.handlers`, and
  `import eventsource._internal` now raise `ModuleNotFoundError` with no
  transition period. Every internal importer (`src/`, `tests/`, `bench/`,
  `docs/` examples) needed the sweep this migration's implementation slices
  carried out — roughly forty `src/` importers plus inline test imports.
- The `handlers/adapter.py` compatibility re-export of `AsyncEventHandler` /
  `SyncEventHandler` from `ports/handlers.py` is gone. Any code that imported
  those two names from `eventsource.handlers.adapter` (rather than from
  `eventsource.handlers` or `eventsource.ports.handlers`, both of which
  still work / now resolve to the same names) must repoint to
  `eventsource.ports.handlers` directly.
- `handlers/registry.py`'s `UnregisteredEventHandling` type alias was never
  re-exported from the `eventsource.handlers` package (only from the
  submodule); it keeps that same asymmetry at its new address,
  `eventsource.application.projections.handlers`.

## Alternatives Considered

**Keep `_internal/` as a rings-exempt shared package, the way it already
existed.** Rejected: `_internal/` was the one top-level package with no
ring assignment and no import-linter contract, which is exactly the
"transitional exception" shape this campaign of ADRs (0025, 0026, 0029,
0030, 0031, 0032) has been closing out one package at a time. A
rings-exempt shared package is also underspecified as a rule — nothing
stops a second, unrelated utility from landing there next, and the
inward-dependency argument above shows a ring-correct home always exists
for genuinely shared code. There is no second module in the same shape
today to justify keeping the exemption open.

**Put `HandlerAdapter` in `application/`, alongside `HandlerRegistry`.**
Rejected: every actual importer of `HandlerAdapter` is a bus adapter, not a
use case. `application/` code never imports it. Moving it to
`application/` would recreate exactly the "package named for one thing,
consumed by another" problem this ADR splits `handlers/` apart to fix —
the same reasoning that separates `domain/decorators.py` from
`application/projections/handlers.py` in the first place. `HandlerAdapter`
also has no Protocol counterpart in `ports/`; it is pure normalization
logic that belongs with the adapters that call it, matching the
`adapters/_bus/base.py` / `adapters/_bus/registry.py` collaborators it
already sits beside.

**Give `BackgroundTaskManager` its own `ports/` Protocol plus per-ring
implementations.** Rejected: there is exactly one implementation and no
second one is anticipated — `BackgroundTaskManager` is `asyncio.Task`
bookkeeping, not a capability that varies by backend technology the way
`EventBus` or `EventStore` do. A Protocol split would add ports-ring
ceremony with no second adapter to justify it, the same "don't invent an
interface for a single implementation" judgment ADR 0009 applied to
`InMemoryLeaderElector` before Kubernetes/Redis/Consul electors existed to
motivate a Protocol.

## References

- `src/eventsource/domain/event.py`, `src/eventsource/domain/event_registry.py`,
  `src/eventsource/domain/decorators.py`, `src/eventsource/domain/exceptions.py`
- `src/eventsource/application/projections/handlers.py`,
  `src/eventsource/application/background_tasks.py`
- `src/eventsource/adapters/_bus/handler_adapter.py`
- [ADR 0013](0013-handler-registry-composition.md) — the original
  `HandlerRegistry`/`HandlerAdapter` composition decision this ADR relocates
  without changing
- [ADR 0025](0025-legacy-store-retirement.md), [ADR 0026](0026-outbox-ring-migration.md),
  [ADR 0029](0029-locks-readmodels-and-engine-rings.md),
  [ADR 0030](0030-top-level-module-ring-consolidation.md),
  [ADR 0031](0031-bus-ring-split.md), [ADR 0032](0032-subscriptions-ring-migration.md) —
  the same no-shim, ring-map-completion pattern this ADR applies to the
  last three top-level packages

## Related

- `docs/api/events.md`, `docs/api/handlers.md`, `docs/api/exceptions.md` —
  API reference updated for the new module layout
- `.claude/rules/architecture.md` — ring map updated to mark
  `domain/event.py`, `domain/event_registry.py`, `domain/decorators.py`,
  `application/projections/handlers.py`, `application/background_tasks.py`,
  and `adapters/_bus/handler_adapter.py` as settled
