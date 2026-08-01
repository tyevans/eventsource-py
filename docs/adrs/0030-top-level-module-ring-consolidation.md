# 0030. Top-Level Module Ring Consolidation

The last six top-level modules that predate the ring migration -- `types.py`,
`exceptions.py`, `protocols.py`, `commands/`, `sync/`, and `serialization/` --
move onto the `domain`/`ports`/`adapters` split ADR 0019, ADR 0024, ADR 0026,
and ADR 0029 already applied to every other pre-ring package. `types.py` and
`exceptions.py` join `domain/` as the last two entities-ring modules named in
`.claude/rules/architecture.md`'s transitional list; `protocols.py` joins
`ports/` as `ports/handlers.py`; `commands/`'s `DomainCommand` joins
`domain/` as `domain/command.py`, which also resolves a dependency-rule
violation `domain/aggregate.py` and `domain/decider.py` had been carrying;
`sync/` and `serialization/` join `adapters/` as `adapters/sync/` and
`adapters/serialization/`. A seventh module, top-level `config.py`, is
deleted outright rather than relocated -- it was a seven-line placeholder
with zero importers anywhere in `src/` or `tests/`.

**All six relocations are clean breaks: the old import paths are deleted,
with no deprecation shim and no transition window.** This departs from the
pattern ADR 0029 established for `eventsource.locks` and
`eventsource.readmodels` -- and this ADR also removes those two shims ahead
of the 0.8.0 schedule ADR 0029 set, for the same reason. The library has no
external users yet: every consumer of these modules is inside this
repository's own `src/` and `tests/`, and a shim's entire purpose is to give
external callers time to migrate before a path disappears. With no such
callers, a shim buys nothing and costs real things -- `__getattr__` +
`__dir__` plumbing to write and maintain, warning-suppression noise in the
test suite, and a second, temporary source of truth for where a name lives.
This is not a new stance; it is the same "standing rule" already applied
verbatim to `eventsource.stores` and `eventsource.repositories` when their
respective ADRs (0025, 0026) retired them: pre-1.0 and pre-external-use, a
move is a move, not a deprecation.

## Status

**Accepted.** Implemented in `src/eventsource/domain/types.py`,
`src/eventsource/domain/exceptions.py`, `src/eventsource/domain/command.py`,
`src/eventsource/ports/handlers.py`, `src/eventsource/adapters/sync/`,
`src/eventsource/adapters/serialization/`. `src/eventsource/types.py`,
`src/eventsource/exceptions.py`, `src/eventsource/protocols.py`,
`src/eventsource/commands/`, `src/eventsource/sync/`,
`src/eventsource/serialization/`, and `src/eventsource/config.py` are all
deleted -- no shims. `src/eventsource/locks/` and `src/eventsource/readmodels/`
(the deprecation shims ADR 0029 introduced) are deleted in this same slice,
ahead of the 0.8.0 removal ADR 0029 originally scheduled.

**Amends [ADR 0022](0022-command-objects-and-decider-style.md).** ADR
0022's Decision does not change and is not retro-edited: `DomainCommand` is
still a frozen pydantic base with `caused_by()`, and the decider aggregate
style it introduced is unaffected. What this ADR changes is purely where the
class lives -- `eventsource.commands.DomainCommand` becomes
`eventsource.domain.command.DomainCommand`, with the old path gone outright.
The move is motivated by the dependency-rule defect described in Decision
§1 below, not by anything wrong with ADR 0022's design.

**Amends [ADR 0029](0029-locks-readmodels-and-engine-rings.md).** ADR
0029's Decision §7 introduced the `eventsource.locks` / `eventsource.readmodels`
deprecation shims and scheduled their removal for 0.8.0. This ADR deletes
both shims now, ahead of that schedule, as part of the same no-shim policy
this ADR applies to the six modules above -- see Consequences below for what
that accelerates and what it costs.

Sibling of [ADR 0025](0025-legacy-store-retirement.md) and
[ADR 0026](0026-outbox-ring-migration.md) in *policy* (clean break, no shim,
"the library is unreleased" standing rule), and sibling of
[ADR 0024](0024-projection-persistence-ports.md) and
[ADR 0029](0029-locks-readmodels-and-engine-rings.md) in *shape* (relocating
the last pre-ring modules onto the ring map).

## Context

`docs/core-surface.md` and `.claude/rules/architecture.md`'s ring map both
tracked `types.py`, `exceptions.py`, and `protocols.py` as "during
transition" entries sitting at the top level of `src/eventsource/` rather
than inside `domain/`, `ports/`, or `adapters/` -- the last three modules
still granted that exception after ADR 0029 closed out `locks/` and
`readmodels/`. `commands/`, added by ADR 0022, was never assigned a ring at
all: it shipped as a standalone top-level package beside `events/` because
nothing forced the question at the time. `sync/` and `serialization/` were
in the same position -- top-level packages with no ring, despite each being
a clean single-technology adapter (`sync/` wraps a `FullEventStore` for
sync callers; `serialization/` wraps `orjson`). `BACKLOG.md` carried these
as the last "campaign residue" entries after the locks/readmodels/engine
slice landed, alongside a P3 entry to remove the `eventsource.locks` /
`eventsource.readmodels` shims in 0.8.0.

`domain/aggregate.py` and `domain/decider.py` -- both entities-ring modules
under the settled `domain/` package -- imported `DomainCommand` from
`eventsource.commands`, a top-level package with no assigned ring. Per the
Dependency Rule, nothing in an inner ring may import from a module the ring
map does not also place at or inside that ring; `commands/` sitting outside
`domain/` while `domain/` modules imported from it was exactly that
violation, just not one import-linter had a contract clause to catch yet
because `commands/` was never enumerated as an outer-ring source in the
first place.

`eventsource.config` remained on the surface for a different reason: ADR
0029's Decision section for `readmodels/` and `locks/` did not address it,
and `docs/core-surface.md` finding 10 had already documented what it is --
a five-line docstring, a blank line, one trailing comment, zero imports,
zero classes, zero functions, no `__all__`, and no importer anywhere in the
codebase, despite `docs/api/index.md` describing it as a "Configuration
helpers" subsystem. The finding's own recommendation was "resolve it --
populate or delete -- before extraction," left open pending this slice.

Separately, a decision was made mid-slice, by the user, to stop carrying
deprecation shims at all for the remainder of the pre-1.0 ring migration,
and to retire the two shims ADR 0029 had already shipped rather than let
them sit until 0.8.0. The library has zero external consumers today --
every import of `eventsource.locks`, `eventsource.readmodels`, or any of the
six modules this ADR relocates originates inside this repository. A
deprecation shim is a promise to someone outside the repository that their
code keeps working for one more release cycle; with no such someone, the
promise has no recipient, and the `__getattr__`/`__dir__`/`DeprecationWarning`
machinery each shim requires is pure carrying cost for a guarantee nobody is
consuming. `eventsource.stores` (ADR 0025) and `eventsource.repositories`
(ADR 0026) already established this exact standing rule -- "no shim, no
deprecation warning: the library is unreleased" -- for the legacy store
retirement and the outbox ring migration; this ADR extends the same rule to
the last six top-level modules and, retroactively, to the two shims ADR 0029
introduced before the rule was made explicit project-wide.

## Decision

### 1. `types.py` and `exceptions.py` join `domain/`

Both move verbatim into `domain/types.py` and `domain/exceptions.py`. Neither
required a Protocol/implementation split the way `locks/` and `readmodels/`
did in ADR 0029 -- `types.py` is nine plain type-alias assignments plus one
`TypeVar`, and `exceptions.py` is a flat exception hierarchy rooted at
`EventSourceError`. Both were already Tier 0 (stdlib + pydantic only, no
sqlalchemy) and already imported only entities-ring names; the move is a
pure relocation onto the ring map. `eventsource/types.py` and
`eventsource/exceptions.py` are deleted; `import eventsource.types` and
`import eventsource.exceptions` now raise `ModuleNotFoundError`.

### 2. `protocols.py` becomes `ports/handlers.py`

`EventHandler`, `SyncEventHandler`, and `FlexibleEventHandler` (Protocols),
plus `EventSubscriber`, `AsyncEventHandler`, and `FlexibleEventSubscriber`
(ABCs), move to `ports/handlers.py`. This is a rename, not a restructuring:
the module was already ports-shaped -- boundary interfaces the use-case ring
calls and adapters implement -- and `docs/core-surface.md` finding 9 had
already identified `events/base.py` as the hard floor beneath it (a plain
module-level `from eventsource.events.base import DomainEvent`, not
`TYPE_CHECKING`-guarded). That floor is unchanged by the move: importing
`ports/handlers.py` still executes `events/base.py` and, through it,
pydantic, exactly as importing `protocols.py` always did.
`eventsource/protocols.py` is deleted; `import eventsource.protocols` now
raises `ModuleNotFoundError`.

### 3. `commands/`'s `DomainCommand` becomes `domain/command.py`, fixing the dependency-rule violation

`DomainCommand` moves from `eventsource.commands.base` to
`eventsource.domain.command`. This is the one relocation in this slice that
is not purely cosmetic: it closes the violation described in Context, where
`domain/aggregate.py` and `domain/decider.py` imported a name from a module
the ring map placed nowhere. With `DomainCommand` inside `domain/` itself,
both call sites now import from within their own ring, and the violation
that had no contract to catch it now has no violation to catch. The
`eventsource/commands/` package is deleted in full; `import
eventsource.commands` now raises `ModuleNotFoundError`.

### 4. `sync/` and `serialization/` join `adapters/`

`SyncEventStoreAdapter` moves to `adapters/sync/`; `EventSourceJSONEncoder`,
`json_dumps`, and `json_loads` move to `adapters/serialization/`. Both are
adapters in the Clean Architecture sense even though neither implements a
`ports/` Protocol: `sync/` adapts an async `FullEventStore` to a sync
calling convention (a driving-side adapter, not a driven one), and
`serialization/` adapts Python objects to and from JSON bytes via `orjson`
(a technology-specific gateway, `orjson` being the "specific technology"
even though nothing calls it through a Protocol). Neither module gained a
port; the move places each next to the other technology-specific gateways
it already resembled in shape, not in a new pairing invented for this ADR.
`eventsource/sync/` and `eventsource/serialization/` are both deleted in
full; `import eventsource.sync` and `import eventsource.serialization` now
raise `ModuleNotFoundError`.

`docs/core-surface.md`'s Tier 0 tracking is unaffected by the move for
`serialization/`: the module already left Tier 0 on 2026-07-28 when
`orjson` became a core dependency, and moving from a top-level package to
`adapters/serialization/` does not change that status either direction.

### 5. `eventsource.config` is deleted

`config.py` is removed outright, the same as the six modules above -- there
was never a question of a shim here regardless of this ADR's shim policy,
since `grep -rn "from eventsource.config\|from \.config" src/ tests/`
returns nothing and `docs/core-surface.md` finding 10 confirmed the same at
the time it was written. The two documentation references that described it
as a real subsystem (`docs/api/index.md`'s "ships without top-level
re-export" list and its "Configuration helpers" module-table entry) are
removed as part of this ADR's docs pass rather than updated to point
anywhere, since there is nowhere for them to point.

### 6. No deprecation shims anywhere in this slice, and the two ADR 0029 shims are removed early

`eventsource/types.py`, `eventsource/exceptions.py`, `eventsource/protocols.py`,
`eventsource/commands/`, `eventsource/sync/`, and `eventsource/serialization/`
are deleted outright rather than replaced with lazy re-export shims. This
extends the "no shim, no deprecation warning: the library is unreleased"
standing rule ADR 0025 and ADR 0026 already applied to `eventsource.stores`
and `eventsource.repositories` -- there was no principled reason those two
retirements got a clean break while six more recently-touched modules would
get a temporary shim, once the question was asked explicitly.

`eventsource/locks/__init__.py` and `eventsource/readmodels/__init__.py`
(the two shims ADR 0029 shipped, previously scheduled for removal in 0.8.0)
are deleted in this same slice. `import eventsource.locks` and `import
eventsource.readmodels` now raise `ModuleNotFoundError`. `BACKLOG.md`'s
"Remove the `eventsource.locks` and `eventsource.readmodels` deprecation
shims (P3)" entry, previously scheduled for 0.8.0, is closed by this ADR
rather than carried forward -- see ADR 0029's Status section for the
pointer to this ADR recording the accelerated timeline.

Top-level `from eventsource import ...` imports are unaffected by any of
this: the barrel re-exports every relocated name from its new home
directly, and always did for the six new relocations (they were never
re-exported from their old top-level path except via the path itself). Only
direct submodule imports (`from eventsource.types import AggregateId`, `from
eventsource.locks import PostgreSQLLockManager`, etc.) are affected, and for
those there is no soft landing -- `ModuleNotFoundError` immediately, not a
warning first.

## Consequences

**Positive.**

- The `.claude/rules/architecture.md` ring map's "during transition" lists
  for entities and ports lose their last two and last one entries
  respectively; `domain/`, `ports/`, and `adapters/` now hold every module
  this slice touches as a settled location, not a transitional one.
- The dependency-rule violation `domain/aggregate.py` and `domain/decider.py`
  carried against `eventsource.commands` is closed, not merely
  undocumented -- both modules now import `DomainCommand` from inside their
  own ring.
- `docs/core-surface.md` finding 10's "resolve it -- populate or delete"
  recommendation for `config.py` is closed by deletion; nothing under
  `src/` or `tests/` referenced it, so nothing breaks.
- Zero deprecation-shim modules to maintain across the whole package, not
  just six fewer: the two ADR 0029 introduced are gone too. No
  `__getattr__`/`__dir__` plumbing, no `DeprecationWarning` noise under
  `-W error::DeprecationWarning` test configurations, and no temporary
  second source of truth for where a name lives, anywhere in the codebase.
- `pyproject.toml`'s "Application ring must not import adapters"
  `forbidden_modules` contract loses its `eventsource.locks` entry (owned
  and applied by a separate task in this slice; ADR 0029's comment beside
  that contract explaining why the entry "stays correct only while the shim
  exists" is now moot, since the shim doesn't exist).
- Eight fewer top-level modules for a new contributor to place on the ring
  map by guesswork; every module under `src/eventsource/` other than
  `events/`, `handlers/`, `subscriptions/`, `migration/`, and
  `bus/interface.py` now sits inside `domain/`, `application/`, `ports/`,
  or `adapters/`.

**Negative / accepted.**

- Any code outside this repository that already imported
  `eventsource.locks`, `eventsource.readmodels`, or any of the six relocated
  modules directly breaks immediately with `ModuleNotFoundError`, with no
  warning period. Accepted because no such code is known to exist -- the
  library has not shipped a release with these modules' current shape to
  any external consumer -- and the standing rule already accepted the
  identical risk for `eventsource.stores` and `eventsource.repositories`.
- `eventsource.commands.base` as a two-level import path (`commands` then
  `.base`) and `eventsource.commands` (the package `__init__`) both
  disappear together; there is no intermediate state where one works and
  the other doesn't, unlike a shim migration would have produced. Any
  in-repo caller must update in the same commit that relies on the new
  location, which `git grep` confirms this slice's other tasks do.
- ADR 0029's Status section now carries an "Amended by ADR 0030" pointer
  recording that its 0.8.0 shim-removal timeline did not hold -- a reader
  of ADR 0029 alone would believe the shims live until 0.8.0 unless they
  follow that pointer. This is the accepted cost of accelerating a schedule
  after publishing it, rather than a reason not to accelerate it.

## Alternatives Considered

**Leave `types.py`, `exceptions.py`, and `protocols.py` at the top level
indefinitely, since they are Tier 0 and cause no import-linter violation
today.** Considered and rejected on the same grounds ADR 0029 rejected it
for `locks/` and `readmodels/`: passing the dependency test today is not the
same as being correctly placed, and every module in the ring migration so
far had exactly this shape -- clean today, but the top-level exception list
in `.claude/rules/architecture.md` was never meant to be permanent
scaffolding. Completing it for the last three entries removes the last
"during transition" qualifier the entities and ports rings carried.

**Keep the `eventsource.locks` / `eventsource.readmodels` shims until
0.8.0 as ADR 0029 originally scheduled, and only apply the no-shim policy
going forward.** Considered and rejected: the rationale for skipping shims
entirely (no external consumers exist to protect) applies exactly as much
to code shipped last week as to code shipped today. Grandfathering the two
existing shims would have meant maintaining `__getattr__`/`__dir__`
machinery and `DeprecationWarning` noise for a promise made to a consumer
that, on inspection, never existed -- the same reasoning that justifies
skipping shims for the six modules in this slice justifies removing the two
that already shipped.

**Fold `commands/`'s relocation into a broader `domain/` reorganization
addressing the aggregate/decider dependency-rule violation more
generally.** Considered and set aside as scope creep, the same reasoning
ADR 0029 applied to `engine.py`: the violation has exactly one cause
(`DomainCommand` living outside `domain/`) and exactly one fix (moving it
inside), and bundling that with a wider `domain/` restructuring would make
a mechanical, easily-reviewed change harder to review for no benefit.
