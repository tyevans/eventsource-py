# 8. Mutation Testing Tool Selection: mutmut Plus cosmic-ray, Not One Tool

This project runs mutation testing against a small, curated set of modules
(named in `scripts/_mutmut_configure.py`'s `MODULES` table and `cosmic-ray/*.toml`,
which are the authoritative lists — this record deliberately does not restate them,
having already gone stale once when the rings campaign moved every path it used to
name) using **two** tools rather than one:
[mutmut](https://mutmut.readthedocs.io/) 3.x as the default, and
[cosmic-ray](https://cosmic-ray.readthedocs.io/) scoped specifically to modules
containing decorator-registered callbacks. This record explains why a single tool
was not sufficient, why the fix is not a version pin, and what was ruled out along
the way.

## Status

Accepted; amended 2026-08-10 (see "Amendment" at the end of this record — the
`@handles` premise this decision was argued from no longer describes the source
tree, though the decision itself stands on a different footing).

`pyproject.toml`'s `[dependency-groups] dev` lists both `mutmut>=3.0` and
`cosmic-ray>=8.4`. `scripts/mutation.sh` drives mutmut; `scripts/mutation-cosmic-ray.sh`
plus `cosmic-ray/*.toml` per-module configs drive cosmic-ray. The full rationale,
baselines, and per-mutant triage for both tools live in
`docs/development/mutation-testing.md`; the comparative research that produced this
decision is `.superpowers/sdd/2026-07-28-m0-sqlalchemy-unification/mutation-framework-spike.md`
(not published docs — an internal task artifact, referenced here for provenance).

## Context

### The problem mutmut alone could not solve

mutmut 3.x's mutation engine (`mutmut/mutation/file_mutation.py`) unconditionally
excludes decorated function bodies from mutation — not behind a config flag, a
hardcoded rule tied to how its trampoline mechanism copies functions for testing.
`staticmethod` and `classmethod` are the only exceptions. This was discovered while
mutation-testing `engine.py`'s `@event.listens_for`-decorated transaction-control
listeners, and it generalizes: the same exclusion blinds mutmut to every
`@handles(EventType)`-decorated handler on `DeclarativeAggregate` and
`DeclarativeProjection` — this library's central event-routing abstraction, and
exactly the layer the delivery-guarantee milestone is expected to build
exactly-once semantics on top of.

Restructuring decorated functions into thin wrappers that delegate to plain,
mutmut-reachable functions (done for `engine.py`'s `_apply_pragmas` and
`_begin_unless_autocommit`) works, but does not scale to 56 existing `@handles`
handlers without a mechanical rewrite of working code purely to make it visible to
one testing tool.

### Why this couldn't be answered by inference

The natural first question — "is this new in 3.x, or has mutmut always worked this
way?" — could not be answered by reading 3.x's changelog or guessing from behavior.
It required reading mutmut 2.x's source directly. That check confirmed the exclusion
is new machinery introduced with 3.x's libcst-based trampoline rewrite: 2.x's
parso-based mutator has a `decorator_mutation` function that *deletes the decorator
node itself* and no blanket exclusion of decorated function bodies at all — verified
by running 2.x against `engine.py` and observing a mutant change a call argument
*inside* the decorated `_emit_begin` body, something 3.x cannot generate under any
configuration.

## Decision

### Add cosmic-ray, scoped to decorated modules only

cosmic-ray ships a `RemoveDecorator` operator that strips a decorator entirely,
leaving the function defined but never registered as a callback — exactly the
defect shape this project needed to test for (a handler silently not wired up),
and a closer match to the real risk than mutating the handler's internals would be.
Verified decisively: applied to `engine.py`'s `begin` listener, the mutant is killed
by the current two-connection isolation test and *survives* against Task 1's
original, known-vacuous single-connection test — the exact self-check mutmut could
not express under any configuration (see
`docs/development/mutation-testing.md#self-check-does-the-configuration-actually-catch-a-known-vacuous-test`).

Scope is deliberately narrow: `cosmic-ray/<module>.toml` per module, one file at a
time, never a whole package or directory. cosmic-ray spawns a fresh pytest
subprocess per mutant with no in-process caching or worker reuse, so its runtime
scales far worse than mutmut's — 151 mutants against the ~130-line
`repositories/_dialect.py` took roughly 110 seconds in the evaluation spike, against
roughly 2 seconds for mutmut's 27 mutants on the same module. A whole-tree cosmic-ray
run is not something anyone would wait for; a whole-module run stays in the tens of
seconds and is fine.

### mutmut stays the default for everything else

mutmut remains faster, already integrated (`scripts/mutation.sh`), and its default
operator set covers ground cosmic-ray's does not — string-literal mutation
(`"journal_mode"` → `"XXjournal_modeXX"`) in particular, which cosmic-ray's operator
set has no equivalent for at all. The two tools are complementary rather than
redundant: re-running both against the finalized `engine.py` found that mutmut's
operator set never generated an integer-literal mutation for
`SQLITE_PRAGMAS["busy_timeout"]`'s `5000` default, while cosmic-ray's `NumberReplacer`
did — and that mutant surfaced a real, previously-undetected test gap (two assertions
comparing the pragma's read-back value against the same module constant the code
under test also reads, which can never fail regardless of the literal's actual
value). A mutmut-only practice would have missed it; a cosmic-ray-only practice would
have missed the string-mutation class of gap mutmut catches routinely. Running one
tool "for everything" was never actually on the table once both had been evaluated —
only which single tool to standardize on, and neither alone covers what the two
together do.

## Alternatives Considered

**Pin mutmut to 2.x.** Cheapest fix, and it does work — 2.x mutates decorator nodes
and decorated function bodies with no exclusion. Rejected because 2.x has had no
release since 2024-08-15, is superseded by the 3.x line this project already
adopted, and pinning the *entire* curated set backward to recover reachability for
a subset of modules trades away whatever 3.x actually improved (broader operator
variety on plain code, ongoing maintenance) for a narrow, arguably better-solved
problem.

**MutPy.** Best operator set on paper of any candidate evaluated — it has both a
decorator-deletion operator (`DDL`) and a genuine statement-deletion operator
(`SDL`), which would have answered the decisive self-check even more directly than
cosmic-ray's `RemoveDecorator` does. Rejected outright: MutPy's last release was
2019, and it crashes before generating a single mutant on Python 3.12+
(`AttributeError: module 'importlib' has no attribute 'find_loader'` — removed from
the stdlib in 3.12). This project requires Python `>=3.11` and tests 3.11/3.12 in
CI; MutPy does not run on either.

**Do nothing; keep the thin-wrapper restructuring as the only mitigation.** Still a
legitimate pattern where it falls out naturally (as it did for `engine.py`), and
still documented as such. Rejected as the *sole* answer specifically because of the
scale mismatch: 56 existing `@handles` handlers is a lot of mechanical extraction to
require before any of them can be mutation-tested at all, and the restructuring
answers a narrower question ("is the handler body's logic tested") than
`RemoveDecorator` does ("would we notice if the handler were never registered").

**A custom cosmic-ray statement-deletion operator**, since cosmic-ray's operator
providers are plugin-extensible (`cosmic_ray.operator_providers` entry points).
Genuinely the closest thing to a complete answer to the remaining gap — neither
mutmut nor cosmic-ray's built-in operators can express "delete this statement,"
which is precisely Task 1's original defect shape — but building and maintaining a
custom operator is real, ongoing engineering work, not something to take on as a
side effect of adding a second tool. Left as a documented open option rather than
implemented.

## Consequences

- Two mutation-testing tools now need to be understood, configured, and kept
  working, not one. `docs/development/mutation-testing.md`'s "Two tools, two jobs"
  section is the canonical explanation of which to reach for and why; anyone
  extending the curated set to a new module needs to read it before choosing.
- Adding a new decorated module to the set means writing a new
  `cosmic-ray/<module>.toml` (template: `cosmic-ray/engine.toml`), not just adding
  a line to an existing config, and running it separately from the mutmut suite.
- Neither tool can express statement/call deletion. This is a real, acknowledged
  gap that persists after this decision — see the custom-operator alternative
  above — and manual break/restore discipline remains necessary for defects that
  live entirely inside one statement neither tool's operators can remove.
- cosmic-ray's per-mutant subprocess cost means it will never be routinely run
  across the whole curated set the way mutmut's combined run is; this is accepted
  as the cost of reaching decorated code at all, not treated as a problem to
  engineer away by widening the scope discipline established for both tools.

## Amendment (2026-08-10)

**The `@handles` argument above no longer describes the source tree.** This record
argues for cosmic-ray substantially on the grounds that mutmut is blind to the
library's `@handles`-decorated aggregate and projection handlers. Those handler
*applications* are not in `src/` — the decorator is defined and exported there, and
every application of it lives in user code, `tests/`, and `examples/`. Whether that
was already true when this was written or became true during the rings campaign is
not recorded; either way, an argument resting on handlers the library itself
declares cannot be checked against the tree today, and should not be repeated
without checking.

**The decision stands, on the narrower ground that was always the stronger one.**
mutmut's exclusion of decorated functions is unconditional and applies to every
decorated definition, not just `@handles` — `@asynccontextmanager`,
`@contextmanager`, and `@event.listens_for` definitions in `src/` are all invisible
to it. The `RemoveDecorator` self-check against a known-vacuous test (recorded in
`docs/development/mutation-testing.md`) is unaffected by any of this, and remains
the empirical basis for keeping both tools.

**Consequence for the scoping discipline.** "Add a decorated module by writing a
`cosmic-ray/<module>.toml`" turned out to be the half of this decision that rots:
`cosmic-ray/checkpoint.toml` was written for an `@asynccontextmanager`-decorated
method, that method's contract later moved into a shared helper, and the config kept
pointing at a deleted path with nothing failing. Both configs' paths are now asserted
by `tests/unit/test_mutmut_configure.py`, which previously guarded only the mutmut
table. A per-module config in a second tool's format is another copy of "where does
this module live" — recurring defect shape #1 — and needs a check that fails when the
copies disagree, not just a documented convention.
