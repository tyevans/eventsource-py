# Impact Report: Hypothesis and Mutation Testing

*Generated 2026-08-10 against the tree at `f6fa06b`. Scope: everything referencing
Hypothesis, mutmut, or cosmic-ray, plus the history that introduced and extended
them. Reviewed adversarially and fact-checked before publication; corrections from
that pass are folded in, and the sourcing of every number is stated below.*

## How to read the numbers in this report

Two different kinds of figure appear here, and they carry different weight:

- **Measured** — counted or collected against the tree at `f6fa06b` during this
  report's preparation. Adoption footprint, line counts, test counts.
- **Prose-sourced** — transcribed from `docs/development/mutation-testing.md`,
  which recorded them when the runs happened. **Every mutation-testing baseline in
  this report is prose-sourced, was recorded before the rings campaign, and has not
  been re-measured.** They are the best record that exists; they are not current
  evidence. This distinction is load-bearing for the recommendations at the end.

## Summary

Both tools entered the project on **2026-07-28** — Hypothesis in `0fb8009`,
cosmic-ray in `f8cf992` — as M0 tasks 2c and 2d. They occupy genuinely different
niches:

| | Hypothesis | Mutation testing (mutmut + cosmic-ray) |
| --- | --- | --- |
| Characteristic find | **case-coverage gaps** — inputs no hand-written test tried | **oracle errors** — tests that pass regardless of what the code does |
| Also finds | ordinary logic bugs | ordinary logic bugs (see Part 2) |
| Runs | every test run, at the 100-example `default` profile | on demand only; deliberately **not** a CI gate |
| Footprint | 24 test modules import it; 21 carry `@given` (67 total); 1 stateful machine | 5 curated mutmut modules, 2 cosmic-ray configs (one of which no longer runs) |

The framing from `docs/development/mutation-testing.md` is the useful one: the
milestone produced one defect of each kind. Property tests caught a byte-level
encoding divergence hand-picked cases had missed; mutation testing caught an
isolation test for `engine.py` that passed against a version of the module with
**all** its transaction-control logic removed. Neither tool would have found the
other's defect. That complementarity is the strongest claim this report makes, and
it holds.

---

## Part 1 — Hypothesis

### Adoption footprint (measured)

- **24 test modules** import Hypothesis; **21** carry `@given`, for **67**
  properties total. Densest: `test_json_properties.py` (9),
  `test_tenant_context_properties.py` (6), `test_death_headers.py` (6).
- Coverage spans every ring — `domain/` (tenant context, tenant events, aggregate
  memento, stream IDs), `ports/` (position ordering), `adapters/` (JSON
  serialization, bus registry/retry/serialization/error isolation, RabbitMQ death
  headers and serialization, memory checkpoints/DLQ/outbox, SQLite conformance,
  dialect round-trips, event round-trip), `application/` (snapshotting, repository
  store port, bulk-copy resume, catch-up resumption), and one integration module
  (`test_postgresql_conformance.py`).
- **One stateful machine** — `StoreStateMachine`
  (`src/eventsource/testing/conformance_ports/stateful.py`), a
  `RuleBasedStateMachine` with 4 rules and 1 invariant, shipped *inside the
  library* so downstream backends can subclass it. `testing/sync_facade.py` also
  imports Hypothesis.
- **Three profiles** registered at `tests/conftest.py:107-114` — `default` (100
  examples), `ci` (500, no deadline), `db` (25, `function_scoped_fixture`
  suppressed).

For scale: 272 test files, **6,276 collected tests** in `tests/unit`, against a
`src/` tree of 193 files and **72,095 lines**. Property tests are roughly 9% of
test modules, concentrated on invariant-bearing code.

### What changed in practice — and the limit of that claim

**Property suites became the observed habit for port migrations.** During the
rings campaign, port work consistently arrived with a conformance suite and a
property suite in the same commit — `37a0cea` (checkpoint and DLQ ports),
`71691f5` (outbox port), `2e51c95` (snapshot policy, schema validation, memento
round-trip).

**But this is a pattern, not a norm, and the repo's own rules say so.**
`.claude/rules/definition-of-done.md` — the file that actually decides what "done"
means here — mentions Hypothesis exactly once, and to *demote* it: `StoreStateMachine`
"is available for stores and is **not a substitute** for the per-port suites."
Property tests appear in none of its New Feature, Bug Fix, or New Backend
checklists. `docs/development/testing.md` (792 lines, the project's testing guide)
contains **zero** property-testing content, as does `docs/tutorials/08-testing.md`.
Within `docs/`, Hypothesis is discussed in exactly two places: this report and one
paragraph of `mutation-testing.md`.

So the honest statement is: a consistent practice during one two-week campaign,
never written into the project's definition of done or its teaching layer. If
property suites *are* meant to be required for new ports, that belongs in
`definition-of-done.md`'s New Backend section and in `testing.md`. Right now the
practice depends entirely on whoever is doing the work remembering it.

### Defects found

| Commit | What Hypothesis contributed |
| --- | --- |
| `857f4bc` (with `ea2abac`, 13 minutes earlier) | The property suite for multitenancy isolation pinned down that `contextvars.Token.reset()` silently resurrects a stale tenant on out-of-LIFO-order reset. `ea2abac`'s own message classifies this as **"a footgun, not a bug"** — the stdlib behaving as documented. The team then *chose* to harden against it: `set_current_tenant()` now returns an opaque token, and `reset_tenant_context()` enforces strict LIFO via a `ContextVar`-backed stack, raising `TenantContextResetError`. This is properties driving a **design decision**, which is a better story than bug-catching, but it is a different story. |
| `da2ff06` | A genuine bug. `SyncStoreFacade.close()` released only its private event loop, leaking the wrapped store. Under the stateful machine with `SQLiteEventStore`, that meant one non-daemon aiosqlite connection thread **per example** — enough to wedge interpreter shutdown and degrade the rest of the suite. Only high-example-count generation surfaces this. |
| `0fb8009` / `fdd2a7b` / `8f75f92` | JSON encoder: out-of-range integer guard, deep-nesting round-trip, and out-of-range ints nested *inside* dicts and lists rather than only at the top level. |

`ab8d6ad` ("replace hardcoded `__all__` export set with property checks") is
sometimes cited alongside these. It is a test refactor — it names no defect — and
is excluded here on that basis.

### The cost side, and a live gap

**Badly-scoped strategies cost time and produce false alarms.** `0665c7c` fixed a
position-ordering property that was generating inputs the contract never promised
to order. The resolution was healthy — three lines of docstring in
`src/eventsource/ports/positions.py` clarifying that key element types are uniform
within a store — but it is a reminder that a property is a specification, and a
wrong one sends you looking for a bug that isn't there.

**The `ci` profile has never run.** `tests/conftest.py:108` registers `ci` at 500
examples; line 115's `load_profile("default")` is the only load call, and there is
no `HYPOTHESIS_PROFILE` handling anywhere in `tests/`, `.github/`, or the
`Makefile`. CI therefore runs the 100-example default — the 5× wider search that
was configured for it has never executed. The `db` profile is likewise registered
and unused (deliberately: its comment defers it to M2). This is a third instance of
the *declared-but-never-dispatched* shape this project already tracks.

---

## Part 2 — Mutation testing

### Adoption footprint (measured)

Two tools, by explicit decision — **ADR 0008, "mutmut Plus cosmic-ray, Not One
Tool"** (148 lines, Accepted).

- **mutmut 3.x** is the default. `scripts/_mutmut_configure.py:23-52` defines the
  authoritative 5-module curated set (`engine`, `dialect`, `json`, `checkpoint`,
  `dlq`); `scripts/mutation.sh` rewrites `[tool.mutmut]` per module — mutmut 3.x
  reads config once at process start with no per-invocation override — and restores
  it via a `trap`. All 16 paths in `pyproject.toml`'s `only_mutate` exist.
- **cosmic-ray** exists because mutmut 3.x **structurally cannot mutate decorated
  function bodies** — a hardcoded libcst-trampoline rule with no config flag. Its
  `RemoveDecorator` operator covers that gap.
- `make mutation` / `make mutation-cosmic MODULE=<name>` drive both. `make check`
  is `lint types arch sec test` — mutation testing is excluded, with an explicit
  "Deliberately NOT part of `make check`" comment at `Makefile:125`.

### Recorded baselines (prose-sourced, pre-rings, not re-measured)

| Module | Tool | Baseline | After triage |
| --- | --- | --- | --- |
| `adapters/_sql/engine.py` | mutmut | 46 mutants, 30 killed, 16 survived | 3 real gaps fixed; remainder equivalent/out-of-scope |
| `adapters/_sql/engine.py` | cosmic-ray | 52 mutants, 45 killed, 7 survived | 47 killed, 5 survived — all verified equivalent |
| `adapters/_sql/dialect.py` | mutmut | 27 mutants, 24 killed, 3 uncovered | **27/27 killed, 0 survivors**, ~2.3s |
| `_bus/registry.py` + `retry.py` + `base.py` | mutmut | 248 mutants, 168 killed, 78 survived | 192 killed; 56 documented survivors |

`retry.py` had zero survivors from the start — the symmetric-jitter regression test
written alongside `RetryPolicy` already covered it. `checkpoint` and `dlq` are in
the curated set but have no recorded baseline table.

### The defects it found

**Oracle errors — the distinctive class, invisible to any other tool:**

- **Tautological assertions.** Two tests asserted
  `busy_timeout == SQLITE_PRAGMAS["busy_timeout"]` — comparing a read-back value
  against the *same module constant the code under test reads*. Cannot fail,
  whatever the literal says. Found by cosmic-ray's `NumberReplacer`; mutmut's
  operators never generated that mutant. This single find is the load-bearing
  justification for running two tools.
- **A cache that worked by coincidence.** Three surviving mutants in
  `registry.handlers_for`'s combined-tuple cache. The existing stability test
  registered a specific handler with an *empty* wildcard tuple — and CPython's
  `tuple.__add__` returns the left operand unchanged on empty-tuple concatenation,
  so "cached" and "recomputed" were the same object by accident.

**Ordinary logic bugs — the tool found these too, and they are real production
defects in shipped adapters:**

- `_drain_background` waited only for the *first* task, not all pending ones.
- `get_subscriber_count` ignored its `event_type` argument entirely.
- `create_async_engine` dropped `**kwargs` on the non-SQLite passthrough.
- `_apply_pragmas`' memory-skip logic was wrong in both directions — `continue` →
  `break` killed every pragma after `journal_mode`; the inverted condition skipped
  everything *except* `journal_mode`.
- `dialect_of()` had **zero test coverage anywhere in the repo**, integration
  included. This one a coverage report would also have shown; mutation testing is
  simply what happened to be pointed at it.

**Mixed attribution, recorded for honesty:** `e156ffa` closed real gaps in
`SQLCheckpointRepository.get_lag_metrics` — a stale-checkpoint scenario, a
sub-second-lag boundary, two vacuous `hasattr`-only constructor tests. Its own
commit message credits gaps found *"by hand-applying mutations and by mutmut"*.
That is a win for the mutation-testing **practice**; it is not a clean win for the
tool, and the two should not be conflated.

### Cost

The report would be dishonest without this section, and no prior document collects
it:

- **A recurring survivor tax.** 56 of the bus modules' 78 survivors (72%) are
  permanently-accepted `logger.info`/`logger.warning` message-text mutants. They
  are correctly classified as not worth chasing — but they regenerate on **every
  future run** of those modules, so every re-run re-pays the cost of recognizing
  and skipping them.
- **Production source shaped by test tooling.** `661478c` restructured `engine.py`
  — extracting `_apply_pragmas` and `_begin_unless_autocommit` as plain
  module-level functions — purely so mutmut could reach the code. That is a real
  cost, and the project itself later drew the line:
  `docs/development/mutation-testing.md` now explicitly says *do not* restructure
  handlers into thin-wrapper pairs just to make them mutmut-reachable.
- **Triage time was never recorded.** Triaging 78 survivors across three modules
  was substantial work; no figure for it exists anywhere in the repo, so the
  benefit ledger above cannot be weighed against a cost ledger. Recording it on the
  next run would make future scope decisions defensible.

### The self-check against a known-vacuous test

Before trusting any result, the configuration was validated against a defect the
project already knew about: Task 1's original single-connection isolation test for
`engine.py`, which passed against an engine with no transaction control at all.

- Real suite + `RemoveDecorator` on the `begin` listener → **fails**.
- Task 1's vacuous suite + the same mutant → **all 4 tests pass**.

The harness demonstrably distinguishes a rigorous suite from a weak one, using a
mutation matching the real bug's shape rather than a proxy. (This too is recorded
in prose, at `mutation-testing.md:405-410`, not re-run for this report.)

---

## Part 3 — Discipline, guards, and known gaps

**No CI gate, on purpose**, with three firm documented reasons: equivalent mutants
make 100% structurally unreachable, so any threshold is arbitrary; the runtime is
neither fast nor deterministic enough; and a slow, occasionally-flaky required gate
gets an exception carved into CI within a month. Revisit only if survivors stay at
zero across several milestones — and then gate on "no new untriaged survivors,"
never a raw percentage.

**Triage, not score, is the deliverable.** Every survivor is filed *real gap*,
*equivalent*, or *out of scope*, with a one-line reason. "Do not chase 100%" is
stated explicitly, with the reason: contorting a test to kill an equivalent mutant
produces a test that passes for the wrong reason — the exact failure mode the tool
exists to catch.

**A foreseen failure mode, guarded before it ever fired.** If `--cov` reached a
mutation test command, the coverage floor could fail on a scoped subset, and
neither tool distinguishes "coverage gate failed" from "mutant killed" — every
mutant would report as killed, producing a flawless, entirely false score. This is
documented as the most dangerous misconfiguration available. It was **pre-empted,
not encountered**: every mutation command passes `--no-cov` and `-p no:randomly`
(`9a7b1f6`, documented in `e382d47`), and coverage measurement was subsequently
made opt-in rather than living in `addopts` (`pyproject.toml:106-113`) so scoped
runs cannot trip it at all. The floor has since ratcheted to `fail_under = 92`.

**Known gaps, in priority order:**

1. **`cosmic-ray/checkpoint.toml` cannot run.** Its `module-path` is
   `src/eventsource/repositories/checkpoint.py`, deleted in the rings campaign, and
   all three paths in its `test-command` are likewise gone. `tests/unit/test_mutmut_configure.py`
   guards `scripts/_mutmut_configure.py`'s `MODULES` table and `mutation.sh`'s
   `VALID` array — it never inspects `cosmic-ray/*.toml`, which is precisely why
   this rotted silently. A live instance of this repo's defect shape #1: one fact
   in two places, with nothing failing when the copies disagree.
2. **`adapters/serialization/json.py` still has no baseline.** Deferred mid-rewrite
   in the original pass; the deferral notice is still there, and only a ring-move
   commit has touched the file since.
3. **Documentation paths have rotted around the tooling.** ADR 0008's opening names
   three pre-rings module paths, two of them dead; the "151 mutants / ~110s"
   scope-justification figure is measured against `repositories/_dialect.py`, which
   no longer exists; `mutation-testing.md`'s "~18k source lines" is off by roughly
   4× against the current 72k.
4. **ADR 0008's central premise no longer matches the source tree.** Its
   justification for adopting cosmic-ray is 56 `@handles`-decorated handlers in the
   library. Today `src/` contains **zero** real `@handles` applications — every
   occurrence there is a docstring example or a log-format string; the 182 real
   applications live in `tests/` and `examples/`. The decorator is still the
   library's central routing abstraction and mutmut is still blind to decorated
   code (`src/` has 5 `@asynccontextmanager`, 4 `@contextmanager`, 4
   `@contextlib.contextmanager` definitions among others), but the specific target
   the ADR was written to reach is not where it said it was.
5. **Neither tool has a statement-deletion operator** — the exact shape of the
   original `engine.py` bug. A custom cosmic-ray operator was evaluated and left as
   a documented open option. Manual break/restore discipline remains necessary
   there.

---

## Assessment

**Hypothesis.** Adoption is real and reasonably broad, and its two best finds — the
tenant-token LIFO footgun and the aiosqlite thread leak — are lifecycle and
concurrency issues that only high-example-count generation surfaces. But it is not
institutionalized: it is absent from the definition of done and from 792 lines of
testing guide, and the wider `ci` profile configured for CI has never actually
loaded. The gap between how much it is used and how little it is written down is
the main risk to it surviving a change of contributor.

**Mutation testing.** It found two classes of defect, and the report should not
flatten them: ordinary logic bugs in shipped adapters (`_drain_background`,
`get_subscriber_count`, `create_async_engine`, `_apply_pragmas`), *and* a class
nothing else catches — a cache that worked by coincidence, assertions that could
never fail. The second class is what justifies the tool's cost, because coverage
percentages and property tests are both blind to it, and the project has a
reproducible self-check proving the harness detects it. Against that: a 72%
recurring-noise rate on the largest run, one instance of production source
restructured to satisfy a tool, and no recorded triage cost.

**Recommendations, in order:**

1. **Repair or delete `cosmic-ray/checkpoint.toml`,** and extend
   `tests/unit/test_mutmut_configure.py` to validate the cosmic-ray configs' paths
   the way it already validates mutmut's. Until then, "2 cosmic-ray configs"
   overstates the tooling by one.
2. **Decide what cosmic-ray is actually for now that ADR 0008's premise has
   moved.** Either point it at decorated code that genuinely exists in `src/`, or
   amend the ADR to say the `@handles` justification no longer applies. Do this
   before re-running anything, since it determines what is worth running.
3. **Then re-run the curated set**, including a first baseline for `json.py`, and
   record triage time. The existing baselines all predate the rings campaign and
   the selectors have already gone stale once (BACKLOG entry, closed 2026-08-04) —
   verify the config before trusting any new numbers.
4. **Write the Hypothesis practice down** in `definition-of-done.md` and
   `testing.md`, or accept that it will decay. Separately, either wire the `ci`
   profile up or delete it.
