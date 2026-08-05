---
paths:
  - "src/**/*.py"
  - "tests/**/*.py"
  - "docs/**/*.md"
---

# Recurring Defect Shapes

Derived from an audit of ~130 `fix:` and correction commits (rings campaign
through ADR 0046). These are the mistakes this project actually makes, in
descending order of cost. Check for them when writing, reviewing, or
diagnosing code — they are cheap to prevent and expensive to find.

**The through-line:** five of the six are the same root cause wearing
different clothes — *one fact stored in more than one place, with no
mechanism that fails when the copies disagree.* Two adapters, three
declaration sites, an ADR table versus the tree, an ADR number versus
another branch, a test's assertion versus the contract.

---

## 1. Silent divergence between implementations of one port

Two adapters implement the same port method with different semantics.
Nothing fails, because each adapter's own tests assert its own behavior.

Real instances:

- `5d3692a` — `get_lag_metrics` with no filter: the PostgreSQL branch built
  `event_type = ANY(:event_types)` over an empty array (matches zero rows in
  PostgreSQL); SQLite's `else: event_filter = ""` fallthrough matched
  everything. The dialects disagreed silently until they shared one function.
- `0c8d032` — DLQ cleanup cutoff: the memory adapter truncated to midnight,
  the SQL adapters used a rolling instant. Three backends, two semantics.
- `1cb21d1` — `InMemoryCheckpointRepository.update_checkpoint` rebuilt
  `CheckpointData` from scratch and dropped `position`; the SQL adapter
  preserved it via its UPSERT column list.
- `97e2af0` / `5f0afe1` — AUTOCOMMIT connections lost writes silently because
  the explicit-BEGIN path assumed one driver state.

**Rule:** a port method's semantics are pinned in
`src/eventsource/testing/conformance*/`, never in a per-adapter test. Adding a
method to a port is not done until every binding runs a shared case for it.
When you fix a divergence, the regression test goes in the conformance suite —
if it lives in `tests/unit/adapters/test_<backend>_*.py`, it cannot catch the
next backend. Deleting the per-backend duplicates is part of the fix
(`0c8d032` removed two).

**Rule, second half:** *behavior a conformance suite asserts is implemented
once.* A conformance suite pins that the adapters agree; it does not stop them
from each deriving the agreement separately, and N independent derivations of
one rule is N chances to drift. When the logic depends only on ports and domain
types — not on storage — it belongs in `adapters/_common/` (or `_sql`/`_bus`
when it is dialect- or transport-specific), not copied. `check_expected` was
four verbatim copies across three adapters and a testing double before it moved
there.

**Reviewing:** for any changed adapter method, open the sibling adapters and
compare. Empty collections, zero/`None` defaults, date and time truncation,
and "not set" versus "set to nothing" are where they diverge. A function that
appears verbatim in two adapters is the same finding one step earlier.

## 2. Redundant declaration sites with undocumented precedence

The same fact declared in N places; one silently wins; the losers look
authoritative and are not.

- `e40c026` — `aggregate_type` had three sources (repository constructor
  param, aggregate class attribute, event field default). The constructor
  param won, invisibly, and was not observable on a save/load round-trip.
  83 of 86 call sites were pure ceremony restating the class attribute; the
  other 3 existed only to test the override.
- `9164a65` / `dc80732` — hand-declared `event_type` on 246 sites, 26 of which
  had silently drifted from the derived name.
- `7481186` / `7c13aed` — event-type derivation duplicated across modules
  until unified onto `event_type_name()`.

**Rule:** a fact has exactly one declaration site. Before adding a parameter,
field, or class attribute, ask whether the value is already derivable from
something the caller must supply anyway — if it is, derive it. Do not add an
override "for flexibility": every one of the above was introduced that way,
and the override was used only by the test written to exercise it.

If a second site is genuinely required, the precedence order is documented at
the declaration and pinned by a test that sets both to conflicting values.

## 3. Inert code and always-zero metrics

Branches never taken and counters never incremented pass every test that does
not assert on them.

- `4609ba8` — `LiveRunner` read a `_position` attribute that **nothing in the
  tree ever set**. Live subscriptions never checkpointed at all; every
  restart replayed the entire live period from the catch-up watermark. The
  duplicate-suppression branch was permanently unreachable and
  `events_skipped_duplicate` / `buffer_events_skipped` were pinned at zero.
  This shipped and passed CI.
- `38e887b` — DLQ critical log and checkpoint span attribute were not truthful.
- `d1e9267`, `a181fb8`, `0a37222` — dead typevars, dead OTel imports, a patch
  target that no longer existed.

**This is the class conformance suites will not catch.** Guard it directly:

- When you add a counter or stat field, add a test that asserts it is
  **non-zero** under the condition it counts. "Asserted zero in the happy
  path" is not coverage.
- When you read an attribute set elsewhere in the tree (`getattr`, duck-typed
  reads, anything the type checker cannot follow), grep for the write site
  before you rely on it. If nothing writes it, you have found this bug.
- Prefer deleting an unreachable branch over preserving it. `4609ba8` removed
  the check and its metrics rather than fixing them, because once the store
  owns ordering the case is unreachable by construction.

## 4. Tests that encode the bug as the spec

A test written from observed output rather than the contract, which then
locks the defect in place and makes the real fix look like a regression.

- `5d3692a` — the test added in the immediately preceding commit asserted
  `latest_event_id is None` as the *expected* result. The fix had to rename
  and invert it.
- `0fbd2da` — an erroneous `register_event` decorator on a test helper.
- `81b9e5d` — a conformance smoke test whose method was never implemented.

**Rule:** write the assertion from the documented contract before running the
code. When adding a regression test, prove it red first — and per the memory
of an earlier wave, verify "fails before fix" with
`git checkout HEAD~1 -- <paths>`, not `git stash`.

## 5. Docs and ADRs rotting the moment a sweep names specifics

There is a whole sub-genre of commits fixing the previous sweep:
`64c66c5` then `d1e9267` (ADR 0045 claims, then its survivor table — two
passes), `042e372` (migration module map), `018fba2` (amend ADR 0024),
`b84b31e` (stale ADR row), `84e3a22` ("second stale
`FeedReadOptions(direction=...)` claim" — the first sweep missed one),
`801a4d7` (wrong subclass count), `04009f7` (wrong API name counts),
`98a21a1`, `dcad6b3`, `8805a8e`.

**Rule:** ADR bodies do not contain counts of things or tables of files.
"13 exceptions moved", "83 of 86 call sites", "the survivor table" — that
belongs in the commit message, which is immutable and correctly scoped to a
moment in time. An ADR states the decision, the forces, and the consequences;
those stay true. A number decays the next time anyone touches the tree.

When a sweep *does* have to touch prose, grep for the symbol across
`docs/`, `README.md`, docstrings, and `examples/` — not a curated list of
files. The repeated failure is a sweep that fixes the pages it thought of.

## 6. ADR number collisions

`ce48fff` (0031 → 0032), `0750dfe` (0019 collision), `4f71b85` (0021 claimed
by in-flight work). Three times, always the same cause: parallel branches each
take the next free number from `main`.

**Rule:** this is structural to parallel work, not carelessness. Draft the ADR
under a provisional name (branch name or date suffix), and allocate the number
at merge time after checking `docs/adrs/` on current `main`. Before merging any
branch that adds an ADR, re-check the number.

---

## Quick checklist

- [ ] Changed an adapter method? Compared the sibling adapters; case lives in the conformance suite.
- [ ] Added a parameter or attribute? It is not derivable from something already supplied.
- [ ] Added a counter or stat? A test asserts it non-zero.
- [ ] Read an attribute the type checker cannot follow? Grepped for the write site.
- [ ] Wrote a regression test? Proved red against `HEAD~1` first.
- [ ] Touched an ADR? No counts, no file tables; number re-checked against `main`.
