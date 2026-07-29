# Mutation Testing

Property-based tests (see the Hypothesis work in `tests/unit/*/test_*_properties.py`)
find *case-coverage gaps* — thin inputs a hand-written test never tried. Mutation
testing finds a different, more dangerous class of defect: *oracle errors* — tests
that pass regardless of whether the code they cover is correct. This milestone
produced one of each. The property tests caught a byte-level encoding divergence
that hand-picked cases had missed. Mutation testing exists because of the other one:
an isolation test for the SQLite engine passed against a version of `engine.py` with
none of its transaction-control logic applied at all, because the test's oracle was
wrong, not its inputs. No amount of additional input generation would have caught
that — only deliberately breaking the implementation and checking the test notices
does.

## What's in the curated set, and why

Mutation testing is `O(mutants × suite runtime)`. This repo is roughly 18k source
lines with ~6,000 tests; running it against everything would take hours and would
simply go unused. Instead it targets a small, curated set of modules chosen for being
small, pure(ish), and high-consequence — code where a silent behavior change would be
expensive and hard to notice by other means:

| Module | Test subset |
| --- | --- |
| `src/eventsource/engine.py` | `tests/unit/test_engine.py` |
| `src/eventsource/repositories/_dialect.py` | `tests/unit/repositories/test_dialect.py` |
| `src/eventsource/serialization/json.py` | `tests/unit/serialization/` |

As Tasks 3-8 land and the merged repositories stabilize, they become candidates for
this set — but only added one module at a time, each with its own pinned test subset,
and only if a run stays in the tens of seconds. Never widen `paths_to_mutate` /
`only_mutate` to a whole directory; that reintroduces the whole-repo runtime problem
this design exists to avoid.

**Two tools, not one.** mutmut 3.x cannot generate a mutant inside — or removing —
a decorated function, which blinds it to every `@event.listens_for` listener and,
going forward, every `@handles`-decorated aggregate/projection handler. cosmic-ray
covers exactly that gap via its `RemoveDecorator` operator. Which tool to reach for,
and why two rather than a version pin or a full switch, is its own section below:
["Two tools, two jobs"](#two-tools-two-jobs-mutmut-and-cosmic-ray).

## How to run it

```bash
scripts/mutation.sh              # mutmut: all three modules, sequentially
scripts/mutation.sh engine       # mutmut: just src/eventsource/engine.py
scripts/mutation.sh dialect      # mutmut: just repositories/_dialect.py
scripts/mutation.sh json         # mutmut: just serialization/json.py

scripts/mutation-cosmic-ray.sh engine   # cosmic-ray: decorated-function slice
```

Each `mutation.sh` invocation prints a per-mutant summary (killed/survived/etc.,
mutmut's emoji legend) and, at the end, the list of surviving mutant names. `mutmut
show <name>` displays the diff for any specific mutant if you need to inspect one
directly. `mutation-cosmic-ray.sh` prints one line per mutant (`killed`/`survived`)
followed by a summary; `uv run cosmic-ray dump <session>.sqlite` gives the raw JSON
if you need a specific mutant's location, and `uv run cosmic-ray apply <module>
<operator> <occurrence>` applies one mutant to disk for manual inspection (remember
to `git checkout` it back afterward).

### Why there's a wrapper script at all: mutmut 3.x's config is process-wide

mutmut 3.x reads its configuration exactly once, from a `[tool.mutmut]` table in
`pyproject.toml`, at process start — there is no CLI flag to point it at a different
config file or to override `only_mutate` / the test selection for a single invocation.
Two options from mutmut 2.x that older docs and blog posts reference,
**`paths_to_mutate`** and **`tests_dir`**, are deprecated in 3.x in favor of
**`source_paths`** and **`pytest_add_cli_args_test_selection`** respectively — the old
names still work but emit a `DeprecationWarning`, and using them is exactly the kind
of thing that costs the next person an hour rediscovering it. This project uses the
3.x names.

Because per-module test scoping (the whole point of the curated-set design) needs a
different `only_mutate` / `pytest_add_cli_args_test_selection` pair for each of the
three modules, and mutmut has no per-invocation override, `scripts/mutation.sh`
rewrites the `[tool.mutmut]` section of `pyproject.toml` between runs via
`scripts/_mutmut_configure.py`, and restores the original file via a `trap` on exit —
including on failure or Ctrl-C. The checked-in `[tool.mutmut]` block itself covers all
three modules and their combined test subset, so a plain `mutmut run` outside the
script (or a stray interrupted run) still does something sane rather than silently
scoping to whatever module ran last.

## How to read a survivor

A **killed** mutant is the expected outcome: some test's assertion failed, or the
mutated code raised where the test expected success (or vice versa). A **survived**
mutant means the entire pinned test subset passed unchanged against a version of the
module with one thing deliberately altered — the code changed observable behavior (or
at least mutmut's mutation operators believe it did) and nothing noticed.

Every survivor gets triaged into exactly one of three buckets. This triage, not a
percentage score, is the actual deliverable of a mutation run:

- **Real gap** — the mutant changes behavior and no test would ever catch it. Write
  the test that kills it.
- **Equivalent mutant** — the mutated code is semantically identical to the original
  for every input a caller could actually construct (e.g. SQLite treats `BEGIN` and
  `begin` as the same keyword, so a mutant that lowercases the literal is
  unobservable). Record it as equivalent with a one-line reason. Do **not** contort a
  test to kill it — that produces a test that passes for the wrong reason, which is
  precisely the failure mode this tool exists to catch.
- **Out of scope** — a docstring, a log message, or a defensive branch that cannot be
  reached given the module's actual callers. Record and move on.

Do not chase 100%. Equivalent mutants make it structurally unreachable, and treating
the score itself as the goal is how you end up gaming it.

## Baseline and triage: `engine.py`

`engine.py` required a source change before it could be meaningfully mutated at all —
see [the decorator blind spot](#the-decorator-blind-spot-mutmut-cannot-mutate-handles-and-event-listener-bodies)
below for why, and the restructuring applied to work around it (extracting
`_apply_pragmas` and `_begin_unless_autocommit` as plain module-level functions that
the `@event.listens_for` listeners now just delegate to). All figures below are
against the restructured module.

**Baseline:** 46 mutants, 30 killed, 16 survived, ~5-35s wall time (variance is mostly
Python import/startup overhead per subprocess, not the mutation count).

**Triage:**

| Mutant | Classification | Reason |
| --- | --- | --- |
| `_apply_pragmas` — `pragma == "journal_mode"` → `pragma == "XXjournal_modeXX"` / `"JOURNAL_MODE"` | Equivalent | Makes the memory-database WAL-skip never trigger, so `PRAGMA journal_mode = WAL` is attempted against `:memory:`. Verified empirically (see below) that SQLite silently accepts and ignores this rather than erroring, and the pragma still reports its unchanged default afterward — so no assertion on the read-back value can distinguish "skipped" from "attempted and ignored." |
| `_apply_pragmas` — skip condition `continue` → `break` | **Real gap → fixed** | Killed a whole class of pragmas after `journal_mode` in iteration order (`busy_timeout`) for `:memory:` connections. Closed by `test_memory_sqlite_applies_busy_timeout_after_journal_mode_skip`, which monkeypatches `busy_timeout` to a value (4321) that could never coincide with SQLite's own compiled default, specifically so the assertion discriminates "applied" from "never touched." (The naive version of this test — asserting the pragma equals `SQLITE_PRAGMAS["busy_timeout"]`, i.e. 5000 — turned out **not** to discriminate, because SQLite's own default busy_timeout in this environment is *also* 5000. Recorded here because that's an easy mistake to repeat.) |
| `_apply_pragmas` — `pragma == "journal_mode"` → `is_memory and pragma != "journal_mode"` | **Real gap → fixed** | Inverted the skip condition: for `:memory:`, this skips every *other* pragma (`foreign_keys`, `busy_timeout`) and only attempts `journal_mode`. Closed by `test_memory_sqlite_applies_pragmas_except_journal_mode`. |
| `_begin_unless_autocommit` — `"BEGIN"` → `"begin"` | Equivalent | SQL keywords are case-insensitive in SQLite; `conn.exec_driver_sql("begin")` behaves identically. |
| `_begin_unless_autocommit` — `"BEGIN"` → `"XXBEGINXX"` / arg → `None` | **Killed** (not a survivor) | Both are invalid as SQL, so `exec_driver_sql` raises immediately regardless of which test suite is active — a crash is an unconditional kill. This is *not* evidence the test suite is doing its job on transaction semantics, just that mutmut's mutation operators can't express "silently drop the statement" — a raised exception from garbage SQL is a much weaker signal than a semantic transaction-isolation failure. cosmic-ray's `RemoveDecorator` mutant on the `begin` listener answers the real question directly; see the [self-check](#self-check-does-the-configuration-actually-catch-a-known-vacuous-test) below. |
| `create_async_engine` — `is_memory = ":memory:" in url` → `is_memory = None` / `is_memory = ":MEMORY:" in url` / `is_memory = "XX:memory:XX" in url` | Equivalent | `is_memory` also gates `kwargs.setdefault("poolclass", StaticPool)`, but SQLAlchemy's own `aiosqlite` dialect already defaults to `StaticPool` for a bare `:memory:` URL regardless — verified directly (`type(engine.pool).__name__ == "StaticPool"` with no `create_async_engine` wrapper involved at all). The `is_memory=None` variant also reaches `_apply_pragmas`, which is the same "WAL-on-`:memory:`-is-silently-ignored" equivalence as the row above. |
| `create_async_engine` — `kwargs.setdefault("poolclass", StaticPool)` → `poolclass=None` / arg dropped | Equivalent | Same reasoning: SQLAlchemy's own default already applies. |
| `create_async_engine` — non-SQLite branch drops `**kwargs` | **Real gap → fixed** | `return _sa_create_async_engine(url, **kwargs)` mutated to drop the forwarded kwargs entirely for the Postgres/other-dialect passthrough. Closed by `test_postgres_url_forwards_kwargs`, which passes `echo=True` and asserts `engine.echo is True`. |
| `create_async_engine` — `logger.debug(...)` argument/message mutations (7 variants) | Out of scope | Log message content and formatting only; no test should assert on it. |

Net result after fixes: two real gaps closed (`_apply_pragmas`'s memory-skip logic in
both directions, `create_async_engine`'s kwargs passthrough), one weak assertion
replaced with a discriminating one (`busy_timeout`), and every remaining survivor is
either equivalent or out of scope. `tests/unit/test_engine.py` grew from 8 to 11
tests as a direct result.

### `engine.py` under cosmic-ray

Run with `scripts/mutation-cosmic-ray.sh engine` (config: `cosmic-ray/engine.toml`).
**Baseline:** 52 mutants, 45 killed, 7 survived, ~35-60s wall time (subprocess per
mutant, no in-process caching — see ["Two tools, two jobs"](#two-tools-two-jobs-mutmut-and-cosmic-ray)
for why that cost is accepted here and not routinely elsewhere).

Both `core/RemoveDecorator` mutants (on the `connect` and `begin` listeners in
`_configure_sqlite`) are killed in every run against the real test suite — mutmut
cannot generate these at all; see the self-check below for what that proves. The 7
initial survivors were all elsewhere:

| Mutant | Classification | Reason |
| --- | --- | --- |
| `core/NumberReplacer` on `SQLITE_PRAGMAS["busy_timeout"]`'s literal `5000` → `5001`/`4999` | **Real gap → fixed** | Neither `test_sqlite_engine_applies_pragmas` nor `test_memory_sqlite_applies_pragmas_except_journal_mode` would have caught this: both asserted `busy_timeout == SQLITE_PRAGMAS["busy_timeout"]`, which reads the same (possibly-mutated) module constant the code under test also reads — tautological, cannot fail regardless of the literal's value. mutmut's own operator set never generated an equivalent mutation here (it doesn't mutate bare integer literals inside a dict literal the way cosmic-ray's `NumberReplacer` does), which is exactly why running only one tool would have missed this. Fixed by hardcoding the expected literal (`assert busy_timeout == 5000`) in both tests. |
| `core/ReplaceBinaryOperator_Mul_Div` on the `*` (keyword-only marker) in `_apply_pragmas(dbapi_connection: Any, *, is_memory: bool)` and `_configure_sqlite(engine: AsyncEngine, *, is_memory: bool)`, → `/` (positional-only marker) | Equivalent | Cosmic-ray's binary-operator operator matches the bare `*`/`/` parameter-list separators as if they were arithmetic operators — a quirk of its AST matching, not a real arithmetic mutation. The resulting signature (`/` makes the parameters before it positional-only rather than `*` making the parameters after it keyword-only) is syntactically valid and, for every actual call site in this codebase (`_apply_pragmas(dbapi_connection, is_memory=is_memory)`, same pattern for `_configure_sqlite`), behaviorally identical: the positional argument stays positional, the keyword argument stays passable by keyword either way. Verified by applying the mutant and confirming `tests/unit/test_engine.py` stays green. |
| `core/ReplaceComparisonOperator_Eq_Gt` / `_Eq_GtE` / `_Eq_Is` on `pragma == "journal_mode"` → `>` / `>=` / `is` | Equivalent, but only for the current fixed key set | `SQLITE_PRAGMAS` has exactly three keys (`foreign_keys`, `journal_mode`, `busy_timeout`), and lexicographically `"journal_mode"` is the alphabetically-largest of the three — so `pragma > "journal_mode"` is always `False` (same as `==` would be for the two non-matching keys) and `pragma >= "journal_mode"` matches only `"journal_mode"` itself, identically to `==`. `is` is guaranteed identical in CPython specifically because `pragma` and the literal `"journal_mode"` are both interned string constants defined in the same module. Confirmed the *other* four comparison-operator mutants at this site (`!=`, `<`, `<=`, `is not`) all get killed, which is the expected asymmetric pattern for an equivalence that depends on the specific key set rather than holding for arbitrary strings — worth re-checking if `SQLITE_PRAGMAS` ever gains a key that sorts differently relative to `"journal_mode"`. |

After the `busy_timeout` fix: 52 mutants, 47 killed, 5 survived — the two
`NumberReplacer` mutants (the real gap) moved from "survived" to "killed"; the 5
remaining survivors are the `Mul_Div` and comparison-operator equivalents above.
None real gaps, none out of scope.

## Baseline and triage: `repositories/_dialect.py`

**Baseline:** 27 mutants, 24 killed, 3 in mutmut's "no tests" category (functionally
the same signal as survived: no test in the pinned subset exercises the mutated code
path at all).

**Triage:**

| Mutant | Classification | Reason |
| --- | --- | --- |
| `dialect_of` — `conn.dialect.name` → `None`; `Dialect(name)` → `Dialect(None)`; error message → blanked | **Real gap → fixed** | `dialect_of()` had zero test coverage anywhere in the suite before this task — not even in integration tests. Closed by three tests in `test_dialect.py` (`test_dialect_of_postgresql`, `test_dialect_of_sqlite`, `test_dialect_of_unsupported_raises_with_name_and_supported_list`), using a `SimpleNamespace` stand-in for the connection since `dialect_of` only reads `conn.dialect.name`. |

After the fix: 27/27 killed, 0 survivors, ~2.3s wall time.

## Deferred: `serialization/json.py`

Left out of this pass — `impl-t2b-2` was mid-round rewriting the encoder (promoting
`orjson` to a hard dependency, deleting the stdlib fallback, folding an integer-range
guard into the existing validation walk) while this task was running, and mutating a
module mid-rewrite produces a baseline that's stale before it's committed. Run
`scripts/mutation.sh json` once that work has landed and stabilized.

## Two tools, two jobs: mutmut and cosmic-ray

**Historical note, since the code below is quoted for a reason that no longer
applies to this project:** an earlier version of this document treated mutmut's
inability to mutate decorated functions as a permanent limitation with no
workaround short of restructuring every decorated function by hand. That framing
is now false. A follow-up spike (`mutation-framework-spike.md`) evaluated
alternative mutation-testing tools specifically to answer this, found cosmic-ray
reaches decorated functions directly via a dedicated operator, and this project now
runs both tools rather than working around the gap in one of them.

**Why mutmut is blind to decorated functions, and why that's not fixable by
configuration.** mutmut 3.x's mutation engine (`mutmut/mutation/file_mutation.py`)
walks the AST looking for nodes to mutate, and unconditionally excludes decorated
function bodies from that walk:

```python
# ignore decorated functions, because
# 1) copying them for the trampoline setup can cause side effects (e.g. multiple @app.post("/foo") definitions)
# 2) decorators are executed when the function is defined, so we don't want to mutate their arguments and cause exceptions
# 3) @property decorators break the trampoline signature assignment (which expects it to be a function)
# Exception: @staticmethod and @classmethod are allowed because they are predictable and it's easy to set up trampolines for them
if isinstance(node, cst.FunctionDef) and len(node.decorators):
    if len(node.decorators) == 1:
        decorator = node.decorators[0].decorator
        if isinstance(decorator, cst.Name) and decorator.value in ("staticmethod", "classmethod"):
            return False
    return True
```

There is no configuration flag anywhere in mutmut that gates this — it is not behind
`Config.get()`, it is a hardcoded, unconditional rule tied to how mutmut's trampoline
mechanism works, not something a future release is likely to lift casually.
`staticmethod` and `classmethod` are the only exceptions. Concretely, for this
codebase, that means mutmut is structurally blind to the body of every
`@event.listens_for(...)`-decorated function *and every `@handles(EventType)`-
decorated handler method* — the declarative event-routing layer that
`DeclarativeAggregate` and `DeclarativeProjection` are built on, and precisely the
layer the delivery-guarantee milestone will build exactly-once semantics on top of.

**cosmic-ray does not have this restriction**, and ships a `RemoveDecorator`
operator specifically for it: it strips the decorator entirely, leaving the
function defined but never registered as a callback. Verified empirically against
`engine.py` (see the self-check below): the `begin`-listener's `RemoveDecorator`
mutant is killed by the real two-connection test and survives against Task 1's
known-vacuous single-connection test — exactly the distinguishing result mutmut
cannot produce for this code, for any configuration.

**Which tool to reach for:**

| Situation | Tool |
| --- | --- |
| Plain functions/methods — no decorator, or `@staticmethod`/`@classmethod` | mutmut (`scripts/mutation.sh`) — faster, richer literal/argument mutation coverage, already the default for the whole curated set |
| A module whose logic lives partly or wholly inside `@event.listens_for`, `@handles`, or any other callback-registration decorator | cosmic-ray (`scripts/mutation-cosmic-ray.sh`) — the only one of the two that can reach that code at all |

This is not "cosmic-ray is the better tool, use it for everything": its default
operator set is *narrower* than mutmut's in other respects (no string-literal
mutation at all, so it cannot express the `"journal_mode"` → `"XXjournal_modeXX"`
class of mutant mutmut generates routinely), and its subprocess-per-mutant execution
model is markedly slower — see `mutation-framework-spike.md` for the `_dialect.py`
comparison (151 cosmic-ray mutants took ~110s there against mutmut's 27 in ~2s).
Running both tools against `engine.py` surfaced a real gap that *neither alone,
run in isolation, would have been trusted to have found completely* — see the
`SQLITE_PRAGMAS["busy_timeout"]` row in the `engine.py` cosmic-ray triage above:
mutmut's own operator set never generated an equivalent mutation for that literal,
so a mutmut-only mutation-testing practice would have missed it entirely, tautological
assertion and all. Two tools with different, overlapping-but-not-identical operator
sets is the practical answer, not a compromise pending a better single tool.

**Adding `@handles`-decorated modules to the curated set going forward:** create a
`cosmic-ray/<module>.toml` (see `cosmic-ray/engine.toml` for the template — the only
things that change per module are `module-path` and `test-command`), scoped to one
file with its own narrow test subset, same discipline as `scripts/mutation.sh`'s
mutmut configs. Do not restructure the handler into a thin-wrapper-plus-plain-
function pair just to make it mutmut-reachable — that pattern is still legitimate
where it falls out naturally, but it is no longer the *only* way to get real
mutation coverage on decorated code, so it should not be treated as a mandatory
prerequisite going forward.

**A separate, narrower limitation that is true of BOTH tools**, found while
triaging `_begin_unless_autocommit` under mutmut and confirmed by checking
cosmic-ray's own operator list (`cosmic-ray operators`, 213 entries): neither tool
has a general "delete this statement" mutation operator. Both mutate literals,
operators, call arguments, and comparisons; cosmic-ray additionally has
`RemoveDecorator` for the specific case of a decorator, but nothing in either tool
expresses "remove the `conn.exec_driver_sql("BEGIN")` call itself while leaving the
surrounding function otherwise intact" — the exact defect class Task 1's known bug
belonged to. The closest available proxies (replacing the argument with `None`, or
corrupting the string) both produce invalid SQL and get killed by *any* test that
reaches the code at all, real or vacuous, because a raised exception is an
unconditional kill — those particular mutants can't distinguish a rigorous test from
a weak one, in either tool. `RemoveDecorator` sidesteps this for the specific case of
"is the callback registered at all," which is what makes it decisive for the
self-check below, but it is not a general statement-deletion operator and should not
be read as one.

## Self-check: does the configuration actually catch a known-vacuous test?

Before trusting any result from this tool, its configuration was checked against a
defect this project already knows about: Task 1 originally shipped a single-connection
isolation test for `engine.py` that passed against an engine with **no** transaction
control applied — see the pre-fix version at `git show 6de02cf^:tests/unit/test_engine.py`
and the fix in `6de02cf`.

**With cosmic-ray, this check now passes as literally specified.** Applied the
`begin`-listener's `RemoveDecorator` mutant by hand
(`uv run cosmic-ray apply src/eventsource/engine.py core/RemoveDecorator 1`), which
strips `@event.listens_for(engine.sync_engine, "begin")` off `_emit_begin` entirely —
the listener is defined but never registered, so `BEGIN` is never emitted by any
connection, which is a strictly stronger and more direct proxy for "the BEGIN
emission is gone" than mutating the string inside it would be:

- Real test suite (`tests/unit/test_engine.py`, current):
  `test_sqlite_engine_holds_read_write_in_one_transaction` **fails**
  (`AssertionError: connection A observed connection B's commit`).
- Task 1's vacuous test (`git show 6de02cf^:tests/unit/test_engine.py`), same
  mutant: all 4 tests **pass**.

The mutant, and `tests/unit/test_engine.py`'s temporary swap to the vacuous version,
were both applied and reverted directly in this checkout (diff-verified clean against
a pre-edit backup of each file before moving on). `scripts/mutation-cosmic-ray.sh
engine`'s full run confirms both `RemoveDecorator` mutants (the `connect` listener
too) are killed by the real suite in the checked-in configuration, not just in this
one hand-run check.

**Historical context, since it's still useful evidence and was the best available
answer before cosmic-ray was added:** under mutmut alone, this check could not be run
as literally specified, for the two combined reasons in ["Two tools, two
jobs"](#two-tools-two-jobs-mutmut-and-cosmic-ray) — mutmut cannot mutate the body of a
`@event.listens_for`-decorated function at all (now moot: cosmic-ray covers exactly
this), and neither tool has an operator that expresses "delete this statement"
(still true of both — see that section). The mutmut-based indirect check from that
investigation still stands as corroborating evidence: restoring the vacuous test and
running `scripts/mutation.sh engine` against it produced `46` mutants, `25` killed,
`21` survived — 2 more survivors than the real test suite's `19` — with the extra 2
being `_driver_is_autocommit` mutated to always return `False` (killed by the real
suite's 3 AUTOCOMMIT persistence tests, survives under the vacuous suite which has
none) and an unrelated `create_async_engine` argument-forwarding mutant. The three
hand-run sanity mutations the team lead specified independently (remove `BEGIN`,
force the autocommit predicate `True`, force it `False`) all failed against the real
suite exactly as expected and passed the vacuous suite, confirmed by hand before
cosmic-ray was evaluated.

**Conclusion**: the harness — now specifically cosmic-ray for this class of defect —
does distinguish the real test suite from the known-weak one, using a mutation that
directly matches the shape of Task 1's actual bug (a callback silently not doing
what it's registered to do), not a proxy for it. mutmut remains the faster, richer
tool for everything the decorator doesn't gate; cosmic-ray is reserved for exactly
the cases mutmut cannot reach. Neither tool replaces manual break/restore discipline
for defects that live inside a single statement neither tool's operator set can
delete — that gap (see ["Two tools, two jobs"](#two-tools-two-jobs-mutmut-and-cosmic-ray))
is real and still requires a hand-run check when it matters.

## Non-goal: no CI score gate

Mutation testing here runs on demand — locally, or dispatched deliberately — and is
explicitly **not** wired into CI as a required check with a score threshold. Three
reasons, all firm:

1. **Equivalent mutants make 100% structurally unreachable.** Any threshold below
   100% is therefore arbitrary, and picking one just moves the argument from "is this
   code correct" to "is 87% the right number," which is a worse conversation.
2. **It's neither fast nor deterministic enough to gate on.** Even the curated set's
   fastest module takes several seconds per run and the runtime scales with mutant
   count, which grows as the modules do; treating it as a required check pushes
   toward shrinking the curated set to keep CI fast, defeating the point.
3. **A slow, occasionally-flaky required gate gets disabled within a month.** A
   diagnostic that people choose to run stays valuable. A mandatory gate that blocks
   merges on a score nobody fully trusts gets an exception carved into CI config the
   first time it's inconvenient, and from there it erodes.

Revisit this only if the survivor count for the curated set stays at zero, with full
triage recorded, across several consecutive milestones — and even then, gate on "no
new untriaged survivors" (a diff against the last recorded baseline), never on a raw
percentage.
