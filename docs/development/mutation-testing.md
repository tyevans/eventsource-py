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

## How to run it

```bash
scripts/mutation.sh              # all three modules, sequentially
scripts/mutation.sh engine       # just src/eventsource/engine.py
scripts/mutation.sh dialect      # just repositories/_dialect.py
scripts/mutation.sh json         # just serialization/json.py
```

Each invocation prints a per-mutant summary (killed/survived/etc., mutmut's emoji
legend) and, at the end, the list of surviving mutant names. `mutmut show <name>`
displays the diff for any specific mutant if you need to inspect one directly.

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
| `_begin_unless_autocommit` — `"BEGIN"` → `"XXBEGINXX"` / arg → `None` | **Killed** (not a survivor) | Both are invalid as SQL, so `exec_driver_sql` raises immediately regardless of which test suite is active — a crash is an unconditional kill. Worth noting explicitly: this is *not* evidence the test suite is doing its job on transaction semantics, just that mutmut's mutation operators can't express "silently drop the statement" (see below) — a raised exception from garbage SQL is a much weaker signal than a semantic transaction-isolation failure. |
| `create_async_engine` — `is_memory = ":memory:" in url` → `is_memory = None` / `is_memory = ":MEMORY:" in url` / `is_memory = "XX:memory:XX" in url` | Equivalent | `is_memory` also gates `kwargs.setdefault("poolclass", StaticPool)`, but SQLAlchemy's own `aiosqlite` dialect already defaults to `StaticPool` for a bare `:memory:` URL regardless — verified directly (`type(engine.pool).__name__ == "StaticPool"` with no `create_async_engine` wrapper involved at all). The `is_memory=None` variant also reaches `_apply_pragmas`, which is the same "WAL-on-`:memory:`-is-silently-ignored" equivalence as the row above. |
| `create_async_engine` — `kwargs.setdefault("poolclass", StaticPool)` → `poolclass=None` / arg dropped | Equivalent | Same reasoning: SQLAlchemy's own default already applies. |
| `create_async_engine` — non-SQLite branch drops `**kwargs` | **Real gap → fixed** | `return _sa_create_async_engine(url, **kwargs)` mutated to drop the forwarded kwargs entirely for the Postgres/other-dialect passthrough. Closed by `test_postgres_url_forwards_kwargs`, which passes `echo=True` and asserts `engine.echo is True`. |
| `create_async_engine` — `logger.debug(...)` argument/message mutations (7 variants) | Out of scope | Log message content and formatting only; no test should assert on it. |

Net result after fixes: two real gaps closed (`_apply_pragmas`'s memory-skip logic in
both directions, `create_async_engine`'s kwargs passthrough), one weak assertion
replaced with a discriminating one (`busy_timeout`), and every remaining survivor is
either equivalent or out of scope. `tests/unit/test_engine.py` grew from 8 to 11
tests as a direct result.

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

## The decorator blind spot: mutmut cannot mutate `@handles` or event-listener bodies

This is the most consequential finding of this task, and it is not specific to
`engine.py` — it applies everywhere in this codebase that uses a decorator to
register a function as a callback.

mutmut 3.x's mutation engine (`mutmut/mutation/file_mutation.py`) walks the AST
looking for nodes to mutate, and unconditionally excludes decorated function bodies
from that walk:

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
`Config.get()`, it is a hardcoded, unconditional rule. `staticmethod` and
`classmethod` are the only exceptions.

**Concretely, for this codebase**, that means mutmut is structurally blind to the
body of every `@event.listens_for(...)`-decorated function (which is what discovered
this — see the `engine.py` triage above) *and every `@handles(EventType)`-decorated
handler method* — the declarative event-routing layer that `DeclarativeAggregate` and
`DeclarativeProjection` are built on, which is this library's central abstraction and
precisely the layer the delivery-guarantee milestone will build exactly-once
semantics on top of. A clean mutation score for a module whose real logic lives
mostly inside `@handles`-decorated methods proves almost nothing — mutmut never had
the chance to touch that logic in the first place.

**The mitigation applied here, and the pattern to reuse:** keep the decorated
function as a thin registration wrapper that does nothing but call a plain,
undecorated, module-level function containing the actual logic. `_configure_sqlite`
in `engine.py` now looks like:

```python
@event.listens_for(engine.sync_engine, "connect")
def _set_pragmas(dbapi_connection, _record):
    _apply_pragmas(dbapi_connection, is_memory=is_memory)

@event.listens_for(engine.sync_engine, "begin")
def _emit_begin(conn):
    _begin_unless_autocommit(conn)
```

`_apply_pragmas` and `_begin_unless_autocommit` are ordinary functions mutmut can see
and mutate freely; the two-line listener bodies have nothing left in them worth
mutating. Applying the same pattern to `@handles`-decorated handlers — extract the
handler body into a plain function the decorated method calls — is the only way to
get real mutation coverage on that layer, and is worth doing deliberately as those
handlers accumulate real logic, rather than discovering the gap by surprise later.

**A second, narrower mutmut limitation worth knowing**, found while triaging
`_begin_unless_autocommit`: mutmut has no "delete this statement" mutation operator.
It mutates literals, operators, call arguments, and comparisons, but it cannot
express "remove the `conn.exec_driver_sql("BEGIN")` call entirely" — the exact defect
class Task 1's known bug belonged to. The closest available proxies (replacing the
argument with `None`, or corrupting the string) both produce invalid SQL and get
killed by *any* test that reaches the code at all, real or vacuous, because a raised
exception is an unconditional kill — which means those particular mutants can't
distinguish a rigorous test from a weak one. See the [self-check](#self-check-does-the-configuration-actually-catch-a-known-vacuous-test)
below for how this was worked around to still validate the harness.

## Self-check: does the configuration actually catch a known-vacuous test?

Before trusting any result from this tool, its configuration was checked against a
defect this project already knows about: Task 1 originally shipped a single-connection
isolation test for `engine.py` that passed against an engine with **no** transaction
control applied — see the pre-fix version at `git show 6de02cf^:tests/unit/test_engine.py`
and the fix in `6de02cf`.

The direct version of this check — restore that test, mutate `engine.py`, confirm a
surviving mutant for the deleted `BEGIN` — could not be run as literally stated, for
the two combined reasons above: mutmut cannot mutate the body of a
`@event.listens_for`-decorated function in the first place (fixed by the restructuring
in this task), and even after restructuring, mutmut has no operator that expresses
"delete this statement" (not fixable — a tool limitation, not a configuration one).

What was run instead, and confirmed by hand:

1. Restored the vacuous test, ran `scripts/mutation.sh engine` against it. `46`
   mutants, `25` killed, `21` survived — 2 more survivors than the real test suite's
   `19`.
2. The 2 additional survivors under the vacuous test, that the real test suite kills:
   - `_driver_is_autocommit` mutated to always return `False` (i.e. "never detect
     AUTOCOMMIT, always issue BEGIN"). Killed by the real suite's three AUTOCOMMIT
     persistence tests (`test_sqlite_autocommit_write_persists_without_explicit_commit`,
     one per route SQLAlchemy offers for requesting AUTOCOMMIT); survives under the
     vacuous suite, which has no AUTOCOMMIT test at all.
   - A `create_async_engine` mutant affecting an unrelated argument-forwarding path.
3. As a second, more literal check (since 1-2 don't touch the exact `BEGIN` statement
   directly, for the reasons above), the three sanity mutations the team lead
   specified were run by hand against the *restored, real* test suite:
   - Removing the `BEGIN` emission entirely → `test_sqlite_engine_holds_read_write_in_one_transaction`
     fails.
   - Forcing `_driver_is_autocommit` to always return `True` → same test fails.
   - Forcing it to always return `False` → all three AUTOCOMMIT persistence tests
     fail.

   All three reproduced exactly as expected, then were reverted (`diff` confirmed
   clean against a pre-edit backup, full suite re-run green) before proceeding.

The vacuous test was restored, the real test suite (`git show 6de02cf`'s version) put
back afterward, and `tests/unit/test_engine.py` reverified passing (8/8, later 11/11
after the real-gap tests were added) before any of this was committed.

**Conclusion**: the harness does distinguish the real test suite from the known-weak
one, and the hand-run sanity checks confirm the restructured code still fails exactly
where it should — but only because those checks bypass mutmut's mutation-operator
limitation rather than exercising it. The honest reading is that mutation testing on
`engine.py`, as currently configured, is a real and useful signal for the parts of the
module it *can* mutate (which, after the restructuring, is now everything except two
two-line listener bodies), but it would not by itself have caught Task 1's original
defect — only the restructuring plus a hand-run check did that. Property tests find
coverage gaps; mutation tests find oracle errors *that involve a mutable expression*;
neither replaces manual break/restore discipline for defects that live in a statement
mutmut structurally can't touch.

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
