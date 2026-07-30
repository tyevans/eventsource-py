# Backlog

Open work items, carried over from the retired `bd` (beads) tracker.

## Investigate making sqlalchemy an optional dependency (P3)

Investigate whether sqlalchemy can be moved from core deps to optional extras. It's
used in `stores/postgresql.py`, `stores/sqlite.py`, `repositories/`,
`snapshots/postgresql.py`, `locks/postgresql.py`, and `migration/`. The key question:
do any core interfaces import sqlalchemy at module level? If the interfaces are clean
(pydantic-only), sqlalchemy can become optional. If not, identify what needs to change.
This further lightens the base install toward the Tier 0 goal.

Prerequisite (done): drop redis from core dependencies.

## Add CI boundary check for core surface purity (P2)

Add a test that imports only core surface modules (events, aggregates, protocols,
`stores/interface`, `projections/base`, handlers, exceptions, types) and asserts no
sqlalchemy/redis modules are in `sys.modules` afterward. This prevents accidental
coupling from creeping in and makes a future Tier 0 extraction cheap.

Note: import-linter contracts were added in commit 260c662 — check whether they
already cover this before doing more work here.

Prerequisite (done): document core surface boundary for future Tier 0 extraction
(`docs/core-surface.md`).

## Deterministic or scheduled coverage for bus performance assertions (P3)

The kafka/rabbitmq metrics-overhead and duration tests assert wall-clock thresholds
and were excluded from the blocking broker-tests CI job (marked `@pytest.mark.benchmark`)
because shared runners make elapsed-time assertions nondeterministic (observed 55.79%
vs a 20% threshold on a green code path). Nothing in CI watches metrics overhead now.
Either rewrite the assertions as deterministic proxies (count instrumentation calls
rather than elapsed time) or add a scheduled, non-blocking benchmark workflow that
runs `-m benchmark` and reports results.
