# Backend Benchmark Matrix — Design

**Date**: 2026-07-29
**Status**: Approved
**Purpose**: Comparative characterization of eventsource-py backends — produce
scaling curves and comparable numbers per backend to inform
`docs/how-to/choose-an-event-store-backend.md` and general understanding of how
each backend reacts and scales under the same workloads.

## Goals

- Run the **same scenarios against every backend** of each interface and report
  comparable numbers.
- Sweep the dimensions that reveal scaling behavior: batch/payload size, stream
  length, concurrency/contention, and subscriber fan-out.
- Local, on-demand execution with raw JSON output plus a generated Markdown
  report. No CI integration.

## Non-Goals

- Regression detection or baseline comparison (the JSON schema is versioned so
  this can bolt on later; nothing else is built for it now).
- Capacity/limit probing or load testing.
- Changes to `src/eventsource/` public API, `tests/benchmarks/` (the existing
  in-memory pytest-benchmark suite stays as-is), or CI workflows.
- Auto-generated, committed benchmark numbers in docs. Numbers land in docs by
  manual curation; the report provides evidence, the doc makes claims.

## Decision Summary

| Decision | Choice |
|---|---|
| Purpose | Comparative characterization (not regression, not load testing) |
| Interfaces | EventStore ×3, EventBus ×4, SnapshotStore ×3, end-to-end aggregate path |
| Dimensions | Batch/payload size, stream length (± snapshots), concurrency/contention, subscriber fan-out |
| Execution | Local CLI + make targets against docker-compose services |
| Output | Raw JSON per run + generated Markdown report |
| Placement | Internal repo tooling (top-level `bench/`), not part of the package |
| Engine | Standalone async harness (not pytest-benchmark: its sync-callable model wraps each timed call in `asyncio.run`, distorting measurements, and it cannot express throughput/concurrency scenarios) |

## Architecture

```
bench/
  __main__.py          # CLI: uv run python -m bench [run|report]
                       #   run: [--interface store|bus|snapshot|e2e]
                       #        [--backend NAME] [--scenario NAME] [--quick]
                       #   report: results/<run>.json [...]
  core/
    runner.py          # matrix expansion, warmup, timed measurement, stats
    results.py         # result dataclasses + versioned JSON schema
    report.py          # Markdown report generator
  adapters/
    stores.py          # memory / postgresql / sqlite
    buses.py           # memory / redis / kafka / rabbitmq
    snapshots.py       # memory / postgresql / sqlite
  scenarios/
    stores.py, buses.py, snapshots.py, aggregate.py
  results/             # gitignored raw JSON output
```

- **Adapters** mirror the conformance-suite factory pattern
  (`src/eventsource/testing/conformance.py`): each backend is a small class
  with `name`, `available() -> bool` (optional-dep guard + service ping),
  `setup()` / `teardown()` (schema creation, data reset), and `create()`
  returning the interface instance. Scenarios never know which backend they
  run on; adding a backend means adding one adapter.
- **Scenarios are declarative**: an async callable plus a parameter grid. The
  runner expands scenarios × parameters × backends into the matrix and runs
  each cell uniformly.
- **Single entry point**: the CLI, wired to `make bench` / `make bench-report`.
  All measurement for a backend session happens inside one event loop — no
  per-measurement `asyncio.run`.

## Infrastructure

- New `docker-compose.bench.yml` with postgres, redis, kafka, and rabbitmq on
  dedicated ports (coexists with `docker-compose.test.yml`). Wrapped by
  `make bench-up` / `make bench-down`. Long-lived warm services are preferred
  over testcontainers for benchmarking.
- **Availability, not failure**: an unavailable backend (missing extra or
  unreachable service) is recorded in the results JSON as skipped-with-reason
  and the run continues. A run with no Docker still benchmarks memory + SQLite.

### Fairness rules (enforced by adapters)

- Tracing disabled everywhere (`NullTracer`, per ADR-0007 guidance).
- Identical event payloads from one shared generator; payload sizes defined
  once in `bench/core/`.
- Fresh schema per backend session; data reset between scenario cells.
- Warmup before every timed cell (warms connection pools, broker topics).
- SQLite runs file-backed in a temp directory (realistic), not `:memory:` —
  the memory backend already covers the no-I/O case.

## Scenario Catalog

Scenario × parameter grid × backend = matrix (~106 cells at full grid: 51
store + 28 bus + 9 snapshot + 18 end-to-end).

### EventStore (memory, PostgreSQL, SQLite)

| Scenario | Grid | Metrics |
|---|---|---|
| `append_batch` | batch [1, 10, 100, 1000] × payload [small ~200B, large ~5KB] | events/s, per-call latency |
| `read_stream` | stream length [100, 1k, 10k] | read latency, events/s |
| `concurrent_append` | writers [1, 10, 50], distinct aggregates | throughput scaling |
| `contended_append` | writers [1, 10, 50], one hot aggregate | effective throughput, OptimisticLockError conflict rate |

### EventBus (memory, Redis, Kafka, RabbitMQ)

| Scenario | Grid | Metrics |
|---|---|---|
| `publish_throughput` | batch [1, 10, 100] | publish events/s |
| `fanout` | subscribers [1, 10, 50] | delivery throughput, publish→handler latency |
| `roundtrip` | single event | p50/p95/p99 latency |

### SnapshotStore (memory, PostgreSQL, SQLite)

| Scenario | Grid | Metrics |
|---|---|---|
| `save_load` | state size [small, ~50KB, ~500KB] | save latency, load latency |

### End-to-end aggregate path (× 3 stores)

| Scenario | Grid | Metrics |
|---|---|---|
| `load_mutate_save` | stream length [100, 1k, 10k] × snapshot strategy [none, threshold] | full AggregateRepository round-trip latency |

This is the "when does backend X need snapshotting" answer and the number
users actually feel.

`--quick` trims each grid to its smallest value and cuts iterations (full run
target < ~20 min; quick < ~2 min).

## Measurement Methodology

Per cell: setup → warmup (untimed, fixed iterations) → timed phase → teardown.

- Timed phase repeats the operation until a small time budget (~2–5s) or an
  iteration cap, using `time.perf_counter()`.
- **Latency scenarios**: per-operation timings → p50/p95/p99, mean, min.
  **Throughput scenarios**: ops ÷ elapsed. **Contention scenarios**: also
  count `OptimisticLockError` conflicts/retries.
- **Rounds**: 3 rounds per cell; report shows the median round; JSON keeps all
  rounds so noise is visible.
- **Bus latency**: publish site and handler both capture `perf_counter()` in
  the same process (monotonic clock, valid deltas); completion detected by an
  `asyncio.Event` when the expected count arrives.
- **Hygiene**: `gc.collect()` between cells; GC stays on during measurement
  (realistic for async code).
- **Metadata**: results JSON embeds commit SHA, Python version, platform/CPU,
  library version, per-service versions, timestamp. Reports lead with this so
  numbers are never context-free.

## Error Handling

- A cell that raises is recorded as `failed` with the exception summary; the
  run continues to the next cell.
- Every cell has a hard timeout (60s default) so a wedged broker cannot hang
  the matrix.
- Teardown always runs (`finally`), so a failed cell does not poison the next
  cell's state.
- Exit code is non-zero if any cell failed.

## Reporting

`uv run python -m bench report results/<run>.json` renders Markdown: one
section per interface; each scenario is a table with the swept parameter as
rows, backends as columns, and the measured metric in cells (plus conflict
rate columns where relevant). The metadata header leads the document.

The JSON carries a `schema_version` field and stable scenario/cell IDs so
future regression tracking (BACKLOG P3 entry) can diff two run files without a
redesign.

## Testing

In `tests/unit/bench/` so `make check` covers it:

- Unit tests: matrix expansion, stats math (percentiles, medians), JSON
  round-trip, report generation from a fixture JSON.
- One end-to-end smoke test: a single tiny cell against memory backends
  (seconds), asserting a valid result file is produced.
- `bench/` joins the ruff and mypy paths — same quality gates as `src/`.
