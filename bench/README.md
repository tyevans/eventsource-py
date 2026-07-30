# bench — cross-backend benchmark harness

Internal tooling (not part of the `eventsource` package). Runs the same
scenario catalog across every backend and reports comparable numbers.
Design: `docs/superpowers/specs/2026-07-29-backend-benchmark-matrix-design.md`.

## Usage

    make bench-up       # start postgres/redis/kafka/rabbitmq (docker-compose.bench.yml)
    make bench          # full matrix (~20 min); unavailable backends are skipped
    make bench-quick    # trimmed grids, ~2 min
    make bench-report RESULTS=bench/results/bench-<ts>.json
    make bench-down

Without Docker, `make bench` still runs the memory and SQLite backends.

Filters: `uv run python -m bench run --interface store --backend postgresql
--scenario store.append_batch --quick`

## Endpoints (env-overridable)

| Variable | Default |
|---|---|
| `BENCH_POSTGRES_URL` | `postgresql+asyncpg://bench:bench@localhost:5434/eventsource_bench` |
| `BENCH_REDIS_URL` | `redis://localhost:6381` |
| `BENCH_KAFKA_SERVERS` | `localhost:9094` |
| `BENCH_RABBITMQ_URL` | `amqp://guest:guest@localhost:5673/` |

## Interpreting results

Raw JSON lands in `bench/results/` (gitignored, schema-versioned). The
Markdown report renders one table per scenario: rows are parameter
combinations, columns are backends. Numbers are only comparable within a
single run on one machine -- the metadata header records the context.
Numbers reach docs by manual curation, never automatically.
