---
paths:
  - "tests/**/*.py"
---

# Testing Conventions

## What a test has to reach

Nearly every test here is an isolation test: construct the mechanism, exercise
it, assert on it. That proves the mechanism works and says nothing about
whether anything reaches it — which is why nine inert capabilities shipped
green in the 2026-08-09 backpressure wave. When the *library* is obligated to
invoke what you are testing, drive the real caller and then inspect the
mechanism. Rule and criterion: `.claude/rules/recurring-defects.md` §3.

## Structure

- `tests/unit/` -- No external dependencies, fast, always run
- `tests/integration/` -- Require Docker services, use pytest markers
- `tests/benchmarks/` -- Performance benchmarks

## Markers

Use markers for tests requiring external services:
- `@pytest.mark.postgres` -- PostgreSQL
- `@pytest.mark.sqlite` -- SQLite
- `@pytest.mark.redis` -- Redis
- `@pytest.mark.kafka` -- Kafka
- `@pytest.mark.rabbitmq` -- RabbitMQ
- `@pytest.mark.integration` -- General integration
- `@pytest.mark.e2e` -- End-to-end
- `@pytest.mark.slow` -- Slow-running

## Async

- `asyncio_mode = "auto"` -- all test functions are async by default
- `asyncio_default_fixture_loop_scope = "session"` -- session-scoped event loop
- No need for `@pytest.mark.asyncio` decorator

## Fixtures

- Shared fixtures in `tests/conftest.py`
- Subdirectory-specific fixtures in local `conftest.py` files

## Running

```bash
uv run pytest tests/unit/ -v              # Unit only (fast)
uv run pytest tests/ -m "not kafka and not rabbitmq"  # Skip heavy backends
uv run pytest tests/ -m postgres          # Just postgres tests
```
