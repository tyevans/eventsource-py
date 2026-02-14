---
paths:
  - "tests/**/*.py"
---

# Testing Conventions

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
