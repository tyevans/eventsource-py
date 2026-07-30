# Local guardrail runner.
#
# `make check` runs every gate CI has a job for. Anything slower or needing
# external services is a separate opt-in target -- see `make help`.
#
# Every recipe goes through `uv run`, so gates execute against the locked
# environment in uv.lock. Run `make install` once first.
#
# .github/workflows/ci.yml installs the same way (`uv sync --all-extras
# --locked`) and runs the same commands via `uv run`, so a green
# `make check` and a green CI mean the same thing. If you add a gate here,
# add the matching job there.

# Marker expression for the unit suite -- kept byte-identical to the `test`
# job in .github/workflows/ci.yml. If you change it here, change it there.
# Note this does not exclude `benchmark`, so `make test` takes ~5 minutes.
UNIT_MARKERS := not integration and not postgres and not redis and not e2e

COMPOSE := docker compose -f docker-compose.test.yml
BENCH_COMPOSE := docker compose -f docker-compose.bench.yml

.DEFAULT_GOAL := help
.PHONY: help install check lint format types arch sec audit test cov \
        integration integration-up integration-down mutation mutation-cosmic \
        docs docs-examples precommit fix clean \
        bench-up bench-down bench bench-quick bench-report

## ---------------------------------------------------------------------------
## Everyday
## ---------------------------------------------------------------------------

help:  ## Show this help
	@echo "eventsource-py -- local guardrails"
	@echo
	@grep -hE '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "} {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'
	@echo
	@echo "  check = CI parity. Green here means green in CI."

install:  ## Sync the dev environment (all extras)
	uv sync --all-extras

check: lint types arch sec test  ## Everything CI runs on a PR
	@echo
	@echo "All checks passed."

fix:  ## Auto-fix what can be auto-fixed (ruff --fix + format)
	uv run ruff check src/ tests/ --fix
	uv run ruff format src/ tests/

## ---------------------------------------------------------------------------
## Individual gates (each mirrors one CI job)
## ---------------------------------------------------------------------------

lint:  ## ruff check + format verification
	uv run ruff check .
	uv run ruff format --check .

format: ## Alias for `fix`
	@$(MAKE) fix

types:  ## mypy strict
	uv run mypy src/ bench/ --config-file=pyproject.toml

arch:  ## import-linter architecture contracts
	uv run lint-imports

sec: audit  ## bandit + pip-audit
	uv run bandit -c pyproject.toml -r src/

audit:  ## pip-audit dependency vulnerability scan
	uv run pip-audit

test:  ## Unit suite with coverage (enforces the fail_under ratchet)
	uv run pytest -m "$(UNIT_MARKERS)" \
		--cov=src/eventsource --cov-report=term-missing

cov: test  ## Unit suite with coverage + an HTML report in htmlcov/
	uv run coverage html
	@echo "Report: htmlcov/index.html"

precommit:  ## Run the pre-commit hooks over all files
	uv run pre-commit run --all-files

## ---------------------------------------------------------------------------
## Opt-in: slower, or needs external services
## ---------------------------------------------------------------------------

integration-up:  ## Start Postgres + Redis test containers
	$(COMPOSE) up -d --wait

integration-down:  ## Stop and remove the test containers
	$(COMPOSE) down -v

integration: integration-up  ## Integration suite against real Postgres/Redis
	uv run pytest -m "integration or postgres or redis" \
		--cov=src/eventsource --cov-report=term-missing

mutation:  ## mutmut against the curated set (slow -- see docs/development/mutation-testing.md)
	scripts/mutation.sh $(MODULE)

## ---------------------------------------------------------------------------
## Benchmark harness (bench/)
## ---------------------------------------------------------------------------

bench-up:  ## Start Postgres + Redis + Kafka + RabbitMQ benchmark services
	$(BENCH_COMPOSE) up -d --wait

bench-down:  ## Stop and remove the benchmark services
	$(BENCH_COMPOSE) down -v

bench:  ## Run the full benchmark matrix (services optional; unavailable backends skip)
	uv run python -m bench run

bench-quick:  ## Fast sanity pass over the matrix
	uv run python -m bench run --quick

bench-report:  ## Render a results file: make bench-report RESULTS=bench/results/bench-<ts>.json
	uv run python -m bench report $(RESULTS)

mutation-cosmic:  ## cosmic-ray for one module: make mutation-cosmic MODULE=engine
	@test -n "$(MODULE)" || { \
		echo "usage: make mutation-cosmic MODULE=<name>" >&2; \
		echo "available: $$(ls cosmic-ray/*.toml | xargs -n1 basename | sed 's/\.toml$$//' | tr '\n' ' ')" >&2; \
		exit 2; }
	scripts/mutation-cosmic-ray.sh $(MODULE)

docs: docs-examples  ## Strict docs build + runnable-example validation
	uv run mkdocs build --strict

docs-examples:  ## Syntax-check and execute everything in examples/
	uv run python scripts/validate_examples.py --syntax
	uv run python scripts/validate_examples.py

## ---------------------------------------------------------------------------

clean:  ## Remove build/test/coverage caches
	rm -rf .pytest_cache .ruff_cache .mypy_cache .benchmarks htmlcov \
	       .coverage coverage.xml site dist build
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
