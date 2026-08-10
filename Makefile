# Local guardrail runner.
#
# `make check` runs every gate CI has a job for. Anything slower or needing
# external services is a separate opt-in target -- see `make help`.
#
# Every recipe goes through $(UV_RUN), which carries the same
# `--all-extras --locked` that .github/workflows/ci.yml syncs with, so gates
# execute against the versions pinned in uv.lock and a green `make check`
# and a green CI mean the same thing. If you add a gate here, add the
# matching job there.
#
# Do not drop `--all-extras` back to a bare `uv run`. The contributor
# toolchain (ruff, pytest, mypy, hypothesis, testcontainers, pre-commit)
# lives in the `dev` *extra*, and extras are opt-in: without the flag, an
# unsynced checkout has no ruff in .venv, `uv run` silently falls through to
# whatever `ruff` is on PATH, and the gate runs an unrelated version. That
# happened -- a pyenv-global ruff 0.7.2 reported seven errors that 0.14.8
# does not, and the same fallthrough can pass code CI rejects.
#
# `--locked` makes a stale uv.lock a loud failure here as well as in CI.
# If it trips, run `uv lock` and commit the result.
UV_RUN := uv run --all-extras --locked

# Marker expression for the unit suite -- kept byte-identical to the `test`
# job in .github/workflows/ci.yml. If you change it here, change it there.
# Note this does not exclude `benchmark`, so `make test` takes ~5 minutes.
UNIT_MARKERS := not integration and not postgres and not redis and not e2e

COMPOSE := docker compose -f docker-compose.test.yml
BENCH_COMPOSE := docker compose -f docker-compose.bench.yml

.DEFAULT_GOAL := help
.PHONY: help install check lint format types arch sec audit test test-changed cov \
        integration integration-up integration-down mutation mutation-cosmic \
        floors docs docs-examples adr precommit fix clean \
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
	uv sync --all-extras --locked

check: lint types arch sec test  ## Everything CI runs on a PR
	@echo
	@echo "All checks passed."

fix:  ## Auto-fix what can be auto-fixed (ruff --fix + format)
	$(UV_RUN) ruff check src/ tests/ --fix
	$(UV_RUN) ruff format src/ tests/

## ---------------------------------------------------------------------------
## Individual gates (each mirrors one CI job)
## ---------------------------------------------------------------------------

lint:  ## ruff check + format verification
	$(UV_RUN) ruff check .
	$(UV_RUN) ruff format --check .

format: ## Alias for `fix`
	@$(MAKE) fix

types:  ## mypy strict
	$(UV_RUN) mypy src/ bench/ --config-file=pyproject.toml

arch:  ## import-linter architecture contracts
	$(UV_RUN) lint-imports

sec: audit  ## bandit + pip-audit
	$(UV_RUN) bandit -c pyproject.toml -r src/

audit:  ## pip-audit dependency vulnerability scan
	$(UV_RUN) pip-audit

test:  ## Unit suite with coverage (enforces the fail_under ratchet)
	$(UV_RUN) pytest -m "$(UNIT_MARKERS)" \
		-n auto --dist worksteal \
		--cov=src/eventsource --cov-report=term-missing

# Stamp file recording the last green `make test` run. `test-changed` only
# re-runs the suite when a file under src/ is newer than the stamp.
TEST_STAMP := .test-stamp
SRC_FILES := $(shell find src -name '*.py' -not -path '*/__pycache__/*')

test-changed: $(TEST_STAMP)  ## `make test`, but skipped if src/ is unchanged since the last green run
	@echo "test-changed: src/ up to date with last green run (rm $(TEST_STAMP) to force)"

$(TEST_STAMP): $(SRC_FILES)
	@$(MAKE) test
	@touch $@

cov: test  ## Unit suite with coverage + an HTML report in htmlcov/
	$(UV_RUN) coverage html
	@echo "Report: htmlcov/index.html"

precommit:  ## Run the pre-commit hooks over all files
	$(UV_RUN) pre-commit run --all-files

## ---------------------------------------------------------------------------
## Opt-in: slower, or needs external services
## ---------------------------------------------------------------------------

integration-up:  ## Start Postgres + Redis test containers
	$(COMPOSE) up -d --wait

integration-down:  ## Stop and remove the test containers
	$(COMPOSE) down -v

integration: integration-up  ## Integration suite against real Postgres/Redis
	$(UV_RUN) pytest -m "integration or postgres or redis" \
		--cov=src/eventsource --cov-report=term-missing

mutation:  ## mutmut against the curated set (slow -- see docs/development/mutation-testing.md)
	scripts/mutation.sh $(MODULE)

# Deliberately NOT part of `make check`. Every other gate there runs against
# uv.lock; this one exists precisely to resolve differently, and it needs
# network access to do it. It has a CI job of its own
# (.github/workflows/dependency-floors.yml) rather than a step in ci.yml, so
# the "gate here, job there" parity the header describes still holds.
floors:  ## Unit suite against the declared dependency floors (slow, needs network)
	scripts/check_dependency_floors.sh

## ---------------------------------------------------------------------------
## Benchmark harness (bench/)
## ---------------------------------------------------------------------------

bench-up:  ## Start Postgres + Redis + Kafka + RabbitMQ benchmark services
	$(BENCH_COMPOSE) up -d --wait

bench-down:  ## Stop and remove the benchmark services
	$(BENCH_COMPOSE) down -v

bench:  ## Run the full benchmark matrix (services optional; unavailable backends skip)
	$(UV_RUN) python -m bench run

bench-quick:  ## Fast sanity pass over the matrix
	$(UV_RUN) python -m bench run --quick

bench-report:  ## Render a results file: make bench-report RESULTS=bench/results/bench-<ts>.json
	$(UV_RUN) python -m bench report $(RESULTS)

mutation-cosmic:  ## cosmic-ray for one module: make mutation-cosmic MODULE=engine
	@test -n "$(MODULE)" || { \
		echo "usage: make mutation-cosmic MODULE=<name>" >&2; \
		echo "available: $$(ls cosmic-ray/*.toml | xargs -n1 basename | sed 's/\.toml$$//' | tr '\n' ' ')" >&2; \
		exit 2; }
	scripts/mutation-cosmic-ray.sh $(MODULE)

docs: adr docs-examples  ## Strict docs build + runnable-example validation
	$(UV_RUN) mkdocs build --strict

# `mkdocs build --strict` does NOT catch a nav that omits a page, nor an
# index.md that forgot a record, which is exactly how docs/adrs/index.md
# drifted five times. Mirrors the ADR Check workflow.
adr:  ## ADR index + mkdocs nav agree with docs/adrs/
	python3 scripts/check_adr_index.py

docs-examples:  ## Syntax-check and execute everything in examples/
	$(UV_RUN) python scripts/validate_examples.py --syntax
	$(UV_RUN) python scripts/validate_examples.py

## ---------------------------------------------------------------------------

clean:  ## Remove build/test/coverage caches
	rm -rf .pytest_cache .ruff_cache .mypy_cache .benchmarks htmlcov \
	       .coverage coverage.xml site dist build .test-stamp
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
