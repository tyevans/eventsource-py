#!/usr/bin/env bash
#
# Run the unit suite against this package's *declared dependency floors*.
#
# Why this exists
# ---------------
# Every other gate in this repo resolves dependencies to the newest version the
# constraints permit -- `uv sync --locked` pins one exact set, and that set sits
# near the top of every range. The `>=` bound in pyproject.toml is therefore the
# one point in the supported range that nothing ever exercises, so a floor that
# is too low is invisible by construction: it passes CI, it ships, and it breaks
# for the first user who installs at the bottom of the range. Not hypothetical
# -- a downstream consumer hit this shape against our own version bound, and
# when this gate was first written five of our declared floors turned out to be
# uninstallable on the only Python we support. See
# docs/development/dependency-floors.md.
#
# What it does
# ------------
# Builds a throwaway virtualenv, installs this project with
# `--resolution lowest-direct` so every *declared* dependency lands exactly on
# its `>=` bound, and runs the existing unit suite against it.
#
# There are deliberately no floor-specific assertions. The suite we already
# trust is the definition of "works" -- a hand-written floor smoke test would be
# a second, partial copy of the public surface, and the copies would drift
# (.claude/rules/recurring-defects.md section 2). It also would not have caught
# the failure that motivated this gate, which was a constructor signature: it
# imports fine and only fails when called.
#
# `lowest-direct`, not `lowest`
# -----------------------------
# `lowest` floors transitive dependencies as well. Those are not versions we
# declare, control, or promise anything about; flooring them fails on other
# projects' decade-old releases, which is noise rather than our bug. We publish
# our own `>=` bounds, and those are exactly what `lowest-direct` measures.
#
# Isolation
# ---------
# Everything happens in a temp dir against a temp venv, via
# `uv pip install --python` rather than `uv sync`, so this can never disturb
# the repo's .venv or uv.lock. Test tooling is pinned to the versions in
# uv.lock (via `uv export`) so that the runtime floors are the only thing
# varying between this gate and `make test`.
#
# Usage:  make floors        (or: scripts/check_dependency_floors.sh)
set -euo pipefail

# uv emits ANSI styling even when its stdout is a pipe or a file, which
# silently breaks anchored greps over its output.
export NO_COLOR=1

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

VENV="$WORK/venv"
PY="$VENV/bin/python"

# Derived from pyproject.toml rather than listed here, so that adding an extra
# cannot silently leave it unmeasured (recurring-defects.md section 2). Every
# runtime extra is included: their drivers all install without an external
# service -- installing asyncpg does not require a PostgreSQL server. What the
# suite then *exercises* is a separate question, see MARKERS below.

# Kept in step with UNIT_MARKERS in the Makefile and the `test` job in
# .github/workflows/ci.yml. Service-backed tests are excluded here for the same
# reason they are excluded there -- no containers. So the asyncpg, redis,
# aiokafka, aio-pika and confluent-kafka floors are verified as far as
# "installs, imports, and passes the adapter unit tests" and no further; the
# core, sqlite and telemetry floors get the whole suite.
MARKERS="not integration and not postgres and not redis and not e2e and not benchmark"

cd "$REPO_ROOT"

FLOORS_PY="$REPO_ROOT/scripts/_declared_floors.py"
EXTRAS="$(python3 "$FLOORS_PY" extras)"

echo "==> creating floor environment in $WORK"
uv venv --python 3.13 "$VENV" >/dev/null

# Test tooling first, constrained to uv.lock so it matches `make test`. A
# constraint bounds a version without requiring the package, so this pins the
# tooling without dragging in the runtime dependencies we are about to floor.
# `--frozen` reads the existing lock and never rewrites it.
echo "==> installing test tooling (pinned to uv.lock)"
uv export --frozen --all-extras --no-hashes --no-emit-project \
    --format requirements-txt > "$WORK/locked.txt"
uv pip install --quiet --python "$PY" --constraint "$WORK/locked.txt" \
    pytest pytest-asyncio pytest-timeout pytest-randomly pytest-benchmark \
    pytest-xdist hypothesis testcontainers

# The project last, so its floors win over anything the tooling dragged in.
echo "==> installing eventsource-py[$EXTRAS] at declared floors"
uv pip install --python "$PY" --resolution lowest-direct "$REPO_ROOT[$EXTRAS]"

echo
echo "==> floors under test"
# Guard the guard. If the resolver served anything *above* a declared bound --
# a transitive constraint, a yank, a wheel missing for this interpreter -- then
# the suite below would pass without having tested the floor at all, which is
# indistinguishable from success. Assert the pin before trusting the run.
uv pip freeze --python "$PY" > "$WORK/freeze.txt"
python3 "$FLOORS_PY" check "$WORK/freeze.txt"

echo
echo "==> running the unit suite against the floors"
# No coverage flags and no pytest-cov in this environment: the fail_under
# ratchet is calibrated against `make test`'s selection, not this one, and
# coverage is not enabled by default in [tool.pytest.ini_options].
# -p no:randomly for a reproducible order.
"$PY" -m pytest tests/unit -m "$MARKERS" \
    -q --no-header -p no:randomly -n auto --dist worksteal

echo
echo "Declared dependency floors hold."
