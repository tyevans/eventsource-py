#!/usr/bin/env bash
# Run mutmut against the curated mutation-testing set.
#
# See docs/development/mutation-testing.md for what's in the set, why, and
# how to read the output. Usage:
#
#   scripts/mutation.sh              # all three modules, each against its
#                                     # own test subset (sequential)
#   scripts/mutation.sh engine       # just src/eventsource/engine.py
#   scripts/mutation.sh dialect      # just repositories/_dialect.py
#   scripts/mutation.sh json         # just serialization/json.py
#
# mutmut 3.x reads [tool.mutmut] from pyproject.toml once at process start
# and has no per-invocation override for only_mutate / test selection, so
# per-module scoping works by rewriting that section between runs via
# scripts/_mutmut_configure.py. pyproject.toml is backed up before the first
# run and restored on exit (including on failure/interrupt) so a checkout
# is never left with a mutated config.

set -euo pipefail
cd "$(dirname "$0")/.."

MODULE="${1:-all}"
VALID=(engine dialect json checkpoint all)
if [[ ! " ${VALID[*]} " =~ " ${MODULE} " ]]; then
    echo "usage: $0 [engine|dialect|json|all]" >&2
    exit 2
fi

BACKUP="$(mktemp)"
cp pyproject.toml "$BACKUP"
cleanup() {
    cp "$BACKUP" pyproject.toml
    rm -f "$BACKUP"
}
trap cleanup EXIT

run_module() {
    local mod="$1"
    echo "=== mutation: $mod ==="
    uv run python scripts/_mutmut_configure.py "$mod"
    rm -rf mutants
    time uv run mutmut run
    uv run mutmut results
}

if [[ "$MODULE" == "all" ]]; then
    for m in engine dialect json; do
        run_module "$m"
    done
else
    run_module "$MODULE"
fi
