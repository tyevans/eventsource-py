#!/usr/bin/env bash
# Sweep for stale references to a retired eventsource package.
# Usage: .claude/skills/migrating-modules-to-rings/sweep.sh <pkg>
#   e.g. sweep.sh subscriptions   (checks for eventsource.subscriptions)
# Run from the repo root.
#
# Exit status is driven by the FATAL checks only:
#   1. import-shaped references (`from eventsource.<pkg> ...` /
#      `import eventsource.<pkg>`) anywhere except locations that reference
#      the retired path by design:
#        CHANGELOG.md                  - BREAKING entries name the old path
#        docs/adrs/, docs/superpowers/ - immutable history
#        tests/unit/test_public_api.py - ModuleNotFoundError guard tests
#      (guard tests elsewhere should use importlib.import_module("...")
#       so they don't trip this check)
#   2. leftover dirs at src/eventsource/<pkg> or tests/unit/<pkg> -- even
#      __pycache__-only debris resurrects the old import path.
#      tests/integration/<pkg> is NOT checked: integration tests stay in
#      place by design, only their imports are repointed.
# Bare path mentions (logger names, prose, comments) are printed for triage
# but are not fatal -- many are intentional.
set -u

if [ $# -ne 1 ]; then
    echo "usage: $0 <pkg>  (the retired top-level package name)" >&2
    exit 2
fi

pkg="$1"
fail=0

GREP_EXCLUDES=(
    --exclude-dir=.git --exclude-dir=.venv --exclude-dir=__pycache__
    --exclude-dir=.claude --exclude-dir=node_modules --exclude-dir=.mypy_cache
    --exclude-dir=.ruff_cache --exclude-dir=.pytest_cache --exclude-dir=htmlcov
    --exclude-dir=site --exclude-dir=adrs --exclude-dir=superpowers
    --exclude=CHANGELOG.md --exclude=test_public_api.py
)

echo "== FATAL: import-shaped references to eventsource.${pkg} =="
if grep -rnE "(from|import)[[:space:]]+eventsource\.${pkg}\b" . "${GREP_EXCLUDES[@]}"; then
    fail=1
else
    echo "(none)"
fi

echo "== FATAL: leftover dirs at retired paths =="
leftovers=0
for old in "src/eventsource/${pkg}" "tests/unit/${pkg}"; do
    if [ -d "$old" ]; then
        echo "$old still exists:"
        find "$old" -not -type d | head -10
        leftovers=1
    fi
done
if [ "$leftovers" -eq 0 ]; then
    echo "(none)"
else
    fail=1
fi

echo "== triage (non-fatal): other mentions of eventsource.${pkg} =="
grep -rn "eventsource\.${pkg}\b" . "${GREP_EXCLUDES[@]}" \
    | grep -vE "(from|import)[[:space:]]+eventsource\.${pkg}\b" \
    || echo "(none)"

if [ "$fail" -ne 0 ]; then
    echo "SWEEP FAILED: fatal findings above" >&2
else
    echo "sweep clean for eventsource.${pkg} (triage list may need review)"
fi
exit "$fail"
