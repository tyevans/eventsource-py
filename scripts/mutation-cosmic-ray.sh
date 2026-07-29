#!/usr/bin/env bash
# Run cosmic-ray against one module of the decorated-function mutation set.
#
# See docs/development/mutation-testing.md ("Two tools, two jobs") for why
# this exists alongside scripts/mutation.sh (mutmut): mutmut categorically
# cannot mutate a decorated function's body or removal of its decorator,
# which is exactly the shape of the code this script targets --
# `@event.listens_for` listeners today, `@handles`-decorated aggregate and
# projection handlers as they're added to the curated set.
#
# Usage:
#
#   scripts/mutation-cosmic-ray.sh engine    # cosmic-ray/engine.toml
#
# Each module needs its own cosmic-ray/<module>.toml (see engine.toml for
# the template) naming its `module-path` and a narrow `test-command`. This
# script does not fall back to a default -- there is no "all" mode, unlike
# scripts/mutation.sh's mutmut runner, because cosmic-ray's per-mutant
# subprocess cost makes a combined run slow enough that nobody would
# actually wait for it (~110s for 151 mutants against a ~130-line module in
# the spike that justified this tool -- see mutation-framework-spike.md).
# Add modules one at a time, run them one at a time.

set -euo pipefail
cd "$(dirname "$0")/.."

MODULE="${1:-}"
CONFIG="cosmic-ray/${MODULE}.toml"

if [[ -z "$MODULE" || ! -f "$CONFIG" ]]; then
    echo "usage: $0 <module>   (needs cosmic-ray/<module>.toml to exist)" >&2
    echo "available: $(ls cosmic-ray/*.toml 2>/dev/null | xargs -n1 basename | sed 's/\.toml$//' | tr '\n' ' ')" >&2
    exit 2
fi

SESSION="$(mktemp -u --suffix=.sqlite)"
cleanup() { rm -f "$SESSION"; }
trap cleanup EXIT

echo "=== cosmic-ray: $MODULE ==="
uv run cosmic-ray init "$CONFIG" "$SESSION"
time uv run cosmic-ray exec "$CONFIG" "$SESSION"
uv run cr-report "$SESSION"
