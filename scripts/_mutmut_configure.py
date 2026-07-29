#!/usr/bin/env python3
"""
Rewrites the `[tool.mutmut]` section of `pyproject.toml` in place, scoped to
one module of the curated mutation-testing set (or all three combined).

Used by `scripts/mutation.sh` rather than invoked directly. mutmut 3.x reads
its config once from `pyproject.toml` at process start and has no CLI flag
to point it at a different file or override `only_mutate` /
`pytest_add_cli_args_test_selection` per invocation, so per-module test
scoping (see docs/development/mutation-testing.md) has to happen by
rewriting the config file between runs. `mutation.sh` backs up and restores
`pyproject.toml` around every invocation of this script, so a run that dies
mid-way never leaves the checked-in config mutated.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# module name -> (file to mutate, test subset to run against every mutant)
MODULES: dict[str, tuple[str, str]] = {
    "engine": ("src/eventsource/engine.py", "tests/unit/test_engine.py"),
    "dialect": (
        "src/eventsource/repositories/_dialect.py",
        "tests/unit/repositories/test_dialect.py",
    ),
    "json": ("src/eventsource/serialization/json.py", "tests/unit/serialization/"),
}

BLOCK_RE = re.compile(r"\[tool\.mutmut\].*?(?=\n\[|\Z)", re.DOTALL)


def build_block(only_mutate: list[str], test_selection: list[str]) -> str:
    def fmt(items: list[str]) -> str:
        inner = ",\n".join(f'    "{item}"' for item in items)
        return "[\n" + inner + ",\n]"

    return (
        "[tool.mutmut]\n"
        f"source_paths = {fmt(['src'])}\n"
        f"only_mutate = {fmt(only_mutate)}\n"
        f"pytest_add_cli_args_test_selection = {fmt(test_selection)}\n"
        f'pytest_add_cli_args = ["--no-cov", "-x", "-q", "-p", "no:randomly"]\n'
    )


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in (*MODULES, "all"):
        names = ", ".join((*MODULES, "all"))
        print(f"usage: {sys.argv[0]} <{names}>", file=sys.stderr)
        raise SystemExit(2)

    target = sys.argv[1]
    if target == "all":
        only_mutate = [f for f, _ in MODULES.values()]
        test_selection = [t for _, t in MODULES.values()]
    else:
        f, t = MODULES[target]
        only_mutate = [f]
        test_selection = [t]

    path = Path("pyproject.toml")
    text = path.read_text()
    new_block = build_block(only_mutate, test_selection)
    if BLOCK_RE.search(text):
        text = BLOCK_RE.sub(new_block.rstrip("\n"), text, count=1)
    else:
        text = text.rstrip("\n") + "\n\n" + new_block
    path.write_text(text)


if __name__ == "__main__":
    main()
