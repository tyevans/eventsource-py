#!/usr/bin/env python3
"""Run cosmic-ray restricted to a specific operator allowlist.

Ad-hoc script, not wired into scripts/mutation-cosmic-ray.sh. The plain CLI
(`cosmic-ray init`) always enumerates every one of the ~213 core operators
against the whole AST of `module-path` -- there is no config knob to
restrict *which* operators get discovered, only to parameterize the ones
that take arguments (see cosmic_ray/commands/init.py:_operators). Against
checkpoint.py's ~800 lines that discovery pass alone was too slow to finish
in a previous session. This script monkeypatches
`cosmic_ray.plugins.operator_names` to return only the requested operators
before calling `init`, then runs `execute` and prints a report -- same
config file (`cosmic-ray/checkpoint.toml`), same test-command, just a
much smaller work-item set.

Usage:
    uv run python scripts/_cosmic_ray_scoped.py cosmic-ray/checkpoint.toml core/RemoveDecorator
"""

import importlib
import sys
import tempfile
from pathlib import Path

import cosmic_ray.modules
import cosmic_ray.plugins as plugins
from cosmic_ray.config import load_config
from cosmic_ray.work_db import WorkDB, use_db

# cosmic_ray/commands/__init__.py does `from .init import init` /
# `from .execute import execute`, which rebinds the `init` / `execute`
# attributes on the `cosmic_ray.commands` package to the functions -- so
# `import cosmic_ray.commands.init as init_mod` resolves to the function,
# not the submodule. Load the submodules directly to get `.init`/`.execute`.
init_mod = importlib.import_module("cosmic_ray.commands.init")
execute_mod = importlib.import_module("cosmic_ray.commands.execute")


def main(argv):
    config_file, *operator_names = argv
    if not operator_names:
        print(
            "usage: _cosmic_ray_scoped.py <config.toml> <operator> [operator...]", file=sys.stderr
        )
        return 2

    orig_operator_names = plugins.operator_names
    plugins.operator_names = lambda: tuple(operator_names)

    cfg = load_config(config_file)
    module_paths = (
        [Path(cfg["module-path"])]
        if isinstance(cfg["module-path"], str)
        else list(map(Path, cfg["module-path"]))
    )
    modules = cosmic_ray.modules.find_modules(module_paths)
    modules = cosmic_ray.modules.filter_paths(modules, cfg.get("excluded-modules", ()))

    session_file = tempfile.mktemp(suffix=".sqlite")
    try:
        with use_db(session_file, WorkDB.Mode.create) as db:
            init_mod.init(modules, db, cfg.operators_config)
            print(f"Discovered {db.num_work_items} work item(s) for operators: {operator_names}")
            execute_mod.execute(db, cfg)

            killed = survived = other = 0
            for item, result in db.completed_work_items:
                outcome = result.test_outcome
                if outcome is not None and outcome.value == "killed":
                    killed += 1
                elif outcome is not None and outcome.value == "survived":
                    survived += 1
                    for m in item.mutations:
                        print(
                            f"SURVIVED: {m.operator_name} occurrence={m.occurrence} "
                            f"{m.module_path}:{m.start_pos}-{m.end_pos} def={m.definition_name}"
                        )
                else:
                    other += 1
                    print(
                        f"OTHER({result.worker_outcome}): job={item.job_id} "
                        f"mutations={[(m.operator_name, m.occurrence) for m in item.mutations]} "
                        f"output={(result.output or '')[:300]!r}"
                    )
            print(
                f"\nTotal: {killed + survived + other}  killed={killed} survived={survived} other={other}"
            )
    finally:
        plugins.operator_names = orig_operator_names
        Path(session_file).unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]) or 0)
