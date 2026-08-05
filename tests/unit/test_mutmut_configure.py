"""The mutation-testing selector table must point at paths that exist.

`scripts/_mutmut_configure.py` names source modules and test subsets as
strings, which no import resolves and no type checker follows. Three of its
five entries had been pointing at `src/eventsource/repositories/` -- a
package deleted during the rings campaign -- plus four test files that had
moved. Nothing failed, because the table is only read by `mutation.sh`,
which nothing in CI runs.

That is recurring defect shape #1 in miniature: one fact (where a module
lives) recorded in a second place, with nothing that fails when the copies
disagree. This is the thing that fails.
"""

from __future__ import annotations

import importlib.util
import re
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIGURE = REPO_ROOT / "scripts" / "_mutmut_configure.py"


def _load_modules_table() -> dict[str, tuple[str, str | list[str]]]:
    """Import the script by path -- `scripts/` is not an importable package."""
    spec = importlib.util.spec_from_file_location("_mutmut_configure", CONFIGURE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    table: dict[str, tuple[str, str | list[str]]] = module.MODULES
    return table


MODULES = _load_modules_table()


@pytest.mark.parametrize("name", sorted(MODULES))
def test_source_path_exists(name: str) -> None:
    source_path, _ = MODULES[name]
    assert (REPO_ROOT / source_path).is_file(), (
        f"mutmut selector {name!r} mutates {source_path}, which does not exist"
    )


def test_shell_selector_list_matches_the_python_table() -> None:
    """`mutation.sh`'s VALID array is the same fact, written in bash."""
    script = (REPO_ROOT / "scripts" / "mutation.sh").read_text()
    match = re.search(r"^VALID=\(([^)]*)\)", script, re.MULTILINE)
    assert match is not None, "could not find VALID=(...) in scripts/mutation.sh"

    valid = set(match.group(1).split())
    assert valid - {"all"} == set(MODULES)


@pytest.mark.parametrize("name", sorted(MODULES))
def test_test_selection_paths_exist(name: str) -> None:
    _, selection = MODULES[name]
    paths = [selection] if isinstance(selection, str) else selection
    for path in paths:
        assert (REPO_ROOT / path).exists(), (
            f"mutmut selector {name!r} runs {path}, which does not exist"
        )
