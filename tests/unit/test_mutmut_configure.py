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
import tomllib
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


# --------------------------------------------------------------------------
# cosmic-ray configs -- the same fact, in a second tool's config format.
#
# The mutmut half of this file was written after three of its five selectors
# rotted. `cosmic-ray/*.toml` then rotted the same way and this file did not
# notice, because it only ever read the Python table: checkpoint.toml's
# module-path and all three of its test-command paths pointed into the
# deleted `repositories/` package. Same defect shape, one directory over.
# --------------------------------------------------------------------------

COSMIC_RAY_DIR = REPO_ROOT / "cosmic-ray"
COSMIC_RAY_CONFIGS = sorted(COSMIC_RAY_DIR.glob("*.toml"))


def _load_cosmic_ray_config(path: Path) -> dict[str, object]:
    config: dict[str, object] = tomllib.loads(path.read_text())
    section = config["cosmic-ray"]
    assert isinstance(section, dict)
    return section


def test_there_is_at_least_one_cosmic_ray_config() -> None:
    """A glob that silently matches nothing would make every test below vacuous."""
    assert COSMIC_RAY_CONFIGS, f"no cosmic-ray/*.toml found under {COSMIC_RAY_DIR}"


@pytest.mark.parametrize("config_path", COSMIC_RAY_CONFIGS, ids=lambda p: p.stem)
def test_cosmic_ray_module_path_exists(config_path: Path) -> None:
    module_path = _load_cosmic_ray_config(config_path)["module-path"]
    assert isinstance(module_path, str)
    assert (REPO_ROOT / module_path).is_file(), (
        f"{config_path.name} mutates {module_path}, which does not exist"
    )


@pytest.mark.parametrize("config_path", COSMIC_RAY_CONFIGS, ids=lambda p: p.stem)
def test_cosmic_ray_test_command_paths_exist(config_path: Path) -> None:
    """Every path-shaped token in `test-command` must resolve.

    Matching on the `tests/` prefix rather than parsing the shell string:
    the command is a plain string handed to a subprocess, so there is no
    structure to parse, and every selection this project writes is a
    `tests/`-rooted path.
    """
    command = _load_cosmic_ray_config(config_path)["test-command"]
    assert isinstance(command, str)

    test_paths = [token for token in command.split() if token.startswith("tests/")]
    assert test_paths, f"{config_path.name}'s test-command selects no tests/ path: {command!r}"

    for path in test_paths:
        assert (REPO_ROOT / path).exists(), f"{config_path.name} runs {path}, which does not exist"


@pytest.mark.parametrize("config_path", COSMIC_RAY_CONFIGS, ids=lambda p: p.stem)
def test_cosmic_ray_test_command_disables_coverage_and_randomization(
    config_path: Path,
) -> None:
    """`--no-cov` and `-p no:randomly` are correctness requirements, not style.

    Neither tool parses pytest output -- a mutant is "killed" purely by a
    nonzero exit code. If the coverage floor fired on a scoped subset, every
    mutant would report as killed and the run would show a perfect, entirely
    false score. See docs/development/mutation-testing.md.
    """
    command = _load_cosmic_ray_config(config_path)["test-command"]
    assert isinstance(command, str)

    assert "--no-cov" in command, (
        f"{config_path.name}'s test-command must pass --no-cov; without it a "
        "coverage-floor failure is indistinguishable from a killed mutant"
    )
    assert "-p no:randomly" in command, (
        f"{config_path.name}'s test-command must pass -p no:randomly; mutation "
        "runs have to be deterministic to classify survivors reliably"
    )
