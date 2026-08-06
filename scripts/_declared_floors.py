"""Read the declared dependency floors out of pyproject.toml, and check an
environment against them.

This exists so that `scripts/check_dependency_floors.sh` does not carry its own
copy of the dependency list. A hardcoded list is the defect shape in
`.claude/rules/recurring-defects.md` section 2: it would go stale the first
time an extra was added, and the gate would quietly stop measuring the new one
while still reporting success.

Two subcommands:

    extras            print the extras to install, comma-separated
    check <freeze>    verify a `uv pip freeze` output pins every declared
                      distribution to exactly its `>=` bound

`check` is what makes the gate honest. Without it, a resolver that quietly
served a *newer* version than the floor -- because of a transitive constraint,
a yanked release, or a wheel missing for this interpreter -- would produce a
green run that proved nothing about the floor.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
PYPROJECT = REPO_ROOT / "pyproject.toml"

# Extras that install contributor tooling or documentation rather than the
# runtime dependencies this package publishes a contract for. `all` and
# `all-backends` are aggregates of extras already covered individually;
# installing them would add nothing and make the failure message ambiguous.
NOT_RUNTIME = {"dev", "docs", "benchmark", "all", "all-backends"}

_NAME = re.compile(r"^\s*([A-Za-z0-9][A-Za-z0-9._-]*)")
_FLOOR = re.compile(r">=\s*([0-9][^,;\s\]]*)")


def _canonical(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def _release(version: str) -> tuple[int, ...]:
    """The numeric release tuple, trailing zeros stripped.

    `>=8.0` and an installed `8.0.0` are the same version; comparing the
    strings says otherwise and would fail the gate on a correct resolution.
    Non-numeric segments (`1.0.post1`, `2.0b1`) stop the parse -- this is only
    ever comparing a declared floor against what a resolver pinned to it, and
    both are plain releases.
    """
    parts: list[int] = []
    for segment in version.split("."):
        if not segment.isdigit():
            break
        parts.append(int(segment))
    while parts and parts[-1] == 0:
        parts.pop()
    return tuple(parts)


def _load() -> tuple[dict, dict]:
    data = tomllib.loads(PYPROJECT.read_text())
    return data["project"], data["project"].get("optional-dependencies", {})


def runtime_extras() -> list[str]:
    _, optional = _load()
    return [name for name in optional if name not in NOT_RUNTIME]


def declared_floors() -> dict[str, str]:
    """Canonical distribution name -> declared `>=` bound.

    Specs without a `>=` bound are skipped rather than treated as an error:
    a dependency may legitimately be declared without a floor. Specs naming
    this package itself (the `all` aggregates) are skipped too.
    """
    project, optional = _load()
    specs = list(project.get("dependencies", []))
    for name in runtime_extras():
        specs.extend(optional[name])

    floors: dict[str, str] = {}
    for spec in specs:
        name_match = _NAME.match(spec)
        floor_match = _FLOOR.search(spec)
        if name_match is None or floor_match is None:
            continue
        name = _canonical(name_match.group(1))
        if name == _canonical(project["name"]):
            continue
        floors[name] = floor_match.group(1)
    return floors


def check(freeze_path: str) -> int:
    installed: dict[str, str] = {}
    for line in Path(freeze_path).read_text().splitlines():
        if "==" not in line or line.startswith("-"):
            continue
        name, _, version = line.partition("==")
        installed[_canonical(name.strip())] = version.strip()

    floors = declared_floors()
    if not floors:
        print("no `>=` bounds found in pyproject.toml -- the parser is wrong", file=sys.stderr)
        return 2

    problems = []
    for name, floor in sorted(floors.items()):
        actual = installed.get(name)
        if actual is None:
            problems.append(f"  {name}: declared >={floor}, but not installed at all")
        elif _release(actual) != _release(floor):
            problems.append(f"  {name}: declared >={floor}, but resolved to {actual}")
        else:
            print(f"  {name}=={actual}")

    if problems:
        print(
            "\nthe floor resolution did not land on the declared bounds, so a green\n"
            "test run below would not prove anything about them:",
            file=sys.stderr,
        )
        print("\n".join(problems), file=sys.stderr)
        return 1
    return 0


def main(argv: list[str]) -> int:
    if len(argv) >= 2 and argv[1] == "extras":
        print(",".join(runtime_extras()))
        return 0
    if len(argv) >= 3 and argv[1] == "check":
        return check(argv[2])
    print(__doc__, file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
