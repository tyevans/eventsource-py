---
id: 0489
title: Limit the sdist to the files a source build needs
status: Complete
governing_adrs:
- ADR-0009
governing_prds:
- PRD-0001
governing_stories:
- US-0013
target_bc: core
priority_rank: 29
---

# TASK-0489: Limit the sdist to the files a source build needs

## Summary
`pyproject.toml` configures only the wheel target. The default hatchling sdist ships every tracked file, including `.claude/`, `.github/`, `specops.toml`, `cosmic-ray/`, `bench/` and about 1000 test files. No secrets were found, but the package publishes internal tool configuration.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-538 (insertion of sensitive information into externally-accessible file)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

`aio-pika` also has no upper version bound (`pyproject.toml:51`); the lock pins it (Info).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `pyproject.toml:100-101`
- `pyproject.toml:51`

**Verification**: pyproject.toml read; hatchling default sdist behavior.

## Definition of Done (Blackbox Frontdoor TDD)
1. A `[tool.hatch.build.targets.sdist]` section lists the included paths.
2. The release workflow lists the sdist contents and fails when an excluded path appears.
3. `aio-pika` has an upper bound like the other optional dependencies.
4. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
