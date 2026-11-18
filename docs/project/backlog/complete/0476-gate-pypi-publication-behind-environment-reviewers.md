---
id: '0476'
title: Gate PyPI publication behind environment reviewers and a validated tag
status: Complete
governing_adrs:
- ADR-0009
governing_prds:
- PRD-0001
governing_stories:
- US-0013
target_bc: core
priority_rank: 16
---

# TASK-0476: Gate PyPI publication behind environment reviewers and a validated tag

## Summary
Any push of a `v*` tag publishes to PyPI with no human approval. The `pypi` and `testpypi` environments have no required reviewers and no branch or tag policy, and no tag ruleset exists. The tag value also goes into shell scripts without validation.

## Problem Statement & Context
- **Severity**: Medium (High if the PyPI trusted publisher has no environment restriction)
- **Weakness**: CWE-345 (insufficient verification of data authenticity)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

RELEASING.md:195-197 says that human approval is necessary before the release tag is pushed. Nothing enforces it. A stolen maintainer token, or any account with write access, can tag any commit and publish. The workflow does not check that the tagged commit is on `main`. Part of the fix is a GitHub settings change that only the maintainer can make. Also confirm that the PyPI trusted publisher names the `release.yml` workflow and the `pypi` environment.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `.github/workflows/release.yml:11-14` (on push tags v*)
- `.github/workflows/release.yml:111`, 127 (environments pypi, testpypi)
- `.github/workflows/release.yml:35-36` (no regex check on the tag)
- `.github/workflows/release.yml:62-63`, 158, 200 (`${{ steps.version.outputs.version }}` in `run:`)
- `RELEASING.md:195-197`
- `GitHub` API (GET, 2026-11-17): environments/pypi protection_rules=[], deployment_branch_policy=null, can_admins_bypass=true; rulesets: only "Main" (branch); no tag ruleset

**Verification**: GitHub API GET results above. `git check-ref-format` accepts tag names such as `v1.0.0$(id)`.

## Definition of Done (Blackbox Frontdoor TDD)
1. Maintainer action: the `pypi` and `testpypi` environments have required reviewers and a deployment policy that allows only `v*` tags.
2. Maintainer action: a tag ruleset limits who can create `v*` tags.
3. The workflow rejects a tag that does not match `^v[0-9]+\.[0-9]+\.[0-9]+((a|b|rc)[0-9]+)?$` before any other step.
4. The workflow passes the version through `env:` and uses `"$VERSION"` in scripts.
5. The workflow fails when the tagged commit is not an ancestor of `origin/main`.
6. RELEASING.md describes the enforced gate.
7. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
