---
id: '0465'
title: Pin release and deploy workflow dependencies to immutable references
status: Complete
governing_adrs:
- ADR-0009
governing_prds:
- PRD-0001
governing_stories:
- US-0013
target_bc: core
priority_rank: 5
---

# TASK-0465: Pin release and deploy workflow dependencies to immutable references

## Summary
The publish jobs use actions pinned to a branch or a tag (`pypa/gh-action-pypi-publish@release/v1`, `softprops/action-gh-release@v3`, `actions/download-artifact@v8`). The build job installs `build` and `hatchling` without versions or hashes. The Pages deploy runs `spec-ops` from the default branch of another repository. A moved reference or a compromised upstream release runs code with OIDC and write permissions.

## Problem Statement & Context
- **Severity**: High
- **Weakness**: CWE-829 (inclusion of functionality from untrusted control sphere)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

`sha_pinning_required` is false for the repository. The build job output is the artifact that the publish job uploads, so a compromised build tool can change the released wheel. CI and compose files use mutable image tags (Low).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `.github/workflows/release.yml:114`, 120, 130, 136, 169
- `.github/workflows/release.yml:87`, 90 (pip install build; python -m build)
- `pyproject.toml:97` (requires = ["hatchling"])
- `.github/workflows/deploy-pages.yml:8-11`, 45 (git+ URL without a ref)
- `.github/workflows/ci.yml:177`, 212, 226 (redis:7, postgres:16)

**Verification**: Workflow files read; GitHub API `actions/permissions` returned `sha_pinning_required: false`.

## Definition of Done (Blackbox Frontdoor TDD)
1. Every `uses:` in all workflows names a full commit SHA, with the version in a comment. Dependabot keeps them current.
2. The build job installs build tools from a hash-locked requirements file, or uses `uv build` with the locked toolchain.
3. `hatchling` has a pinned version range in `build-system.requires`.
4. The Pages deploy pins `spec-ops` to a commit SHA or a release tag.
5. CI service images use digests.
6. A CI check (for example, a grep or `zizmor`) fails when a `uses:` line has no SHA.
7. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
