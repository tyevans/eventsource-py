---
id: '0477'
title: Scope workflow token permissions to the jobs that need them
status: Proposed
governing_adrs:
- ADR-0009
governing_prds:
- PRD-0001
governing_stories:
- US-0013
target_bc: core
priority_rank: 17
---

# TASK-0477: Scope workflow token permissions to the jobs that need them

## Summary
`release.yml` grants `contents: write` and `id-token: write` at workflow level, so the validate and build jobs can mint OIDC tokens and write to the repository. `adr-check.yml` has no permissions block.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-732 (incorrect permission assignment for critical resource)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The repository default token is read-only, so `adr-check.yml` is Info. The release workflow is the main issue.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `.github/workflows/release.yml:16-18`
- `.github/workflows/adr-check.yml:1-38`

**Verification**: Workflow files read; GitHub API `default_workflow_permissions: read`.

## Definition of Done (Blackbox Frontdoor TDD)
1. `release.yml` sets `permissions: {}` at top level.
2. Only the publish jobs have `id-token: write`. Only the GitHub release job has `contents: write`.
3. `adr-check.yml` declares `permissions: contents: read`.
4. A tag push to TestPyPI (or a dry run) still succeeds.
5. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
