---
id: 0488
title: Enforce the declared secret scanning and narrow agent permissions
status: Proposed
governing_adrs:
- ADR-0008
- ADR-0010
governing_prds:
- PRD-0004
governing_stories:
- US-0013
target_bc: core
priority_rank: 28
---

# TASK-0488: Enforce the declared secret scanning and narrow agent permissions

## Summary
`specops.toml` declares `secret_scanning = true` and `sandbox_enabled = true`, but no secret scanner runs in pre-commit or CI. The configured agent command disables permission prompts. `.claude/settings.json` allows `uv run python:*` and `git branch:*` without a prompt. No CODEOWNERS file protects `.github/`, `.claude/` or `specops.toml`.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-250 (execution with unnecessary privileges)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

The agent runs on the maintainer's machine, so the risk is prompt injection from backlog or issue text into an agent that has no permission prompts.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `specops.toml:51`, 57, 60, 61
- `.pre-commit-config.yaml:58` (bandit only); .github/workflows/ci.yml:128-129
- `.claude/settings.json:5`, 6, 15

**Verification**: grep for gitleaks, trufflehog and detect-secrets in the repository returns nothing.

## Definition of Done (Blackbox Frontdoor TDD)
1. A secret scanner (for example gitleaks) runs in pre-commit and in CI, or the `secret_scanning` flag is removed.
2. The agent command keeps permission prompts, or `specops.toml` documents the accepted risk.
3. `.claude/settings.json` allows only the specific commands the project needs.
4. A CODEOWNERS file covers `.github/`, `.claude/` and `specops.toml`.
5. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
