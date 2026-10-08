# ADR-0004: Continuous Pre-flight Verification and Self-Healing CI Loops

## Status
Accepted

## Context
Autonomous coding agents operating in git worktrees can push unverified code or fail CI checks without realizing it, abandoning broken branches in pull request limbo.

## Decision
We establish automated **Pre-flight Verification and In-Worktree CI Healing**:
1. Before any commit or PR is dispatched, workers run local preflight checks (`make preflight` / `spec-ops health`).
2. If preflight fails, the agent is prompted with failure diagnostics up to 3 repair attempts.
3. When pull requests fail remote CI checks, the orchestrator captures failed job logs via CLI (`gh run view --log-failed`) and prompts the agent directly in the isolated worktree to repair the failure before re-pushing.

## Consequences
- **Positive**: Branches merged to `main` have an extraordinarily high pass rate; CI failures are treated as immediate feedback loops rather than discarded work.
- **Negative**: Requires robust local tooling and access to GitHub CLI.
