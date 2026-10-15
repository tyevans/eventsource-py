---
id: "0194"
title: Retire Stale Grandfathered File Debt Baseline and Enforce Zero-Warning Health Gate
status: Complete
target_bc: core
governing_adrs:
  - ADR-0002
  - ADR-0004
governing_prds:
  - PRD-0001
governing_stories:
  - US-0012
  - US-0013
priority: P2
---

# TASK-0194: Retire Stale Grandfathered File Debt Baseline and Enforce Zero-Warning Health Gate

## Summary
Remove all 45 retired grandfathered file entries from `specops.toml` (`[invariants.file_limits] grandfathered`) and `.specops/grandfathered_debt.json` now that 100% of production and test modules are strictly under 400 lines.

## Required Actions
1. Clear `grandfathered = []` in `specops.toml` and `files = {}` in `.specops/grandfathered_debt.json`.
2. Verify `uv run spec-ops health` passes with 0 violations, 0 warnings, and 0 grandfathered exemptions.

## Definition of Done (Blackbox Frontdoor TDD)
1. Zero grandfathered file exemptions remain in `specops.toml` and `.specops/grandfathered_debt.json`.
2. `uv run spec-ops health` reports 0 violations and 0 proactive warnings.
