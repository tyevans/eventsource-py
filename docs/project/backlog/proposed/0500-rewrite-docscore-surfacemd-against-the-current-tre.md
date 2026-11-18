---
id: '0500'
title: Rewrite docs/core-surface.md against the current tree
status: Proposed
governing_adrs:
- ADR-0107
governing_prds:
- PRD-0004
governing_stories:
- US-0004
target_bc: docs
---

# TASK-0500: Rewrite docs/core-surface.md against the current tree

## Summary
`docs/core-surface.md` describes module paths and couplings from before the ring migration. Parts of it are wrong.

## Problem Statement & Context
- **Source**: Discovered during the 2026-11-17 security audit remediation

Found during the 2026-11-17 audit remediation. The coupling table, boundary finding 10 and rule 7 refer to removed paths such as `_internal/background_tasks` and `testing/conformance.py` as the only conformance module.

## Evidence
- `docs/core-surface.md`

## Definition of Done (Blackbox Frontdoor TDD)
1. Each path and symbol in the page exists in the tree. A whole-tree grep for each removed path comes back clean.
2. `mkdocs build --strict` passes.
3. Tests use public entry points only. All files stay under the line ceilings.
