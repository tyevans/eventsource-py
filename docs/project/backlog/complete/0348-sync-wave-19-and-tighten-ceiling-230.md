---
id: TASK-0348
title: "Sync Wave 19 backlog, fix migration mypy typing, and tighten line ceiling to <230 src / <262 tests"
status: Complete
target_bc: architecture
prd: docs/project/product/accepted/PRD-0008-ergonomic-aggregate-testing-and-compact-adapters.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0008-ergonomic-aggregate-testing-and-compact-adapters.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-500-line-file-limit.md
  - docs/project/adrs/accepted/ADR-0005-multi-agent-worktree-concurrency.md
dependencies: []
---

# TASK-0348: Sync Wave 19 backlog, fix migration mypy typing, and tighten line ceiling to <230 src / <262 tests

## Summary
Synchronize Wave 19 completed tasks (`TASK-0338`..`TASK-0347`) to `complete/`, fix mypy type inference in `subscription_migrator_execution.py` and `coordinator_cutover.py`, tighten `MAX_SRC_LINES = 230` and `MAX_TEST_EXAMPLE_LINES = 262`, and refine Wave 20 (`TASK-0349`..`TASK-0358`).
