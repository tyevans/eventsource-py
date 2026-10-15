---
id: TASK-0349
title: "Compact 8 src/ modules at 227-229 lines below 210 lines"
status: Refined
target_bc: adapters
prd: docs/project/product/accepted/PRD-0008-ergonomic-aggregate-testing-and-compact-adapters.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0008-ergonomic-aggregate-testing-and-compact-adapters.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-500-line-file-limit.md
  - docs/project/adrs/accepted/ADR-0003-blackbox-testing.md
  - docs/project/adrs/accepted/ADR-0009-property-and-mutation-testing.md
dependencies: []
---

# TASK-0349: Compact 8 src/ modules at 227-229 lines below 210 lines

## Summary
Compact 8 `src/eventsource/` modules (`dlq_replay.py`, `postgresql/snapshots.py`, `readmodel_filters.py`, `models/routing.py`, `sql/readmodel_projection.py`, `sql/projection.py`, `kafka/publisher.py`, `aggregate_declarative.py`) strictly below 210 lines while preserving public contracts and 100% test pass rate.
