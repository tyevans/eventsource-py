---
id: TASK-0350
title: "Compact 9 src/ modules at 224-227 lines below 210 lines"
status: Complete
target_bc: application
prd: docs/project/product/accepted/PRD-0008-ergonomic-aggregate-testing-and-compact-adapters.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0008-ergonomic-aggregate-testing-and-compact-adapters.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-500-line-file-limit.md
  - docs/project/adrs/accepted/ADR-0003-blackbox-testing.md
  - docs/project/adrs/accepted/ADR-0009-property-and-mutation-testing.md
dependencies: []
---

# TASK-0350: Compact 9 src/ modules at 224-227 lines below 210 lines

## Summary
Compact 9 `src/eventsource/` modules (`consistency_stream_checker.py`, `sqlite/snapshots.py`, `sqlite/outbox.py`, `rabbitmq/bus_health.py`, `subscription_migrator_types.py`, `manager_lifecycle.py`, `error_handling_classifier.py`, `bulk_copier.py`, `error_classification.py`) strictly below 210 lines while preserving public contracts and 100% test pass rate.
