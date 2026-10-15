---
id: TASK-0351
title: "Compact 8 src/ modules at 220-223 lines below 210 lines"
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

# TASK-0351: Compact 8 src/ modules at 220-223 lines below 210 lines

## Summary
Compact 8 `src/eventsource/` modules (`migration/metrics_types.py`, `testing/scenario.py`, `health_models.py`, `projections/replay.py`, `postgresql/store_read.py`, `conformance_ports/bus.py`, `rabbitmq/publisher_many.py`, `postgresql/outbox_queries.py`) strictly below 210 lines while preserving public contracts and 100% test pass rate.
