---
id: TASK-0362
title: "Compact 10 src/ modules at 213-215 lines below 200 lines"
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

# TASK-0362: Compact 10 src/ modules at 213-215 lines below 200 lines

## Summary
Compact 10 `src/eventsource/` modules (`postgresql/locks.py`, `memory/snapshots.py`, `kafka/config.py`, `kafka/bus.py`, `ports/dlq.py`, `migration/coordinator_cutover.py`, `rabbitmq/dlq.py`, `conformance_ports/readmodels.py`, `runners/catchup_batch.py`, `projections/handlers.py`) strictly below 200 lines.
