---
id: TASK-0360
title: "Compact 9 src/ modules at 217-219 lines below 200 lines"
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

# TASK-0360: Compact 9 src/ modules at 217-219 lines below 200 lines

## Summary
Compact 9 `src/eventsource/` modules (`subscriptions/shutdown.py`, `subscriptions/filtering.py`, `redis/bus_dlq.py`, `runners/live_runner.py`, `rabbitmq/connection.py`, `ports/bus.py`, `migration/write_pause_manager.py`, `migration/coordinator_post_cutover.py`, `aggregates/snapshotting.py`) strictly below 200 lines.
