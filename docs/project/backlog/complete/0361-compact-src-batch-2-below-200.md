---
id: TASK-0361
title: "Compact 9 src/ modules at 216-217 lines below 200 lines"
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

# TASK-0361: Compact 9 src/ modules at 216-217 lines below 200 lines

## Summary
Compact 9 `src/eventsource/` modules (`sqlite/readmodels_query.py`, `sql/dlq.py`, `conformance_ports/dlq.py`, `subscriptions/shutdown_runner.py`, `runners/live_drain.py`, `runners/live_delivery.py`, `subscriptions/health_subscription_indicators.py`, `migration/router_resolution.py`, `migration/cutover_execution.py`) strictly below 200 lines.
