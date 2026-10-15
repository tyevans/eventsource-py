---
id: TASK-0355
title: "Compact 10 test and example modules at 257 lines below 235 lines"
status: Refined
target_bc: testing
prd: docs/project/product/accepted/PRD-0008-ergonomic-aggregate-testing-and-compact-adapters.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0008-ergonomic-aggregate-testing-and-compact-adapters.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-500-line-file-limit.md
  - docs/project/adrs/accepted/ADR-0003-blackbox-testing.md
  - docs/project/adrs/accepted/ADR-0009-property-and-mutation-testing.md
dependencies: []
---

# TASK-0355: Compact 10 test and example modules at 257 lines below 235 lines

## Summary
Compact 10 test and example modules (`test_decorators.py`, `test_subscription_metrics.py`, `test_subscription_error_handler.py`, `test_recovery_and_scenarios.py`, `test_repository_snapshot_background.py`, `test_combined_resilience.py`, `test_live.py`, `test_checkpoint.py`, `examples/subscriptions/resilient_projection/runner.py`, `examples/aggregate_example.py`) strictly below 235 lines while preserving every test assertion.
