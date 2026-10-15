---
id: TASK-0352
title: "Compact 10 test modules at 260-261 lines below 235 lines"
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

# TASK-0352: Compact 10 test modules at 260-261 lines below 235 lines

## Summary
Compact 10 test modules (`test_redis_event_bus_consumer.py`, `test_query.py`, `test_coordination_topics_and_notifications.py`, `test_sync_lag_tracker_stats_and_history.py`, `test_metrics_and_status.py`, `test_lookups.py`, `test_exceptions.py`, `test_version_validation.py`, `test_catchup_batch_dispatch.py`, `test_consistency_verifier_versions_and_helpers.py`) strictly below 235 lines while preserving every test assertion.
