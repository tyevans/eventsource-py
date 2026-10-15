---
id: TASK-0354
title: "Compact 10 test modules at 257-259 lines below 235 lines"
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

# TASK-0354: Compact 10 test modules at 257-259 lines below 235 lines

## Summary
Compact 10 test modules (`test_append_read.py`, `test_kafka_event_bus_core.py`, `test_exceptions_snapshots.py`, `test_projections_and_domain_coverage.py`, `test_subscription_migrator_models_and_workflows.py`, `test_phase3_edge_cases.py`, `test_coordinator_lifecycle.py`, `test_mappings_audit_and_constraints.py`, `test_base.py`, `test_rabbitmq_shutdown.py`) strictly below 235 lines while preserving every test assertion.
