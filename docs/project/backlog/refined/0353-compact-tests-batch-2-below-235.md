---
id: TASK-0353
title: "Compact 10 test modules at 259-260 lines below 235 lines"
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

# TASK-0353: Compact 10 test modules at 259-260 lines below 235 lines

## Summary
Compact 10 test modules (`test_sqlite_outbox_standalone.py`, `test_protocol_and_crud.py`, `test_schema_helpers.py`, `test_leadership_lease.py`, `_anchor_base.py`, `test_bulk_copy_and_dual_write.py`, `test_consistency_verifier_init_and_tenant.py`, `test_concurrency.py`, `test_contract.py`, `test_catchup.py`) strictly below 235 lines while preserving every test assertion.
