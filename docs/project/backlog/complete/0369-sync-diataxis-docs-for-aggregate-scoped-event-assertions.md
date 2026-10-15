---
id: TASK-0369
title: "Sync Diataxis testing guide for EventAssertions.for_aggregate and assert_events_for_aggregate"
status: Complete
target_bc: documentation
prd: docs/project/product/accepted/PRD-0008-ergonomic-aggregate-testing-and-compact-adapters.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0008-ergonomic-aggregate-testing-and-compact-adapters.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-500-line-file-limit.md
  - docs/project/adrs/accepted/ADR-0003-blackbox-testing.md
  - docs/project/adrs/accepted/ADR-0009-property-and-mutation-testing.md
dependencies: []
---

# TASK-0369: Sync Diataxis testing guide for EventAssertions.for_aggregate and assert_events_for_aggregate

## Summary
Update `docs/guides/testing.md` to document `assertions.for_aggregate(aggregate_id)` and `assertions.assert_events_for_aggregate(aggregate_id, expected_count=...)`.
