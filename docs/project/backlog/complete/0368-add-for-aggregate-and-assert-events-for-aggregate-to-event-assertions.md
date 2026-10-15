---
id: TASK-0368
title: "Add for_aggregate(aggregate_id) and assert_events_for_aggregate() to EventAssertions"
status: Complete
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

# TASK-0368: Add for_aggregate(aggregate_id) and assert_events_for_aggregate() to EventAssertions

## Summary
Add `for_aggregate(self, aggregate_id: UUID) -> EventAssertions` (returning a scoped `EventAssertions` view over events for `aggregate_id`) and `assert_events_for_aggregate(self, aggregate_id: UUID, expected_count: int | None = None) -> list[DomainEvent]` to `EventAssertions` in `src/eventsource/testing/assertions.py`.
