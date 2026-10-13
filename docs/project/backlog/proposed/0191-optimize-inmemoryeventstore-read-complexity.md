---
id: "0191"
title: Optimize InMemoryEventStore Read Complexity with Category Index and Binary Search
status: Proposed
target_bc: adapters
governing_adrs:
  - ADR-0002
  - ADR-0009
  - ADR-0105
governing_prds:
  - PRD-0001
governing_stories:
  - US-0002
  - US-0010
priority: P2
---

# TASK-0191: Optimize InMemoryEventStore Read Complexity with Category Index and Binary Search

## Summary
Improve `InMemoryEventStore` read performance by maintaining an O(1) category index (`_categories: dict[str, list[int]]`) and using `bisect` binary search on monotonic global positions and stream versions instead of full O(N) list copies in `_do_read_all`, `_do_read_stream`, and `_do_read_category`.

## Required Actions
1. Maintain `_categories: dict[str, list[int]]` on `append()` in `src/eventsource/adapters/memory/store.py` so `read_category` and category-filtered `read_all` iterate only matching category indexes instead of scanning every event in the store.
2. Use `bisect` binary search on `from_position` and `from_version`/`to_version` in `_do_read_all`, `_do_read_stream`, and `_do_read_category` to avoid O(N) scans and unnecessary list copies.
3. Add Hypothesis property tests verifying exact behavioral equivalence with `AppenderConformance`, `StreamReaderConformance`, `GlobalFeedConformance`, and `CategoryQueryConformance`.

## Definition of Done (Blackbox Frontdoor TDD)
1. `read_all`, `read_stream`, and `read_category` use binary search and category index lookups without O(N) full-store list copies.
2. All conformance and property tests pass cleanly.
