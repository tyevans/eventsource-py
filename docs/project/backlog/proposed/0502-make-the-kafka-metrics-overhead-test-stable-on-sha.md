---
id: '0502'
title: Make the Kafka metrics overhead test stable on shared CI runners
status: Proposed
governing_prds:
- PRD-0002
governing_stories:
- US-0002
target_bc: adapters
---

# TASK-0502: Make the Kafka metrics overhead test stable on shared CI runners

## Summary
`test_metrics_overhead_acceptable` fails on CI from timing noise, not from a regression.

## Problem Statement & Context
- **Source**: Discovered during the 0.16.0 release

The main CI run on `d6213af9` failed with "Metrics overhead 22.17% exceeds 20% threshold". The same source passed this test on the run before it, and the rerun passed. The test publishes one batch with metrics off and one batch with metrics on, and compares the two times. It has no warm-up, and the first bus also pays for topic creation and connection setup. One sample from each side on a shared runner gives a noisy result.

## Evidence
- `tests/integration/bus/test_kafka/test_metrics_perf.py`
- CI run 44235943489 (attempt 1 failed, attempt 2 passed)

## Definition of Done (Blackbox Frontdoor TDD)
1. The test does a warm-up publish on each bus before it measures.
2. The test measures several rounds for each side, and compares the medians (or alternates the two sides).
3. The 20% limit stays, or a change to the limit has a stated reason in the test.
4. Tests use public entry points only. All files stay under the line ceilings.
