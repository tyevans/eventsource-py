---
id: '0002'
title: Delta-Compress Stored Event Payloads
status: Refined
created: 2026-10-07
governing_adrs:
  - ADR-0001
  - ADR-0002
governing_prds:
  - PRD-0001
governing_stories:
  - US-0002
target_bc: adapters
---

# TASK-0002: Delta-Compress Stored Event Payloads

## Summary
Event payloads are stored verbatim. When aggregates retain whole document contents across frequent revisions, storage scales with (size * revisions). This task explores opt-in delta compression using zstd dictionary chains below the snapshot boundary (BACKLOG item).

## Definition of Done
1. Investigate and benchmark zstd dictionary compression for sequential stream payloads.
2. Ensure delta layer remains invisible to domain model and projections.
3. Validate storage round-trip integrity with content hash verification.
4. All modified files strictly under 500 lines limit.
