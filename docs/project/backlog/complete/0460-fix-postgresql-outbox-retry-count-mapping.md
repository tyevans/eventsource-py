---
id: TASK-0460
title: "Fix PostgreSQLOutboxRepository.get_pending_events retry_count mapping and cut v0.15.1"
status: Complete
target_bc: adapters
allows_dependencies: true
allows_lockfile_mutation: true
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0001-domain-aggregates-and-events.md
adrs:
  - docs/project/adrs/accepted/ADR-0002-file-length-limit.md
  - docs/project/adrs/accepted/ADR-0005-backlog-isolation.md
  - docs/project/adrs/accepted/ADR-0011-lockfile-immutability.md
depends_on:
  - TASK-0459
---

# TASK-0460: Fix PostgreSQLOutboxRepository.get_pending_events retry_count mapping and cut v0.15.1

## Summary
- Fix `PostgreSQLOutboxRepository.get_pending_events()` so `retry_count` is passed by keyword (`retry_count=r[8] or 0`) rather than positionally into `published_at`.
- Add unit test coverage in `tests/unit/adapters/test_postgresql_outbox.py` verifying `retry_count` and `published_at` mapping.
- Update `README.md` Python version badge to `3.13+` and cut patch release `v0.15.1`.
