---
id: TASK-0459
title: "Cut release v0.15.0"
status: Complete
target_bc: architecture
allows_dependencies: true
allows_lockfile_mutation: true
prd: docs/project/product/accepted/PRD-0001-core-event-sourcing-engine.md
persona: docs/project/user_stories/PERSONAS.md
story: docs/project/user_stories/accepted/US-0001-domain-aggregates-and-events.md
adrs:
  - docs/project/adrs/accepted/ADR-0001-spec-ops-pmac.md
  - docs/project/adrs/accepted/ADR-0002-file-length-limit.md
  - docs/project/adrs/accepted/ADR-0005-backlog-isolation.md
  - docs/project/adrs/accepted/ADR-0011-lockfile-immutability.md
depends_on:
  - TASK-0458
---

# TASK-0459: Cut release v0.15.0

## Summary
Prepare and publish release `v0.15.0` of `eventsource-py`:
- Bump `pyproject.toml` and `uv.lock` package version from `0.14.0` to `0.15.0`.
- Author the `[0.15.0] - 2026-10-15` release notes in `CHANGELOG.md` and update comparison links.
- Update version references in `docs/faq.md` and `docs/installation.md`.
- Merge release PR and push annotated tag `v0.15.0` to trigger the GitHub Actions release pipeline.
