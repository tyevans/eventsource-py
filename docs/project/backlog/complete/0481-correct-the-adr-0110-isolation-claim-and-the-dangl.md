---
id: 0481
title: Correct the ADR-0110 isolation claim and the dangling multi-tenant guide references
status: Complete
governing_adrs:
- ADR-0110
governing_prds:
- PRD-0002
governing_stories:
- US-0005
target_bc: documentation
priority_rank: 21
---

# TASK-0481: Correct the ADR-0110 isolation claim and the dangling multi-tenant guide references

## Summary
ADR-0110 says "Bulletproof cross-tenant data leak defense". The code does not give that guarantee. The multi-tenant guide refers to a "Limitations" section that does not exist. The guide does not say that the outbox and bus send all tenants' events to every subscriber.

## Problem Statement & Context
- **Severity**: Low
- **Weakness**: CWE-1059 (insufficient technical documentation)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

Readers who trust the ADR can skip the controls that the guide recommends. Amend the ADR with a new record; do not rewrite the accepted decision.

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `docs/project/adrs/accepted/adr-0110-ambient-multi-tenant-saas-isolation-model.md:28`
- `docs/guides/multi-tenant.md:5`, 91-94
- `src/eventsource/ports/outbox.py:20;` adapters/postgresql/outbox.py:50-60; adapters/kafka/publisher.py:150

**Verification**: Heading grep of the guide; ADR line read.

## Definition of Done (Blackbox Frontdoor TDD)
1. An ADR amendment states the real isolation guarantees and links the guide.
2. The guide has a Limitations section that lists the known gaps, or the reference is removed.
3. The guide states that the bus is a single trust domain and that tenant is a header only.
4. `make docs` (strict build) passes.
5. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
