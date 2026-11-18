---
id: '0475'
title: Remove security and compliance claims that the code does not support
status: Proposed
governing_adrs:
- ADR-0010
governing_prds:
- PRD-0004
governing_stories:
- US-0013
target_bc: documentation
priority_rank: 15
---

# TASK-0475: Remove security and compliance claims that the code does not support

## Summary
The docs name an `eventsource.gdpr` erasure module that does not exist. PRD-0001 calls the in-memory adapters production-ready, but their docstrings say they are for tests. The event-store guide says that `append` defaults to `ExpectedVersion.any_()` on every store; only the in-memory store does that. No user guide covers production security.

## Problem Statement & Context
- **Severity**: Medium
- **Weakness**: CWE-1059 (insufficient technical documentation)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

A team that relies on a documented erasure capability can fail a GDPR erasure request. The docs also give no hardening guidance: TLS for brokers, credential handling, log redaction, tenant isolation limits, payload encryption at rest (only `SCHEMA_DESIGN.md:238` mentions it).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `docs/api/index.md:85`, 827; docs/architecture.md:84; docs/guides/index.md:34-35; docs/index.md:119
- `docs/project/product/accepted/prd-0001-production-ready-event-sourcing-framework.md:49`
- `docs/guides/event-stores.md:133-136;` docs/api/sync.md:264 (signature drift)
- `src/eventsource/adapters/sql/schemas/SCHEMA_DESIGN.md:238`

**Verification**: `ls src/eventsource` has no gdpr package; grep for gdpr in src is empty. In-memory docstrings: memory/dlq.py:23, memory/outbox.py:1, memory/checkpoints.py:14.

## Definition of Done (Blackbox Frontdoor TDD)
1. No doc page names `eventsource.gdpr`. Crypto-shredding is described as an application pattern only.
2. PRD-0001 calls the in-memory adapters test doubles.
3. The event-store guide and `docs/api/sync.md` show the real `append` signatures.
4. A new how-to page "Production security considerations" covers broker TLS, secrets, log redaction, tenant limits and encryption at rest. It is in the mkdocs nav.
5. `make docs` (strict build) passes.
6. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
