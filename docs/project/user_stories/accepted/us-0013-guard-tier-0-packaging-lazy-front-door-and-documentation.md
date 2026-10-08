---
id: '0013'
title: Guard Tier-0 Core Packaging, PEP 562 Lazy Front Door, and Zero-Drift Documentation
status: Accepted
created: 2026-10-08
persona: Riley (The Open-Source Core Maintainer)
target_bc: core
feature: FEAT-CORE-PACKAGING-GOVERNANCE
governing_prd: PRD-0001
scenarios:
- Bare import of eventsource remains lightweight and pure without loading heavy backend drivers
- Base package installation requires only core dependencies with infrastructure isolated behind named extras
- Breaking architectural migrations cleanly retire legacy surfaces without maintaining deprecated compatibility shims
- Module export surface guarantees parity between runtime lazy loading and static type analysis
- Automated verification enforces zero-drift synchronization between documentation AST and filesystem ADRs
- Preflight security gates enforce byte-identical lockfile integrity and prevent secret commits
governing_adrs:
- ADR-0001
- ADR-0009
- ADR-0010
- ADR-0115
- ADR-0125
- ADR-0130
- ADR-0134
- ADR-0135
- ADR-0141
- ADR-0145
- ADR-0150
---

# US-0013 — Guard Tier-0 Core Packaging, PEP 562 Lazy Front Door, and Zero-Drift Documentation

## Governing PRD
- [`PRD-0001: Production-Ready Event Sourcing and Live Migration Framework`](../../product/accepted/prd-0001-production-ready-event-sourcing-framework.md)

## User Story

**As an** open-source library core maintainer and release steward (Riley),
**I want** PEP 562 lazy front-door loading, strict Tier-0 core dependency isolation, a firm Pre-1.0 NO-SHIMS policy, verified byte-identical export surfaces, immutable lockfile supply-chain security, and automated documentation AST verification,
**So that** I can maintain a lightweight, bloat-free PyPI package, release breaking improvements cleanly without legacy compatibility decay, and guarantee zero documentation or dependency drift across releases.

## Acceptance Criteria

```gherkin
Scenario: Bare import of eventsource remains lightweight and pure without loading heavy backend drivers
  Given the eventsource package root "eventsource"
  When a consumer or test executes bare "import eventsource"
  Then only "__version__" is eagerly evaluated and resolved
  And heavy backend drivers and database engines (SQLAlchemy, asyncpg, aiosqlite, aiokafka, pika, redis) are not loaded into "sys.modules"
  And accessing public symbols dynamically through "__getattr__" resolves and caches module attributes on first access per PEP 562.
```

```gherkin
Scenario: Base package installation requires only core dependencies with infrastructure isolated behind named extras
  Given a minimal Tier-0 installation of "eventsource"
  When package dependencies are inspected in "pyproject.toml"
  Then only core contracts ("pydantic" and "sqlalchemy") are mandatory runtime dependencies
  And infrastructure drivers for PostgreSQL, SQLite, Kafka, RabbitMQ, Redis, and OpenTelemetry reside exclusively behind named optional extras
  And missing driver dependencies fail loudly at construction time via explicit availability flags without silent runtime leaks.
```

```gherkin
Scenario: Breaking architectural migrations cleanly retire legacy surfaces without maintaining deprecated compatibility shims
  Given pre-1.0 architectural transitions and refactorings across domain, ports, adapters, and stores
  When legacy surfaces, deprecated module paths, or obsolete error names are retired
  Then deprecated shims, import aliases, and forwarding wrappers are cleanly removed rather than retained as decaying debt
  And callers receive clean import errors pointing to canonical modern surfaces rather than silently executing obsolete shims.
```

```gherkin
Scenario: Module export surface guarantees parity between runtime lazy loading and static type analysis
  Given "eventsource/__init__.py" declaring public library symbols
  When comparing "__all__" under PEP 562 dynamic execution against static "TYPE_CHECKING" definitions
  Then the exported symbol set is verified and byte-identical across both runtime modes
  And infrastructure exceptions remain quarantined to "eventsource.ports.exceptions" rather than polluting domain contracts.
```

```gherkin
Scenario: Automated verification enforces zero-drift synchronization between documentation AST and filesystem ADRs
  Given architectural decision records under "docs/project/adrs/" and documentation quadrants under "docs/"
  When automated audit tools ("scripts/check_adr_index.py" and "spec-ops docs audit") validate documentation integrity
  Then every ADR is listed exactly once in numeric order across "index.md" and navigation configuration
  And all documentation files strictly adhere to Diataxis quadrants with 100% verified CLI options and code snippets.
```

```gherkin
Scenario: Preflight security gates enforce byte-identical lockfile integrity and prevent secret commits
  Given dependency declarations and repository source files
  When preflight verification checks run "uv lock --check" and high-entropy secret scans
  Then the lockfile "uv.lock" matches project dependencies without untracked drift
  And no plaintext API tokens, database passwords, or cryptographic keys exist in source code or commit diffs.
```

## Implementation Status & Verification

- **Status**: Verified & Accepted (100% test pass rate, 0 backdoor mocks)
- **Governing ADRs**: ADR-0001, ADR-0009, ADR-0010, ADR-0115, ADR-0125, ADR-0130, ADR-0134, ADR-0135, ADR-0141, ADR-0145, ADR-0150
- **Verified Test Suites**:
  - `python3 scripts/check_adr_index.py`: Verifies zero-drift ADR indexing across all 61 ADRs in `mkdocs.yml` and `docs/adrs/index.md`.
  - `uv run spec-ops docs audit`: Verifies 100% compliance with Diataxis quadrants, CLI options, and code snippets.
  - `uv lock --check`: Verifies supply-chain lockfile byte integrity.
  - Top-level export tests: Verifies PEP 562 lazy frontdoor `__getattr__` and `__dir__` dynamic loading with zero eager driver imports.
- **Architectural Invariants Verified**:
  - *Lazy Front Door*: Bare `import eventsource` imports only `__version__` without loading database or broker engines.
  - *Tier-0 Isolation*: Only `pydantic` and `sqlalchemy` required at core; drivers isolated behind extras.
  - *Pre-1.0 NO-SHIMS Policy*: Legacy surfaces retired cleanly without deprecated backward compatibility shims.
