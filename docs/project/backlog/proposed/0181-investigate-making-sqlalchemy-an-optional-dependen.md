---
id: "0181"
title: Investigate Making SQLAlchemy an Optional Dependency
status: Proposed
target_bc: core
governing_adrs:
  - ADR-0002
  - ADR-0102
priority: P3
---

# TASK-0181: Investigate Making SQLAlchemy an Optional Dependency

## Summary
Investigate and implement moving SQLAlchemy from core dependencies to an optional package extra, ensuring core domain and application rings have zero mandatory dependency on SQLAlchemy.

## Problem Statement & Context
SQLAlchemy is currently a required dependency in core packaging (`pyproject.toml`). However, core domain and application modules do not require SQLAlchemy:
- SQLAlchemy imports occur only in specific database adapters:
  - PostgreSQL adapters (`src/eventsource/adapters/postgresql/`): `locks.py`, `outbox.py`, `readmodels.py`, `snapshots.py`, and `store.py`.
  - Common SQL adapters (`src/eventsource/adapters/_sql/`): `connection.py`, `dialect.py`, and `engine.py`.
  - SQL adapters (`src/eventsource/adapters/sql/`): `checkpoints.py` and `dlq.py`.
  - SQL migration modules: `src/eventsource/adapters/sql/migration/`.
  - Alembic schema templates: `src/eventsource/adapters/sql/schemas/`.
- Core interfaces do not import SQLAlchemy at the module level.
- The Tier 0 import-linter contract and `test_core_surface_purity.py` enforce interface purity.
- Redis was previously decoupled from core dependencies as an extra.

## Required Actions
1. Move `sqlalchemy` from core dependencies to an optional package extra (`[project.optional-dependencies] postgresql / sql`) in `pyproject.toml`.
2. Define functionality available in a default installation without optional extras (in-memory and domain core).
3. Provide descriptive import error messages (`OptionalDependencyError` or clear ImportError hints) when an application accesses database adapters without SQLAlchemy installed.
4. Update and document installation instructions for database extras in project documentation (`docs/`).

## Definition of Done (Blackbox Frontdoor TDD)
1. Default installation without `[postgres]` or `[sql]` extras can import and execute core domain and in-memory event stores without `sqlalchemy`.
2. Accessing SQL-specific adapter modules without SQLAlchemy raises a clean, informative error explaining how to install the extra.
3. Verified via automated blackbox tests without private backdoor manipulation.
4. All source files strictly comply with ADR-0002 (<500 lines limit).
