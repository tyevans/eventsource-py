---
id: "0181"
title: Remove EventSourceJSONEncoder Legacy Shim and Migrate All JSON Call Sites to orjson
status: Refined
target_bc: adapters
governing_adrs:
  - ADR-0002
  - ADR-0003
  - ADR-0102
  - ADR-0112
governing_prds:
  - PRD-0001
governing_stories:
  - US-0013
  - US-0017
priority: P1
---

# TASK-0181: Remove EventSourceJSONEncoder Legacy Shim and Migrate All JSON Call Sites to orjson

## Summary
Eliminate the legacy `EventSourceJSONEncoder` stdlib `json.JSONEncoder` shim and migrate remaining stdlib `json.dumps`/`json.loads` call sites across storage adapters, outbox repositories, migration repositories, and `Position.encode`/`decode` to `orjson`-backed `json_dumps`/`json_loads`, while slimming `src/eventsource/__init__.py` below 400 lines.

## Problem Statement & Context
- `EventSourceJSONEncoder` in `src/eventsource/adapters/serialization/json.py` is a legacy stdlib `json.JSONEncoder` subclass retained only because outbox and adapter call sites had not yet been migrated to `json_dumps`/`json_loads`.
- Multiple storage and migration modules (`postgresql/outbox.py`, `postgresql/store_append.py`, `sqlite/outbox.py`, `sqlite/store.py`, `memory/outbox.py`, `sql/migration/migration_helpers.py`, `sql/migration/migration_mutation.py`, and `ports/positions.py`) still invoke stdlib `json.dumps`/`json.loads`.
- `src/eventsource/__init__.py` is 430 lines (above the 400-line proactive warning threshold).

## Required Actions
1. Migrate stdlib `json.dumps`/`json.loads` call sites in `postgresql/outbox.py`, `postgresql/store_append.py`, `sqlite/outbox.py`, `sqlite/store.py`, `memory/outbox.py`, `sql/migration/migration_helpers.py`, `sql/migration/migration_mutation.py`, and `ports/positions.py` to use `orjson` (`json_dumps`/`json_loads` or `orjson.dumps`/`orjson.loads` in `ports/positions.py`).
2. Remove `EventSourceJSONEncoder` from `src/eventsource/adapters/serialization/json.py`, `src/eventsource/adapters/serialization/__init__.py`, `src/eventsource/_lazy.py`, `src/eventsource/__init__.py`, and `src/eventsource/adapters/serialization/README.md`.
3. Update `tests/unit/adapters/serialization/test_json/` to verify `json_dumps`/`json_loads` directly without the legacy `EventSourceJSONEncoder` shim.
4. Compact `src/eventsource/__init__.py` below 400 lines while preserving all PEP 562 lazy exports.

## Definition of Done (Blackbox Frontdoor TDD)
1. Zero occurrences of `EventSourceJSONEncoder` remain in `src/eventsource/`.
2. All outbox, store append, migration, and `Position` JSON serialization paths use `orjson`.
3. `src/eventsource/__init__.py` is strictly under 400 lines.
4. All unit, property, and conformance tests pass cleanly.
