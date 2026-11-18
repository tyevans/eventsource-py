---
id: '0461'
title: Validate read-model Query sort and paging values before SQL rendering
status: Complete
governing_adrs:
- ADR-0105
governing_prds:
- PRD-0001
governing_stories:
- US-0008
target_bc: adapters
priority_rank: 1
---

# TASK-0461: Validate read-model Query sort and paging values before SQL rendering

## Summary
The SQL read-model repositories write `Query.order_by`, `Query.order_direction`, `Query.limit` and `Query.offset` directly into SQL text. Validate these values, or bind them as parameters, before the adapters render SQL.

## Problem Statement & Context
- **Severity**: High
- **Weakness**: CWE-89 (SQL injection)
- **Source**: Security audit, 2026-11-17 (haiku sweep, then adversarial verification against the code)

`Filter.field` is validated against the model fields, but `order_by` is not. An application that passes a sort parameter from an HTTP request into `Query.with_order()` lets the caller run a subquery inside the SELECT. The verifier used a `CASE` expression in `order_by` to read a secret table one bit at a time through the public `SQLiteReadModelRepository`. That read can include the `events` table, so it also bypasses tenant scoping. `limit` and `offset` have type annotations only; a `str` value is rendered as is. The in-memory adapter rejects unknown sort fields, so the adapters disagree on the same input (recurring defect 1).

## Evidence
Paths without a directory prefix such as `docs/` or `.github/` are under `src/eventsource/`.

- `src/eventsource/adapters/postgresql/readmodels_query.py:70-75` (ORDER BY, LIMIT, OFFSET f-strings)
- `src/eventsource/adapters/postgresql/readmodels_query.py:101-108` (deleted-row query) and :113-124 (find)
- `src/eventsource/adapters/sqlite/readmodels_query.py:119-126`
- `src/eventsource/adapters/sqlite/readmodels_soft_delete.py:106-113`
- `src/eventsource/adapters/sql/migration/audit_log.py:93-94` (LIMIT f-string)
- `src/eventsource/ports/readmodels/query.py` (order_by, order_direction, limit, offset: no runtime check)
- `src/eventsource/adapters/_common/readmodel_filters.py` (~line 84: field check exists for filters only)

**Verification**: Proven on SQLite: `Query(order_by="(CASE WHEN (SELECT substr(v,1,1) FROM secret)='A' THEN count ELSE -count END)")` changes the row order by the secret value. `Query(limit="(SELECT count(*) FROM secret)")` renders as is and returns the secret row count.

## Definition of Done (Blackbox Frontdoor TDD)
1. `order_by` must name a field of the read-model class. All adapters reject other values with the same error type as an unknown `Filter.field`.
2. `order_direction` accepts only `asc` or `desc` (any case). Other values cause an error.
3. `limit` and `offset` accept only non-negative `int` values. The SQL adapters bind them as parameters.
4. A conformance case in `testing/conformance_ports/` sends the injection payloads above through `find()`, `find_one()` and `find_deleted()` on each adapter. The case fails on the current code.
5. `AuditLogRepository` binds its LIMIT value as a parameter.
6. Tests use public entry points only, with zero private backdoor mocks. All files stay under 500 lines.
