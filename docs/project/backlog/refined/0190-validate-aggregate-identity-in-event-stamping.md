---
id: "0190"
title: Fluent AggregateRepository & Scenario Interfaces and Domain/Projection Module Decomposition
status: Refined
target_bc: domain
governing_adrs:
  - ADR-0002
  - ADR-0007
  - ADR-0103
  - ADR-0104
governing_prds:
  - PRD-0001
  - PRD-0004
governing_stories:
  - US-0001
  - US-0016
priority: P1
---

# TASK-0190: Fluent AggregateRepository & Scenario Interfaces and Domain/Projection Module Decomposition

## Summary
Add an ergonomic `AggregateRepository.execute(aggregate_id, command)` load-decide-save helper and fluent `Scenario` multi-command / state-assertion methods, add Hypothesis property tests for `DeciderAggregate._stamp` identity validation, and decompose `domain/event.py` (429 lines), `domain/tenant_context.py` (423 lines), `application/projections/handlers.py` (416 lines), and `tests/unit/domain/test_tenant_events.py` (404 lines) below 400 lines.

## Problem Statement & Context
- Executing a command against an aggregate persisted in `AggregateRepository` currently requires boilerplate (`agg = await repo.get_or_create(id); agg.execute(cmd); await repo.save(agg)`). Providing `await repo.execute(aggregate_id, command)` makes the common load-decide-save cycle a single ergonomic call.
- `Scenario` (`src/eventsource/testing/scenario.py`) can be made even more expressive with `.when_all(*commands)` and `.then_state(expected_or_predicate)` fluent assertions.
- `src/eventsource/domain/event.py` (429 lines), `src/eventsource/domain/tenant_context.py` (423 lines), `src/eventsource/application/projections/handlers.py` (416 lines), and `tests/unit/domain/test_tenant_events.py` (404 lines) exceed the 400-line warning threshold.

## Required Actions
1. Add `async def execute(self, aggregate_id: UUID, command: Any) -> TAggregate` on `AggregateRepository` (`src/eventsource/application/aggregates/repository.py`) and `TenantAwareRepository` that loads or initializes the aggregate, invokes `aggregate.execute(command)`, saves the aggregate, and returns it.
2. Add fluent `.then_state(expected_or_predicate)` on `ScenarioResult` and `.when_all(*commands)` on `Scenario` in `src/eventsource/testing/scenario.py`, plus Hypothesis property tests verifying `DeciderAggregate._stamp` aggregate identity and type invariants.
3. Decompose `src/eventsource/domain/event.py` (429 lines), `src/eventsource/domain/tenant_context.py` (423 lines), `src/eventsource/application/projections/handlers.py` (416 lines), and `tests/unit/domain/test_tenant_events.py` (404 lines) below 400 lines.

## Definition of Done (Blackbox Frontdoor TDD)
1. `await repo.execute(aggregate_id, command)` loads/creates, executes, and persists in a single call.
2. `Scenario(...).given(...).when_all(...).then_state(...)` works fluently through public frontdoors.
3. `domain/event.py`, `domain/tenant_context.py`, `projections/handlers.py`, and `test_tenant_events` are strictly under 400 lines.
