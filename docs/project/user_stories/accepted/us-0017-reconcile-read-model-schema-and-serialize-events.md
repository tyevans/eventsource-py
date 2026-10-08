---
id: '0017'
title: Reconcile Read Model Schema Variations and Serialize Polymorphic Events
status: Accepted
persona: Alex (The Event-Sourced Domain Architect)
target_bc: adapters
governing_prd: PRD-0001
governing_adrs:
- ADR-0001
- ADR-0112
- ADR-0127
- ADR-0139
- ADR-0166
scenarios:
- Additive column reconciliation executes on read models without data loss
- Polymorphic JSON serializer derives wire names without type drift
- Schema drift rejects breaking column deletions or datatype alterations
---

# US-0017: Reconcile Read Model Schema Variations and Serialize Polymorphic Events

## User Story

As Alex, the Event-Sourced Domain Architect,
I want read-model database schemas to reconcile additively and event payloads to serialize deterministically to JSON using canonical event wire types,
So that event streams can evolve safely over time and relational projections can adapt to new read-model fields without manual schema downtime.

## Acceptance Criteria

### Scenario 1: Additive column reconciliation executes on read models without data loss
```gherkin
Given a read model table with an existing schema definition
When an updated read model class introduces new optional columns
Then the schema reconciliation utility inspects the physical table and executes additive ALTER TABLE statements
And all existing rows and indexes remain intact without table recreation.
```

### Scenario 2: Polymorphic JSON serializer derives wire names without type drift
```gherkin
Given a registered domain event class with auto-derived or explicit event type
When serializing the event to a JSON storage payload
Then the canonical event wire name is recorded in metadata
And deserialization faithfully reconstructs the concrete Python domain event class.
```

### Scenario 3: Schema drift rejects breaking column deletions or datatype alterations
```gherkin
Given a proposed read model schema modification that attempts to drop columns or alter conflicting column types
When the additive reconciliation engine audits the migration plan
Then the operation is rejected with an informative schema conflict exception
And destructive operations are prevented in automated pipelines.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified
- **Implementation Modules**:
  - `src/eventsource/adapters/sql/readmodel_reconcile.py`: Additive SQL read model schema reconciliation.
  - `src/eventsource/adapters/serialization/json.py`: Polymorphic event payload JSON serialization and deserialization.
  - `src/eventsource/adapters/sql/readmodel_schema.py`: Schema DDL generation and validation.
- **Verification Proof**:
  - `tests/unit/adapters/sql/schemas/test_migration_schema.py`: Schema DDL validation and migration tables.
  - `tests/unit/readmodels/test_schema.py`: Table creation and additive column reconciliation.
  - `tests/unit/adapters/serialization/test_json.py`: JSON round-trip serialization and type registry resolution.
  - Test suites passed 100%.
