---
id: '0016'
title: Test Aggregate Invariants Fluently via Scenario Testing Harness
status: Accepted
persona: Morgan (The Autonomous Coding Agent & Pair Programmer)
target_bc: testing
governing_prd: PRD-0004
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0006
- ADR-0103
scenarios:
- Given prior events, when executing command, then assert expected events emitted
- When invalid command is dispatched, then assert expected domain error raised
- Blackbox state assertion matches expected state model without private field access
---

# US-0016: Test Aggregate Invariants Fluently via Scenario Testing Harness

## User Story

As Morgan, the Autonomous Coding Agent & Pair Programmer,
I want to test event-sourced aggregates using a fluent declarative Given/When/Then scenario DSL,
So that I can verify state transitions, event emissions, and business invariant rejections strictly through public frontdoor contracts without private state tampering.

## Acceptance Criteria

### Scenario 1: Given prior events, when executing command, then assert expected events emitted
```gherkin
Given an aggregate class and a sequence of historical domain events
When a valid domain command is executed on the reconstructed aggregate
Then the scenario harness verifies that exactly the expected events are emitted
And the aggregate version and state fold correctly according to domain logic.
```

### Scenario 2: When invalid command is dispatched, then assert expected domain error raised
```gherkin
Given an aggregate initialized to a specific state via historical events
When a command violating business rules is executed
Then the scenario harness catches and asserts the expected domain exception
And zero new events are emitted or persisted.
```

### Scenario 3: Blackbox state assertion matches expected state model without private field access
```gherkin
Given a completed scenario execution
When verifying the resulting aggregate state against expected state
Then the state comparison checks public typed Pydantic models
And no internal private attributes or backdoor test state are accessed.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified
- **Implementation Modules**:
  - `src/eventsource/testing/harness.py`: `AggregateTestHarness`, `Scenario` fluent testing DSL.
  - `src/eventsource/testing/builder.py`: `EventBuilder` for typed domain event construction.
  - `src/eventsource/testing/assertions.py`: Assertion matchers for domain events and aggregate state.
  - `src/eventsource/testing/bdd.py`: BDD scenario runners and step definitions.
- **Verification Proof**:
  - `tests/unit/testing/test_harness.py`: Given/When/Then scenario execution and version verification.
  - `tests/unit/testing/test_builder.py`: Fluent builder event factory and envelope assembly.
  - `tests/unit/testing/test_assertions.py`: Event emission assertions and exception matching.
  - 100% test pass rate across unit testing suite.
