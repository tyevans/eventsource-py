---
id: '0004'
title: Developer Ergonomics, Scenario Testing Harness & Distributed Observability
status: Accepted
created: 2026-10-07
target_persona: Morgan
component: testing
governing_adrs:
- ADR-0001
- ADR-0002
- ADR-0003
- ADR-0006
- ADR-0007
- ADR-0102
- ADR-0103
- ADR-0112
---

# PRD-0004 — Developer Ergonomics, Scenario Testing Harness & Distributed Observability

## Who this is for

- **Morgan (The Autonomous Coding Agent & Pair Programmer)**: Autonomous developers and pair programmers requiring declarative, blackbox testing DSLs (`Given / When / Then`) and pure static typing without backdoor state tampering.
- **Alex (The Event-Sourced Domain Architect)**: Domain modelers seeking fluent assertion harnesses to verify aggregate state transitions, command rejections, and event schemas in isolation.
- **Chris (The SRE / Resilience & Cutover Operator)**: Operations and resilience engineers observing end-to-end event lifecycles, span context propagation across distributed brokers, and standard OpenTelemetry telemetry attributes.

## What the person cannot do today

- **Fragile State Setup via Backdoors**: Testing event-sourced aggregates often leads developers to construct mock stores or mutate private internal fields, causing tests to rot when internal representations evolve.
- **Verbose Boilerplate in Behavioral Tests**: Testing command-driven aggregates without a dedicated scenario harness forces repetitive setup code for event history, envelope construction, and version sequencing.
- **Black-Hole Telemetry in Async Pipelines**: In high-throughput distributed pipelines (Kafka, RabbitMQ, background subscription runners), trace context is frequently lost across asynchronous boundaries, making cross-service root cause analysis impossible.
- **Telemetry Overhead & Vendor Lock-in**: Hard dependency on heavy tracing collectors forces overhead on Tier-0 users who do not require distributed tracing.

## What good looks like

1. **Fluent Behavioral Scenario Testing (`testing/harness.py`, `testing/builder.py`)**:
   - Clean BDD-style DSL: `Scenario(aggregate_cls).given(events...).when(command).then(expected_events...)`.
   - Rejection verification: `.then_raises(ExpectedException)`.
   - 100% blackbox frontdoor execution: aggregate state is exercised strictly through public command handling and state folding.

2. **Deterministic Event Generation & Recording (`testing/recording.py`)**:
   - In-memory event recording and mock sinks that capture uncommitted domain events without disk or network I/O.
   - Conformance test harness validating custom store and bus adapter implementations against the port contract suite.

3. **Zero-Overhead OpenTelemetry Tracing (`observability/`)**:
   - Transparent no-op implementation when OpenTelemetry is not installed (`ADR-0116`).
   - Strict telemetry attribute catalog adhering to semantic conventions (`ADR-0164`), capturing aggregate type, stream ID, message bus topic, and handler execution duration.
   - W3C Trace Context injection and extraction over broker message envelopes and headers.

## Traceability

- **Governing ADRs**: ADR-0001, ADR-0002, ADR-0003, ADR-0006, ADR-0007, ADR-0108, ADR-0116, ADR-0164
- **Linked User Stories**:
  - `US-0012`: Enforce Hexagonal Ring Layering and Strict Blackbox Frontdoor Verification
  - `US-0015`: Propagate Distributed Tracing Spans Across Message Buses and Aggregates
  - `US-0016`: Test Aggregate Invariants Fluently via Scenario Testing Harness
