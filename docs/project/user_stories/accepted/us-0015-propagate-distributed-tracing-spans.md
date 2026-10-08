---
id: '0015'
title: Propagate Distributed Tracing Spans Across Message Buses and Aggregates
status: Accepted
persona: Chris (The SRE / Resilience & Cutover Operator)
target_bc: observability
governing_prd: PRD-0004
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0116
- ADR-0164
scenarios:
- Transparent no-op tracer when telemetry extra is omitted
- Inject W3C trace headers into published event envelopes
- Extract trace context in subscription runners and record semantic attributes
---

# US-0015: Propagate Distributed Tracing Spans Across Message Buses and Aggregates

## User Story

As Chris, the SRE & Resilience Operator,
I want end-to-end distributed tracing contexts to propagate across message buses, background workers, and projection runners using standard OpenTelemetry semantic conventions,
So that I can diagnose latency bottlenecks, identify slow event handlers, and trace causation trees without incurring overhead when telemetry is disabled.

## Acceptance Criteria

### Scenario 1: Transparent no-op tracer when telemetry extra is omitted
```gherkin
Given eventsource-py is initialized without the opentelemetry dependency extra
When tracing methods or decorators are invoked across repositories or event buses
Then tracing acts as a zero-overhead pass-through without raising import errors
And no span objects or memory overhead are allocated.
```

### Scenario 2: Inject W3C trace headers into published event envelopes
```gherkin
Given an active OpenTelemetry trace and span context
When an aggregate publishes domain events through Kafka, RabbitMQ, or Redis
Then W3C traceparent and tracestate headers are injected into broker message envelopes
And downstream consumers can reconstruct the parent-child span hierarchy.
```

### Scenario 3: Extract trace context in subscription runners and record semantic attributes
```gherkin
Given an incoming event payload carrying distributed trace headers
When a subscription runner dispatches the event to registered handlers
Then the carrier trace context is extracted and a consumer span is created
And standard semantic attributes (aggregate_type, stream_id, event_type, handler_name) are recorded according to the telemetry attribute catalogue.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified
- **Implementation Modules**:
  - `src/eventsource/observability/tracer.py`: `TraceProvider`, `Tracer` protocol and no-op implementations.
  - `src/eventsource/observability/tracing.py`: Span life cycle managers, tracer injection, and carrier extraction helpers.
  - `src/eventsource/observability/attributes.py`: Semantic telemetry attribute catalogue conforming to ADR-0164.
- **Verification Proof**:
  - `tests/unit/observability/test_tracer.py`: No-op behavior, provider initialization, and carrier header injection.
  - `tests/integration/observability/test_distributed_tracing.py`: End-to-end W3C trace propagation across simulated broker pipelines.
  - `tests/unit/adapters/_bus/test_eventbus_tracing_patterns.py`: Event bus tracer integration across message handlers.
  - All tests passed cleanly with 100% contract compliance.
