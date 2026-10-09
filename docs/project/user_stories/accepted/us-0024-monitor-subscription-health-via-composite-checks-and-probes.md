---
id: '0024'
title: Monitor Subscription Health via Composite Checks and Kubernetes Probes
status: Accepted
created: 2026-10-09
persona: Chris (The SRE / Resilience & Cutover Operator)
target_bc: subscriptions
feature: FEAT-SUB-HEALTH-PROBES
governing_prd: PRD-0003
scenarios:
- Kubernetes readiness probe evaluates catchup lag and active state
- Kubernetes liveness probe verifies runner task loop vitality
- Dual circuit breaker evaluation separates application and infrastructure faults
- Composite health aggregation classifies manager health status
governing_adrs:
- ADR-0001
- ADR-0003
- ADR-0108
---

# US-0024: Monitor Subscription Health via Composite Checks and Kubernetes Probes

## Governing PRD
- [`PRD-0003: Distributed Streaming and Subscription Coordination`](../../product/accepted/prd-0003-distributed-streaming-and-subscription-coordination.md)

## User Story

**As an** SRE and resilience operator (Chris),
**I want** `SubscriptionManager` to expose unified health checks, Kubernetes readiness/liveness probes, and dual circuit breaker diagnostics,
**So that** orchestrators (like Kubernetes) route traffic only to fully caught-up instances, restart wedged worker pods automatically, and distinguish projection code bugs from storage outages.

## Acceptance Criteria

```gherkin
Scenario: Kubernetes readiness probe evaluates catchup lag and active state
  Given a SubscriptionManager running subscriptions with catchup lag thresholds
  When "readiness_check()" is probed by an orchestrator
  Then it returns "READY" (HTTP 200) only if all subscriptions are RUNNING and lag is within tolerance
  And returns "NOT_READY" (HTTP 503) during initial historical catchup replay.
```

```gherkin
Scenario: Kubernetes liveness probe verifies runner task loop vitality
  Given active runner coroutines processing event streams
  When "liveness_check()" is probed
  Then it evaluates runner task states and loop heartbeat timestamps
  And returns "DEAD" (HTTP 500) if any runner task has crashed or locked up.
```

```gherkin
Scenario: Dual circuit breaker evaluation separates application and infrastructure faults
  Given subscriptions configured with separate handler and store circuit breakers
  When an external projection handler throws exceptions and trips the application breaker
  Then the diagnostic status reports "HANDLER_CIRCUIT_OPEN"
  And the underlying event store and bus connections remain reported as healthy.
```

```gherkin
Scenario: Composite health aggregation classifies manager health status
  Given multiple subscriptions with diverse lag, circuit breaker, and error metrics
  When "health_check()" is evaluated
  Then overall manager health is synthesized deterministically into HEALTHY, DEGRADED, UNHEALTHY, or CRITICAL.
```

## Implementation Status & Verification

- **Status**: Implemented & Verified (100% test pass rate, 0 backdoor mocks)
- **Implementation Modules**:
  - `src/eventsource/application/subscriptions/health_provider.py`: `HealthCheckProvider`.
  - `src/eventsource/application/subscriptions/health.py`: `ManagerHealthChecker`, `SubscriptionHealthChecker`.
- **Verified Test Suites**:
  - `tests/unit/application/subscriptions/test_health_api.py`: Comprehensive readiness/liveness tests.
  - `tests/unit/application/subscriptions/test_health.py`: Health aggregation and status enum checks.
  - `tests/unit/application/subscriptions/test_error_rate_gates_health.py`: Error rate thresholds.
