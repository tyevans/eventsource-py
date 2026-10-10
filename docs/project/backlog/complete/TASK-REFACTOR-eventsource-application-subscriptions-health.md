---
id: REFACTOR-eventsource-application-subscriptions-health
title: Refactor and Decompose Legacy File health.py
status: Complete
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-health: Refactor Legacy File health.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/health.py` contains 860 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (health_status.py, health_check.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/health/` with submodules:
- `health_status.py`: HealthStatus, ReadinessStatus, LivenessStatus, HealthIndicator, SubscriptionHealthChecker, ManagerHealthChecker, ManagerHealth, SubscriptionHealth
- `health_check.py`: HealthCheckResult, HealthCheckConfig

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/health.py (860 lines):
  Submodule 'health_status.py' (~721 lines):
    - [class] HealthStatus (lines 31-51)
    - [class] ReadinessStatus (lines 760-797)
    - [class] LivenessStatus (lines 801-837)
    - [class] HealthIndicator (lines 55-74)
    - [class] SubscriptionHealthChecker (lines 139-510)
    - [class] ManagerHealthChecker (lines 513-627)
    - [class] ManagerHealth (lines 636-712)
    - [class] SubscriptionHealth (lines 716-756)
  Submodule 'health_check.py' (~56 lines):
    - [class] HealthCheckResult (lines 78-99)
    - [class] HealthCheckConfig (lines 103-136)
  Suggested barrel exports:
    from .health_status import HealthStatus, ReadinessStatus, LivenessStatus, HealthIndicator, SubscriptionHealthChecker, ManagerHealthChecker, ManagerHealth, SubscriptionHealth
    from .health_check import HealthCheckResult, HealthCheckConfig

    __all__ = ["HealthStatus", "ReadinessStatus", "LivenessStatus", "HealthIndicator", "SubscriptionHealthChecker", "ManagerHealthChecker", "ManagerHealth", "SubscriptionHealth", "HealthCheckResult", "HealthCheckConfig"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
