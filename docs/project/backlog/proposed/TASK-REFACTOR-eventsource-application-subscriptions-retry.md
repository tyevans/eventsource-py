---
id: REFACTOR-eventsource-application-subscriptions-retry
title: Refactor and Decompose Legacy File retry.py
status: Proposed
created: 2026-09-29
governing_adrs:
  - ADR-0002
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-retry: Refactor Legacy File retry.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/retry.py` contains 643 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (retry_circuit.py, retry_config.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/retry/` with submodules:
- `retry_circuit.py`: CircuitState, CircuitBreakerConfig, CircuitBreakerOpenError, CircuitBreaker, RetryStats, RetryError, calculate_backoff, is_retryable_exception, retry_async, RetryableOperation
- `retry_config.py`: RetryConfig

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/retry.py (643 lines):
  Submodule 'retry_circuit.py' (~507 lines):
    - [class] CircuitState (lines 41-53)
    - [class] CircuitBreakerConfig (lines 147-170)
    - [class] CircuitBreakerOpenError (lines 189-200)
    - [class] CircuitBreaker (lines 364-550)
    - [class] RetryStats (lines 110-143)
    - [class] RetryError (lines 173-186)
    - [function] calculate_backoff (lines 203-236)
    - [function] is_retryable_exception (lines 239-253)
    - [function] retry_async (lines 256-361)
    - [class] RetryableOperation (lines 554-621)
  Submodule 'retry_config.py' (~50 lines):
    - [class] RetryConfig (lines 57-106)
  Suggested barrel exports:
    from .retry_circuit import CircuitState, CircuitBreakerConfig, CircuitBreakerOpenError, CircuitBreaker, RetryStats, RetryError, calculate_backoff, is_retryable_exception, retry_async, RetryableOperation
    from .retry_config import RetryConfig

    __all__ = ["CircuitState", "CircuitBreakerConfig", "CircuitBreakerOpenError", "CircuitBreaker", "RetryStats", "RetryError", "calculate_backoff", "is_retryable_exception", "retry_async", "RetryableOperation", "RetryConfig"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
