---
id: REFACTOR-eventsource-application-subscriptions-subscription
title: Refactor and Decompose Legacy File subscription.py
status: Refined
governing_adrs:
- ADR-0002
governing_stories:
- US-0012
target_bc: core
---

# TASK-REFACTOR-eventsource-application-subscriptions-subscription: Refactor Legacy File subscription.py

## Summary
The grandfathered debt file `src/eventsource/application/subscriptions/subscription.py` contains 839 lines and violates the hard file length invariant governed by ADR-0002 (<500 lines).
This task plans the incremental extraction of cohesive submodules (subscription_position.py, subscription_state.py) and establishes a public barrel export facade.

## Target Submodule Decomposition Path
Target decomposition destination: `src/eventsource/application/subscriptions/subscription/` with submodules:
- `subscription_position.py`: render_position, PauseReason, is_valid_transition, RecentErrorInfo, SubscriptionStatus, Subscription
- `subscription_state.py`: SubscriptionState

## AST Decomposition Blueprint
Decomposition Blueprint for src/eventsource/application/subscriptions/subscription.py (839 lines):
  Submodule 'subscription_position.py' (~699 lines):
    - [function] render_position (lines 41-47)
    - [class] PauseReason (lines 82-102)
    - [function] is_valid_transition (lines 138-152)
    - [class] RecentErrorInfo (lines 164-199)
    - [class] SubscriptionStatus (lines 203-259)
    - [class] Subscription (lines 263-825)
  Submodule 'subscription_state.py' (~30 lines):
    - [class] SubscriptionState (lines 50-79)
  Suggested barrel exports:
    from .subscription_position import render_position, PauseReason, is_valid_transition, RecentErrorInfo, SubscriptionStatus, Subscription
    from .subscription_state import SubscriptionState

    __all__ = ["render_position", "PauseReason", "is_valid_transition", "RecentErrorInfo", "SubscriptionStatus", "Subscription", "SubscriptionState"]  # pragma: allowlist secret

## INVEST Criteria
- **Independent**: Executed in isolated task branch without modifying shared backlog on branch.
- **Negotiable**: Concrete boundaries derived from AST seams.
- **Valuable**: Retires grandfathered technical debt from `.specops/grandfathered_debt.json`.
- **Estimable**: Symbol-level decomposition blueprint provided.
- **Small (<500 lines)**: Target submodules each strictly under 400 lines.
- **Testable**: Validated via blackbox frontdoor tests (ADR-0003).
